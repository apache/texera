/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.translator.verify

import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameList,
  AutofillAttributeNameOnPort1,
  SampleColumn
}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.lang.reflect.{Field, Modifier, ParameterizedType}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
  * Produces a valid configuration for an operator automatically, from the
  * metadata the operator already carries — field defaults, enums, and the
  * `@AutofillAttributeName` annotation family (which marks a field as "a column
  * name from input port N"). This is the baseline layer of the combined
  * config-generation plan: every registered operator gets a runnable config with
  * no per-operator handler.
  *
  * We only need a *valid* config, not a *meaningful* one — both verification
  * paths get the identical OpDesc and are compared to each other, so a
  * degenerate-but-valid config still tests translation fidelity. Free-form value
  * fields are filled with a canonical value (see [[CanonicalString]]) that the
  * synthetic dataset is built to contain, so the operator actually does
  * something rather than matching nothing.
  *
  * Strategy: reflect over the operator's config fields (those carrying
  * `@JsonProperty` or an autofill annotation), build a JSON object of
  * field → value, and let Jackson deserialize it into the OpDesc. Using the
  * same `objectMapper` Texera uses everywhere means enums (`@JsonValue`),
  * `Option`, and `@JsonCreator` nested objects are handled by existing,
  * battle-tested deserialization rather than bespoke reflection.
  */
object ConfigGenerator {

  /** Canonical literal for free-form STRING fields; present in the synthetic
    * dataset so filters/comparisons actually match rows.
    */
  private val CanonicalString = "1"

  /** Row count used to size the numeric "middle of the range" fallback when a
    * caller doesn't supply one (real verification callers pass the fixture's
    * actual row count). See [[numericFill]].
    */
  private val DefaultRowCount = 10

  /**
    * @param opClass      the operator descriptor class to configure.
    * @param inputSchemas schema present at each 0-based input port; supplies the
    *                     column names that `@AutofillAttributeName*` fields draw
    *                     from.
    * @return Right(configured opDesc), or Left(reason) if a required field can't
    *         be filled from the available metadata (the operator is then
    *         reported as uncovered rather than silently passed).
    */
  def generate(
      opClass: Class[_ <: LogicalOp],
      inputSchemas: Map[Int, Schema],
      rowCount: Int = DefaultRowCount
  ): Either[String, LogicalOp] = {
    buildObject(opClass, inputSchemas, rowCount).flatMap { node =>
      // LogicalOp is polymorphic (@JsonTypeInfo on `operatorType`); Jackson needs
      // the registered type id to deserialize the concrete subtype.
      typeNameByClass.get(opClass) match {
        case Some(typeName) => node.put("operatorType", typeName)
        case None =>
          return Left(s"${opClass.getSimpleName} not registered in LogicalOp @JsonSubTypes")
      }
      Try(objectMapper.treeToValue(node, opClass)).toEither.left
        .map(e => s"deserialization failed: ${e.getMessage}")
    }
  }

  /**
    * Like [[generate]], but also sweeps every enum field: returns the base
    * config plus one variant per non-default enum value (one enum flipped at a
    * time — linear, NOT the combinatorial product). Lets the runner exercise
    * each enum branch (e.g. LineChart's line mode = line / dots / line+dots)
    * instead of only the default. The label identifies the flipped value.
    */
  def generateVariants(
      opClass: Class[_ <: LogicalOp],
      inputSchemas: Map[Int, Schema],
      rowCount: Int = DefaultRowCount
  ): Either[String, Seq[(String, LogicalOp)]] =
    typeNameByClass.get(opClass) match {
      case None => Left(s"${opClass.getSimpleName} not registered in LogicalOp @JsonSubTypes")
      case Some(typeName) =>
        buildObject(opClass, inputSchemas, rowCount).flatMap { baseNode =>
          baseNode.put("operatorType", typeName)
          deserialize(baseNode, opClass).flatMap(baseOp => sweepVariants(opClass, baseNode, baseOp))
        }
    }

  /**
    * Enum-sweep an ALREADY-configured op (e.g. a curated handler's OpDesc):
    * serialize it to JSON, then return the base op plus one variant per
    * non-default enum value found anywhere in it (including inside lists with
    * more than one element). Lets curated fixtures cover every enum branch too,
    * not just the single value the handler hard-coded.
    */
  def variantsOf(opDesc: LogicalOp): Either[String, Seq[(String, LogicalOp)]] = {
    val opClass = opDesc.getClass.asInstanceOf[Class[_ <: LogicalOp]]
    objectMapper.valueToTree[JsonNode](opDesc) match {
      case node: ObjectNode =>
        if (!node.has("operatorType"))
          typeNameByClass.get(opClass).foreach(node.put("operatorType", _))
        // base = the original op (preserve the curated config exactly); variants
        // are deserialized from the JSON with one enum flipped.
        sweepVariants(opClass, node, opDesc)
      case _ =>
        Left(s"${opClass.getSimpleName} did not serialize to a JSON object")
    }
  }

  private def deserialize(
      node: ObjectNode,
      opClass: Class[_ <: LogicalOp]
  ): Either[String, LogicalOp] =
    Try(objectMapper.treeToValue(node, opClass)).toEither.left
      .map(e => s"deserialization failed: ${e.getMessage}")

  /** `baseOp` (already valid, from `baseNode`) plus one variant per non-default
    * enum value reachable in `baseNode`. One enum flipped at a time — linear.
    */
  private def sweepVariants(
      opClass: Class[_ <: LogicalOp],
      baseNode: ObjectNode,
      baseOp: LogicalOp
  ): Either[String, Seq[(String, LogicalOp)]] = {
    val variantResults: Seq[Either[String, (String, LogicalOp)]] =
      enumSites(opClass, baseNode, "").flatMap { site =>
        val baseVal = baseNode.at(site.pointer)
        site.values.filterNot(_ == baseVal).map { v =>
          val clone = baseNode.deepCopy()
          setAtPointer(clone, site.pointer, v)
          deserialize(clone, opClass).map(op =>
            (s"${site.pointer.stripPrefix("/")}=${v.asText}", op)
          )
        }
      }
    variantResults.collectFirst { case Left(err) => err } match {
      case Some(err) => Left(err)
      case None      => Right(("default", baseOp) +: variantResults.collect { case Right(ok) => ok })
    }
  }

  /** An enum-typed position in the config JSON: its JSON Pointer plus every
    * possible JSON value (each enum constant serialized via its `@JsonValue`).
    */
  private final case class EnumSite(pointer: String, values: Seq[JsonNode])

  /** Collect every enum-typed leaf reachable in `node`. Walks the operator's
    * fields for type info but the ACTUAL JSON for structure, so it honours real
    * list lengths (a curated fixture may hold >1 element) and skipped optionals.
    * `path` is the JSON Pointer of the sub-node currently typed by `clazz`.
    */
  private def enumSites(clazz: Class[_], node: JsonNode, path: String): Seq[EnumSite] =
    configFields(clazz).flatMap { f =>
      if (hasAutofill(f)) Seq.empty
      else {
        val jp = Option(f.getAnnotation(classOf[JsonProperty]))
        val jsonName = jp.map(_.value).filter(_.nonEmpty).getOrElse(f.getName)
        val childPath = s"$path/$jsonName"
        val child = node.at(childPath)
        if (child.isMissingNode || child.isNull) Seq.empty
        else {
          val t = f.getType
          if (isList(t))
            elementType(f).toOption.toSeq.flatMap { elem =>
              if (child.isArray)
                (0 until child.size()).flatMap(i => enumSiteFor(elem, node, s"$childPath/$i"))
              else Seq.empty
            }
          else if (isOption(t))
            elementType(f).toOption.toSeq.flatMap(elem => enumSiteFor(elem, node, childPath))
          else enumSiteFor(t, node, childPath)
        }
      }
    }

  private def enumSiteFor(t: Class[_], node: JsonNode, path: String): Seq[EnumSite] =
    if (t.isEnum) {
      val vals = t.getEnumConstants.toSeq.map(c => objectMapper.valueToTree[JsonNode](c))
      if (vals.size > 1) Seq(EnumSite(path, vals)) else Seq.empty
    } else if (t == classOf[Boolean] || t == classOf[java.lang.Boolean]) {
      // A Boolean is a 2-value "enum": sweep both true and false.
      val nf = objectMapper.getNodeFactory
      Seq(EnumSite(path, Seq(nf.booleanNode(true), nf.booleanNode(false))))
    } else if (isNestedObject(t)) enumSites(t, node, path)
    else Seq.empty

  /** Set `value` at a JSON Pointer inside `root` — used to clone the base config
    * and flip one enum leaf. Handles object fields and array indices.
    */
  private def setAtPointer(root: ObjectNode, pointer: String, value: JsonNode): Unit = {
    val tokens = pointer.stripPrefix("/").split("/").toList
    var cur: JsonNode = root
    tokens.dropRight(1).foreach { tk =>
      cur = if (cur.isArray) cur.get(tk.toInt) else cur.get(tk)
    }
    (cur, tokens.last) match {
      case (o: ObjectNode, name) => o.set[JsonNode](name, value)
      case (a: ArrayNode, idx)   => a.set(idx.toInt, value); ()
      case _                     => ()
    }
  }

  /** Maps each registered operator class to its `operatorType` discriminator,
    * read from [[LogicalOp]]'s `@JsonSubTypes` (the same registry Jackson uses).
    */
  private val typeNameByClass: Map[Class[_], String] = {
    Option(classOf[LogicalOp].getAnnotation(classOf[JsonSubTypes]))
      .map(_.value().toSeq.map(t => (t.value(): Class[_]) -> t.name()).toMap)
      .getOrElse(Map.empty)
  }

  // ── object assembly ──────────────────────────────────────────────────────

  /** Build a JSON object for `clazz` by filling each of its config fields.
    * `rowCount` sizes the numeric fallback for range-less fields (e.g. Limit).
    */
  private def buildObject(
      clazz: Class[_],
      schemas: Map[Int, Schema],
      rowCount: Int
  ): Either[String, ObjectNode] =
    buildObject(clazz, schemas, mutable.Set.empty[(Int, String)], rowCount)

  /** `used` tracks (port, column) already assigned within THIS operator, so that
    * sibling autofill fields resolve to DISTINCT columns (e.g. a scatter's x and
    * y don't both collapse onto the first numeric column, which would be a
    * degenerate diagonal). Shared across the operator, nested objects included.
    * An explicit `@SampleColumn` always wins even if the column is already taken;
    * only the type-match and first-column tiers avoid reuse.
    */
  private def buildObject(
      clazz: Class[_],
      schemas: Map[Int, Schema],
      used: mutable.Set[(Int, String)],
      rowCount: Int
  ): Either[String, ObjectNode] = {
    val node = objectMapper.createObjectNode()
    configFields(clazz).foreach { f =>
      decide(f, schemas, used, rowCount) match {
        case Fill(name, value) => node.set[JsonNode](name, value)
        case Skip              => ()
        case Fail(reason)      => return Left(s"${clazz.getSimpleName}.${f.getName}: $reason")
      }
    }
    Right(node)
  }

  private sealed trait Decision
  private case class Fill(jsonName: String, value: JsonNode) extends Decision
  private case object Skip extends Decision
  private case class Fail(reason: String) extends Decision

  /** Decide whether/how to fill one field, applying required-vs-optional policy:
    * required (or required autofill) fields that can't be filled fail the whole
    * operator; optional fields without a meaningful value are skipped (left at the
    * operator's default).
    */
  private def decide(
      f: Field,
      schemas: Map[Int, Schema],
      used: mutable.Set[(Int, String)],
      rowCount: Int
  ): Decision = {
    val jp = Option(f.getAnnotation(classOf[JsonProperty]))
    val jsonName = jp.map(_.value).filter(_.nonEmpty).getOrElse(f.getName)
    val required = jp.exists(_.required)
    val autofill = hasAutofill(f)
    val isBoolean = f.getType == classOf[Boolean] || f.getType == classOf[java.lang.Boolean]

    // An OPTIONAL column-name field (`@AutofillAttributeName*` with required=false)
    // is left at its operator default rather than force-filled. These are the
    // "No Selection" grouping/pattern knobs (e.g. BarChart's categoryColumn /
    // pattern); forcing a real column into one produces a degenerate config (one
    // trace per row) that the native and generated paths disagree on.
    if (autofill && !required) Skip
    else {
      val meaningful = required || autofill || f.getType.isEnum || isBoolean || isList(f.getType) ||
        isOption(f.getType) || isNestedObject(f.getType) || jp
        .map(_.defaultValue)
        .exists(_.nonEmpty)

      valueFor(f, schemas, used, rowCount) match {
        case Right(v) if meaningful               => Fill(jsonName, v)
        case Right(_)                             => Skip // optional plain scalar w/o default — leave operator default
        case Left(reason) if required || autofill => Fail(reason)
        case Left(_)                              => Skip
      }
    }
  }

  // ── value resolution ─────────────────────────────────────────────────────

  /** Resolve a JSON value node for a field: autofill column refs first, then by
    * declared type (list / option / scalar / nested object).
    */
  private def valueFor(
      f: Field,
      schemas: Map[Int, Schema],
      used: mutable.Set[(Int, String)],
      rowCount: Int
  ): Either[String, JsonNode] = {
    if (f.isAnnotationPresent(classOf[AutofillAttributeNameList]))
      columnNames(schemas, 0).map { names =>
        // Honor the field's attributeTypeRules (same production metadata scalar
        // autofill fields use) so a numeric-only list doesn't pick up string
        // columns; fall back to all columns if no column matches the rule.
        val filtered = allowedTypes(f) match {
          case Some(types) =>
            val matching = schemas
              .get(0)
              .map(_.getAttributes.filter(a => types.contains(a.getType)).map(_.getName))
              .getOrElse(Seq.empty)
            if (matching.nonEmpty) matching else names
          case None => names
        }
        val arr = objectMapper.createArrayNode(); filtered.foreach(arr.add); arr
      }
    else if (f.isAnnotationPresent(classOf[AutofillAttributeNameOnPort1]))
      resolveColumn(f, schemas, 1, used).map(objectMapper.getNodeFactory.textNode)
    else if (f.isAnnotationPresent(classOf[AutofillAttributeName]))
      resolveColumn(f, schemas, 0, used).map(objectMapper.getNodeFactory.textNode)
    else {
      val t = f.getType
      if (isList(t))
        elementType(f).flatMap(scalarOrNested(_, schemas, used, rowCount)).map { e =>
          val arr: ArrayNode = objectMapper.createArrayNode(); arr.add(e); arr
        }
      else if (isOption(t))
        elementType(f).flatMap(scalarOrNested(_, schemas, used, rowCount))
      else scalarNode(t, defaultOf(f), schemas, used, NumHint(declaredRange(f), rowCount))
    }
  }

  /** A node for a list element or Option inner type — no field-level default or
    * range annotation (those live on the field, not the element type).
    */
  private def scalarOrNested(
      clazz: Class[_],
      schemas: Map[Int, Schema],
      used: mutable.Set[(Int, String)],
      rowCount: Int
  ): Either[String, JsonNode] =
    scalarNode(clazz, None, schemas, used, NumHint(None, rowCount))

  /** How to fill a numeric field: `@JsonProperty(defaultValue)` if present, else
    * the middle of the declared `[min, max]` range (e.g. an opacity's 0–1 → 0.5),
    * else half the row count (the middle of `[0, rowCount]`, e.g. Limit).
    */
  private final case class NumHint(range: Option[(Double, Double)], rowCount: Int)

  private def numericFill(default: Option[String], hint: NumHint): Double =
    default.flatMap(s => Try(s.trim.toDouble).toOption) match {
      case Some(d) => d
      case None =>
        hint.range match {
          case Some((mn, mx)) => (mn + mx) / 2.0
          case None           => hint.rowCount / 2.0
        }
    }

  /** A node for a concrete (non-list, non-option) type. Numeric fields follow
    * [[numericFill]]; enums/strings honor an optional `@JsonProperty(defaultValue)`.
    */
  private def scalarNode(
      t: Class[_],
      default: Option[String],
      schemas: Map[Int, Schema],
      used: mutable.Set[(Int, String)],
      hint: NumHint
  ): Either[String, JsonNode] = {
    val nf = objectMapper.getNodeFactory
    if (t.isEnum)
      Right(
        default
          .map(nf.textNode)
          .getOrElse(objectMapper.valueToTree[JsonNode](t.getEnumConstants.head))
      )
    else if (t == classOf[Boolean] || t == classOf[java.lang.Boolean])
      Right(nf.booleanNode(default.map(_.trim.toBoolean).getOrElse(false)))
    else if (t == classOf[Int] || t == classOf[java.lang.Integer] || t == classOf[Short])
      Right(nf.numberNode(numericFill(default, hint).round.toInt))
    else if (t == classOf[Long] || t == classOf[java.lang.Long])
      Right(nf.numberNode(numericFill(default, hint).round))
    else if (t == classOf[Double] || t == classOf[java.lang.Double] || t == classOf[Float])
      Right(nf.numberNode(numericFill(default, hint)))
    else if (t == classOf[String])
      Right(nf.textNode(default.getOrElse(CanonicalString)))
    else if (isNestedObject(t))
      buildObject(t, schemas, used, hint.rowCount)
    else Left(s"unhandled type ${t.getName}")
  }

  /** The `[minimum, maximum]` a field declares via `@JsonSchemaInject` (e.g. an
    * opacity's 0.0–1.0). `None` if the field declares no complete range.
    */
  private def declaredRange(f: Field): Option[(Double, Double)] =
    Option(f.getAnnotation(classOf[JsonSchemaInject])).map(_.json).filter(_.nonEmpty).flatMap {
      js =>
        Try {
          val node = objectMapper.readTree(js)
          val mn = node.path("minimum")
          val mx = node.path("maximum")
          if (mn.isNumber && mx.isNumber) Some((mn.asDouble(), mx.asDouble())) else None
        }.toOption.flatten
    }

  // ── reflection helpers ───────────────────────────────────────────────────

  /** Config fields declared on `clazz` and its superclasses up to (not
    * including) [[LogicalOp]] — i.e. the operator's own knobs, not the
    * framework's bookkeeping. A field counts if it carries `@JsonProperty` or an
    * autofill annotation.
    */
  private def configFields(clazz: Class[_]): Seq[Field] = {
    val out = mutable.LinkedHashMap.empty[String, Field] // de-dup by name, keep most-derived
    var c: Class[_] = clazz
    while (c != null && c != classOf[LogicalOp] && c != classOf[Object]) {
      c.getDeclaredFields
        .filterNot(f => Modifier.isStatic(f.getModifiers))
        .filter(isConfigField)
        .foreach(f => out.getOrElseUpdate(f.getName, { f.setAccessible(true); f }))
      c = c.getSuperclass
    }
    out.values.toSeq
  }

  private def isConfigField(f: Field): Boolean =
    f.isAnnotationPresent(classOf[JsonProperty]) ||
      f.isAnnotationPresent(classOf[AutofillAttributeName]) ||
      f.isAnnotationPresent(classOf[AutofillAttributeNameOnPort1]) ||
      f.isAnnotationPresent(classOf[AutofillAttributeNameList])

  private def hasAutofill(f: Field): Boolean =
    f.isAnnotationPresent(classOf[AutofillAttributeName]) ||
      f.isAnnotationPresent(classOf[AutofillAttributeNameOnPort1]) ||
      f.isAnnotationPresent(classOf[AutofillAttributeNameList])

  private def defaultOf(f: Field): Option[String] =
    Option(f.getAnnotation(classOf[JsonProperty])).map(_.defaultValue).filter(_.nonEmpty)

  private def isList(t: Class[_]): Boolean =
    classOf[scala.collection.Seq[_]].isAssignableFrom(t) ||
      classOf[java.util.List[_]].isAssignableFrom(t)

  private def isOption(t: Class[_]): Boolean = classOf[Option[_]].isAssignableFrom(t)

  /** The element class of a `List[X]` / `Option[X]` field, from its generic
    * signature.
    */
  private def elementType(f: Field): Either[String, Class[_]] =
    f.getGenericType match {
      case p: ParameterizedType =>
        p.getActualTypeArguments.headOption match {
          case Some(c: Class[_])           => Right(c)
          case Some(pt: ParameterizedType) => Right(pt.getRawType.asInstanceOf[Class[_]])
          case _                           => Left(s"cannot resolve element type of ${f.getName}")
        }
      case _ => Left(s"${f.getName} has no generic element type")
    }

  /** A type we should recurse into and build as a nested JSON object: not a
    * primitive/boxed/String/enum/collection, and it actually declares config
    * fields or a creator.
    */
  private def isNestedObject(t: Class[_]): Boolean = {
    val excluded = t.isPrimitive || t.isEnum || t == classOf[String] ||
      isList(t) || isOption(t) || t.getName.startsWith("java.lang.")
    !excluded && (configFields(t).nonEmpty || t.getDeclaredConstructors.exists(
      _.getParameterCount > 0
    ))
  }

  private def columnNames(schemas: Map[Int, Schema], port: Int): Either[String, Seq[String]] =
    schemas.get(port).map(_.getAttributeNames).filter(_.nonEmpty) match {
      case Some(names) => Right(names)
      case None        => Left(s"no input columns at port $port")
    }

  private def firstColumn(schemas: Map[Int, Schema], port: Int): Either[String, String] =
    columnNames(schemas, port).map(_.head)

  /** First column at `port` not yet claimed by a sibling field of the same
    * operator (so two un-annotated / same-type fields don't collapse onto the
    * same column); the first column if every column is already taken. Marks the
    * pick in `used`.
    */
  private def firstUnused(
      schemas: Map[Int, Schema],
      port: Int,
      used: mutable.Set[(Int, String)]
  ): Either[String, String] =
    columnNames(schemas, port).map { names =>
      val col = names.find(c => !used.contains((port, c))).getOrElse(names.head)
      used += ((port, col)); col
    }

  /** Pick which input column fills an `@AutofillAttributeName*` field, in
    * priority order:
    *   1. `@SampleColumn("x")` — an explicit semantic pick (e.g. a valid ISO
    *      country code or a real OHLC column) that the column's type can't
    *      express; always honored, even if already used;
    *   2. the first *unused* column whose [[AttributeType]] satisfies the field's
    *      `attributeTypeRules` (falling back to the first matching column if all
    *      are taken);
    *   3. the first unused column (the original first-column behavior, made
    *      distinct-aware).
    * Tiers 1–2 keep the parity test on realistic, type-correct input; the
    * distinct-column preference stops sibling fields (x/y, source/target) from
    * collapsing onto one column and producing a degenerate result.
    */
  private def resolveColumn(
      f: Field,
      schemas: Map[Int, Schema],
      port: Int,
      used: mutable.Set[(Int, String)]
  ): Either[String, String] = {
    def take(col: String): String = { used += ((port, col)); col }
    Option(f.getAnnotation(classOf[SampleColumn])).map(_.value) match {
      case Some(col) =>
        columnNames(schemas, port).flatMap { names =>
          if (names.contains(col)) Right(take(col))
          else
            Left(
              s"""@SampleColumn("$col") not present at port $port (have: ${names.mkString(", ")})"""
            )
        }
      case None =>
        allowedTypes(f) match {
          case Some(types) =>
            schemas
              .get(port)
              .map(_.getAttributes.filter(a => types.contains(a.getType)).map(_.getName)) match {
              case Some(cols) if cols.nonEmpty =>
                Right(take(cols.find(c => !used.contains((port, c))).getOrElse(cols.head)))
              case _ => firstUnused(schemas, port, used) // no type-matching column; fall back
            }
          case None => firstUnused(schemas, port, used)
        }
    }
  }

  /** [[AttributeType]]s permitted for `f` by its declaring class's
    * `@JsonSchemaInject(json = ...)` `attributeTypeRules`, keyed by the field's
    * JSON name. `None` when the field is unconstrained.
    */
  private def allowedTypes(f: Field): Option[Set[AttributeType]] = {
    val jsonName =
      Option(f.getAnnotation(classOf[JsonProperty]))
        .map(_.value)
        .filter(_.nonEmpty)
        .getOrElse(f.getName)
    Option(f.getDeclaringClass.getAnnotation(classOf[JsonSchemaInject]))
      .map(_.json)
      .filter(_.nonEmpty)
      .flatMap { js =>
        Try {
          val enumNode =
            objectMapper.readTree(js).path("attributeTypeRules").path(jsonName).path("enum")
          if (enumNode.isArray) {
            val set = enumNode.elements().asScala.flatMap(n => typeFromString(n.asText())).toSet
            if (set.nonEmpty) Some(set) else None
          } else None
        }.toOption.flatten
      }
  }

  private def typeFromString(s: String): Option[AttributeType] =
    AttributeType.values().find(_.name.equalsIgnoreCase(s))
}
