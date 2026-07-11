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

  /** Canonical literal for free-form fields; present in the synthetic dataset so
    * filters/comparisons actually match rows. Parses as INTEGER/LONG/DOUBLE and
    * is a valid STRING. */
  private val CanonicalString = "1"

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
      inputSchemas: Map[Int, Schema]
  ): Either[String, LogicalOp] = {
    buildObject(opClass, inputSchemas).flatMap { node =>
      // LogicalOp is polymorphic (@JsonTypeInfo on `operatorType`); Jackson needs
      // the registered type id to deserialize the concrete subtype.
      typeNameByClass.get(opClass) match {
        case Some(typeName) => node.put("operatorType", typeName)
        case None           => return Left(s"${opClass.getSimpleName} not registered in LogicalOp @JsonSubTypes")
      }
      Try(objectMapper.treeToValue(node, opClass))
        .toEither
        .left
        .map(e => s"deserialization failed: ${e.getMessage}")
    }
  }

  /** Maps each registered operator class to its `operatorType` discriminator,
    * read from [[LogicalOp]]'s `@JsonSubTypes` (the same registry Jackson uses). */
  private val typeNameByClass: Map[Class[_], String] = {
    Option(classOf[LogicalOp].getAnnotation(classOf[JsonSubTypes]))
      .map(_.value().toSeq.map(t => (t.value(): Class[_]) -> t.name()).toMap)
      .getOrElse(Map.empty)
  }

  // ── object assembly ──────────────────────────────────────────────────────

  /** Build a JSON object for `clazz` by filling each of its config fields. */
  private def buildObject(
      clazz: Class[_],
      schemas: Map[Int, Schema]
  ): Either[String, ObjectNode] =
    buildObject(clazz, schemas, mutable.Set.empty[(Int, String)])

  /** `used` tracks (port, column) already assigned within THIS operator, so that
    * sibling autofill fields resolve to DISTINCT columns (e.g. a scatter's x and
    * y don't both collapse onto the first numeric column, which would be a
    * degenerate diagonal). Shared across the operator, nested objects included.
    * An explicit `@SampleColumn` always wins even if the column is already taken;
    * only the type-match and first-column tiers avoid reuse. */
  private def buildObject(
      clazz: Class[_],
      schemas: Map[Int, Schema],
      used: mutable.Set[(Int, String)]
  ): Either[String, ObjectNode] = {
    val node = objectMapper.createObjectNode()
    configFields(clazz).foreach { f =>
      decide(f, schemas, used) match {
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
    * required (or autofill) fields that can't be filled fail the whole operator;
    * optional scalars without a meaningful value are skipped (left at default). */
  private def decide(f: Field, schemas: Map[Int, Schema], used: mutable.Set[(Int, String)]): Decision = {
    val jp = Option(f.getAnnotation(classOf[JsonProperty]))
    val jsonName = jp.map(_.value).filter(_.nonEmpty).getOrElse(f.getName)
    val required = jp.exists(_.required)
    val autofill = hasAutofill(f)
    val meaningful = required || autofill || f.getType.isEnum || isList(f.getType) ||
      isOption(f.getType) || isNestedObject(f.getType) || jp.map(_.defaultValue).exists(_.nonEmpty)

    valueFor(f, schemas, used) match {
      case Right(v) if meaningful => Fill(jsonName, v)
      case Right(_)               => Skip // optional plain scalar w/o default — leave operator default
      case Left(reason) if required || autofill => Fail(reason)
      case Left(_)                              => Skip
    }
  }

  // ── value resolution ─────────────────────────────────────────────────────

  /** Resolve a JSON value node for a field: autofill column refs first, then by
    * declared type (list / option / scalar / nested object). */
  private def valueFor(f: Field, schemas: Map[Int, Schema], used: mutable.Set[(Int, String)]): Either[String, JsonNode] = {
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
        elementType(f).flatMap(scalarOrNested(_, schemas, used)).map { e =>
          val arr: ArrayNode = objectMapper.createArrayNode(); arr.add(e); arr
        }
      else if (isOption(t))
        elementType(f).flatMap(scalarOrNested(_, schemas, used))
      else scalarNode(t, defaultOf(f), schemas, used)
    }
  }

  /** A node for a list element or Option inner type — no `defaultValue` to read
    * (that lives on the field, not the element), so scalars get the canonical. */
  private def scalarOrNested(clazz: Class[_], schemas: Map[Int, Schema], used: mutable.Set[(Int, String)]): Either[String, JsonNode] =
    scalarNode(clazz, None, schemas, used)

  /** A node for a concrete (non-list, non-option) type, honoring an optional
    * `defaultValue` string from the field's `@JsonProperty`. */
  private def scalarNode(
      t: Class[_],
      default: Option[String],
      schemas: Map[Int, Schema],
      used: mutable.Set[(Int, String)]
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
      Right(nf.numberNode(default.flatMap(s => Try(s.trim.toInt).toOption).getOrElse(1)))
    else if (t == classOf[Long] || t == classOf[java.lang.Long])
      Right(nf.numberNode(default.flatMap(s => Try(s.trim.toLong).toOption).getOrElse(1L)))
    else if (t == classOf[Double] || t == classOf[java.lang.Double] || t == classOf[Float])
      Right(nf.numberNode(default.flatMap(s => Try(s.trim.toDouble).toOption).getOrElse(1.0)))
    else if (t == classOf[String])
      Right(nf.textNode(default.getOrElse(CanonicalString)))
    else if (isNestedObject(t))
      buildObject(t, schemas, used)
    else Left(s"unhandled type ${t.getName}")
  }

  // ── reflection helpers ───────────────────────────────────────────────────

  /** Config fields declared on `clazz` and its superclasses up to (not
    * including) [[LogicalOp]] — i.e. the operator's own knobs, not the
    * framework's bookkeeping. A field counts if it carries `@JsonProperty` or an
    * autofill annotation. */
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
    * signature. */
  private def elementType(f: Field): Either[String, Class[_]] =
    f.getGenericType match {
      case p: ParameterizedType =>
        p.getActualTypeArguments.headOption match {
          case Some(c: Class[_])         => Right(c)
          case Some(pt: ParameterizedType) => Right(pt.getRawType.asInstanceOf[Class[_]])
          case _                         => Left(s"cannot resolve element type of ${f.getName}")
        }
      case _ => Left(s"${f.getName} has no generic element type")
    }

  /** A type we should recurse into and build as a nested JSON object: not a
    * primitive/boxed/String/enum/collection, and it actually declares config
    * fields or a creator. */
  private def isNestedObject(t: Class[_]): Boolean = {
    val excluded = t.isPrimitive || t.isEnum || t == classOf[String] ||
      isList(t) || isOption(t) || t.getName.startsWith("java.lang.")
    !excluded && (configFields(t).nonEmpty || t.getDeclaredConstructors.exists(_.getParameterCount > 0))
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
    * pick in `used`. */
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
    * collapsing onto one column and producing a degenerate result. */
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
          else Left(s"@SampleColumn(\"$col\") not present at port $port (have: ${names.mkString(", ")})")
        }
      case None =>
        allowedTypes(f) match {
          case Some(types) =>
            schemas.get(port).map(_.getAttributes.filter(a => types.contains(a.getType)).map(_.getName)) match {
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
    * JSON name. `None` when the field is unconstrained. */
  private def allowedTypes(f: Field): Option[Set[AttributeType]] = {
    val jsonName =
      Option(f.getAnnotation(classOf[JsonProperty])).map(_.value).filter(_.nonEmpty).getOrElse(f.getName)
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
