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

import com.fasterxml.jackson.annotation.{
  JsonIgnore,
  JsonIgnoreProperties,
  JsonProperty,
  JsonSubTypes
}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorMetadataGenerator
import org.apache.texera.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameList,
  AutofillAttributeNameOnPort1,
  CommonOpDescAnnotation,
  HideAnnotation,
  SampleColumn
}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.lang.reflect.{Field, Modifier, ParameterizedType, Type, TypeVariable}
import javax.validation.constraints.{DecimalMin, Min}
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
  val DefaultRowCount = 10

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
      rowCount: Int = DefaultRowCount,
      pinned: Map[String, JsonNode] = Map.empty,
      switches: Map[String, JsonNode] = Map.empty
  ): Either[String, Seq[(String, LogicalOp)]] =
    typeNameByClass.get(opClass) match {
      case None => Left(s"${opClass.getSimpleName} not registered in LogicalOp @JsonSubTypes")
      case Some(typeName) =>
        val used = mutable.Set.empty[(Int, String)]
        buildObject(opClass, inputSchemas, used, rowCount, pinned = pinned).flatMap { baseNode =>
          baseNode.put("operatorType", typeName)
          pinned.foreach { case (field, value) => baseNode.set[JsonNode](field, value) }
          applyAll(
            opClass,
            baseNode,
            None,
            allVariants(
              opClass,
              baseNode,
              inputSchemas,
              used,
              rowCount,
              pinned = pinned.keySet,
              switches = switches
            )
          )
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
    // base = the original op (preserve the curated config exactly); variants are
    // deserialized from the JSON with one enum flipped.
    nodeOf(opDesc).flatMap(node =>
      applyAll(opClass, node, Some(opDesc), Variant.Base +: enumVariants(opClass, node))
    )
  }

  /**
    * [[variantsOf]] plus the two multi-knob variants [[generateVariants]] gives an
    * auto-configured op: `optionals` and `hostileText` (see [[extraVariants]]).
    *
    * A separate entry point rather than a widening of [[variantsOf]] so a caller
    * states which it wants, and a failure points at one of them. Curated fixtures
    * are the reason it exists: a hand-written config is the ONLY config its
    * operator ever runs, so without this its optional knobs stay at their defaults
    * and nothing ever splices a quote into the code it generates.
    *
    * `inputSchemas` describes the op's OWN inputs — a curated handler writes its
    * own fixture, so this is not necessarily the canonical one.
    */
  def fullVariantsOf(
      opDesc: LogicalOp,
      inputSchemas: Map[Int, Schema],
      rowCount: Int = DefaultRowCount,
      sweepEnums: Boolean = true
  ): Either[String, Seq[(String, LogicalOp)]] = {
    val opClass = opDesc.getClass.asInstanceOf[Class[_ <: LogicalOp]]
    for {
      node <- nodeOf(opDesc)
      variants <- fullVariantEditsOf(opDesc, inputSchemas, rowCount, sweepEnums)
      ops <- applyAll(opClass, node, Some(opDesc), variants)
    } yield ops
  }

  /**
    * What [[fullVariantsOf]] runs, as the edits themselves rather than the finished
    * ops — for a caller that has to REBUILD its fixture per variant and so must
    * apply them to a FRESH op. A source is that caller: its exported script reads
    * the file by bare name out of the directory it runs in, so every variant needs
    * its own directory and its own copy, produced by calling the handler again.
    *
    * `sweepEnums = false` keeps the fills but drops the enum sweep, for a fixture
    * whose enums are cross-constrained with the data it holds — flipping one then
    * describes a table the fixture is not.
    */
  def fullVariantEditsOf(
      opDesc: LogicalOp,
      inputSchemas: Map[Int, Schema],
      rowCount: Int = DefaultRowCount,
      sweepEnums: Boolean = true
  ): Either[String, Seq[Variant]] = {
    val opClass = opDesc.getClass.asInstanceOf[Class[_ <: LogicalOp]]
    nodeOf(opDesc).map { node =>
      val used = occupiedColumns(opClass, node, inputSchemas)
      allVariants(opClass, node, inputSchemas, used, rowCount, sweepEnums)
    }
  }

  /** `opDesc` with `variant`'s edits applied. The base variant carries no edits and
    * hands the instance straight back, so a curated config is never round-tripped
    * through JSON just to be left unchanged.
    */
  def applyVariant(opDesc: LogicalOp, variant: Variant): Either[String, LogicalOp] =
    if (variant.at.isEmpty) Right(opDesc)
    else
      nodeOf(opDesc).flatMap { node =>
        variant.at.foreach { case (pointer, value) => setAtPointer(node, pointer, value) }
        deserialize(node, opDesc.getClass.asInstanceOf[Class[_ <: LogicalOp]])
      }

  /** One named configuration, as the pointer → value edits that turn a base config
    * into it. Applied to one clone of that base.
    */
  final case class Variant(label: String, at: Seq[(String, JsonNode)])

  object Variant {

    /** The base config itself — no edits, so [[applyVariant]] returns it unchanged. */
    val Base: Variant = Variant("default", Seq.empty)
  }

  /** Every variant of `baseNode`: the config itself, one per non-default enum value,
    * then the two multi-knob fills.
    */
  private def allVariants(
      opClass: Class[_ <: LogicalOp],
      baseNode: ObjectNode,
      schemas: Map[Int, Schema],
      used: mutable.Set[(Int, String)],
      rowCount: Int,
      sweepEnums: Boolean = true,
      pinned: Set[String] = Set.empty,
      switches: Map[String, JsonNode] = Map.empty
  ): Seq[Variant] =
    Variant.Base +: ((if (sweepEnums)
                        enumVariants(
                          opClass,
                          baseNode,
                          pinned,
                          FillContext(schemas, mutable.Set.empty ++ used, rowCount)
                        )
                      else Seq.empty) ++
      extraVariants(opClass, baseNode, schemas, used, rowCount, switches))

  /** The two multi-knob variants, so called because each moves every knob of its kind
    * at once. An operator's knobs are worth exercising, but bisecting a rare failure
    * by hand costs less than a run per field: all optional knobs are filled together,
    * and all free-text knobs take the hostile value together.
    *
    * A row from the UI's `+` button is one of those optional knobs, not a variant of
    * its own: for a list the base leaves empty it IS the "now it is set" case, exactly
    * like a scalar going from unset to set.
    */
  private def extraVariants(
      opClass: Class[_ <: LogicalOp],
      baseNode: ObjectNode,
      schemas: Map[Int, Schema],
      used: mutable.Set[(Int, String)],
      rowCount: Int,
      switches: Map[String, JsonNode] = Map.empty
  ): Seq[Variant] =
    Seq(
      merged(
        "optionals", {
          // One counter and one `used` set for the whole variant: the three walks below
          // land in ONE config, so a column taken by any of them is taken for all, and
          // restarting the counter per walk gave two rows the same value. A copy, so the
          // base pass's own set is left alone.
          val ordinal = new Ordinal
          val taken = mutable.Set.empty[(Int, String)] ++ used
          optionalColumnFills(opClass, schemas, taken, baseNode) ++
            optionalScalarFills(opClass, baseNode, "", schemas, taken, rowCount, ordinal) ++
            extraRowFills(opClass, baseNode, schemas, taken, rowCount, ordinal) ++
            switches.toSeq.sortBy(_._1).map {
              case (field, value) => Variant(s"$field=${value.asText}", Seq((s"/$field", value)))
            }
        }
      ),
      merged("hostileText", numbered(hostileTextFills(opClass, baseNode, "")))
    ).flatten

  /** Apply each variant to its own clone of `baseNode` and read the result back as an
    * op. `base` is the instance to hand back for the unedited variant, when the caller
    * has one whose exact state matters (a curated fixture); `None` deserializes it
    * from `baseNode` like any other.
    */
  private def applyAll(
      opClass: Class[_ <: LogicalOp],
      baseNode: ObjectNode,
      base: Option[LogicalOp],
      variants: Seq[Variant]
  ): Either[String, Seq[(String, LogicalOp)]] = {
    val results = variants.map { variant =>
      base.filter(_ => variant.at.isEmpty) match {
        case Some(op) => Right((variant.label, op))
        case None =>
          val clone = baseNode.deepCopy()
          variant.at.foreach { case (pointer, value) => setAtPointer(clone, pointer, value) }
          deserialize(clone, opClass).map((variant.label, _))
      }
    }
    results.collectFirst { case Left(err) => err }.toLeft(results.collect { case Right(ok) => ok })
  }

  /** An already-configured op as the JSON this generator edits: its serialized form,
    * carrying the polymorphic type id Jackson needs to read the concrete subtype back.
    */
  private def nodeOf(opDesc: LogicalOp): Either[String, ObjectNode] = {
    val opClass = opDesc.getClass.asInstanceOf[Class[_ <: LogicalOp]]
    objectMapper.valueToTree[JsonNode](opDesc) match {
      case node: ObjectNode =>
        if (!node.has("operatorType"))
          typeNameByClass.get(opClass).foreach(node.put("operatorType", _))
        Right(node)
      case _ => Left(s"${opClass.getSimpleName} did not serialize to a JSON object")
    }
  }

  /** The (port, column) pairs an already-configured op's column pickers already hold,
    * as the `used` set the optional-knob fill resolves against — so a knob it fills
    * lands on a column the fixture is not using yet, the same rule the base pass keeps
    * for sibling pickers. Without it a curated x/y and a filled-in optional colour all
    * collapse onto one column.
    *
    * Walks the fixture's nested rows too, not just its top-level fields: the picker an
    * appended row has to differ from usually lives in the rows ALREADY there (a
    * projection's column list), and re-picking one of those asks the operator for the
    * same output column twice.
    */
  private def occupiedColumns(
      clazz: Class[_],
      node: JsonNode,
      schemas: Map[Int, Schema],
      path: String = ""
  ): mutable.Set[(Int, String)] = {
    val used = mutable.Set.empty[(Int, String)]
    configFields(clazz).foreach { f =>
      val childPath = pointerOf(f, path)
      rowType(f) match {
        case Some(row) =>
          rowPaths(f, node.at(childPath), childPath)
            .foreach(rowPath => used ++= occupiedColumns(row, node, schemas, rowPath))
        case None if hasAutofill(f) =>
          val port = autofillSpec(f).map(_.port).getOrElse(0)
          val columns =
            schemas.get(port).map(_.getAttributes.map(_.getName).toSet).getOrElse(Set.empty)
          val held = node.at(childPath)
          val values = if (held.isArray) held.elements().asScala.toSeq else Seq(held)
          values.filter(_.isTextual).map(_.asText).filter(columns).foreach(c => used += ((port, c)))
        case None => ()
      }
    }
    used
  }

  /** One fill per OPTIONAL column knob, which [[decide]] leaves unset. Unset is the
    * right base config — it is what most workflows carry — but it also means the
    * branch each generator emits for a knob that IS set never runs on either path,
    * so the two hand-written branches are never compared.
    *
    * Resolved against the `used` set the whole variant shares, so the column a knob
    * takes differs from what the config already reads. A list knob takes a SINGLE
    * column: the "every matching column" fill suits a required axes list, not an
    * optional narrowing one — all thirty columns as group-by keys would make every row
    * its own group.
    *
    * A knob the config ALREADY points at a column is left alone. That never happens
    * to the auto base — [[decide]] skips every optional picker, so each one still
    * holds the value a fresh instance has — but a curated config picks its columns
    * deliberately, and overwriting one would discard the fixture's whole point.
    *
    * Only the operator's OWN fields: a picker inside a nested row is filled by
    * [[rowFills]], on the row walk that knows which row it belongs to.
    */
  private def optionalColumnFills(
      clazz: Class[_],
      schemas: Map[Int, Schema],
      used: mutable.Set[(Int, String)],
      baseNode: JsonNode
  ): Seq[Variant] =
    configFields(clazz).filter(hasAutofill).filterNot(hiddenBySibling(_, baseNode)).flatMap { f =>
      columnFill(clazz, f, baseNode, pointerOf(f, ""), schemas, used, baseNode).map {
        case (pointer, value) =>
          // A list knob holds its one column in an array; name the column either way.
          val col = if (value.isArray) value.path(0).asText else value.asText
          Variant(s"${pointer.stripPrefix("/")}=$col", Seq((pointer, value)))
      }
    }

  /** One fill per `+`-row list, appending ONE MORE row than the base carries: the first
    * row for an optional list (empty, as the UI starts it), a second for a required one.
    *
    * For an optional list that is the point — its rows are otherwise never populated.
    * For a required one it reaches only the code BETWEEN rows (the separator each path
    * joins them with, whatever an operator does with several at once); NOT a mis-indexed
    * value, since both generators read every value off the loop variable.
    *
    * The row is built by the same pass as the first, against the `used` set the whole
    * variant shares, so its column knobs land on columns nothing else is reading.
    */
  private def extraRowFills(
      clazz: Class[_],
      baseNode: JsonNode,
      schemas: Map[Int, Schema],
      used: mutable.Set[(Int, String)],
      rowCount: Int,
      ordinal: Ordinal
  ): Seq[Variant] = {
    // The appended row is built outside [[buildObject]]'s walk, so what that walk
    // carries down to a row has to be handed over here as well: without them the new
    // row's type-variable fields and schema-ruled fields come back empty, and the
    // variant then fails on a value this generator left out rather than on anything
    // the operator does.
    val ownBindings = typeBindingsOf(clazz)
    val ownScope = SchemaScope.of(clazz)
    configFields(clazz).flatMap { f =>
      val childPath = pointerOf(f, "")
      val rows = baseNode.at(childPath)
      for {
        row <- if (isList(f.getType)) elementType(f).toOption.filter(isNestedObject) else None
        if rows.isArray
        next <- buildObject(
          row,
          schemas,
          used,
          rowCount,
          elementBindings(f, ownBindings),
          ownScope.descend(jsonNameOf(f))
        ).toOption
      } yield {
        // Fill the new row's own optional knobs too — the `optionals` variant is
        // computed against the BASE config, where this row does not exist yet, so
        // otherwise the row arrives with every free-value knob at its default, a step
        // whose bounds are both empty is dropped by the operator, and an optional column
        // picker (which [[decide]] skips) stays null.
        rowFills(row, next, "", schemas, used, rowCount, ordinal).foreach {
          case (pointer, value) => setAtPointer(next, pointer, value)
        }
        distinguish(
          row,
          next,
          rows,
          elementBindings(f, ownBindings),
          ownScope.descend(jsonNameOf(f)),
          FillContext(schemas, used, rowCount)
        )
        Variant(childPath, Seq((s"$childPath/${rows.size()}", next)))
      }
    }
  }

  /** Move the appended row off a value a row beside it already holds, where holding
    * the same one makes the two rows the same row. Every row is built the same way and
    * so comes back with the same values, which for a second hyperparameter row means
    * `C` twice: not two settings but one written twice, which the emitted Python
    * rejects outright as a repeated keyword argument.
    *
    * Only the field the rest of the row is stated in terms of, which is the one a
    * `valueRules` condition reads. A row's other choices are its own business, and two
    * lines of a chart drawn in the same style are still two lines. That a knob has to
    * differ is something only the schema can say, and this is where it says it.
    */
  private def distinguish(
      rowClass: Class[_],
      next: ObjectNode,
      siblings: JsonNode,
      bindings: TypeBindings,
      scope: SchemaScope,
      fill: FillContext
  ): Unit = {
    val deciding = decidingFields(rowClass, scope)
    enumSites(rowClass, next, "", bindings, scope, fill)
      .filter(site => deciding.contains(site.pointer.stripPrefix("/")))
      .foreach { site =>
        val taken = siblings.elements().asScala.map(_.at(site.pointer)).toSet
        if (taken.contains(next.at(site.pointer)))
          site.values.find(v => !taken.contains(v)).foreach { v =>
            setAtPointer(next, site.pointer, v)
            site.companions(v).foreach {
              case (pointer, value) => setAtPointer(next, pointer, value)
            }
          }
      }
  }

  /** The fields of `clazz` that some other field's `valueRules` is stated in terms of.
    * A hyperparameter row has one, `parameter`, and most objects have none.
    */
  private def decidingFields(clazz: Class[_], scope: SchemaScope): Set[String] =
    configFields(clazz).iterator
      .flatMap(f => scope.child(jsonNameOf(f)).path("valueRules").path("allOf").elements().asScala)
      .flatMap(_.path("if").fieldNames().asScala)
      .toSet

  /** One variant out of many fills, labelled with the fields it sets. `None` when
    * there is nothing to fill, so an operator without such knobs gains no variant.
    */
  private def merged(kind: String, fills: Seq[Variant]): Option[Variant] = {
    val at = fills.flatMap(_.at)
    if (at.isEmpty) None
    else {
      val names = at.map(_._1.stripPrefix("/")).distinct
      val shown = names.mkString(",")
      val label = if (shown.length <= 60) shown else s"${names.size} fields"
      Some(Variant(s"$kind($label)", at))
    }
  }

  /** Extra variants for the OPTIONAL free-value scalar knobs — a number or a
    * string the user types in, as opposed to a column picker or a dropdown.
    * [[decide]] leaves these unset for the same reason [[optionalColumnFills]]'s
    * knobs are unset, and they need the same treatment: the branch each generator
    * emits for a knob that IS set (a gauge's delta arrow, a step row's range)
    * never runs on either path, so the two hand-written branches are never
    * compared.
    *
    * Every knob found here ends up in ONE variant (see [[merged]]), the row ones
    * included: a row is what the UI's `+` button adds, and its fields are read as a
    * unit anyway (a step's start AND end make one range).
    *
    * "Unset" is read off `baseNode` rather than re-derived, and it means the knob
    * still holds what a fresh instance holds (see [[leafFill]]). A `defaultValue`
    * does not exempt it: a knob sitting at its default is indistinguishable from
    * one the user never touched, so the only way to exercise the branch for a
    * knob that IS set is to move it off that value.
    */
  private def optionalScalarFills(
      clazz: Class[_],
      baseNode: JsonNode,
      path: String,
      schemas: Map[Int, Schema],
      used: mutable.Set[(Int, String)],
      rowCount: Int,
      ordinal: Ordinal
  ): Seq[Variant] =
    configFields(clazz).filterNot(hiddenBySibling(_, baseNode.at(path))).flatMap { f =>
      val childPath = pointerOf(f, path)
      rowType(f) match {
        case Some(row) =>
          // Recurse into containers whatever their own required-ness: an optional
          // knob often sits inside a required list of rows.
          rowPaths(f, baseNode.at(childPath), childPath).flatMap { rowPath =>
            val fills = rowFills(row, baseNode, rowPath, schemas, used, rowCount, ordinal)
            if (fills.isEmpty) None
            else Some(Variant(s"${rowPath.stripPrefix("/")}=filled", fills))
          }
        case None =>
          leafFill(clazz, f, baseNode, childPath, schemas, rowCount)
            .map(fill => Variant(s"${fill._1.stripPrefix("/")}=${fill._2.asText}", Seq(fill)))
            .toSeq
      }
    }

  /** Value for the hostile variant of a knob that takes arbitrary text. Legal —
    * a user can type it into any text box — but it ends a Python string literal,
    * which is what a generator splicing it unescaped gets wrong.
    */
  private val HostileString = "a\"b"

  /** Every knob that accepts ARBITRARY TEXT, to carry [[HostileString]] — all of
    * them in one variant (see [[merged]]). This is the escaping check, and it is
    * generic on purpose: a new operator is covered the day it is verified, with
    * nothing to register.
    *
    * "Arbitrary text" excludes every string whose value is constrained, because
    * there the hostile value would be rejected before any escaping mattered: a
    * column picker, a declared enum, a CSS color, and a number-in-a-string (which
    * declares bounds). Unlike [[optionalScalarFills]] this does not care whether
    * the base pass filled the knob — a label carrying a default is spliced just the
    * same — so the variant replaces whatever value is there.
    */
  private def hostileTextFills(clazz: Class[_], baseNode: JsonNode, path: String): Seq[Variant] =
    configFields(clazz).filterNot(hiddenBySibling(_, baseNode.at(path))).flatMap { f =>
      val childPath = pointerOf(f, path)
      rowType(f) match {
        case Some(row) =>
          rowPaths(f, baseNode.at(childPath), childPath).flatMap { rowPath =>
            val fills = hostileTextFills(row, baseNode, rowPath).flatMap(_.at)
            if (fills.isEmpty) None
            else Some(Variant(s"${rowPath.stripPrefix("/")}=hostileText", fills))
          }
        case None =>
          hostileLeaf(f, childPath)
            .map(fill => Variant(s"${fill._1.stripPrefix("/")}=hostileText", Seq(fill)))
            .toSeq
      }
    }

  private def hostileLeaf(f: Field, childPath: String): Option[(String, JsonNode)] =
    if (
      hasAutofill(f) || f.getType != classOf[String] || declaredEnumValues(f).nonEmpty ||
      !patternAccepts(f, HostileString) || declaredRange(f) != Bounds(None, None)
    ) None
    else Some((childPath, objectMapper.getNodeFactory.textNode(HostileString)))

  /** Number the knobs of the hostile variant so no two carry the same text: the first
    * keeps [[HostileString]], the n-th reads `a"b2`, `a"b3`, … Every one still holds
    * the quote, so the escaping this variant exists for is unchanged.
    *
    * Needed because the knobs land in ONE variant (see [[merged]]). Where they are the
    * names of columns the operator CREATES, one shared value asks for several columns
    * of the same name and the schema rejects the config outright — the run then fails
    * on something this generator invented rather than on a divergence. Numbering also
    * says which knob a surviving value came from.
    *
    * Applied here rather than inside [[hostileTextFills]] because that walk recurses
    * into nested rows, and the count has to span the whole variant, not restart per
    * row the way [[rowFills]]'s ordinal does.
    */
  private def numbered(fills: Seq[Variant]): Seq[Variant] = {
    var n = 0
    fills.map(f =>
      Variant(
        f.label,
        f.at.map {
          case (pointer, _) =>
            n += 1
            val text = if (n == 1) HostileString else s"$HostileString$n"
            (pointer, objectMapper.getNodeFactory.textNode(text))
        }
      )
    )
  }

  /** Whether a field's declared `pattern` accepts `value` — the field's own answer to
    * "can this be typed here", so the declaration decides rather than this generator.
    * A field that declares nothing accepts anything.
    *
    * The point of asking instead of skipping every field that HAS a pattern: a pattern
    * exists to exclude what the consumer would reject, which for many fields is nothing
    * at all. Such a field still needs the escaping check — and the escaping bugs this
    * variant found were in exactly that kind of knob.
    *
    * `matches` is a full-string match, which is what the property editor applies too
    * (`Validators.pattern` wraps a string pattern in `^(?:…)$`).
    */
  private def patternAccepts(f: Field, value: String): Boolean =
    schemaKey(f, "pattern").filter(_.isTextual).map(_.asText) match {
      case Some(p) => Try(value.matches(p)).getOrElse(false)
      case None    => true
    }

  /** A running position shared by every row filled into one variant, so no two of
    * those knobs are handed the same value. One counter rather than one per row:
    * rows collide with each other as readily as knobs within a row do, and where the
    * knob is an output column NAME — Projection's `alias` — two rows carrying the
    * same one is a config the operator refuses outright.
    */
  private final class Ordinal {
    private var n = 0

    /** The position a knob would take. Advances only once one actually does, so a
      * field that yields no fill leaves no gap in the numbering.
      */
    def peek: Int = n
    def taken(): Unit = n += 1
  }

  /** Every optional knob under one nested row — a column picker as well as a scalar —
    * as pointer → value.
    *
    * The scalar knobs get DISTINCT values, ascending: a row is often a pair that has to
    * differ to mean anything — a step's start and end, where the operator drops the
    * step unless `start < end` — and one shared value would collapse it. The first
    * knob keeps the value it would have had on its own, so a lone knob is unaffected.
    *
    * The column pickers are resolved against the same `used` set as the top-level ones,
    * so a row's column differs from what the rest of the config already reads. The row
    * itself is the sibling context: whether a picker is type-constrained can depend on
    * another knob of the SAME row (an aggregation's function decides whether its column
    * must be numeric), so the rule is evaluated against the row, not the operator.
    */
  private def rowFills(
      clazz: Class[_],
      baseNode: JsonNode,
      path: String,
      schemas: Map[Int, Schema],
      used: mutable.Set[(Int, String)],
      rowCount: Int,
      ordinal: Ordinal
  ): Seq[(String, JsonNode)] =
    configFields(clazz).flatMap { f =>
      val childPath = pointerOf(f, path)
      rowType(f) match {
        case Some(row) =>
          rowPaths(f, baseNode.at(childPath), childPath)
            .flatMap(rowPath => rowFills(row, baseNode, rowPath, schemas, used, rowCount, ordinal))
        case None if hasAutofill(f) =>
          columnFill(clazz, f, baseNode, childPath, schemas, used, baseNode.at(path)).toSeq
        case None =>
          val fill = leafFill(clazz, f, baseNode, childPath, schemas, rowCount, ordinal.peek)
          if (fill.nonEmpty) ordinal.taken()
          fill.toSeq
      }
    }

  /** The fill for ONE optional column knob, or `None` when it is required (the base
    * pass filled it), already points at a column, or no column resolves.
    *
    * Shared by the top-level pass and the row pass so both obey the same rule: an
    * optional picker takes the first unused column that fits its declared type.
    *
    * `owner` is the object the field belongs to, NOT the class that declares it: a
    * knob a family shares is declared on the abstract base, which has no instance to
    * read a default off, and every such knob then read as one the config had already
    * set and was skipped.
    */
  private def columnFill(
      owner: Class[_],
      f: Field,
      baseNode: JsonNode,
      childPath: String,
      schemas: Map[Int, Schema],
      used: mutable.Set[(Int, String)],
      siblings: JsonNode
  ): Option[(String, JsonNode)] = {
    val required = Option(f.getAnnotation(classOf[JsonProperty])).exists(_.required)
    val untouched = defaultsOf(owner).path(jsonNameOf(f))
    if (required || baseNode.at(childPath) != untouched) None
    else {
      val spec = autofillSpec(f).getOrElse(AutofillSpec(port = 0, holdsList = false))
      // A list knob takes every matching column here exactly as it does in the
      // base config: `required` decides WHETHER a field is filled, not how many
      // values go in once it is. Filled with one, a list knob runs the same code
      // as a scalar — the per-element numbering and the joining between elements,
      // which is the whole of what a list does differently, is never reached.
      if (spec.holdsList)
        listColumnFill(f, schemas, spec.port, used, siblings).toOption.map(childPath -> _)
      else
        resolveColumn(f, schemas, spec.port, used, siblings).toOption
          .map(col => (childPath, objectMapper.getNodeFactory.textNode(col): JsonNode))
    }
  }

  /** Every column at `port` a list knob may hold: the ones its `attributeTypeRules`
    * admits, or all of them when the rule matches nothing (or there is no rule),
    * minus the ones a single-column knob beside it already took.
    * The same fill for a required and an optional field, so the two cannot drift.
    *
    * Subtracting `used` is what [[resolveColumn]] already does for a single-column
    * knob, and the two knobs answer to the same rule: a column means something
    * different to each field that names it, so handing one column to two of them
    * writes a config nobody would. Radar Chart's name column arrived inside its own
    * value columns that way, and sklearn's label inside the features it is fitted
    * against. Not marked used in turn, since a list knob wants every column its rule
    * admits and marking them would leave a later single-column knob nothing to take.
    */
  private def listColumnFill(
      f: Field,
      schemas: Map[Int, Schema],
      port: Int,
      used: collection.Set[(Int, String)],
      siblings: JsonNode
  ): Either[String, JsonNode] =
    columnNames(schemas, port).map { names =>
      val filtered = allowedTypes(f, siblings) match {
        case Some(types) =>
          val matching = schemas
            .get(port)
            .map(_.getAttributes.filter(a => types.contains(a.getType)).map(_.getName))
            .getOrElse(Seq.empty)
          if (matching.nonEmpty) matching else names
        case None => names
      }
      val free = filtered.filterNot(name => used.contains((port, name)))
      val arr = objectMapper.createArrayNode()
      (if (free.nonEmpty) free else filtered).foreach(arr.add)
      arr
    }

  /** A field's JSON Pointer, under the pointer of the object that holds it. */
  private def pointerOf(f: Field, path: String): String = s"$path/${jsonNameOf(f)}"

  /** The key a field carries in the config JSON. */
  private def jsonNameOf(f: Field): String =
    Option(f.getAnnotation(classOf[JsonProperty]))
      .map(_.value)
      .filter(_.nonEmpty)
      .getOrElse(f.getName)

  /** Whether the config has to carry a value for this field. Two sources say so and
    * both count: the annotation, and a schema branch the siblings have selected. A
    * field required only under a branch carries no annotation, so reading the
    * annotation alone leaves it unfilled in exactly the configuration that needs it.
    */
  private def isRequired(f: Field, scope: SchemaScope, siblings: JsonNode): Boolean =
    Option(f.getAnnotation(classOf[JsonProperty])).exists(_.required) ||
      requiredUnder(scope, siblings).contains(jsonNameOf(f))

  /** The nested-row type a field holds — its `List[Row]` / `Option[Row]` element
    * type, or its own type when the field IS the row. `None` for a scalar field.
    */
  private def rowType(f: Field): Option[Class[_]] = {
    val t = f.getType
    if (isList(t) || isOption(t)) elementType(f).toOption.filter(isNestedObject)
    else if (isNestedObject(t)) Some(t)
    else None
  }

  /** The pointer of each row present at `childPath` — one per array element, or
    * the node itself when the field holds a single row. Empty when nothing is
    * there to fill (an absent `Option`, a scalar list).
    */
  private def rowPaths(f: Field, child: JsonNode, childPath: String): Seq[String] =
    if (isList(f.getType) || isOption(f.getType))
      if (child.isArray) (0 until child.size()).map(i => s"$childPath/$i")
      else if (child.isObject) Seq(childPath)
      else Seq.empty
    else if (child.isObject) Seq(childPath)
    else Seq.empty

  /** The fill for one optional free-value scalar knob, or `None` if this field
    * isn't one (a column picker, a required field, or a knob the base pass filled).
    *
    * `ordinal` is the knob's position among the ones filled in the same row (0 for a
    * top-level knob, which has no siblings to differ from): it offsets the value so
    * the knobs of one row do not collide — see [[rowFills]].
    */
  private def leafFill(
      owner: Class[_],
      f: Field,
      baseNode: JsonNode,
      childPath: String,
      schemas: Map[Int, Schema],
      rowCount: Int,
      ordinal: Int = 0
  ): Option[(String, JsonNode)] = {
    val required = Option(f.getAnnotation(classOf[JsonProperty])).exists(_.required)
    // "Unset" means the base pass did not fill it: the key still carries the value a
    // fresh instance has (see [[defaultsOf]] — every key is present, as the UI sends
    // them, so absence alone no longer tells us anything).
    val current = baseNode.at(childPath)
    val unset = current.isMissingNode ||
      current == defaultsOf(owner).path(jsonNameOf(f))
    // A knob whose values the field DECLARES is left to its declaration: the enum
    // sweep covers a declared value list, and a knob offering an `examples` value
    // takes that one. Reading `examples` on its own, rather than only alongside a
    // `pattern`, is the point: a field can state a realistic value ("https://
    // example.com" for a URL) without having to invent a constraint to hang it on,
    // and inventing one to steer this generator would reject values the platform
    // accepts.
    // An optional knob is typed by what its Option holds, so `start`/`end` declared
    // as Option[Double] are swept like the bare numbers they are.
    val scalarType = effectiveScalarType(f)
    if (
      hasAutofill(f) || required || !unset ||
      declaredEnumValues(f).size > 1 || !isFreeScalar(scalarType)
    ) None
    else if (declaredExample(f).isDefined) declaredExample(f).map(v => (childPath, v))
    else if (scalarType == classOf[String])
      // The canonical string is "1", so the n-th knob reads as "1", "2", … — distinct
      // and ascending, so the knobs filled in one row do not collide.
      Some((childPath, objectMapper.getNodeFactory.textNode((ordinal + 1).toString)))
    else
      scalarNode(
        scalarType,
        None,
        schemas,
        mutable.Set.empty,
        NumHint(declaredRange(f), rowCount)
      ).toOption
        .map { v =>
          // Step away from the value rather than scaling it: the n-th knob lands next
          // to the first instead of at n times it, so a pair stays inside the span the
          // fixture actually holds — doubling walked `end` past the last row.
          val stepped =
            if (ordinal == 0) v
            else objectMapper.getNodeFactory.numberNode(v.asDouble() + ordinal)
          (childPath, stepped)
        }
  }

  /** The first value a field offers under `examples` — a legal sample the operator
    * states itself, so nothing here has to invent one.
    */
  private def declaredExample(f: Field): Option[JsonNode] =
    schemaKey(f, "examples").filter(_.isArray).flatMap(_.elements().asScala.toSeq.headOption)

  /** One key out of a field's own `@JsonSchemaInject` JSON. */
  private def schemaKey(f: Field, key: String): Option[JsonNode] =
    Option(f.getAnnotation(classOf[JsonSchemaInject]))
      .map(_.json)
      .filter(_.nonEmpty)
      .flatMap(js => Try(objectMapper.readTree(js)).toOption)
      .map(_.path(key))
      .filterNot(_.isMissingNode)

  /** A type whose value the user types in freely — the fills of
    * [[optionalScalarFills]]. Boolean is excluded: the enum sweep already covers
    * both of its values.
    */
  private def isFreeScalar(t: Class[_]): Boolean =
    t == classOf[String] || t == classOf[Int] || t == classOf[java.lang.Integer] ||
      t == classOf[Short] || t == classOf[Long] || t == classOf[java.lang.Long] ||
      t == classOf[Double] || t == classOf[java.lang.Double] || t == classOf[Float]

  private def deserialize(
      node: ObjectNode,
      opClass: Class[_ <: LogicalOp]
  ): Either[String, LogicalOp] =
    Try(objectMapper.treeToValue(node, opClass)).toEither.left
      .map(e => s"deserialization failed: ${e.getMessage}")

  /** One variant per non-default enum value reachable in `baseNode`. One enum
    * flipped at a time — linear, NOT the combinatorial product.
    */
  private def enumVariants(
      opClass: Class[_ <: LogicalOp],
      baseNode: ObjectNode,
      pinned: Set[String] = Set.empty,
      fill: FillContext = FillContext()
  ): Seq[Variant] =
    enumSites(opClass, baseNode, "", Map.empty, SchemaScope.of(opClass), fill)
      .filterNot(site => pinned.contains(site.pointer.stripPrefix("/")))
      .flatMap { site =>
        val baseVal = baseNode.at(site.pointer)
        site.values.filterNot(_ == baseVal).map { v =>
          Variant(
            s"${site.pointer.stripPrefix("/")}=${v.asText}",
            (site.pointer, v) +: site.companions(v)
          )
        }
      }

  /** An enum-typed position in the config JSON: its JSON Pointer plus every
    * possible JSON value (each enum constant serialized via its `@JsonValue`).
    *
    * `companions` names the edits a value has to arrive with. A hyperparameter's
    * `parameter` needs them: the `value` beside it holds something the PREVIOUS
    * parameter accepted, and a flip that left it there would ask the operator to
    * put a kernel name through `int()`.
    *
    * A choice the operator offers and no value runs is NOT dropped here: it is
    * generated, it fails, and it is withheld by name in
    * [[TransformVerificationRunner.variantsNotRun]], where a reader sees it and the row
    * goes away when the operator is fixed.
    */
  private final case class EnumSite(
      pointer: String,
      values: Seq[JsonNode],
      companions: JsonNode => Seq[(String, JsonNode)] = _ => Seq.empty
  )

  /** Collect every enum-typed leaf reachable in `node`. Walks the operator's
    * fields for type info but the ACTUAL JSON for structure, so it honours real
    * list lengths (a curated fixture may hold >1 element) and skipped optionals.
    * `path` is the JSON Pointer of the sub-node currently typed by `clazz`.
    *
    * `bindings` and `scope` are what [[buildObject]] filled the config with, and
    * are needed here for the same two reasons: a field declared as a type variable
    * reports `Object` from [[Field.getType]] and so hides the enum it really holds,
    * and a field whose values are stated in the schema rather than on itself has
    * none to sweep as far as reflection can see.
    */
  private def enumSites(
      clazz: Class[_],
      node: JsonNode,
      path: String,
      bindings: TypeBindings,
      scope: SchemaScope,
      fill: FillContext
  ): Seq[EnumSite] = {
    val bound = bindings ++ typeBindingsOf(clazz)
    val row = node.at(path)
    val sites = configFields(clazz).filterNot(hiddenBySibling(_, row)).flatMap { f =>
      if (hasAutofill(f)) Seq.empty
      else {
        val jsonName = jsonNameOf(f)
        val childPath = s"$path/$jsonName"
        val child = node.at(childPath)
        if (child.isMissingNode || child.isNull) Seq.empty
        else {
          val t = f.getType
          val declared = declaredEnumValues(f)
          val nested = scope.descend(jsonName)
          if (declared.size > 1) Seq(EnumSite(childPath, declared))
          else if (isList(t))
            elementType(f).toOption.toSeq.flatMap { elem =>
              if (child.isArray)
                (0 until child.size()).flatMap(i =>
                  enumSiteFor(elem, node, s"$childPath/$i", elementBindings(f, bound), nested, fill)
                )
              else Seq.empty
            }
          else if (isOption(t))
            elementType(f).toOption.toSeq
              .flatMap(elem => enumSiteFor(elem, node, childPath, bound, nested, fill))
          else
            ruledEnumSite(f, row, childPath, scope)
              .map(Seq(_))
              .getOrElse(enumSiteFor(boundType(f, bound), node, childPath, bound, nested, fill))
        }
      }
    }
    withCompanions(clazz, row, path, bound, scope, fill, sites)
  }

  private def enumSiteFor(
      t: Class[_],
      node: JsonNode,
      path: String,
      bindings: TypeBindings,
      scope: SchemaScope,
      fill: FillContext
  ): Seq[EnumSite] =
    if (t.isEnum) {
      val vals = t.getEnumConstants.toSeq.map(c => objectMapper.valueToTree[JsonNode](c))
      if (vals.size > 1) Seq(EnumSite(path, vals)) else Seq.empty
    } else if (t == classOf[Boolean] || t == classOf[java.lang.Boolean]) {
      // A Boolean is a 2-value "enum": sweep both true and false.
      val nf = objectMapper.getNodeFactory
      Seq(EnumSite(path, Seq(nf.booleanNode(true), nf.booleanNode(false))))
    } else if (isNestedObject(t)) enumSites(t, node, path, bindings, scope, fill)
    else Seq.empty

  /** The site a `valueRules` branch gives a field whose own type names no values:
    * the set the branch holding for `row` accepts. `None` where that branch names
    * none, a numeric hyperparameter's `value` being one value out of a range rather
    * than a choice between named ones.
    */
  private def ruledEnumSite(
      f: Field,
      row: JsonNode,
      childPath: String,
      scope: SchemaScope
  ): Option[EnumSite] =
    schemaValueRule(f, scope, row)
      .map(_.path("enum"))
      .filter(e => e.isArray && e.size() > 1)
      .map(e => EnumSite(childPath, e.elements().asScala.toSeq))

  /** Every site paired with the fields that move with it, in the two ways a schema
    * says one field's content depends on another's.
    *
    * The first is `valueRules`: what such a field may hold is decided by the sibling
    * the rule reads. The pairing is derived from the rules themselves rather than
    * named here, so an operator stating a rule over some other sibling gets the same
    * treatment. The paired field is REFILLED rather than left as it was, since what
    * sits there belongs to the PREVIOUS choice, and it is refilled the way any field
    * is: by the rule where the rule names a value, and by the field's own type where
    * it does not. A branch naming nothing is the operator saying it knows of no value
    * worth offering, which is not the same as there being none, and a choice this
    * generator cannot fill is a choice that has to fail loudly and be withheld by name
    * in [[TransformVerificationRunner.variantsNotRun]] rather than disappear here.
    *
    * The second is a conditional `required`, which is how an object says that exactly
    * one of two fields applies and therefore that neither can be marked required on
    * its own. A hyperparameter row is written that way: `value` is required while its
    * switch is off and `attribute` once it is on. The base pass filled the one the
    * base config needs, so flipping the switch has to fill the other, which until then
    * was rightly left empty.
    */
  private def withCompanions(
      clazz: Class[_],
      row: JsonNode,
      path: String,
      bindings: TypeBindings,
      scope: SchemaScope,
      fill: FillContext,
      sites: Seq[EnumSite]
  ): Seq[EnumSite] = {
    val ruled = configFields(clazz)
      .map(f => (f, scope.child(jsonNameOf(f)).path("valueRules").path("allOf")))
      .filter { case (_, branches) => branches.isArray }
    val conditional = conditionallyRequiredFields(scope)
    if ((ruled.isEmpty && conditional.isEmpty) || !row.isObject) sites
    else
      sites.map { site =>
        val sibling = site.pointer.stripPrefix(s"$path/")
        val paired = ruled.filter {
          case (_, branches) =>
            branches.elements().asScala.exists(_.path("if").has(sibling))
        }
        if (paired.isEmpty && conditional.isEmpty) site
        else
          site.copy(companions = v => {
            val hypothetical = row.deepCopy[ObjectNode]()
            hypothetical.set[JsonNode](sibling, v)
            val ruleFills = paired.map {
              case (f, _) => companionFill(f, path, hypothetical, bindings, scope, fill)
            }
            // Only what this value newly asks for and the row does not already
            // carry: a config that set the field by hand keeps what it set.
            val revealed = requiredUnder(scope, hypothetical)
              .diff(requiredUnder(scope, row))
              .flatMap(name => configFields(clazz).find(jsonNameOf(_) == name))
              .filter(f => isBlank(hypothetical.path(jsonNameOf(f))))
              .map(f => companionFill(f, path, hypothetical, bindings, scope, fill))
            ruleFills ++ revealed
          })
      }
  }

  /** One companion edit, through [[valueFor]], which reads the rule first and falls back
    * to the field's own type: the order the base pass filled it in.
    *
    * A field this generator cannot fill ENDS the run rather than quietly costing the
    * choice its variant, the auto tier already ending it when a whole config cannot be
    * built. Both are the same thing said about a smaller piece, and a generator that
    * came up empty is a gap here rather than a statement about the operator.
    */
  private def companionFill(
      f: Field,
      path: String,
      row: JsonNode,
      bindings: TypeBindings,
      scope: SchemaScope,
      fill: FillContext
  ): (String, JsonNode) =
    valueFor(f, fill.schemas, fill.used, fill.rowCount, row, bindings, scope) match {
      case Right(value) => (s"$path/${jsonNameOf(f)}", value)
      case Left(reason) =>
        throw new IllegalStateException(s"cannot fill ${jsonNameOf(f)} beside it: $reason")
    }

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
      // One past the end appends — the `+`-row fill adds a row rather than
      // replacing one.
      case (a: ArrayNode, idx) if idx.toInt == a.size() => a.add(value); ()
      case (a: ArrayNode, idx)                          => a.set(idx.toInt, value); ()
      case _                                            => ()
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
      rowCount: Int,
      bindings: TypeBindings = Map.empty,
      scope: SchemaScope = SchemaScope.empty,
      pinned: Map[String, JsonNode] = Map.empty
  ): Either[String, ObjectNode] = {
    // What `clazz` itself supplies is added to what its caller passed in: an operator
    // names the arguments for its own supertypes, a row class receives them from the
    // field that holds it.
    val bound = bindings ++ typeBindingsOf(clazz)
    // An operator carries its own schema, so it is derived here rather than at each
    // entry point: a caller that forgot would lose every rule the schema states and
    // get a config that merely looks filled. A nested class has no schema of its own
    // and uses the scope the field holding it handed down.
    val doc = if (classOf[LogicalOp].isAssignableFrom(clazz)) SchemaScope.of(clazz) else scope
    val node = defaultsOf(clazz)
    // Pins go in BEFORE the fields are decided, not after: `node` is the sibling
    // context below, so a knob pinned on decides what its dependents do. Set
    // afterwards, a pin cannot reach the field it was pinned to steer.
    pinned.foreach { case (name, value) => node.set[JsonNode](name, value) }
    configFields(clazz).foreach { f =>
      // A pinned knob keeps the value it was pinned to. Deciding it again would
      // refill it from its default and undo the pin before the fields that read it
      // are reached.
      if (!pinned.contains(jsonNameOf(f))) {
        // `node` doubles as the sibling context: a field whose rule depends on another
        // field of the same object reads it here, so declaration order decides what is
        // visible — the knob a rule branches on is declared before the column it binds.
        decide(f, schemas, used, rowCount, node, bound, doc) match {
          case Fill(name, value) => node.set[JsonNode](name, value)
          case Skip              => ()
          case Fail(reason)      => return Left(s"${clazz.getSimpleName}.${f.getName}: $reason")
        }
      }
    }
    Right(node)
  }

  /** A fresh instance's own values, as the starting JSON — what the UI submits for a
    * form nobody touched, where every key is present carrying the operator's default.
    *
    * Leaving a skipped knob's key OUT instead produces a shape the UI cannot: a
    * config object built through a `@JsonCreator` constructor then receives `null`
    * for the missing keys, overwriting the field initializers, and a generator that
    * reads them crashes on a value no user can enter (BulletChart's step bounds).
    * Empty when the class has no usable no-arg constructor.
    */
  private def defaultsOf(clazz: Class[_]): ObjectNode =
    Try(clazz.getDeclaredConstructor())
      .flatMap { ctor =>
        ctor.setAccessible(true)
        Try(objectMapper.valueToTree[JsonNode](ctor.newInstance()))
      }
      .toOption
      .collect { case o: ObjectNode => o }
      .getOrElse(objectMapper.createObjectNode())

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
      rowCount: Int,
      siblings: JsonNode = noSiblings,
      bindings: TypeBindings = Map.empty,
      scope: SchemaScope = SchemaScope.empty
  ): Decision = {
    val jp = Option(f.getAnnotation(classOf[JsonProperty]))
    val jsonName = jp.map(_.value).filter(_.nonEmpty).getOrElse(f.getName)
    val required = isRequired(f, scope, siblings)
    val autofill = hasAutofill(f)
    // An optional knob is judged by what it WRAPS: `Option[Double]` is a number the
    // user may leave blank, not a thing the base config has to carry.
    val held = effectiveScalarType(f, bindings)
    val isBoolean = held == classOf[Boolean] || held == classOf[java.lang.Boolean]

    // An OPTIONAL column-name field (`@AutofillAttributeName*` with required=false)
    // is left at its operator default rather than force-filled. These are the
    // "No Selection" grouping/pattern knobs (e.g. BarChart's categoryColumn /
    // pattern); forcing a real column into one produces a degenerate config (one
    // trace per row) that the native and generated paths disagree on.
    if (hiddenBySibling(f, siblings)) Skip
    else if (autofill && !required) Skip
    else {
      // A field declaring its values in the annotation counts as meaningful just as
      // an enum-TYPED one does: the sweep flips it from the base config, so it has
      // to BE in the base config (a `defaultValue = ""` alone would skip it). So does
      // one whose schema states a rule for it: an untyped hyperparameter `value` is
      // an optional plain string, which alone would be skipped, but the operator does
      // read it and the rule says what it should hold.
      val meaningful = required || autofill || held.isEnum || isBoolean || isList(f.getType) ||
        isNestedObject(held) || declaredEnumValues(f).size > 1 ||
        schemaValueRule(f, scope, siblings).isDefined || jp
        .map(_.defaultValue)
        .exists(_.nonEmpty)

      valueFor(f, schemas, used, rowCount, siblings, bindings, scope) match {
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
      rowCount: Int,
      siblings: JsonNode = noSiblings,
      bindings: TypeBindings = Map.empty,
      scope: SchemaScope = SchemaScope.empty
  ): Either[String, JsonNode] = {
    val ruled = schemaValueRule(f, scope, siblings).flatMap(ruleFill)
    val nested = scope.descend(jsonNameOf(f))
    autofillSpec(f) match {
      case Some(spec) if spec.holdsList =>
        listColumnFill(f, schemas, spec.port, used, siblings)
      case Some(spec) =>
        resolveColumn(f, schemas, spec.port, used, siblings)
          .map(objectMapper.getNodeFactory.textNode)
      // A rule stated in the schema wins over the type-driven fill below: it names a
      // value this field may hold given the sibling chosen beside it, which the type
      // alone — a bare `String` — cannot narrow.
      case None if ruled.isDefined => Right(ruled.get)
      case None =>
        val t = boundType(f, bindings)
        if (isList(t))
          // An OPTIONAL list starts EMPTY, the way the UI does: its `+` button adds the
          // first row, so a config nobody touched has none, and the branch an operator
          // takes for "no rows at all" is only reached this way. A REQUIRED list gets
          // one row — its operator asserts the list is non-empty, so zero is not a
          // config it can run. Either way the extra row comes from [[extraRowFills]].
          //
          // Required counts the schema's conditional form too: a list the operator
          // needs only on one branch is empty-by-annotation, and reading the
          // annotation alone hands that branch the empty list it cannot run.
          if (!isRequired(f, scope, siblings))
            Right(objectMapper.createArrayNode())
          else
            elementType(f)
              .flatMap(
                scalarOrNested(_, schemas, used, rowCount, elementBindings(f, bindings), nested)
              )
              .map { e =>
                val arr: ArrayNode = objectMapper.createArrayNode(); arr.add(e); arr
              }
        else if (isOption(t))
          // An optional scalar is filled like the bare type: the `defaultValue` and any
          // declared range sit on the field, not on the element, so a Grid Size that
          // declares 10 is still filled with 10 rather than a generic number.
          elementType(f).flatMap { elem =>
            if (isNestedObject(elem))
              scalarOrNested(elem, schemas, used, rowCount, elementBindings(f, bindings), nested)
            else
              scalarNode(elem, baseValueOf(f), schemas, used, NumHint(declaredRange(f), rowCount))
          }
        else if (declaredEnumValues(f).size > 1) Right(declaredEnumDefault(f))
        else
          scalarNode(
            t,
            baseValueOf(f),
            schemas,
            used,
            NumHint(declaredRange(f), rowCount),
            Map.empty,
            nested
          )
    }
  }

  /** The base value for a field whose values are declared in its annotation: the
    * `default` the annotation names, else its first value. Never the canonical
    * string — for such a field that is a value the operator does not accept.
    */
  private def declaredEnumDefault(f: Field): JsonNode = {
    val declared = declaredEnumValues(f)
    Option(f.getAnnotation(classOf[JsonSchemaInject]))
      .map(_.json)
      .filter(_.nonEmpty)
      .flatMap(js => Try(objectMapper.readTree(js).path("default")).toOption)
      .filterNot(_.isMissingNode)
      .filter(declared.contains)
      .getOrElse(declared.head)
  }

  /** What the base config should carry for a scalar field, before this generator
    * invents anything: the operator's own `defaultValue` if it has one, else the
    * value it offers under `examples`.
    *
    * `examples` matters most on a REQUIRED field, which [[leafFill]] never reaches —
    * a required knob with no default would otherwise take the canonical "1", and "1"
    * is not a URL, a regex or a delimiter. A field can now say what a realistic value
    * looks like without declaring a constraint it does not have.
    */
  private def baseValueOf(f: Field): Option[String] =
    defaultOf(f).orElse(declaredExample(f).filter(_.isTextual).map(_.asText))

  /** A node for a list element or Option inner type — no field-level default or
    * range annotation (those live on the field, not the element type).
    */
  private def scalarOrNested(
      clazz: Class[_],
      schemas: Map[Int, Schema],
      used: mutable.Set[(Int, String)],
      rowCount: Int,
      bindings: TypeBindings = Map.empty,
      scope: SchemaScope = SchemaScope.empty
  ): Either[String, JsonNode] =
    scalarNode(clazz, None, schemas, used, NumHint(Bounds(None, None), rowCount), bindings, scope)

  /** How to fill a numeric field: `@JsonProperty(defaultValue)` if present, else the
    * middle of a declared `[min, max]` (an opacity's 0.0–1.0 → 0.5), else twice a
    * lower bound declared on its own, else half the row count (the middle of
    * `[0, rowCount]`, e.g. Limit).
    *
    * Twice, because a field that declares `>= 30` usually also defaults to 30, so
    * filling the bound itself would just re-run the base config; `max mid` keeps a
    * `>= 0` knob off zero. Doubling can only overshoot a ceiling the field does not
    * declare, and a field with a ceiling is supposed to declare it — which is why the
    * `[min, max]` case must stay: RadarChart's and Scatterplot's opacity declare one,
    * and doubling their floor of 0 would hand them 5.
    *
    * An upper bound declared ALONE is not handled: no field does that, so there would
    * be no way to tell whether the code was right.
    */
  private final case class NumHint(bounds: Bounds, rowCount: Int)

  private final case class Bounds(min: Option[Double], max: Option[Double])

  private def numericFill(default: Option[String], hint: NumHint): Double =
    default.flatMap(s => Try(s.trim.toDouble).toOption) match {
      case Some(d) => d
      case None =>
        val mid = hint.rowCount / 2.0
        hint.bounds match {
          case Bounds(Some(mn), Some(mx)) => (mn + mx) / 2.0
          case Bounds(Some(mn), None)     => (mn * 2) max mid
          case _                          => mid
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
      hint: NumHint,
      bindings: TypeBindings = Map.empty,
      scope: SchemaScope = SchemaScope.empty
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
      buildObject(t, schemas, used, hint.rowCount, bindings, scope)
    else Left(s"unhandled type ${t.getName}")
  }

  /** The values a field declares via its own `@JsonSchemaInject(json = ...)`
    * `enum` array — a String field the UI renders as a dropdown (e.g. an ECDF's
    * cdfMode = standard / reversed / complementary). To the JVM these are plain
    * Strings, so [[enumSiteFor]]'s `isEnum` check can't see them, yet each value
    * takes a different branch in the generated code exactly as a real enum does.
    * Empty unless the annotation carries an array (TimeSeries declares
    * `"enum": "autofill"`, a UI directive rather than a value list).
    */
  private def declaredEnumValues(f: Field): Seq[JsonNode] =
    Option(f.getAnnotation(classOf[JsonSchemaInject]))
      .map(_.json)
      .filter(_.nonEmpty)
      .toSeq
      .flatMap { js =>
        Try(objectMapper.readTree(js).path("enum")).toOption.toSeq
          .filter(_.isArray)
          .flatMap(_.elements().asScala.toSeq)
      }

  /** The bounds a field declares, from either of the two places an operator states
    * them: `@JsonSchemaInject`'s `minimum`/`maximum` (an opacity's 0.0–1.0), which the
    * UI reads, and javax validation's `@DecimalMin`/`@Min` (a row height's floor of 30),
    * which the compiler's validation pass reads. Either bound may be absent.
    */
  private def declaredRange(f: Field): Bounds = {
    val schema = Option(f.getAnnotation(classOf[JsonSchemaInject]))
      .map(_.json)
      .filter(_.nonEmpty)
      .flatMap(js => Try(objectMapper.readTree(js)).toOption)
    def fromSchema(key: String): Option[Double] =
      schema.map(_.path(key)).filter(_.isNumber).map(_.asDouble())
    Bounds(
      fromSchema("minimum")
        .orElse(Option(f.getAnnotation(classOf[DecimalMin])).flatMap(a => asDouble(a.value)))
        .orElse(Option(f.getAnnotation(classOf[Min])).map(_.value.toDouble)),
      fromSchema("maximum")
    )
  }

  private def asDouble(s: String): Option[Double] = Try(s.trim.toDouble).toOption

  // ── reflection helpers ───────────────────────────────────────────────────

  /** Config fields declared on `clazz` and its superclasses up to (not
    * including) [[LogicalOp]] — i.e. the operator's own knobs, not the
    * framework's bookkeeping. A field counts if it carries `@JsonProperty` or an
    * autofill annotation.
    */
  private def configFields(clazz: Class[_]): Seq[Field] = {
    val ignored = ignoredProperties(clazz)
    val out = mutable.LinkedHashMap.empty[String, Field] // de-dup by name, keep most-derived
    var c: Class[_] = clazz
    while (c != null && c != classOf[LogicalOp] && c != classOf[Object]) {
      c.getDeclaredFields
        .filterNot(f => Modifier.isStatic(f.getModifiers))
        .filter(isConfigField)
        .filterNot(f => ignored.contains(jsonNameOf(f)))
        .foreach(f => out.getOrElseUpdate(f.getName, { f.setAccessible(true); f }))
      c = c.getSuperclass
    }
    out.values.toSeq
  }

  /** The properties an operator declares it does NOT carry, via `@JsonIgnoreProperties`.
    *
    * An operator that inherits a knob it does not read says so this way — FileScanSource
    * over `ScanSourceOpDesc`'s `limit`/`offset` — and the annotation sits on the operator
    * while the field sits on the parent, so the field alone cannot be judged. Jackson
    * drops these on the way back in, so filling one yields the config it started from:
    * the variant built from it would run a second time over the same config and report
    * the two paths agreeing about nothing.
    */
  private def ignoredProperties(clazz: Class[_]): Set[String] = {
    val names = mutable.Set.empty[String]
    var c: Class[_] = clazz
    while (c != null && c != classOf[Object]) {
      Option(c.getAnnotation(classOf[JsonIgnoreProperties])).foreach(names ++= _.value)
      c = c.getSuperclass
    }
    names.toSet
  }

  private def isConfigField(f: Field): Boolean =
    // `@JsonIgnore` is the field's own way of saying the same thing
    // [[ignoredProperties]] handles for the class: not part of the config.
    !f.isAnnotationPresent(classOf[JsonIgnore]) &&
      (f.isAnnotationPresent(classOf[JsonProperty]) || hasAutofill(f))

  private def hasAutofill(f: Field): Boolean = autofillSpec(f).isDefined

  /** How a field says "fill me with a column name from input port N", and whether
    * it holds one name or a list of them.
    *
    * Two spellings mean the same thing: the `@AutofillAttributeName` family, or
    * the `@JsonSchemaInject` that family is defined as, which `SklearnModelOpDesc.text`
    * writes out so its `hide*` keys sit in one annotation. They emit identical
    * schema keys, so reading only the annotations left such a field out of the
    * config entirely — which read as the operator having no such knob.
    */
  private def autofillSpec(f: Field): Option[AutofillSpec] =
    if (f.isAnnotationPresent(classOf[AutofillAttributeNameList]))
      Some(AutofillSpec(port = 0, holdsList = true))
    else if (f.isAnnotationPresent(classOf[AutofillAttributeNameOnPort1]))
      Some(AutofillSpec(port = 1, holdsList = false))
    else if (f.isAnnotationPresent(classOf[AutofillAttributeName]))
      Some(AutofillSpec(port = 0, holdsList = false))
    else injectedAutofill(f)

  private final case class AutofillSpec(port: Int, holdsList: Boolean)

  /** The `@JsonSchemaInject` spelling: an `autofill` string key naming one of
    * the two autofill kinds, plus an optional port. Anything else in the
    * annotation (titles, `hide*`) is ignored here.
    */
  private def injectedAutofill(f: Field): Option[AutofillSpec] =
    for {
      inject <- Option(f.getAnnotation(classOf[JsonSchemaInject]))
      kind <- inject.strings.find(_.path == CommonOpDescAnnotation.autofill).map(_.value)
      holdsList <-
        if (kind == CommonOpDescAnnotation.attributeNameList) Some(true)
        else if (kind == CommonOpDescAnnotation.attributeName) Some(false)
        else None
    } yield AutofillSpec(
      port = inject.ints
        .find(_.path == CommonOpDescAnnotation.autofillAttributeOnPort)
        .map(_.value)
        .getOrElse(0),
      holdsList = holdsList
    )

  private def defaultOf(f: Field): Option[String] =
    Option(f.getAnnotation(classOf[JsonProperty])).map(_.defaultValue).filter(_.nonEmpty)

  /** Whether the UI hides this field, given what its siblings currently hold.
    *
    * A `hide*` triple says "hide me when THAT field holds THIS value", and the UI
    * honours it, so a config that fills a hidden field is one no user can submit.
    * Filling one was harmless where nothing read it and misleading where something
    * did: sklearn's `text` was filled off the numeric projection with the
    * vectorizer off, a form the UI never shows.
    *
    * The sibling's value is read from the node being built, which starts as the
    * operator's own defaults, so the target is present whatever the declaration
    * order.
    */
  private def hiddenBySibling(f: Field, siblings: JsonNode): Boolean =
    Option(f.getAnnotation(classOf[JsonSchemaInject])).exists { inject =>
      val by = inject.strings.find(_.path == HideAnnotation.hideTarget).map(_.value)
      val expected = inject.strings.find(_.path == HideAnnotation.hideExpectedValue).map(_.value)
      val kind = inject.strings
        .find(_.path == HideAnnotation.hideType)
        .map(_.value)
        .getOrElse(HideAnnotation.Type.equals)
      (by, expected) match {
        case (Some(target), Some(want)) =>
          val actual = Option(siblings.get(target)).map(_.asText).getOrElse("")
          if (kind == HideAnnotation.Type.regex) Try(actual.matches(want)).getOrElse(false)
          else actual == want
        case _ => false
      }
    }

  private def isList(t: Class[_]): Boolean =
    classOf[scala.collection.Seq[_]].isAssignableFrom(t) ||
      classOf[java.util.List[_]].isAssignableFrom(t)

  private def isOption(t: Class[_]): Boolean = classOf[Option[_]].isAssignableFrom(t)

  /** The element class of a `List[X]` / `Option[X]` field, from its generic
    * signature.
    */
  private def elementType(f: Field): Either[String, Class[_]] =
    contentAs(f) match {
      case Some(c) => Right(c)
      case None =>
        f.getGenericType match {
          case p: ParameterizedType =>
            p.getActualTypeArguments.headOption match {
              case Some(c: Class[_])           => Right(c)
              case Some(pt: ParameterizedType) => Right(pt.getRawType.asInstanceOf[Class[_]])
              case _                           => Left(s"cannot resolve element type of ${f.getName}")
            }
          case _ => Left(s"${f.getName} has no generic element type")
        }
    }

  /** What a field holds as a scalar: an `Option`'s element type, else the field type
    * itself. Everything that reasons about a knob's type goes through this, so an
    * optional knob is treated exactly like the bare value it wraps.
    */
  private def effectiveScalarType(f: Field, bindings: TypeBindings = Map.empty): Class[_] =
    if (isOption(f.getType)) elementType(f).getOrElse(f.getType) else boundType(f, bindings)

  /** The concrete classes standing in for the type variables in scope, keyed by the
    * variable itself so that two classes declaring a `T` cannot be confused.
    *
    * Needed because a field declared as a type variable — a trainer's hyperparameter
    * row holds `var parameter: T` — reports `Object` from [[Field.getType]], which is
    * not a type anything can be filled with. The operator does name the class it means,
    * one level up in `SklearnMLOperatorDescriptor[SklearnAdvancedKNNParameters]`, and
    * these carry that down to the field.
    */
  private type TypeBindings = Map[TypeVariable[_], Class[_]]

  /** What `clazz` supplies for the variables its generic supertypes declare, walking up
    * the chain so an argument stated several levels above still arrives. An argument
    * that is itself a variable is followed through what the subclass already bound,
    * which is why the walk goes downward-first.
    */
  private def typeBindingsOf(clazz: Class[_]): TypeBindings = {
    val acc = mutable.Map.empty[TypeVariable[_], Class[_]]
    var t: Type = clazz.getGenericSuperclass
    while (t != null) t match {
      case p: ParameterizedType =>
        val raw = p.getRawType.asInstanceOf[Class[_]]
        raw.getTypeParameters.zip(p.getActualTypeArguments).foreach {
          case (declared, arg: Class[_])        => acc(declared) = arg
          case (declared, arg: TypeVariable[_]) => acc.get(arg).foreach(acc(declared) = _)
          case _                                => ()
        }
        t = raw.getGenericSuperclass
      case c: Class[_] => t = c.getGenericSuperclass
      case _           => t = null
    }
    acc.toMap
  }

  /** `f`'s type with a type variable resolved against the bindings in scope. Falls back
    * to [[Field.getType]], i.e. to `Object`, so an unresolvable variable still reaches
    * [[scalarNode]] and is reported there rather than silently mis-filled.
    */
  private def boundType(f: Field, bindings: TypeBindings): Class[_] =
    f.getGenericType match {
      case tv: TypeVariable[_] => bindings.getOrElse(tv, f.getType)
      case _                   => f.getType
    }

  /** What a `List[Row[T]]` field passes down to its row class: `Row`'s own variables
    * bound to the arguments the field names. Those arguments are usually the enclosing
    * operator's variables rather than classes, so they are resolved against the bindings
    * already in scope before being handed on.
    */
  private def elementBindings(f: Field, bindings: TypeBindings): TypeBindings =
    f.getGenericType match {
      case p: ParameterizedType =>
        p.getActualTypeArguments.headOption match {
          case Some(row: ParameterizedType) =>
            val raw = row.getRawType.asInstanceOf[Class[_]]
            raw.getTypeParameters
              .zip(row.getActualTypeArguments)
              .flatMap {
                case (declared, arg: Class[_])        => Some(declared -> arg)
                case (declared, arg: TypeVariable[_]) => bindings.get(arg).map(declared -> _)
                case _                                => None
              }
              .toMap
          case _ => Map.empty
        }
      case _ => Map.empty
    }

  /** The element class `@JsonDeserialize(contentAs = ...)` names, and the only place
    * a Scala `Option[Double]`'s element type survives: the generic signature erases
    * it to Object, which is why Jackson needs the annotation too. Checked before the
    * signature so an operator that carries it is read the way Jackson reads it.
    */
  private def contentAs(f: Field): Option[Class[_]] =
    Option(f.getAnnotation(classOf[JsonDeserialize]))
      .map(_.contentAs())
      .filterNot(c => c == classOf[java.lang.Void] || c == classOf[Void])

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
      used: mutable.Set[(Int, String)],
      siblings: JsonNode = noSiblings
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
        allowedTypes(f, siblings) match {
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
    *
    * A rule may be CONDITIONAL — an `allOf` of `if`/`then` branches naming a sibling
    * field, which is how an operator says "what this column may hold depends on that
    * knob" (an aggregation's `attribute` is numeric for sum/min/max, string for
    * concat). `siblings` is the JSON object holding `f`, against which each branch's
    * condition is tested; branches that do not apply contribute nothing, and `allOf`
    * means the ones that do all bind, so their sets intersect.
    */
  private def allowedTypes(f: Field, siblings: JsonNode): Option[Set[AttributeType]] =
    Option(f.getDeclaringClass.getAnnotation(classOf[JsonSchemaInject]))
      .map(_.json)
      .filter(_.nonEmpty)
      .flatMap(js => Try(objectMapper.readTree(js)).toOption)
      .map(_.path("attributeTypeRules").path(jsonNameOf(f)))
      .flatMap { rule =>
        val branches =
          if (rule.path("allOf").isArray) rule.path("allOf").elements().asScala.toSeq
          else Seq.empty
        val bound = typeSet(rule.path("enum")).toSeq ++ branches
          .filter(branch => conditionHolds(branch.path("if"), siblings))
          .flatMap(branch => typeSet(branch.path("then").path("enum")))
        bound.reduceOption(_ intersect _).filter(_.nonEmpty)
      }

  /** The [[AttributeType]]s an `enum` array names, or `None` if it names none. */
  private def typeSet(enumNode: JsonNode): Option[Set[AttributeType]] =
    if (!enumNode.isArray) None
    else {
      val set = enumNode.elements().asScala.flatMap(n => typeFromString(n.asText())).toSet
      if (set.nonEmpty) Some(set) else None
    }

  /** Whether every `sibling: { valEnum: [...] }` clause of a rule's `if` holds for the
    * object the field sits in. An empty condition holds vacuously; a clause naming a
    * sibling the object has not set does not.
    */
  private def conditionHolds(cond: JsonNode, siblings: JsonNode): Boolean =
    cond.isObject && cond.fields().asScala.forall { clause =>
      val permitted = clause.getValue.path("valEnum")
      permitted.isArray &&
      permitted.elements().asScala.exists(_.asText == siblings.path(clause.getKey).asText)
    }

  /** The empty object, for a caller with no sibling context: only the unconditional
    * part of a rule can bind.
    */
  private def noSiblings: JsonNode = objectMapper.getNodeFactory.objectNode()

  /** Where a field's constraints are read from when its own annotation cannot carry
    * them: the operator's finished JSON schema, and the node within it describing the
    * object currently being built.
    *
    * An operator implementing `JsonSchemaCustomizer` writes rules into that document
    * after the annotations have been read. A hyperparameter row's `value` is stated only
    * there, because what it may hold depends on the `parameter` chosen beside it and so
    * cannot be annotated on a field every parameter shares. Reflection alone does not
    * see those, which is why the document travels alongside the walk.
    */
  private final case class SchemaScope(root: JsonNode, node: JsonNode) {

    /** The node describing one field of this object. */
    def child(jsonName: String): JsonNode = node.path("properties").path(jsonName)

    /** The scope a nested object or list element is built under, following the `$ref`
      * Jackson emits in place of a class it has already defined.
      */
    def descend(jsonName: String): SchemaScope = {
      val field = child(jsonName)
      val target = if (field.path("items").isObject) field.path("items") else field
      val ref = target.path("$ref").asText("")
      SchemaScope(
        root,
        if (ref.isEmpty) target
        else root.path("definitions").path(ref.stripPrefix("#/definitions/"))
      )
    }
  }

  private object SchemaScope {
    val empty: SchemaScope = {
      val nothing = objectMapper.getNodeFactory.objectNode()
      SchemaScope(nothing, nothing)
    }

    /** An operator's finished schema, or [[empty]] where one cannot be produced — such a
      * class is then read from its annotations alone, as every operator was before.
      */
    def of(clazz: Class[_]): SchemaScope =
      Try(
        OperatorMetadataGenerator
          .generateOperatorJsonSchema(clazz.asInstanceOf[Class[_ <: LogicalOp]])
      ).toOption.map(s => SchemaScope(s, s)).getOrElse(empty)
  }

  /** What filling a field needs beyond the field itself: the input schemas a column
    * picker resolves against, the columns already spoken for, and the row count a
    * range-less number is sized from. Carried into the enum walk so that a value which
    * makes a field apply can fill it the way the base pass would have.
    *
    * Empty for a caller sweeping an already-configured operator: such a config states
    * both sides of a conditional itself, so nothing there is left to fill.
    */
  private final case class FillContext(
      schemas: Map[Int, Schema] = Map.empty,
      used: mutable.Set[(Int, String)] = mutable.Set.empty,
      rowCount: Int = DefaultRowCount
  )

  /** Whether a config holds nothing at this key: absent, null, the empty string a
    * field initialised to `""` starts as, or the empty list a `List()` field starts
    * as. All four mean the same thing to an operator reading it, so a conditional
    * `required` is unmet by any of them.
    */
  private def isBlank(node: JsonNode): Boolean =
    node.isMissingNode || node.isNull || (node.isTextual && node.asText.isEmpty) ||
      (node.isArray && node.isEmpty)

  /** The fields an object's schema requires only under some condition, named by every
    * `required` its `allOf` states in either branch. Empty for an object whose
    * requirements are all unconditional, which is every one but a hyperparameter row.
    */
  private def conditionallyRequiredFields(scope: SchemaScope): Set[String] =
    scope.node
      .path("allOf")
      .elements()
      .asScala
      .flatMap(branch => Seq(branch.path("then"), branch.path("else")))
      .flatMap(_.path("required").elements().asScala)
      .map(_.asText)
      .toSet

  /** The fields an object's conditional `allOf` requires of `row` as it stands. The
    * condition is JSON Schema's own `properties`/`const`, not the `valEnum` form a
    * Texera rule uses, because this one is read by the validator rather than by the
    * form.
    */
  private def requiredUnder(scope: SchemaScope, row: JsonNode): Set[String] =
    scope.node
      .path("allOf")
      .elements()
      .asScala
      .flatMap { branch =>
        val holds = branch.path("if").path("properties").fields().asScala.forall { clause =>
          clause.getValue.path("const") == row.path(clause.getKey)
        }
        val outcome = if (holds) branch.path("then") else branch.path("else")
        outcome.path("required").elements().asScala.map(_.asText)
      }
      .toSet

  /** What a field's `valueRules` call for, given the object it sits in: the one branch
    * whose condition holds. `None` for a field declaring no such rule, which is every
    * field but a trainer's hyperparameter `value`.
    *
    * One branch at most: each names a single parameter, so unlike `attributeTypeRules`
    * there is nothing to intersect.
    */
  private def schemaValueRule(
      f: Field,
      scope: SchemaScope,
      siblings: JsonNode
  ): Option[JsonNode] = {
    val branches = scope.child(jsonNameOf(f)).path("valueRules").path("allOf")
    if (!branches.isArray) None
    else
      branches
        .elements()
        .asScala
        .find(branch => conditionHolds(branch.path("if"), siblings))
        .map(_.path("then"))
  }

  /** The value a `valueRules` branch calls for: the example it offers, else the head of
    * its accepted set, which the branch states default-first. Both arrive as text and
    * the field they fill is a `String` — the branch's `type` says how the OPERATOR will
    * convert that text, not how the config carries it.
    */
  private def ruleFill(rule: JsonNode): Option[JsonNode] =
    rule
      .path("examples")
      .elements()
      .asScala
      .toSeq
      .headOption
      .orElse(rule.path("enum").elements().asScala.toSeq.headOption)

  private def typeFromString(s: String): Option[AttributeType] =
    AttributeType.values().find(_.name.equalsIgnoreCase(s))
}
