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
import org.apache.texera.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameList,
  AutofillAttributeNameOnPort1,
  SampleColumn
}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.lang.reflect.{Field, Modifier, ParameterizedType}
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
      rowCount: Int = DefaultRowCount
  ): Either[String, Seq[(String, LogicalOp)]] =
    typeNameByClass.get(opClass) match {
      case None => Left(s"${opClass.getSimpleName} not registered in LogicalOp @JsonSubTypes")
      case Some(typeName) =>
        val used = mutable.Set.empty[(Int, String)]
        buildObject(opClass, inputSchemas, used, rowCount).flatMap { baseNode =>
          baseNode.put("operatorType", typeName)
          applyAll(
            opClass,
            baseNode,
            None,
            allVariants(opClass, baseNode, inputSchemas, used, rowCount)
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
      sweepEnums: Boolean = true
  ): Seq[Variant] =
    Variant.Base +: ((if (sweepEnums) enumVariants(opClass, baseNode) else Seq.empty) ++
      extraVariants(opClass, baseNode, schemas, used, rowCount))

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
      rowCount: Int
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
            extraRowFills(opClass, baseNode, schemas, taken, rowCount, ordinal)
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
          val port = if (f.isAnnotationPresent(classOf[AutofillAttributeNameOnPort1])) 1 else 0
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
    configFields(clazz).filter(hasAutofill).flatMap { f =>
      columnFill(f, baseNode, pointerOf(f, ""), schemas, used, baseNode).map {
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
  ): Seq[Variant] =
    configFields(clazz).flatMap { f =>
      val childPath = pointerOf(f, "")
      val rows = baseNode.at(childPath)
      for {
        row <- if (isList(f.getType)) elementType(f).toOption.filter(isNestedObject) else None
        if rows.isArray
        next <- buildObject(row, schemas, used, rowCount).toOption
      } yield {
        // Fill the new row's own optional knobs too — the `optionals` variant is
        // computed against the BASE config, where this row does not exist yet, so
        // otherwise the row arrives with every free-value knob at its default, a step
        // whose bounds are both empty is dropped by the operator, and an optional column
        // picker (which [[decide]] skips) stays null.
        rowFills(row, next, "", schemas, used, rowCount, ordinal).foreach {
          case (pointer, value) => setAtPointer(next, pointer, value)
        }
        Variant(childPath, Seq((s"$childPath/${rows.size()}", next)))
      }
    }

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
    * "Unset" is read off `baseNode` rather than re-derived, so a knob the base
    * pass DID fill — one carrying a `defaultValue` or a declared enum — is left
    * alone.
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
    configFields(clazz).flatMap { f =>
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
          leafFill(f, baseNode, childPath, schemas, rowCount)
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
    configFields(clazz).flatMap { f =>
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
          columnFill(f, baseNode, childPath, schemas, used, baseNode.at(path)).toSeq
        case None =>
          val fill = leafFill(f, baseNode, childPath, schemas, rowCount, ordinal.peek)
          if (fill.nonEmpty) ordinal.taken()
          fill.toSeq
      }
    }

  /** The fill for ONE optional column knob, or `None` when it is required (the base
    * pass filled it), already points at a column, or no column resolves.
    *
    * Shared by the top-level pass and the row pass so both obey the same rule: an
    * optional picker takes the first unused column that fits its declared type.
    */
  private def columnFill(
      f: Field,
      baseNode: JsonNode,
      childPath: String,
      schemas: Map[Int, Schema],
      used: mutable.Set[(Int, String)],
      siblings: JsonNode
  ): Option[(String, JsonNode)] = {
    val required = Option(f.getAnnotation(classOf[JsonProperty])).exists(_.required)
    val untouched = defaultsOf(f.getDeclaringClass).path(jsonNameOf(f))
    if (required || baseNode.at(childPath) != untouched) None
    else {
      val port = if (f.isAnnotationPresent(classOf[AutofillAttributeNameOnPort1])) 1 else 0
      resolveColumn(f, schemas, port, used, siblings).toOption.map { col =>
        val value: JsonNode =
          if (f.isAnnotationPresent(classOf[AutofillAttributeNameList]))
            objectMapper.createArrayNode().add(col)
          else objectMapper.getNodeFactory.textNode(col)
        (childPath, value)
      }
    }
  }

  /** A field's JSON Pointer, under the pointer of the object that holds it. */
  private def pointerOf(f: Field, path: String): String = s"$path/${jsonNameOf(f)}"

  /** The key a field carries in the config JSON. */
  private def jsonNameOf(f: Field): String =
    Option(f.getAnnotation(classOf[JsonProperty]))
      .map(_.value)
      .filter(_.nonEmpty)
      .getOrElse(f.getName)

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
      current == defaultsOf(f.getDeclaringClass).path(jsonNameOf(f))
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
      baseNode: ObjectNode
  ): Seq[Variant] =
    enumSites(opClass, baseNode, "").flatMap { site =>
      val baseVal = baseNode.at(site.pointer)
      site.values.filterNot(_ == baseVal).map { v =>
        Variant(s"${site.pointer.stripPrefix("/")}=${v.asText}", Seq((site.pointer, v)))
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
          val declared = declaredEnumValues(f)
          if (declared.size > 1) Seq(EnumSite(childPath, declared))
          else if (isList(t))
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
      rowCount: Int
  ): Either[String, ObjectNode] = {
    val node = defaultsOf(clazz)
    configFields(clazz).foreach { f =>
      // `node` doubles as the sibling context: a field whose rule depends on another
      // field of the same object reads it here, so declaration order decides what is
      // visible — the knob a rule branches on is declared before the column it binds.
      decide(f, schemas, used, rowCount, node) match {
        case Fill(name, value) => node.set[JsonNode](name, value)
        case Skip              => ()
        case Fail(reason)      => return Left(s"${clazz.getSimpleName}.${f.getName}: $reason")
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
      siblings: JsonNode = noSiblings
  ): Decision = {
    val jp = Option(f.getAnnotation(classOf[JsonProperty]))
    val jsonName = jp.map(_.value).filter(_.nonEmpty).getOrElse(f.getName)
    val required = jp.exists(_.required)
    val autofill = hasAutofill(f)
    // An optional knob is judged by what it WRAPS: `Option[Double]` is a number the
    // user may leave blank, not a thing the base config has to carry.
    val held = effectiveScalarType(f)
    val isBoolean = held == classOf[Boolean] || held == classOf[java.lang.Boolean]

    // An OPTIONAL column-name field (`@AutofillAttributeName*` with required=false)
    // is left at its operator default rather than force-filled. These are the
    // "No Selection" grouping/pattern knobs (e.g. BarChart's categoryColumn /
    // pattern); forcing a real column into one produces a degenerate config (one
    // trace per row) that the native and generated paths disagree on.
    if (autofill && !required) Skip
    else {
      // A field declaring its values in the annotation counts as meaningful just as
      // an enum-TYPED one does: the sweep flips it from the base config, so it has
      // to BE in the base config (a `defaultValue = ""` alone would skip it).
      val meaningful = required || autofill || held.isEnum || isBoolean || isList(f.getType) ||
        isNestedObject(held) || declaredEnumValues(f).size > 1 || jp
        .map(_.defaultValue)
        .exists(_.nonEmpty)

      valueFor(f, schemas, used, rowCount, siblings) match {
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
      siblings: JsonNode = noSiblings
  ): Either[String, JsonNode] = {
    if (f.isAnnotationPresent(classOf[AutofillAttributeNameList]))
      columnNames(schemas, 0).map { names =>
        // Honor the field's attributeTypeRules (same production metadata scalar
        // autofill fields use) so a numeric-only list doesn't pick up string
        // columns; fall back to all columns if no column matches the rule.
        val filtered = allowedTypes(f, siblings) match {
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
      resolveColumn(f, schemas, 1, used, siblings).map(objectMapper.getNodeFactory.textNode)
    else if (f.isAnnotationPresent(classOf[AutofillAttributeName]))
      resolveColumn(f, schemas, 0, used, siblings).map(objectMapper.getNodeFactory.textNode)
    else {
      val t = f.getType
      if (isList(t))
        // An OPTIONAL list starts EMPTY, the way the UI does: its `+` button adds the
        // first row, so a config nobody touched has none, and the branch an operator
        // takes for "no rows at all" is only reached this way. A REQUIRED list gets
        // one row — its operator asserts the list is non-empty, so zero is not a
        // config it can run. Either way the extra row comes from [[extraRowFills]].
        if (!Option(f.getAnnotation(classOf[JsonProperty])).exists(_.required))
          Right(objectMapper.createArrayNode())
        else
          elementType(f).flatMap(scalarOrNested(_, schemas, used, rowCount)).map { e =>
            val arr: ArrayNode = objectMapper.createArrayNode(); arr.add(e); arr
          }
      else if (isOption(t))
        // An optional scalar is filled like the bare type: the `defaultValue` and any
        // declared range sit on the field, not on the element, so a Grid Size that
        // declares 10 is still filled with 10 rather than a generic number.
        elementType(f).flatMap { elem =>
          if (isNestedObject(elem)) scalarOrNested(elem, schemas, used, rowCount)
          else scalarNode(elem, baseValueOf(f), schemas, used, NumHint(declaredRange(f), rowCount))
        }
      else if (declaredEnumValues(f).size > 1) Right(declaredEnumDefault(f))
      else scalarNode(t, baseValueOf(f), schemas, used, NumHint(declaredRange(f), rowCount))
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
      rowCount: Int
  ): Either[String, JsonNode] =
    scalarNode(clazz, None, schemas, used, NumHint(Bounds(None, None), rowCount))

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
      (f.isAnnotationPresent(classOf[JsonProperty]) ||
        f.isAnnotationPresent(classOf[AutofillAttributeName]) ||
        f.isAnnotationPresent(classOf[AutofillAttributeNameOnPort1]) ||
        f.isAnnotationPresent(classOf[AutofillAttributeNameList]))

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
  private def effectiveScalarType(f: Field): Class[_] =
    if (isOption(f.getType)) elementType(f).getOrElse(f.getType) else f.getType

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

  private def typeFromString(s: String): Option[AttributeType] =
    AttributeType.values().find(_.name.equalsIgnoreCase(s))
}
