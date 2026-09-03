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
