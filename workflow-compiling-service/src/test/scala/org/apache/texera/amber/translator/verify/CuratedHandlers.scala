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

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.distinct.DistinctOpDesc
import org.apache.texera.amber.operator.aggregate.{
  AggregateOpDesc,
  AggregationFunction,
  AggregationOperation
}
import org.apache.texera.amber.operator.filter.{
  ComparisonType,
  FilterPredicate,
  SpecializedFilterOpDesc
}
import org.apache.texera.amber.operator.hashJoin.{HashJoinOpDesc, JoinType}
import org.apache.texera.amber.operator.keywordSearch.KeywordSearchOpDesc
import org.apache.texera.amber.operator.projection.{AttributeUnit, ProjectionOpDesc}
import org.apache.texera.amber.operator.regex.RegexOpDesc
import org.apache.texera.amber.operator.sleep.SleepOpDesc
import org.apache.texera.amber.operator.typecasting.{TypeCastingOpDesc, TypeCastingUnit}
import org.apache.texera.amber.operator.visualization.ImageViz.ImageVisualizerOpDesc

import org.apache.texera.amber.operator.visualization.dumbbellPlot.{
  DumbbellDotConfig,
  DumbbellPlotOpDesc
}
import org.apache.texera.amber.operator.visualization.filledAreaPlot.FilledAreaPlotOpDesc
import org.apache.texera.amber.operator.machineLearning.Scorer.classificationMetricsFnc
import org.apache.texera.amber.operator.machineLearning.Scorer.regressionMetricsFnc
import org.apache.texera.amber.operator.machineLearning.Scorer.MachineLearningScorerOpDesc
import org.apache.texera.amber.operator.sklearn.training.SklearnTrainingOpDesc
import org.apache.texera.amber.operator.sklearn.training.SklearnTrainingGaussianNaiveBayesOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnClassifierOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnGaussianNaiveBayesOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnLinearRegressionOpDesc
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base.SklearnMLOperatorDescriptor
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base.{
  HyperParameters,
  ParamClass
}
import org.apache.texera.amber.operator.ifStatement.IfOpDesc
import org.apache.texera.amber.operator.huggingFace.HuggingFaceSpamSMSDetectionOpDesc
import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.file.{Files, Path}
import java.util
import scala.jdk.CollectionConverters._

/**
  * A curated handler ships a configured OpDesc and the input fixtures it
  * needs, written once into `testRoot`. Register it in [[CuratedHandlers.all]]
  * to override the auto-config tier for that operator.
  */
trait TransformHandler {
  def opDescClass: Class[_ <: LogicalOp]
  def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path])

  /** Extra independent scenarios beyond [[fixture]], each a self-contained
    * (label, configured op, its own inputs). The runner runs each as a PINNED
    * config (no enum sweep), in its own work subdir. Default: none.
    *
    * Used where one operator needs structurally different inputs per config
    * branch that a single swept fixture can't cover — e.g. the sklearn
    * `countVectorizer=true` text path, whose feature column must be text and so
    * is incompatible with the numeric default fixture (`X = table.drop(target)`
    * would feed a string column to a numeric estimator). Each scenario must
    * write its input files somewhere unique (e.g. a `testRoot` subdir) so it
    * does not clobber the primary fixture's files.
    */
  def extraScenarios(testRoot: Path): Seq[(String, LogicalOp, Map[PortIdentity, Path])] =
    Seq.empty

  /** Variant kinds this fixture cannot take — `"optionals"`, `"hostileText"`. Default:
    * none, and that is the answer for almost every fixture.
    *
    * The one case it exists for: a knob whose legal values are decided by a SIBLING
    * field, which the knob itself has no way to declare. A filter predicate's `value`
    * is parsed as the type of the column its `attribute` names, so what may be typed
    * there is a property of the pair, not of `value`.
    *
    * It lives on the handler rather than in a table beside the runner because it
    * describes THIS fixture, not the operator: a predicate over a string column takes
    * the hostile value fine. Whoever rewrites the fixture sees it and can drop it.
    */
  def unfillableVariants: Set[String] = Set.empty

  /** Opts this fixture into the `nulls` case, naming the columns it must never
    * empty because their VALUE is what the fixture was built to arrange rather
    * than data under test: a join key that has to pair, a grouping key that has
    * to group. Emptying one of those changes what the test asks instead of asking
    * what the operator does with a null.
    *
    * The default is `Some(Set.empty)`: most curated tables arrange nothing that a
    * hole would disturb, so taking part is the normal case and a fixture that
    * cannot afford a hole says so. `None` sits the case out entirely, for a table
    * whose every column is load-bearing.
    */
  def nullsKeepFilled: Option[Set[String]] = Some(Set.empty)
}

/**
  * The curated override tier of the config/fixture resolution chain: an
  * operator listed here is verified with its hand-written fixture instead of
  * the auto-generated one. This is also the seam where Xuan's curated
  * operator-field-values JSON plugs in later, as a second curated source.
  */
object CuratedHandlers {

  /** Concrete `LogicalOp` classes discovered from the `@JsonSubTypes` registry
    * on [[LogicalOp]] — the same source [[ConfigGenerator]] enumerates. The
    * sklearn handler families below are auto-derived from this list, so a newly
    * registered sklearn estimator is picked up with zero per-operator
    * boilerplate here.
    */
  private val registeredOps: Seq[Class[_ <: LogicalOp]] =
    Option(classOf[LogicalOp].getAnnotation(classOf[com.fasterxml.jackson.annotation.JsonSubTypes]))
      .map(_.value().toSeq.map(_.value().asInstanceOf[Class[_ <: LogicalOp]]))
      .getOrElse(Seq.empty)

  private def isConcrete(cls: Class[_]): Boolean =
    !java.lang.reflect.Modifier.isAbstract(cls.getModifiers)

  /** Auto-discovery factory for the [[SklearnAdvancedTrainerTransformHandler]]
    * family: every concrete `SklearnMLOperatorDescriptor` subclass.
    */
  private def advancedTrainerHandler(
      cls: Class[_ <: LogicalOp]
  ): SklearnAdvancedTrainerTransformHandler =
    new SklearnAdvancedTrainerTransformHandler {
      override val opDescClass: Class[_ <: LogicalOp] = cls
      override protected def newDesc(): SklearnMLOperatorDescriptor[_] =
        cls.getDeclaredConstructor().newInstance().asInstanceOf[SklearnMLOperatorDescriptor[_]]
    }

  /** The concrete leaf ops under one sklearn base, excluding the base itself.
    *
    * No hard-coded baseline: a new sklearn operator is picked up automatically
    * the moment it is registered in LogicalOp's @JsonSubTypes — zero per-op code
    * here. The test suite (ConfigCoverageSpec / TransformVerificationRunnerSpec)
    * is the safety net: a mis-discovered or misbehaving op fails its own parity
    * check rather than being frozen by an assertion.
    */
  private def sklearnFamily(base: Class[_]): Seq[Class[_ <: LogicalOp]] =
    registeredOps.filter(c => base.isAssignableFrom(c) && c != base && isConcrete(c))

  private def trainingOps = sklearnFamily(classOf[SklearnTrainingOpDesc])
  private def classifierOps = sklearnFamily(classOf[SklearnClassifierOpDesc])
  private def advancedOps = sklearnFamily(classOf[SklearnMLOperatorDescriptor[_]])

  /** Every sklearn op, whichever tier serves it. `X = table.drop(target)` feeds
    * each remaining column to `fit`, so these take the numeric table rather than
    * canonical, whose string columns end the fit.
    *
    * Linear Regression is named on its own because it descends from
    * `PythonOperatorDescriptor` directly rather than from one of the three
    * bases, so no family picks it up.
    */
  val sklearnNumericClasses: Set[Class[_ <: LogicalOp]] =
    (trainingOps ++ classifierOps ++ advancedOps).toSet + classOf[SklearnLinearRegressionOpDesc]

  /** The sklearn family still served by a curated handler: an advanced trainer
    * holds a `paraList` of `HyperParameters[T]`, whose `T` is the operator's own
    * parameter enum — erased, and resolvable only from the descriptor's generic
    * superclass, which [[ConfigGenerator]] cannot do. The training and
    * classifier families need no handler: they take the numeric table through
    * [[TransformVerificationRunner.fixtureFor]] and the text branch through an
    * [[TransformVerificationRunner.AltScenario]].
    */
  private def sklearnAutoHandlers: Seq[TransformHandler] =
    advancedOps.map(advancedTrainerHandler)

  /** Op classes served by the auto-discovered sklearn tier. Exposed so
    * [[TransformVerificationRunner.disposition]] can label them `ml-auto`
    * (a systematic shared-fixture + predict-compare category) instead of
    * `curated`, which is then reserved for genuine one-off fixtures.
    */
  val sklearnAutoClasses: Set[Class[_ <: LogicalOp]] =
    sklearnAutoHandlers.map(_.opDescClass).toSet

  /** The sklearn families first, the hand-written handlers after, because
    * [[byClass]] keeps the LAST entry for a class: a handler written by hand wins
    * over the one its family derives, the same way a hand-written handler is simply
    * the only one anywhere else. Registering one is then a single edit, with no
    * second step to remember and nothing that fails silently when it is forgotten.
    */
  val all: Seq[TransformHandler] = sklearnAutoHandlers ++ Seq(
    AggregateTransformHandler,
    SpecializedFilterTransformHandler,
    DistinctTransformHandler,
    ProjectionTransformHandler,
    HashJoinTransformHandler,
    TypeCastingTransformHandler,
    SleepTransformHandler,
    KeywordSearchTransformHandler,
    DumbbellPlotVisualizationHandler,
    ImageVisualizerVisualizationHandler,
    FilledAreaPlotVisualizationHandler,
    IfTransformHandler,
    MachineLearningScorerTransformHandler,
    HuggingFaceSpamSMSDetectionTransformHandler,
    RegexTransformHandler
  )

  val byClass: Map[Class[_ <: LogicalOp], TransformHandler] =
    all.map(h => h.opDescClass -> h).toMap

  /** Generic fixture writer: builds a JSONL file with the given typed columns
    * and rows, boxing each value per its declared [[AttributeType]]. Lets a
    * curated handler declare bespoke per-operator input data in one call
    * instead of hand-rolling a Schema + Tuple.builder loop.
    */
  def writeFixture(
      path: Path,
      columns: Seq[(String, AttributeType)],
      rows: Seq[Seq[Any]]
  ): Path = {
    val schema = new Schema(columns.map { case (n, t) => new Attribute(n, t) }: _*)
    val tuples = rows.map { row =>
      val builder = Tuple.builder(schema)
      columns.zip(row).foreach {
        case ((name, attrType), value) =>
          val boxed: AnyRef = (attrType, value) match {
            case (_, null)                           => null
            case (AttributeType.INTEGER, x: Int)     => Int.box(x)
            case (AttributeType.INTEGER, x: Long)    => Int.box(x.toInt)
            case (AttributeType.INTEGER, x: Double)  => Int.box(x.toInt)
            case (AttributeType.LONG, x: Long)       => Long.box(x)
            case (AttributeType.LONG, x: Int)        => Long.box(x.toLong)
            case (AttributeType.DOUBLE, x: Double)   => Double.box(x)
            case (AttributeType.DOUBLE, x: Int)      => Double.box(x.toDouble)
            case (AttributeType.DOUBLE, x: Long)     => Double.box(x.toDouble)
            case (AttributeType.BOOLEAN, x: Boolean) => Boolean.box(x)
            case (AttributeType.STRING, x)           => x.toString
            case (_, x)                              => x.toString
          }
          builder.add(schema.getAttribute(name), boxed)
      }
      builder.build()
    }
    TupleIO.writeTuples(path, tuples.iterator, schema)
    path
  }

  /** Two-input balanced binary-classification fixture (train on port 0, test on
    * port 1) from the shared [[SklearnFixture]] resource. Both ports get the
    * same rows.
    */
  def writeClassification2Input(testRoot: Path): (Path, Path) = {
    val inputs = SklearnFixture.write(testRoot, inputPortCount = 2, withGaps = false)
    (inputs(PortIdentity(0)), inputs(PortIdentity(1)))
  }

  /** Estimators whose `fit` rejects the sparse matrix `CountVectorizer` emits
    * (dense-only), so the `countVectorizer=true` text scenario doesn't apply.
    * `GaussianNB` validates X without `accept_sparse` and raises "Sparse data
    * was passed for X, but dense data is required"; text classification uses the
    * Multinomial/Bernoulli/Complement NB variants instead. `countVectorizer=true`
    * on these is an invalid production config, not a translation gap, so the
    * scenario is skipped rather than flagged.
    */
  private val denseOnlyForCountVectorizer: Set[Class[_ <: LogicalOp]] =
    Set(
      classOf[SklearnGaussianNaiveBayesOpDesc],
      classOf[SklearnTrainingGaussianNaiveBayesOpDesc]
    )

  def supportsCountVectorizer(opClass: Class[_ <: LogicalOp]): Boolean =
    !denseOnlyForCountVectorizer.contains(opClass)

  /** One real hyperparameter for an advanced sklearn trainer, so the parameter-
    * handling logic is actually exercised (vs. an empty sweep). Resolves the
    * operator's parameter enum (its `SklearnMLOperatorDescriptor[T]` type
    * argument), picks the first numeric hyperparameter, and gives it a fixed
    * value — e.g. KNN `n_neighbors = 3`, SVC/SVR `C = 1.0`.
    */
  def sampleHyperParameter(opClass: Class[_ <: LogicalOp]): HyperParameters[ParamClass] = {
    val consts = allHyperParameters(opClass)
    val chosen =
      consts.find(c => Set("int", "float", "double").contains(c.getType)).getOrElse(consts.head)
    val hp = new HyperParameters[ParamClass]()
    hp.parameter = chosen
    hp.parametersSource = false
    hp.value = sampleValueFor(chosen)
    hp.attribute = "param_val" // column read when the sweep flips parametersSource=true
    hp
  }

  /** Hyperparameters whose declared type does not match what sklearn accepts, so
    * no value can make them run. Both are KNN's and both are on main.
    *
    * `metric` is declared `int` while KNeighborsClassifier wants a string from a
    * fixed set, so the emitted `int(...)` hands it the wrong type. `metric_params`
    * wants a dict, and the emitted `str(...)` can only ever produce a string, so
    * no text the user types can reach it as one. Choosing either in the UI fails
    * the run outright, on both paths.
    *
    * Excluded so the operator's other hyperparameters stay covered rather than
    * the whole operator going dark. Fixing the enum is an operator change, not a
    * translation one.
    */
  val miswiredHyperParameters: Set[String] = Set("metric", "metric_params")

  /** Every hyperparameter this operator offers, in declaration order. */
  def allHyperParameters(opClass: Class[_ <: LogicalOp]): Seq[ParamClass] =
    resolveParamEnum(opClass).getEnumConstants.map(_.asInstanceOf[ParamClass]).toSeq

  /** A value the emitted Python can convert for this hyperparameter's type. The
    * type strings come from the enum itself, and the fourth kind is a lambda
    * comparing against "true", which is how the family spells a boolean.
    */
  def sampleValueFor(p: ParamClass): String =
    p.getType match {
      case "int"              => "3"
      case "float" | "double" => "1.0"
      case "str"              => sampleStringValueFor(p.getName)
      case _                  => "true" // the lambda form, e.g. probability
    }

  /** A legal value for a string-valued hyperparameter. sklearn takes these from
    * a fixed set per parameter, so a made-up string would be rejected by `fit`
    * rather than exercising the conversion.
    */
  private def sampleStringValueFor(name: String): String =
    name match {
      case "kernel"    => "rbf"
      case "weights"   => "uniform"
      case "algorithm" => "auto"
      case "metric"    => "minkowski"
      case "gamma"     => "scale"
      // sklearn takes a dict here, and the emitted code wraps the value in
      // `str(...)`, so there is no text that reaches it as one. Passing None is
      // what leaving it unset means.
      case "metric_params" => "None"
      case other =>
        throw new IllegalStateException(
          s"no sample value known for string hyperparameter '$other' — add one here"
        )
    }

  /** The concrete enum bound to `T` in an operator's
    * `SklearnMLOperatorDescriptor[T]` supertype (e.g. `SklearnAdvancedKNNParameters`).
    */
  private def resolveParamEnum(opClass: Class[_]): Class[_] = {
    var t: java.lang.reflect.Type = opClass.getGenericSuperclass
    while (t != null) t match {
      case pt: java.lang.reflect.ParameterizedType =>
        val raw = pt.getRawType.asInstanceOf[Class[_]]
        if (raw == classOf[SklearnMLOperatorDescriptor[_]])
          return pt.getActualTypeArguments()(0).asInstanceOf[Class[_]]
        t = raw.getGenericSuperclass
      case c: Class[_] => t = c.getGenericSuperclass
      case _           => t = null
    }
    throw new IllegalStateException(s"cannot resolve param enum for ${opClass.getName}")
  }
}

/**
  * The shared numeric dataset for the Sklearn operator families, promoted to a
  * checked-in JSON resource (mirrors [[CanonicalFixture]]) so the table is
  * human-readable and lives in one place instead of duplicated inline across
  * handlers. A small, well-separated 2-feature binary-classification table:
  * 6 rows per class — enough members for cv=5 estimators (LogisticRegressionCV,
  * probability calibration). Numeric-only because sklearn cannot fit the
  * canonical auto-fixture's string columns.
  *
  * Source of truth: src/test/resources/verify/sklearn_fixture.json. `schema`
  * below stays authoritative for column types (JSON has no typed columns).
  */
object SklearnFixture extends SharedFixture {

  val schema: Schema = new Schema(
    new Attribute("x1", AttributeType.DOUBLE),
    new Attribute("x2", AttributeType.DOUBLE),
    new Attribute("y", AttributeType.INTEGER)
  )

  /** Both ports get the whole table: the classifier operators train on port 0 and
    * test on port 1, and the point of the pair is the two ports, not two datasets.
    */
  override def rowsFor(port: Int): Seq[Tuple] = rows

  /** `y` keeps every value: it is the label the estimator fits against, so a hole
    * in it asks what sklearn does with an unlabelled row rather than what the
    * operator does with a missing feature.
    */
  override val keepFilled: Set[String] = Set("y")

  private val fixtureResource = "/verify/sklearn_fixture.json"

  val rows: Vector[Tuple] = {
    val stream = Option(getClass.getResourceAsStream(fixtureResource))
      .getOrElse(sys.error(s"sklearn fixture not found on classpath: $fixtureResource"))
    val root =
      try new ObjectMapper().readTree(stream)
      finally stream.close()
    root
      .elements()
      .asScala
      .map { node =>
        val b = Tuple.builder(schema)
        schema.getAttributes.foreach { attr =>
          val cell = node.get(attr.getName)
          require(cell != null, s"sklearn fixture row missing column '${attr.getName}'")
          val value: AnyRef = attr.getType match {
            case AttributeType.INTEGER => Int.box(cell.asInt())
            case AttributeType.DOUBLE  => Double.box(cell.asDouble())
            case _                     => cell.asText()
          }
          b.add(attr, value)
        }
        b.build()
      }
      .toVector
  }
}

/**
  * Text dataset for the sklearn `countVectorizer=true` path. Two token-disjoint
  * classes so `CountVectorizer` + any estimator separates them perfectly and
  * both paths predict identically (deterministic parity). Mirrors
  * [[SklearnFixture]]'s 6-rows-per-class size so cv=5 estimators
  * (LogisticRegressionCV, probability calibration) have enough members. `note`
  * is column 0 so the model probe — which feeds a text pipeline the probe's
  * first column as a Series — picks it up as the vectorized feature.
  *
  * Source of truth: src/test/resources/verify/sklearn_text_fixture.json (mirrors
  * [[SklearnFixture]]); `schema` below stays authoritative for column types.
  */
object SklearnTextFixture extends SharedFixture {

  val schema: Schema = new Schema(
    new Attribute("note", AttributeType.STRING),
    new Attribute("y", AttributeType.INTEGER)
  )

  /** Both ports get the whole table, as in [[SklearnFixture]]. */
  override def rowsFor(port: Int): Seq[Tuple] = rows

  /** `y` is the label, for the reason [[SklearnFixture.keepFilled]] gives. */
  override val keepFilled: Set[String] = Set("y")

  private val fixtureResource = "/verify/sklearn_text_fixture.json"

  val rows: Vector[Tuple] = {
    val stream = Option(getClass.getResourceAsStream(fixtureResource))
      .getOrElse(sys.error(s"sklearn text fixture not found on classpath: $fixtureResource"))
    val root =
      try new ObjectMapper().readTree(stream)
      finally stream.close()
    root
      .elements()
      .asScala
      .map { node =>
        val b = Tuple.builder(schema)
        schema.getAttributes.foreach { attr =>
          val cell = node.get(attr.getName)
          require(cell != null, s"sklearn text fixture row missing column '${attr.getName}'")
          val value: AnyRef = attr.getType match {
            case AttributeType.INTEGER => Int.box(cell.asInt())
            case _                     => cell.asText()
          }
          b.add(attr, value)
        }
        b.build()
      }
      .toVector
  }
}

/**
  * Handler for `SpecializedFilterOpDesc`. Curated CONFIG over the shared
  * canonical fixture: the auto tier fills a free-form predicate `value` with
  * the canonical "1", which pins the shape of the comparison but not its
  * corners. `id > 8 OR name == "eve"` exercises numeric comparison, string
  * equality (the JSON predicate `value` is always a string) and OR-combination
  * in one run, and keeps 5 of port 0's 10 rows — a proper subset either way.
  *
  * Both JVM `SpecializedFilterOpExec` and pandas boolean indexing preserve
  * input row order, so positional comparator equality holds.
  */
object SpecializedFilterTransformHandler extends TransformHandler {

  override val opDescClass: Class[_ <: LogicalOp] = classOf[SpecializedFilterOpDesc]

  /** `id > 8` compares against an INTEGER column, and the platform parses the predicate
    * `value` as that column's type — so the hostile string is refused by the number
    * parser before any escaping could matter. Drop this the day the fixture filters on
    * string columns only.
    */
  override val unfillableVariants: Set[String] = Set("hostileText")

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val desc = new SpecializedFilterOpDesc()
    desc.predicates = List(
      new FilterPredicate("id", ComparisonType.GREATER_THAN, "8"),
      new FilterPredicate("name", ComparisonType.EQUAL_TO, "eve")
    )

    (desc, CanonicalFixture.writeInputs(testRoot, 1))
  }
}

/** Handler for `DistinctOpDesc`. The canonical auto-fixture is all-distinct
  * (uniq_name is globally unique by invariant), so it never exercises dedup.
  * This 5-row table repeats two rows so both paths must actually drop
  * duplicates; survivors keep first-occurrence order (JVM LinkedHashSet ==
  * pandas drop_duplicates keep="first"), so the positional comparator holds.
  */
/**
  * Curated handler for [[ProjectionOpDesc]]. Its `attributes` list is not declared
  * `required`, so the auto tier starts it empty the way the UI does — and
  * `getPhysicalOp` refuses an empty list. Pinning one row is all this needs; the
  * runner derives the rest of the variants from it.
  */
object ProjectionTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[ProjectionOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val columns = Seq(
      ("id", AttributeType.INTEGER),
      ("name", AttributeType.STRING),
      ("score", AttributeType.DOUBLE)
    )
    val rows = Seq(
      Seq[Any](1, "a", 1.5),
      Seq[Any](2, "b", 2.5),
      Seq[Any](3, "c", 3.5)
    )
    val inputPath =
      CuratedHandlers.writeFixture(testRoot.resolve("input_port_0.jsonl"), columns, rows)
    val op = new ProjectionOpDesc()
    // A blank alias is the untouched state of the row the `+` button adds, and it is
    // the branch where the operator keeps the original name.
    op.attributes = List(new AttributeUnit("id", ""))
    (op, Map(PortIdentity(0) -> inputPath))
  }
}

object DistinctTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[DistinctOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val columns = Seq(
      ("id", AttributeType.INTEGER),
      ("name", AttributeType.STRING)
    )
    val rows = Seq(
      Seq[Any](1, "a"),
      Seq[Any](2, "b"),
      Seq[Any](1, "a"), // duplicate of row 0
      Seq[Any](3, "c"),
      Seq[Any](2, "b") // duplicate of row 1
    )
    val inputPath =
      CuratedHandlers.writeFixture(testRoot.resolve("input_port_0.jsonl"), columns, rows)
    (new DistinctOpDesc(), Map(PortIdentity(0) -> inputPath))
  }
}

/**
  * Curated handler for [[RegexOpDesc]]. The auto tier only ever feeds it the
  * trivial pattern `"1"` against the first column, which never exercises real
  * regex semantics. This handler pins genuine patterns so the JVM↔Python engine
  * parity is actually tested:
  *
  *   - Primary fixture: `[a-z]+` over a mixed-case `text` column. The runner
  *     enum-sweeps the Boolean `caseInsensitive`, so BOTH branches run against
  *     the same data. The two branches select DIFFERENT row sets (case-sensitive
  *     keeps only rows with a lowercase letter; case-insensitive also keeps the
  *     all-caps rows), proving the flag actually flows through to both paths.
  *   - `extraScenarios`: `\d+` (a backslash class — verifies the escape survives
  *     `toPyDoubleQuotedLiteral` into Python's engine) and `\.` (an escaped
  *     metachar — an escaping bug would turn it into "match any char" and change
  *     the result, so this pins literal-vs-metachar handling).
  *
  * All fixture data is ASCII, where Java `\d` / `[a-z]` / CASE_INSENSITIVE and
  * Python's `re` agree exactly; each pattern yields a proper subset (never
  * all/none) so the comparison is meaningful.
  */
object RegexTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[RegexOpDesc]

  private def regexOp(attribute: String, regex: String, caseInsensitive: Boolean): RegexOpDesc = {
    val op = new RegexOpDesc()
    op.attribute = attribute
    op.regex = regex
    op.caseInsensitive = caseInsensitive
    op
  }

  // Rows chosen so `[a-z]+` differs by case flag: "ABC"/"XY9" have no lowercase
  // (dropped when case-sensitive) but are all-letter (kept when insensitive).
  private val textColumn = Seq(("text", AttributeType.STRING))
  private val caseRows: Seq[Seq[Any]] =
    Seq(Seq[Any]("abc"), Seq[Any]("ABC"), Seq[Any]("123"), Seq[Any]("a1B"), Seq[Any]("XY9"))

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath =
      CuratedHandlers.writeFixture(testRoot.resolve("input_port_0.jsonl"), textColumn, caseRows)
    (regexOp("text", "[a-z]+", caseInsensitive = false), Map(PortIdentity(0) -> inputPath))
  }

  override def extraScenarios(
      testRoot: Path
  ): Seq[(String, LogicalOp, Map[PortIdentity, Path])] = {
    // `\d+`: rows where a digit is present form a proper subset.
    val digitDir = testRoot.resolve("digits")
    Files.createDirectories(digitDir)
    val digitRows: Seq[Seq[Any]] =
      Seq(Seq[Any]("abc"), Seq[Any]("a1B"), Seq[Any]("XY9"), Seq[Any]("123"), Seq[Any]("ab"))
    val digitInput =
      CuratedHandlers.writeFixture(digitDir.resolve("input_port_0.jsonl"), textColumn, digitRows)

    // `\.`: only rows with a literal dot match. If the backslash were lost, the
    // pattern would become bare `.` (match any char) and select every row.
    val dotDir = testRoot.resolve("dot")
    Files.createDirectories(dotDir)
    val dotRows: Seq[Seq[Any]] =
      Seq(Seq[Any]("a.b"), Seq[Any]("abc"), Seq[Any]("x.y.z"), Seq[Any]("no"))
    val dotInput =
      CuratedHandlers.writeFixture(dotDir.resolve("input_port_0.jsonl"), textColumn, dotRows)

    Seq(
      (
        "regex=\\d+",
        regexOp("text", "\\d+", caseInsensitive = false),
        Map(PortIdentity(0) -> digitInput)
      ),
      (
        "regex=\\.",
        regexOp("text", "\\.", caseInsensitive = false),
        Map(PortIdentity(0) -> dotInput)
      )
    )
  }
}

/** HashJoin INNER on `id`. Build (port 0) and probe (port 1) intentionally
  *  arrive in different id orders so any probe-major / left-major mismatch
  *  between the JVM emit and `pd.merge` shows up. HashJoin inherits the
  *  unordered `LogicalOp.orderSensitive` default, so rows compare as a set.
  */
object HashJoinTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[HashJoinOpDesc[_]]

  /** `id` is what the two sides pair on: empty it and the rows stop matching, so
    * the run would be asking about an inner join that finds nothing rather than
    * about a null. The payload columns carry no arrangement and take the holes.
    */
  override def nullsKeepFilled: Option[Set[String]] = Some(Set("id"))

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val buildSchema = new Schema(
      new Attribute("id", AttributeType.INTEGER),
      new Attribute("name", AttributeType.STRING)
    )
    val probeSchema = new Schema(
      new Attribute("id", AttributeType.INTEGER),
      new Attribute("score", AttributeType.INTEGER)
    )

    def buildTup(id: Int, name: String): Tuple = {
      val b = Tuple.builder(buildSchema)
      b.add(buildSchema.getAttribute("id"), Int.box(id))
      b.add(buildSchema.getAttribute("name"), name)
      b.build()
    }
    def probeTup(id: Int, score: Int): Tuple = {
      val b = Tuple.builder(probeSchema)
      b.add(probeSchema.getAttribute("id"), Int.box(id))
      b.add(probeSchema.getAttribute("score"), Int.box(score))
      b.build()
    }

    val buildRows = Seq(
      buildTup(3, "carol"),
      buildTup(1, "alice"),
      buildTup(5, "eve"),
      buildTup(2, "bob"),
      buildTup(4, "dave")
    )
    val probeRows = Seq(
      probeTup(1, 95),
      probeTup(2, 80),
      probeTup(3, 88),
      probeTup(4, 72),
      probeTup(5, 91)
    )
    val buildPath = testRoot.resolve("input_port_0.jsonl")
    val probePath = testRoot.resolve("input_port_1.jsonl")
    TupleIO.writeTuples(buildPath, buildRows.iterator, buildSchema)
    TupleIO.writeTuples(probePath, probeRows.iterator, probeSchema)

    val desc = new HashJoinOpDesc[Integer]()
    desc.buildAttributeName = "id"
    desc.probeAttributeName = "id"
    desc.joinType = JoinType.INNER

    (desc, Map(PortIdentity(0) -> buildPath, PortIdentity(1) -> probePath))
  }
}

/** HuggingFace Spam SMS Detection: the auto-config tier fills the free-form
  *  `resultAttributeProbability` with the op's production default `"score"`,
  *  which collides with the shared fixture's existing `score` column, so
  *  `getOutputSchemas` throws when it `.add("score", …)` on a schema that
  *  already has it ("Output schema … not propagated"). Curate a non-colliding
  *  output name here — purely a test-side override; the production default stays
  *  `"score"`. `attribute` is pointed at the sentence column (the op's own
  *  @SampleColumn target) so the classifier gets real text. Map op running the
  *  same HuggingFace pipeline on both paths → compared as a DataFrame.
  */
object HuggingFaceSpamSMSDetectionTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[HuggingFaceSpamSMSDetectionOpDesc]
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val desc = new HuggingFaceSpamSMSDetectionOpDesc()
    desc.attribute = "short_text"
    desc.resultAttributeSpam = "is_spam"
    desc.resultAttributeProbability = "spam_score" // avoid colliding with fixture's `score`
    (desc, CanonicalFixture.writeInputs(testRoot, 1))
  }
}

/**
  * Handler for `TypeCastingOpDesc`. The auto tier points `attribute` at the
  * canonical fixture's first column (`id`, INTEGER) and then sweeps `resultType`
  * across ALL `AttributeType` values — but `TypeCastingUnit`'s attributeTypeRules
  * only permit certain source types per target (e.g. `timestamp` accepts only
  * string/long), and the native `TypeCastingOpExec` throws on an illegal cast
  * (INTEGER → Timestamp). So the auto variant `resultType=timestamp` crashes
  * Path A before any comparison.
  *
  * This fixture gives each cast a type-compatible source column and a value that
  * round-trips identically on both paths (JVM `AttributeTypeUtils` vs the
  * generated pandas), covering the value-comparable branches of
  * `generateStandaloneCode`'s `resultType` match: STRING, INTEGER, LONG, DOUBLE,
  * BOOLEAN. The op is listed in [[TransformVerificationRunner.enumSweepExemptOps]]
  * so the blind one-enum-at-a-time sweep — which would re-pair each fixed column
  * with every target type — is suppressed; the units below already exercise each
  * branch. Map op: both paths keep input row order, so strict positional equality
  * holds.
  *
  * TIMESTAMP is intentionally omitted: the two runtimes serialize a Timestamp
  * differently to JSONL (native emits an ISO string `"2024-01-01 09:00:00.0"`,
  * pandas emits epoch millis `1704099600000`), so the dataframe comparator flags
  * a representation mismatch even though the instant is identical — a harness-wide
  * timestamp-serialization gap, not a TypeCasting translation defect.
  */
object TypeCastingTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[TypeCastingOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    // One dedicated source column per target so the casts don't chain.
    val columns = Seq(
      ("str_to_int", AttributeType.STRING), // numeric string  → INTEGER
      ("int_to_dbl", AttributeType.INTEGER), // integer         → DOUBLE
      ("int_to_str", AttributeType.INTEGER), // integer         → STRING
      ("int_to_lng", AttributeType.INTEGER), // integer         → LONG
      ("int_to_bool", AttributeType.INTEGER) // 1/0            → BOOLEAN
    )
    val rows = Seq(
      Seq[Any]("10", 1, 6, 11, 1),
      Seq[Any]("20", 2, 7, 12, 0),
      Seq[Any]("30", 3, 8, 13, 1),
      Seq[Any]("40", 4, 9, 14, 0),
      Seq[Any]("50", 5, 10, 15, 1)
    )
    val inputPath =
      CuratedHandlers.writeFixture(testRoot.resolve("input_port_0.jsonl"), columns, rows)

    def unit(attr: String, t: AttributeType): TypeCastingUnit = {
      val u = new TypeCastingUnit()
      u.attribute = attr
      u.resultType = t
      u
    }
    val desc = new TypeCastingOpDesc()
    desc.typeCastingUnits = List(
      unit("str_to_int", AttributeType.INTEGER),
      unit("int_to_dbl", AttributeType.DOUBLE),
      unit("int_to_str", AttributeType.STRING),
      unit("int_to_lng", AttributeType.LONG),
      unit("int_to_bool", AttributeType.BOOLEAN)
    )

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/**
  * Handler for `SleepOpDesc`. Sleep is a pure passthrough (it delays each tuple
  * but never changes the data), so the auto tier's `sleepTime` — filled to
  * rowCount/2 and multiplied by 1000ms per tuple in `SleepOpExec` — makes Path A
  * sleep for ~tens of seconds with no effect on the compared output. Pin
  * `sleepTime = 0` so the passthrough is verified instantly; the delay is
  * irrelevant to the data-equivalence check.
  */
object SleepTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SleepOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val desc = new SleepOpDesc()
    desc.sleepTime = 0
    (desc, CanonicalFixture.writeInputs(testRoot, 1))
  }
}

/**
  * Handler for `KeywordSearchOpDesc`. The auto tier points `attribute` at the
  * canonical fixture's first column (`id`) and fills `keyword` with the canonical
  * "1", so the search runs against numeric ids and never touches a real text
  * column. This fixture searches a genuine free-text column with a two-term
  * query, exercising the standalone regex's meaningful branches — multi-term OR,
  * whole-word boundaries — that both the JVM Lucene path and the pandas path
  * agree on. Query "love day" keeps rows 1 and 2 (contain the whole words
  * love/day); row 3 has neither; row 4's "lovely"/"today" are different tokens,
  * so the shared word-boundary rule drops it. 4 rows → 2 kept.
  *
  * The rows are intentionally punctuation-free. The `isCaseSensitive` enum is
  * swept (true and false), and the case-sensitive path uses `CaseSensitiveAnalyzer`
  * (a `WhitespaceTokenizer` that leaves punctuation attached, e.g. "perfect."),
  * which diverges from the standalone regex's `\b`-boundary matching on any
  * punctuated word — and the standalone does NOT honor case at all. Clean
  * whitespace-delimited words keep both tokenizers (and both case modes) in
  * agreement; this is why the canonical fixture's punctuated `short_text` column
  * cannot be reused here. Lucene phrase/boolean/wildcard syntax is likewise
  * avoided — the regex approximation cannot reproduce it.
  */
object KeywordSearchTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[KeywordSearchOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val columns = Seq(("txt", AttributeType.STRING))
    val rows = Seq(
      Seq[Any]("i love this product"),
      Seq[Any]("what a great day"),
      Seq[Any]("terrible experience"),
      Seq[Any]("lovely weather today")
    )
    val inputPath =
      CuratedHandlers.writeFixture(testRoot.resolve("input_port_0.jsonl"), columns, rows)

    val desc = new KeywordSearchOpDesc()
    desc.attribute = "txt"
    desc.keyword = "love day"
    desc.isCaseSensitive = false

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** DumbbellPlot: curated CONFIG over the shared canonical fixture. A dumbbell is
  * one line per entity between the entity's value in two categories, so the two
  * category values have to be values the entity actually has — which the auto tier
  * cannot know: it fills both with the canonical string, leaving start == end and
  * every line a point.
  *
  * `node_src` = n3 / n1 is the pair that works on this fixture: `bob` holds both
  * (score 1.2 → 2.8) and so does `1` (1.7 → 0.5), giving two real dumbbells, while
  * eve, dave and grace hold one each and stay single points — both branches drawn
  * at once. `comparedColumnName` is a STRING column on purpose: plotly's trace
  * `name` rejects a numpy number, so a numeric column raises there instead of
  * plotting (reported upstream, not worked around here).
  */
object DumbbellPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[DumbbellPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val dots = new util.ArrayList[DumbbellDotConfig]()
    val dot = new DumbbellDotConfig()
    dot.dotValue = "open"
    dots.add(dot)

    val desc = new DumbbellPlotOpDesc()
    desc.categoryColumnName = "node_src"
    desc.dumbbellStartValue = "n3"
    desc.dumbbellEndValue = "n1"
    desc.measurementColumnName = "score"
    desc.comparedColumnName = "name"
    desc.dots = dots

    (desc, CanonicalFixture.writeInputs(testRoot, 1))
  }
}

/** ImageVisualizer fixture. Uses deterministic binary payloads; the operator
  * base64-encodes them into img tags.
  */
object ImageVisualizerVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[ImageVisualizerOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(new Attribute("image_bytes", AttributeType.BINARY))

    def tup(bytes: Array[Byte]): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("image_bytes"), bytes)
      builder.build()
    }

    val rows = Seq(
      tup(Array[Byte](1, 2, 3, 4)),
      tup(Array[Byte](10, 20, 30, 40))
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new ImageVisualizerOpDesc()
    desc.binaryContent = "image_bytes"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** FilledAreaPlot: curated CONFIG over the shared canonical fixture. Two things
  * the auto tier cannot supply. `lineGroup` is an optional autofill field, so
  * auto leaves it empty — but the Boolean sweep still flips `facetColumn` to
  * true, and `facet_col` needs that column name, so the generated code is
  * invalid. And the operator rejects line groups whose x sets are disjoint
  * (tolerance is 0), which pins which (group, x) pair is usable: grouping by
  * `node_src` (n1/n2/n3 in port 0), every group shares a `comp_a` value with
  * the first, so both sweep variants render.
  */
object FilledAreaPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[FilledAreaPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val desc = new FilledAreaPlotOpDesc()
    desc.x = "comp_a"
    desc.y = "score"
    desc.lineGroup = "node_src"
    desc.facetColumn = false

    (desc, CanonicalFixture.writeInputs(testRoot, 1))
  }
}

/** Advanced (hyperparameter-sweep) trainers: train on port 0, parameter table
  * on port 1. We set one real hyperparameter (the family's first numeric one —
  * KNN `n_neighbors = 3`, SVC/SVR `C = 1.0`) so the parameter-handling logic
  * (`getParameter` and the param string both code paths build) is actually
  * exercised, not skipped with an empty paraList. The model lands in a BINARY
  * column compared by prediction behavior.
  */
abstract class SklearnAdvancedTrainerTransformHandler extends TransformHandler {
  protected def newDesc(): SklearnMLOperatorDescriptor[_]

  /** `paraList/0/value` is this operator's only free-text field, and no hostile
    * string can legally reach it. Its meaning is decided by the `parameter`
    * beside it, and every one of those is either a number or a fixed set of
    * words — `C` a float, `degree` an int, `kernel` one of sklearn's kernels,
    * `probability` "true" or "false". A spliced `a"b` therefore fails at the
    * conversion rather than at any escaping, on both paths, and the run ends
    * before there is anything to compare.
    *
    * The escaping this variant looks for is worth checking and simply has no
    * target here. What the field's values do get is [[extraScenarios]], which
    * exercises every hyperparameter with a value its own type accepts.
    */
  override val unfillableVariants: Set[String] = Set("hostileText")

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val train = SklearnFixture.write(testRoot, inputPortCount = 1, withGaps = false)(
      PortIdentity(0)
    )

    val desc = newDesc()
    desc.groundTruthAttribute = "y"
    desc.selectedFeatures = List("x1", "x2")
    val hp = CuratedHandlers.sampleHyperParameter(opDescClass)
    desc.asInstanceOf[SklearnMLOperatorDescriptor[ParamClass]].paraList = List(hp)

    // Port 1 is the "parameter" table. It holds one valid value for the chosen
    // hyperparameter so BOTH branches run: parametersSource=false uses hp.value,
    // and the swept parametersSource=true reads this `param_val` column.
    val isInt = hp.parameter.getType == "int"
    val param = CuratedHandlers.writeFixture(
      testRoot.resolve("input_port_1.jsonl"),
      Seq(("param_val", if (isInt) AttributeType.INTEGER else AttributeType.DOUBLE)),
      Seq(Seq(if (isInt) 3 else 1.0))
    )
    (desc, Map(PortIdentity(0) -> train, PortIdentity(1) -> param))
  }

  /** One scenario per hyperparameter the operator offers.
    *
    * [[fixture]] can only carry one, and the sweep cannot reach the rest: the
    * `parameter` field is typed `T`, so the generator sees an erased Object and
    * has no enum to walk. Yet each hyperparameter is converted differently in
    * the emitted Python — `kernel` stays a string, `degree` goes through `int`,
    * `probability` through a lambda comparing against "true" — and those are the
    * lines this operator family exists to translate. Testing one of six said
    * nothing about the other five.
    *
    * The enum is resolvable here, as [[fixture]] already shows, so walk it.
    */
  override def extraScenarios(
      testRoot: Path
  ): Seq[(String, LogicalOp, Map[PortIdentity, Path])] = {
    val chosen = CuratedHandlers.sampleHyperParameter(opDescClass).parameter
    CuratedHandlers
      .allHyperParameters(opDescClass)
      .filterNot(_.getName == chosen.getName)
      .filterNot(p => CuratedHandlers.miswiredHyperParameters.contains(p.getName))
      .map { p =>
        val dir = testRoot.resolve(s"param_${p.getName}")
        Files.createDirectories(dir)
        val train = SklearnFixture.write(dir, inputPortCount = 1, withGaps = false)(PortIdentity(0))

        val desc = newDesc()
        desc.groundTruthAttribute = "y"
        desc.selectedFeatures = List("x1", "x2")
        val hp = new HyperParameters[ParamClass]()
        hp.parameter = p
        hp.parametersSource = false
        hp.value = CuratedHandlers.sampleValueFor(p)
        hp.attribute = "param_val"
        desc.asInstanceOf[SklearnMLOperatorDescriptor[ParamClass]].paraList = List(hp)

        val isInt = p.getType == "int"
        val param = CuratedHandlers.writeFixture(
          dir.resolve("input_port_1.jsonl"),
          Seq(("param_val", if (isInt) AttributeType.INTEGER else AttributeType.DOUBLE)),
          Seq(Seq(if (isInt) 3 else 1.0))
        )
        (s"param_${p.getName}", desc, Map(PortIdentity(0) -> train, PortIdentity(1) -> param))
      }
  }
}

/** If operator: routes the data port (port 1) to the True (port 1) or False
  * (port 0) output. We feed an EMPTY Condition port (port 0) so IfOpExec
  * forwards no condition rows; with no State message it keeps its default
  * active output (True), matching the standalone's default-True branch — so
  * the True output gets all data rows and the False output is empty on both
  * paths.
  */
/** Aggregate fixture exercising every aggregation function in one op, including
  * COUNT(*) (empty attribute). Auto-config can't build this: upstream #5896 made
  * `AggregationOperation.attribute` optional (required only for non-count via a
  * conditional JSON-schema rule), so ConfigGenerator skips the optional autofill
  * field and leaves it null — invalid for a non-count function, which NPEs in
  * AggregateOpExec. This pins valid (function, column) pairs. Enum-sweep-exempt
  * (see [[TransformVerificationRunner.enumSweepExemptOps]]): the sweep flips each
  * element's function in isolation and would re-pair, e.g., concat with a numeric
  * column; the fixture already covers each function with a type-compatible column.
  * Aggregate inherits the unordered `orderSensitive` default, so
  * the comparator lex-sorts rows before comparing.
  */
object AggregateTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[AggregateOpDesc]

  private def agg(
      fn: AggregationFunction,
      attr: String,
      result: String
  ): AggregationOperation = {
    val a = new AggregationOperation()
    a.aggFunction = fn
    a.attribute = attr
    a.resultAttribute = result
    a
  }

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val desc = new AggregateOpDesc()
    desc.groupByKeys = List("name")
    desc.aggregations = List(
      agg(AggregationFunction.SUM, "score", "sum_score"),
      agg(AggregationFunction.COUNT, "", "count_all"), // empty attribute => COUNT(*)
      agg(AggregationFunction.COUNT, "score", "count_score"),
      agg(AggregationFunction.AVERAGE, "score", "avg_score"),
      agg(AggregationFunction.MIN, "score", "min_score"),
      agg(AggregationFunction.MAX, "score", "max_score"),
      agg(AggregationFunction.CONCAT, "iso_country", "cat_country")
    )
    (desc, CanonicalFixture.writeInputs(testRoot, 1))
  }
}

object IfTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[IfOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val cols = Seq("id" -> AttributeType.INTEGER, "name" -> AttributeType.STRING)
    val condition =
      CuratedHandlers.writeFixture(
        testRoot.resolve("input_port_0.jsonl"),
        cols,
        Seq.empty[Seq[Any]]
      )
    val data = CuratedHandlers.writeFixture(
      testRoot.resolve("input_port_1.jsonl"),
      cols,
      Seq(Seq(1, "a"), Seq(2, "b"), Seq(3, "c"))
    )
    val desc = new IfOpDesc()
    desc.conditionName = "cond"
    (desc, Map(PortIdentity(0) -> condition, PortIdentity(1) -> data))
  }
}

/** Machine learning scorer fixture with a tiny labeled prediction table. */
object MachineLearningScorerTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[MachineLearningScorerOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("y", AttributeType.INTEGER),
      new Attribute("pred", AttributeType.INTEGER)
    )

    def tup(y: Int, pred: Int): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("y"), Int.box(y))
      builder.add(schema.getAttribute("pred"), Int.box(pred))
      builder.build()
    }

    val rows = Seq(
      tup(0, 0),
      tup(0, 0),
      tup(1, 1),
      tup(1, 0),
      tup(1, 1)
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new MachineLearningScorerOpDesc()
    desc.isRegression = false
    desc.actualValueColumn = "y"
    desc.predictValueColumn = "pred"
    desc.classificationMetrics = List(classificationMetricsFnc.accuracy)
    // Populate regressionMetrics too so the enum sweep's isRegression=true variant
    // has a real metric. Left empty, getSelectedMetrics() renders [''] (mkString
    // on an empty list), and regression_metrics does metrics_func[''] → KeyError.
    // The metrics func runs on the same y/pred integers on both paths → parity.
    desc.regressionMetrics = List(regressionMetricsFnc.mse)

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}
