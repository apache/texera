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
import org.apache.texera.amber.operator.dictionary.{DictionaryMatcherOpDesc, MatchingType}
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
import org.apache.texera.amber.operator.sort.{
  SortCriteriaUnit,
  SortPreference,
  StableMergeSortOpDesc
}
import org.apache.texera.amber.operator.typecasting.{TypeCastingOpDesc, TypeCastingUnit}
import org.apache.texera.amber.operator.visualization.ImageViz.ImageVisualizerOpDesc
import org.apache.texera.amber.operator.visualization.bulletChart.{
  BulletChartOpDesc,
  BulletChartStepDefinition
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
import scala.collection.mutable.ListBuffer
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

  /** Auto-discovery factory for the [[SklearnTrainingTransformHandler]] family:
    * every concrete `SklearnTrainingOpDesc` subclass, instantiated by
    * reflection.
    */
  private def trainingHandler(cls: Class[_ <: LogicalOp]): SklearnTrainingTransformHandler =
    new SklearnTrainingTransformHandler {
      override val opDescClass: Class[_ <: LogicalOp] = cls
      override protected def newDesc(): SklearnTrainingOpDesc =
        cls.getDeclaredConstructor().newInstance().asInstanceOf[SklearnTrainingOpDesc]
    }

  /** Auto-discovery factory for the [[SklearnClassifierTransformHandler]]
    * family: every concrete `SklearnClassifierOpDesc` subclass.
    */
  private def classifierHandler(cls: Class[_ <: LogicalOp]): SklearnClassifierTransformHandler =
    new SklearnClassifierTransformHandler {
      override val opDescClass: Class[_ <: LogicalOp] = cls
      override protected def newDesc(): SklearnClassifierOpDesc =
        cls.getDeclaredConstructor().newInstance().asInstanceOf[SklearnClassifierOpDesc]
    }

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

  /** All sklearn curated handlers, auto-derived from [[registeredOps]]. Filters
    * per family by `isAssignableFrom`, keeping only concrete leaf ops and
    * excluding each abstract base. `SklearnLinearRegressionOpDesc` is excluded
    * from the classifier family so its bespoke standalone handler
    * ([[SklearnLinearRegressionTransformHandler]]) stays authoritative — it is
    * not actually a `SklearnClassifierOpDesc` subclass, but the exclusion makes
    * that invariant explicit and future-proof. The three families are disjoint
    * hierarchies, so no op is double-counted.
    */
  private def sklearnAutoHandlers: Seq[TransformHandler] = {
    val trainingBase = classOf[SklearnTrainingOpDesc]
    val classifierBase = classOf[SklearnClassifierOpDesc]
    val advancedBase = classOf[SklearnMLOperatorDescriptor[_]]

    val trainingOps = registeredOps.filter(c =>
      trainingBase.isAssignableFrom(c) && c != trainingBase && isConcrete(c)
    )
    val classifierOps = registeredOps.filter(c =>
      classifierBase.isAssignableFrom(c) &&
        c != classifierBase &&
        c != classOf[SklearnLinearRegressionOpDesc] &&
        isConcrete(c)
    )
    val advancedOps = registeredOps.filter(c =>
      advancedBase.isAssignableFrom(c) && c != advancedBase && isConcrete(c)
    )

    // No hard-coded baseline: a new sklearn operator is picked up automatically
    // the moment it is registered in LogicalOp's @JsonSubTypes — zero per-op
    // code here. The test suite (ConfigCoverageSpec / TransformVerificationRunnerSpec)
    // is the safety net: a mis-discovered or misbehaving op fails its own parity
    // check rather than being frozen by an assertion.
    trainingOps.map(trainingHandler) ++
      classifierOps.map(classifierHandler) ++
      advancedOps.map(advancedTrainerHandler)
  }

  /** Op classes served by the auto-discovered sklearn tier. Exposed so
    * [[TransformVerificationRunner.disposition]] can label them `ml-auto`
    * (a systematic shared-fixture + predict-compare category) instead of
    * `curated`, which is then reserved for genuine one-off fixtures.
    */
  val sklearnAutoClasses: Set[Class[_ <: LogicalOp]] =
    sklearnAutoHandlers.map(_.opDescClass).toSet

  val all: Seq[TransformHandler] = Seq(
    AggregateTransformHandler,
    SpecializedFilterTransformHandler,
    DistinctTransformHandler,
    DictionaryMatcherTransformHandler,
    HashJoinTransformHandler,
    TypeCastingTransformHandler,
    SleepTransformHandler,
    KeywordSearchTransformHandler,
    ProjectionTransformHandler,
    BulletChartVisualizationHandler,
    ImageVisualizerVisualizationHandler,
    FilledAreaPlotVisualizationHandler,
    SklearnLinearRegressionTransformHandler,
    IfTransformHandler,
    MachineLearningScorerTransformHandler,
    HuggingFaceSpamSMSDetectionTransformHandler,
    RegexTransformHandler,
    StableMergeSortTransformHandler
  ) ++ sklearnAutoHandlers

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
    * port 1) for the Sklearn classifier/regressor operators. Both ports get the
    * same rows from the shared [[SklearnFixture]] resource.
    */
  def writeClassification2Input(testRoot: Path): (Path, Path) = {
    val train = testRoot.resolve("input_port_0.jsonl")
    val test = testRoot.resolve("input_port_1.jsonl")
    TupleIO.writeTuples(train, SklearnFixture.rows.iterator, SklearnFixture.schema)
    TupleIO.writeTuples(test, SklearnFixture.rows.iterator, SklearnFixture.schema)
    (train, test)
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
    val consts = resolveParamEnum(opClass).getEnumConstants.map(_.asInstanceOf[ParamClass])
    val chosen =
      consts.find(c => Set("int", "float", "double").contains(c.getType)).getOrElse(consts.head)
    val hp = new HyperParameters[ParamClass]()
    hp.parameter = chosen
    hp.parametersSource = false
    hp.value = if (chosen.getType == "int") "3" else "1.0"
    hp.attribute = "param_val" // column read when the sweep flips parametersSource=true
    hp
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
object SklearnFixture {

  val schema: Schema = new Schema(
    new Attribute("x1", AttributeType.DOUBLE),
    new Attribute("x2", AttributeType.DOUBLE),
    new Attribute("y", AttributeType.INTEGER)
  )

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
object SklearnTextFixture {

  val schema: Schema = new Schema(
    new Attribute("note", AttributeType.STRING),
    new Attribute("y", AttributeType.INTEGER)
  )

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

  /** Write the text table to `path` (columns in order: note, y). */
  def write(path: Path): Path = {
    TupleIO.writeTuples(path, rows.iterator, schema)
    path
  }
}

/**
  * Handler for `SpecializedFilterOpDesc`. Writes a 5-row table with an
  * integer and a string column, then filters on `age > 18 OR name == "eve"`.
  * Exercises numeric comparison, string equality (the JSON predicate `value`
  * is a string), and OR-combination of predicates — three corners of the
  * generator's coercion logic in one fixture.
  *
  * Both JVM `SpecializedFilterOpExec` and pandas boolean indexing preserve
  * input row order, so positional comparator equality holds.
  */
object SpecializedFilterTransformHandler extends TransformHandler {

  override val opDescClass: Class[_ <: LogicalOp] = classOf[SpecializedFilterOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("age", AttributeType.INTEGER),
      new Attribute("name", AttributeType.STRING)
    )

    val rows: Seq[Tuple] = Seq(
      tupleOf(schema, 21, "alice"),
      tupleOf(schema, 17, "bob"),
      tupleOf(schema, 30, "carol"),
      tupleOf(schema, 15, "eve"),
      tupleOf(schema, 14, "dave")
    )

    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new SpecializedFilterOpDesc()
    desc.predicates = List(
      new FilterPredicate("age", ComparisonType.GREATER_THAN, "18"),
      new FilterPredicate("name", ComparisonType.EQUAL_TO, "eve")
    )

    (desc, Map(PortIdentity(0) -> inputPath))
  }

  private def tupleOf(schema: Schema, age: Int, name: String): Tuple = {
    val builder = Tuple.builder(schema)
    builder.add(schema.getAttribute("age"), Int.box(age))
    builder.add(schema.getAttribute("name"), name)
    builder.build()
  }
}

/** Handler for `DistinctOpDesc`. The canonical auto-fixture is all-distinct
  * (uniq_name is globally unique by invariant), so it never exercises dedup.
  * This 5-row table repeats two rows so both paths must actually drop
  * duplicates; survivors keep first-occurrence order (JVM LinkedHashSet ==
  * pandas drop_duplicates keep="first"), so the positional comparator holds.
  */
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

/** DictionaryMatcher fixture that actually matches, chosen so all three swept
  * MatchingType branches agree between the JVM (Lucene) and standalone paths.
  * cat/dog/car are Porter2-invariant (stemming leaves them unchanged), so the
  * CONJUNCTION branch — where standalone lacks Lucene's stemmer — still matches.
  * dictionary = "cat,dog" over `word` = [cat, dog, car] yields
  * [matched, matched, unmatched] under exact / substring / conjunction alike.
  */
object DictionaryMatcherTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[DictionaryMatcherOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val columns = Seq(("word", AttributeType.STRING))
    val rows = Seq(
      Seq[Any]("cat"),
      Seq[Any]("dog"),
      Seq[Any]("car")
    )
    val inputPath =
      CuratedHandlers.writeFixture(testRoot.resolve("input_port_0.jsonl"), columns, rows)

    val desc = new DictionaryMatcherOpDesc()
    desc.attribute = "word"
    desc.dictionary = "cat,dog"
    desc.resultAttribute = "matched"
    desc.matchingType = MatchingType.SCANBASED
    (desc, Map(PortIdentity(0) -> inputPath))
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

/**
  * Curated handler for [[StableMergeSortOpDesc]]. Reuses the shared canonical
  * fixture (no bespoke input) and only PINS the sort key to `name` — a column
  * that carries tie groups ("1"×3, alice/bob/carol/dave ×2) in shuffled row
  * order. The auto tier instead sorts on `id` (the first unused column), which
  * is unique, so ties never occur and the operator's defining STABLE property
  * is never exercised. With a tied key, a non-stable sort would reorder
  * equal-key rows differently from the JVM incremental stable merge, so this
  * pins that behavior; the runner's enum sweep runs both ASC and DESC (ties stay
  * in input order regardless of direction, matching `na_position`-independent
  * stability on both sides). Sort output is order-sensitive, so the comparator
  * checks row order strictly.
  */
object StableMergeSortTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[StableMergeSortOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val criteria = new SortCriteriaUnit()
    criteria.attributeName = "name"
    criteria.sortPreference = SortPreference.ASC
    val desc = new StableMergeSortOpDesc()
    desc.keys = ListBuffer(criteria)
    (desc, CanonicalFixture.writeInputs(testRoot, 1))
  }
}

/** HashJoin INNER on `id`. Build (port 0) and probe (port 1) intentionally
  *  arrive in different id orders so any probe-major / left-major mismatch
  *  between the JVM emit and `pd.merge` shows up. HashJoin inherits the
  *  unordered `LogicalOp.orderSensitive` default, so rows compare as a set.
  */
object HashJoinTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[HashJoinOpDesc[_]]

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

/** Projection: `attributes` carries no @JsonProperty, so the auto-config tier
  *  leaves it empty and ProjectionOpExec rejects the empty list
  *  (Preconditions.checkArgument). Keep two columns, renaming one, to
  *  exercise both select and alias. Map op — strict order holds.
  */
object ProjectionTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[ProjectionOpDesc]
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val desc = new ProjectionOpDesc()
    desc.attributes = List(
      new AttributeUnit("id", ""),
      new AttributeUnit("score", "points")
    )
    (desc, CanonicalFixture.writeInputs(testRoot, 1))
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

/** BulletChart: curated CONFIG over the shared canonical fixture. The auto tier
  * builds `steps: [{}]` — a list field always gets exactly one element, and the
  * element's `start`/`end` are free-form strings it has no basis to fill — and
  * `generatePythonCode` then NPEs on the null `start`. The steps below bracket
  * the fixture's `score` range (0.5–5.5); the runtime path renders one bullet
  * per row while the JSON comparator validates the first Plotly payload.
  */
object BulletChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[BulletChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val steps = new util.ArrayList[BulletChartStepDefinition]()
    steps.add(new BulletChartStepDefinition("0", "3"))
    steps.add(new BulletChartStepDefinition("3", "6"))

    val desc = new BulletChartOpDesc()
    desc.value = "score"
    desc.deltaReference = "3"
    desc.thresholdValue = "4.5"
    desc.steps = steps

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

/** FilledAreaPlot visualization fixture with a simple monotonic series. */
object FilledAreaPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[FilledAreaPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("x", AttributeType.INTEGER),
      new Attribute("y", AttributeType.INTEGER),
      new Attribute("grp", AttributeType.STRING)
    )

    def tup(x: Int, y: Int, grp: String): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("x"), Int.box(x))
      builder.add(schema.getAttribute("y"), Int.box(y))
      builder.add(schema.getAttribute("grp"), grp)
      builder.build()
    }

    // Both groups share the same x values (1,2): the operator rejects line
    // groups with disjoint x sets, and facetColumn=true facets by grp.
    val rows = Seq(tup(1, 2, "a"), tup(2, 4, "a"), tup(1, 3, "b"), tup(2, 5, "b"))
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new FilledAreaPlotOpDesc()
    desc.x = "x"
    desc.y = "y"
    // Supply the column facetColumn=true depends on (facet_col=lineGroup) so the
    // sweep can exercise facetColumn=true without an empty column name.
    desc.lineGroup = "grp"
    desc.facetColumn = false

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** Shared fixture for the Sklearn training operators: a small, separable
  * 2-feature binary-classification dataset (numeric only — the canonical
  * auto-fixture's string column can't be fit by sklearn). The fitted model
  * lands in a BINARY column the comparator ignores (functionally equivalent
  * but not bit-identical across paths); model_name and output shape are still
  * compared, so each operator is verified to run to completion on both paths.
  * Each concrete handler only supplies its estimator-specific OpDesc.
  */
abstract class SklearnTrainingTransformHandler extends TransformHandler {
  protected def newDesc(): SklearnTrainingOpDesc

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, SklearnFixture.rows.iterator, SklearnFixture.schema)

    val desc = newDesc()
    desc.target = "y"
    desc.countVectorizer = false
    desc.tfidfTransformer = false

    (desc, Map(PortIdentity(0) -> inputPath))
  }

  /** The `countVectorizer=true` branch: features come from a single text column
    * (`X = X[text]`), incompatible with the numeric default, so it runs as its
    * own scenario on the text fixture rather than as an enum-sweep variant.
    */
  override def extraScenarios(
      testRoot: Path
  ): Seq[(String, LogicalOp, Map[PortIdentity, Path])] = {
    if (!CuratedHandlers.supportsCountVectorizer(opDescClass)) return Seq.empty
    val dir = testRoot.resolve("cv_text")
    Files.createDirectories(dir)
    val input = SklearnTextFixture.write(dir.resolve("input_port_0.jsonl"))

    val desc = newDesc()
    desc.target = "y"
    desc.countVectorizer = true
    desc.tfidfTransformer = false
    desc.text = "note"

    Seq(("countVectorizer_text", desc, Map(PortIdentity(0) -> input)))
  }
}

/** Shared two-input fixture for the Sklearn classifier operators (training
  * port 0 + testing port 1). The fitted model lands in a BINARY column; the
  * comparator unpickles both paths' models and compares their predictions on
  * the training features, verifying behavior, not bytes. Each concrete handler
  * supplies its estimator-specific OpDesc.
  */
abstract class SklearnClassifierTransformHandler extends TransformHandler {
  protected def newDesc(): SklearnClassifierOpDesc

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val (train, test) = CuratedHandlers.writeClassification2Input(testRoot)
    val desc = newDesc()
    desc.target = "y"
    desc.countVectorizer = false
    (desc, Map(PortIdentity(0) -> train, PortIdentity(1) -> test))
  }

  /** The `countVectorizer=true` branch: train/test features come from a single
    * text column (`X = X[text]`), incompatible with the numeric default, so it
    * runs as its own scenario on the text fixture (both ports) rather than as an
    * enum-sweep variant.
    */
  override def extraScenarios(
      testRoot: Path
  ): Seq[(String, LogicalOp, Map[PortIdentity, Path])] = {
    if (!CuratedHandlers.supportsCountVectorizer(opDescClass)) return Seq.empty
    val dir = testRoot.resolve("cv_text")
    Files.createDirectories(dir)
    val train = SklearnTextFixture.write(dir.resolve("input_port_0.jsonl"))
    val test = SklearnTextFixture.write(dir.resolve("input_port_1.jsonl"))

    val desc = newDesc()
    desc.target = "y"
    desc.countVectorizer = true
    desc.text = "note"

    Seq(("countVectorizer_text", desc, Map(PortIdentity(0) -> train, PortIdentity(1) -> test)))
  }
}

object SklearnLinearRegressionTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SklearnLinearRegressionOpDesc]
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val (train, test) = CuratedHandlers.writeClassification2Input(testRoot)
    val desc = new SklearnLinearRegressionOpDesc()
    desc.target = "y"
    (desc, Map(PortIdentity(0) -> train, PortIdentity(1) -> test))
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

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val train = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(train, SklearnFixture.rows.iterator, SklearnFixture.schema)

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
