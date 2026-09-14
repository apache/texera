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
import org.apache.texera.amber.operator.typecasting.{TypeCastingOpDesc, TypeCastingUnit}
import org.apache.texera.amber.operator.visualization.ImageViz.ImageVisualizerOpDesc

import org.apache.texera.amber.operator.visualization.dumbbellPlot.{
  DumbbellDotConfig,
  DumbbellPlotOpDesc
}
import org.apache.texera.amber.operator.sklearn.training.SklearnTrainingOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnClassifierOpDesc
import org.apache.texera.amber.operator.sklearn.SklearnLinearRegressionOpDesc
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base.SklearnMLOperatorDescriptor
import org.apache.texera.amber.operator.ifStatement.IfOpDesc
import java.nio.file.{Files, Path}
import java.util

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
  * the auto-generated one.
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
    * each remaining column to `fit`, so these take canonical's petal-and-label
    * projection rather than the whole table, whose string columns end the fit.
    *
    * Linear Regression is named on its own because it descends from
    * `PythonOperatorDescriptor` directly rather than from one of the three
    * bases, so no family picks it up.
    */
  val sklearnNumericClasses: Set[Class[_ <: LogicalOp]] =
    (trainingOps ++ classifierOps ++ advancedOps).toSet + classOf[SklearnLinearRegressionOpDesc]

  val all: Seq[TransformHandler] = Seq(
    AggregateTransformHandler,
    SpecializedFilterTransformHandler,
    DistinctTransformHandler,
    ProjectionTransformHandler,
    HashJoinTransformHandler,
    TypeCastingTransformHandler,
    KeywordSearchTransformHandler,
    DumbbellPlotVisualizationHandler,
    ImageVisualizerVisualizationHandler,
    IfTransformHandler,
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

}

/**
  * Handler for `SpecializedFilterOpDesc`. Curated CONFIG over the shared
  * canonical fixture: the auto tier fills a free-form predicate `value` with
  * the canonical "1", which pins the shape of the comparison but not its
  * corners. `id > 8 OR name == "eve"` exercises numeric comparison, string
  * equality (the JSON predicate `value` is always a string) and OR-combination
  * in one run, and keeps 5 of port 0's 10 rows — a proper subset either way.
  */
object SpecializedFilterTransformHandler extends TransformHandler {

  override val opDescClass: Class[_ <: LogicalOp] = classOf[SpecializedFilterOpDesc]

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
  * (the name column is unique by invariant), so it never exercises dedup.
  * This 5-row table repeats two rows so both paths must actually drop
  * duplicates, and both keep the first occurrence: JVM LinkedHashSet and pandas
  * `drop_duplicates(keep="first")` agree on which of a pair survives.
  */
/**
  * Curated CONFIG for [[ProjectionOpDesc]] over the shared table. Its `attributes`
  * list is not declared `required`, so the auto tier starts it empty the way the UI
  * does — and `getPhysicalOp` refuses an empty list. Pinning one row is all this
  * needs; the runner derives the rest of the variants from it, and the table stays
  * the one every other operator reads.
  */
object ProjectionTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[ProjectionOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val op = new ProjectionOpDesc()
    // A blank alias is the untouched state of the row the `+` button adds, and it is
    // the branch where the operator keeps the original name.
    op.attributes = List(new AttributeUnit("id", ""))
    (op, CanonicalFixture.writeInputs(testRoot, 1))
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
  * pattern `"1"`, which asks nothing of either regex engine. Three real ones do:
  *
  *   - `[a-z]+` over a mixed-case column, with `caseInsensitive` swept. The two
  *     branches keep DIFFERENT rows, so the flag is shown to reach both paths.
  *   - `\d+`, a backslash class, which checks the escape survives
  *     `toPyDoubleQuotedLiteral` into Python's engine.
  *   - `\.`, an escaped metachar: an escaping bug turns it into "match any char"
  *     and changes the answer.
  *
  * The data is ASCII, where Java and Python's `re` agree exactly, and each
  * pattern keeps a proper subset rather than all or none of the rows.
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
  private val textColumn = Seq(("a\"b\\c_text", AttributeType.STRING))
  private val caseRows: Seq[Seq[Any]] =
    Seq(Seq[Any]("abc"), Seq[Any]("ABC"), Seq[Any]("123"), Seq[Any]("a1B"), Seq[Any]("XY9"))

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val inputPath =
      CuratedHandlers.writeFixture(testRoot.resolve("input_port_0.jsonl"), textColumn, caseRows)
    (regexOp("a\"b\\c_text", "[a-z]+", caseInsensitive = false), Map(PortIdentity(0) -> inputPath))
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

    // A hole widens an integer column to float, so `^6$` meets "6.0" and selects
    // nothing unless the rendering reads the declared type.
    val numberDir = testRoot.resolve("numbers")
    Files.createDirectories(numberDir)
    // `a"b\c_big` only rides along. It is past the float64 exact-integer window,
    // and its column has a hole, so a reader that parses it through a float
    // hands the operator 9007199254740992 where the engine still has ...993.
    val numberColumn =
      Seq(("a\"b\\c_n", AttributeType.INTEGER), ("a\"b\\c_big", AttributeType.LONG))
    val numberRows: Seq[Seq[Any]] = Seq(
      Seq[Any](6, 9007199254740993L),
      Seq[Any](null, null),
      Seq[Any](7, 1L),
      Seq[Any](60, 2L),
      Seq[Any](16, 3L)
    )
    val numberInput = CuratedHandlers.writeFixture(
      numberDir.resolve("input_port_0.jsonl"),
      numberColumn,
      numberRows
    )

    // A boolean column. The engine matches "true", where a hole leaves pandas
    // holding 1.0. Case-sensitive on purpose, since case is half the difference.
    val boolDir = testRoot.resolve("booleans")
    Files.createDirectories(boolDir)
    val boolColumn = Seq(("a\"b\\c_b", AttributeType.BOOLEAN))
    val boolRows: Seq[Seq[Any]] =
      Seq(Seq[Any](true), Seq[Any](false), Seq[Any](null), Seq[Any](true))
    val boolInput =
      CuratedHandlers.writeFixture(boolDir.resolve("input_port_0.jsonl"), boolColumn, boolRows)

    Seq(
      (
        "regex=\\d+",
        regexOp("a\"b\\c_text", "\\d+", caseInsensitive = false),
        Map(PortIdentity(0) -> digitInput)
      ),
      (
        "regex=\\.",
        regexOp("a\"b\\c_text", "\\.", caseInsensitive = false),
        Map(PortIdentity(0) -> dotInput)
      ),
      (
        "regex=^6$ on an integer column",
        regexOp("a\"b\\c_n", "^6$", caseInsensitive = false),
        Map(PortIdentity(0) -> numberInput)
      ),
      (
        "regex=^true$ on a boolean column",
        regexOp("a\"b\\c_b", "^true$", caseInsensitive = false),
        Map(PortIdentity(0) -> boolInput)
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
  override def nullsKeepFilled: Option[Set[String]] = Some(Set("a\"b\\c_id"))

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val buildSchema = new Schema(
      new Attribute("a\"b\\c_id", AttributeType.INTEGER),
      new Attribute("name", AttributeType.STRING)
    )
    val probeSchema = new Schema(
      new Attribute("a\"b\\c_id", AttributeType.INTEGER),
      new Attribute("score", AttributeType.INTEGER)
    )

    def buildTup(id: Int, name: String): Tuple = {
      val b = Tuple.builder(buildSchema)
      b.add(buildSchema.getAttribute("a\"b\\c_id"), Int.box(id))
      b.add(buildSchema.getAttribute("name"), name)
      b.build()
    }
    def probeTup(id: Int, score: Int): Tuple = {
      val b = Tuple.builder(probeSchema)
      b.add(probeSchema.getAttribute("a\"b\\c_id"), Int.box(id))
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
    desc.buildAttributeName = "a\"b\\c_id"
    desc.probeAttributeName = "a\"b\\c_id"
    desc.joinType = JoinType.INNER

    (desc, Map(PortIdentity(0) -> buildPath, PortIdentity(1) -> probePath))
  }
}

/**
  * Handler for `TypeCastingOpDesc`. A blind sweep of `resultType` crashes Path A,
  * for the reason its `enumSweep` row in
  * [[TransformVerificationRunner.variantsNotRun]] gives. Each unit below instead
  * pairs a target type with a source column that type accepts, holding a value
  * that round-trips identically through JVM `AttributeTypeUtils` and through the
  * generated pandas.
  *
  * TIMESTAMP is left out: the two runtimes serialize one differently to JSONL,
  * native as an ISO string and pandas as epoch millis, so the comparator flags a
  * representation mismatch for an identical instant. That is a harness-wide gap
  * rather than a TypeCasting defect.
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
      ("int_to_bool", AttributeType.INTEGER), // 1/0            → BOOLEAN
      // A string is the only source a boolean reads differently on the two
      // sides: Python answers true for every non-empty one, the engine reads
      // the word and then the number. Text the engine refuses cannot go here,
      // since Path A would end before there is anything to compare; that half
      // is pinned in TypeCastingOpDescSpec.
      ("str_to_bool", AttributeType.STRING), // "true"/"0"     → BOOLEAN
      // The other direction, which the five above cannot ask: the engine writes
      // "true" where Python's str() writes "True".
      ("bool_to_str", AttributeType.BOOLEAN), // true/false     → STRING
      // Named by two units at once. `tupleCasting` takes a Map, so the second
      // replaces the first and casts the original value; running both would put
      // the column through a datetime on the way.
      ("twice_cast", AttributeType.STRING), // → TIMESTAMP, then → STRING
      // Long.toInt keeps the low 32 bits rather than raising or saturating, so
      // these straddle the Int bounds on purpose.
      ("lng_to_int", AttributeType.LONG), // beyond Int range → INTEGER
      // `new Timestamp(long)` reads the number as milliseconds; pd.to_datetime
      // defaults to nanoseconds and puts every one of these in 1970.
      ("ms_to_ts", AttributeType.LONG) // epoch millis → TIMESTAMP
    )
    // `str_to_int` carries the text NumberFormat reads and Python's int refuses:
    // a decimal point, a grouping comma, trailing letters.
    val rows = Seq(
      Seq[Any]("10", 1, 6, 11, 1, "true", true, "2024-01-07 00:00:00", 2147483648L, 1700000000000L),
      Seq[Any]("6.7", 2, 7, 12, 0, "false", false, "2024-03-15 08:30:00", 2147483647L, 0L),
      Seq[Any](
        "1,234",
        3,
        8,
        13,
        1,
        "0",
        true,
        "2024-06-01 12:00:00",
        -2147483649L,
        1000000000000L
      ),
      Seq[Any]("12abc", 4, 9, 14, 0, "1", false, "2024-09-20 23:59:59", 0L, 1893456000000L),
      Seq[Any]("-6.7", 5, 10, 15, 1, "TRUE", true, "2024-12-31 01:02:03", -1L, 946684800000L)
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
      unit("int_to_bool", AttributeType.BOOLEAN),
      unit("str_to_bool", AttributeType.BOOLEAN),
      unit("bool_to_str", AttributeType.STRING),
      unit("twice_cast", AttributeType.TIMESTAMP),
      unit("twice_cast", AttributeType.STRING),
      unit("lng_to_int", AttributeType.INTEGER),
      unit("ms_to_ts", AttributeType.TIMESTAMP)
    )

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/**
  * Handler for `KeywordSearchOpDesc`. The auto tier points `attribute` at the
  * canonical fixture's first column (`id`) and fills `keyword` with the canonical
  * "1", so the search runs against numeric ids and never touches a real text
  * column. This fixture searches a genuine free-text column with a two-term
  * query, so the branches both the JVM Lucene path and the pandas path have to
  * agree on, multi-term OR and whole-word boundaries, actually run.
  *
  * The rows are intentionally punctuation-free. Sweeping `isCaseSensitive` puts
  * the JVM's `CaseSensitiveAnalyzer` in play, a `WhitespaceTokenizer` that leaves
  * punctuation attached to the word ("perfect."), while the standalone regex
  * matches on `\b` boundaries and honours no case at all. Clean words keep the
  * two in agreement, which is why the canonical table's punctuated `short_text`
  * cannot be reused here, and why no Lucene phrase or wildcard syntax appears.
  */
object KeywordSearchTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[KeywordSearchOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val columns = Seq(("a\"b\\c_txt", AttributeType.STRING))
    val rows = Seq(
      Seq[Any]("i love this product"),
      Seq[Any]("what a great day"),
      Seq[Any]("terrible experience"),
      Seq[Any]("lovely weather today")
    )
    val inputPath =
      CuratedHandlers.writeFixture(testRoot.resolve("input_port_0.jsonl"), columns, rows)

    val desc = new KeywordSearchOpDesc()
    desc.attribute = "a\"b\\c_txt"
    desc.keyword = "love day"
    desc.isCaseSensitive = false

    (desc, Map(PortIdentity(0) -> inputPath))
  }

  override def extraScenarios(
      testRoot: Path
  ): Seq[(String, LogicalOp, Map[PortIdentity, Path])] = {
    // A hole widens an integer column to float and every value grows a ".0" the
    // term "0" then matches on a word boundary, so the rendering decides between
    // one row and all of them. A digit term cannot: `\b6\b` finds "6.0" too.
    val numberDir = testRoot.resolve("numbers")
    Files.createDirectories(numberDir)
    val numberColumn = Seq(("a\"b\\c_n", AttributeType.INTEGER))
    val numberRows: Seq[Seq[Any]] =
      Seq(Seq[Any](6), Seq[Any](null), Seq[Any](70), Seq[Any](0))
    val numberInput = CuratedHandlers.writeFixture(
      numberDir.resolve("input_port_0.jsonl"),
      numberColumn,
      numberRows
    )

    val desc = new KeywordSearchOpDesc()
    desc.attribute = "a\"b\\c_n"
    desc.keyword = "0"
    desc.isCaseSensitive = false

    // The base fixture is all lower-case, so sweeping the flag there decides
    // nothing: the two analyzers agree on every row it holds.
    val casedDir = testRoot.resolve("cased")
    Files.createDirectories(casedDir)
    val casedColumn = Seq(("a\"b\\c_txt", AttributeType.STRING))
    val casedRows: Seq[Seq[Any]] =
      Seq(Seq[Any]("i love this"), Seq[Any]("I Love this"), Seq[Any]("LOVE it"))
    val casedInput =
      CuratedHandlers.writeFixture(casedDir.resolve("input_port_0.jsonl"), casedColumn, casedRows)

    val cased = new KeywordSearchOpDesc()
    cased.attribute = "a\"b\\c_txt"
    cased.keyword = "Love"
    cased.isCaseSensitive = true

    Seq(
      ("keyword=0 on an integer column", desc, Map(PortIdentity(0) -> numberInput)),
      ("keyword=Love, case sensitive", cased, Map(PortIdentity(0) -> casedInput))
    )
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
    val schema = new Schema(new Attribute("a\"b\\c_image_bytes", AttributeType.BINARY))

    def tup(bytes: Array[Byte]): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("a\"b\\c_image_bytes"), bytes)
      builder.build()
    }

    val rows = Seq(
      tup(Array[Byte](1, 2, 3, 4)),
      tup(Array[Byte](10, 20, 30, 40))
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new ImageVisualizerOpDesc()
    desc.binaryContent = "a\"b\\c_image_bytes"

    (desc, Map(PortIdentity(0) -> inputPath))
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
  * COUNT(*) with its empty attribute. Auto-config cannot build it: `attribute` is
  * optional, required only for the functions other than count, so the generator
  * leaves it unset and any other function reaches the executor with no column to
  * read. This pins a valid (function, column) pair for each. The matching
  * `enumSweep` withholding is in [[TransformVerificationRunner.variantsNotRun]].
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

  /** CONCAT over a column that starts a group with an empty string.
    *
    * The accumulator earns its separator only once it holds something, so a
    * leading empty value contributes neither text nor comma. A null cannot ask
    * this: it reads as the empty string either way. Canonical has none.
    */
  override def extraScenarios(
      testRoot: Path
  ): Seq[(String, LogicalOp, Map[PortIdentity, Path])] = {
    val dir = testRoot.resolve("leading-empty")
    Files.createDirectories(dir)
    val columns = Seq(("a\"b\\c_grp", AttributeType.STRING), ("a\"b\\c_txt", AttributeType.STRING))
    val rows: Seq[Seq[Any]] = Seq(
      Seq[Any]("g", ""),
      Seq[Any]("g", "a"),
      Seq[Any]("g", ""),
      Seq[Any]("h", "x")
    )
    val input = CuratedHandlers.writeFixture(dir.resolve("input_port_0.jsonl"), columns, rows)

    val desc = new AggregateOpDesc()
    desc.groupByKeys = List("a\"b\\c_grp")
    desc.aggregations = List(agg(AggregationFunction.CONCAT, "a\"b\\c_txt", "joined"))

    Seq(("concat over a leading empty string", desc, Map(PortIdentity(0) -> input)))
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
