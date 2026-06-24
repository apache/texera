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
import org.apache.texera.amber.operator.aggregate.{
  AggregateOpDesc,
  AggregationFunction,
  AggregationOperation
}
import org.apache.texera.amber.operator.dictionary.{DictionaryMatcherOpDesc, MatchingType}
import org.apache.texera.amber.operator.difference.DifferenceOpDesc
import org.apache.texera.amber.operator.filter.{
  ComparisonType,
  FilterPredicate,
  SpecializedFilterOpDesc
}
import org.apache.texera.amber.operator.hashJoin.{HashJoinOpDesc, JoinType}
import org.apache.texera.amber.operator.intersect.IntersectOpDesc
import org.apache.texera.amber.operator.projection.{AttributeUnit, ProjectionOpDesc}
import org.apache.texera.amber.operator.sort.{SortCriteriaUnit, SortOpDesc, SortPreference}
import org.apache.texera.amber.operator.symmetricDifference.SymmetricDifferenceOpDesc
import org.apache.texera.amber.operator.visualization.DotPlot.DotPlotOpDesc
import org.apache.texera.amber.operator.visualization.ImageViz.ImageVisualizerOpDesc
import org.apache.texera.amber.operator.visualization.IcicleChart.IcicleChartOpDesc
import org.apache.texera.amber.operator.visualization.barChart.BarChartOpDesc
import org.apache.texera.amber.operator.visualization.boxViolinPlot.BoxViolinPlotOpDesc
import org.apache.texera.amber.operator.visualization.boxViolinPlot.BoxViolinPlotQuartileFunction
import org.apache.texera.amber.operator.visualization.bubbleChart.BubbleChartOpDesc
import org.apache.texera.amber.operator.visualization.bulletChart.{
  BulletChartOpDesc,
  BulletChartStepDefinition
}
import org.apache.texera.amber.operator.visualization.candlestickChart.CandlestickChartOpDesc
import org.apache.texera.amber.operator.visualization.carpetPlot.CarpetPlotOpDesc
import org.apache.texera.amber.operator.visualization.choroplethMap.ChoroplethMapOpDesc
import org.apache.texera.amber.operator.visualization.continuousErrorBands.{
  BandConfig,
  ContinuousErrorBandsOpDesc
}
import org.apache.texera.amber.operator.visualization.contourPlot.{
  ContourPlotColoringFunction,
  ContourPlotOpDesc
}
import org.apache.texera.amber.operator.visualization.dendrogram.DendrogramOpDesc
import org.apache.texera.amber.operator.visualization.lineChart.LineMode
import org.apache.texera.amber.operator.visualization.ScatterMatrixChart.ScatterMatrixChartOpDesc
import org.apache.texera.amber.operator.visualization.hierarchychart.HierarchySection

import java.nio.file.Path
import java.util

/**
  * A curated handler ships a configured OpDesc and the input fixtures it
  * needs, written once into `testRoot`. Register it in [[CuratedHandlers.all]]
  * to override the auto-config tier for that operator.
  */
trait TransformHandler {
  def opDescClass: Class[_ <: LogicalOp]
  def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path])
}

/**
  * The curated override tier of the config/fixture resolution chain: an
  * operator listed here is verified with its hand-written fixture instead of
  * the auto-generated one. This is also the seam where Xuan's curated
  * operator-field-values JSON plugs in later, as a second curated source.
  */
object CuratedHandlers {
  val all: Seq[TransformHandler] = Seq(
    SpecializedFilterTransformHandler,
    IntersectTransformHandler,
    DifferenceTransformHandler,
    SymmetricDifferenceTransformHandler,
    HashJoinTransformHandler,
    SortTransformHandler,
    AggregateTransformHandler,
    DictionaryMatcherTransformHandler,
    ProjectionTransformHandler,
    BarChartVisualizationHandler,
    BulletChartVisualizationHandler,
    CandlestickChartVisualizationHandler,
    CarpetPlotVisualizationHandler,
    ChoroplethMapVisualizationHandler,
    ContinuousErrorBandsVisualizationHandler,
    ContourPlotVisualizationHandler,
    DotPlotVisualizationHandler,
    IcicleChartVisualizationHandler,
    BubbleChartVisualizationHandler,
    BoxViolinPlotVisualizationHandler,
    ImageVisualizerVisualizationHandler,
    ScatterMatrixVisualizationHandler,
    DendrogramVisualizationHandler
  )

  val byClass: Map[Class[_ <: LogicalOp], TransformHandler] =
    all.map(h => h.opDescClass -> h).toMap
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

/**
  * Two-port (id INTEGER, name STRING) fixture shared by the set-op handlers.
  * 25 left rows × 31 right rows with deliberate overlap — large enough to
  * defeat the small-set / identity-hash bucket coincidences that a 3-row
  * fixture lets through, while still small enough to compare in milliseconds.
  */
private object SetOpFixture {
  val schema: Schema = new Schema(
    new Attribute("id", AttributeType.INTEGER),
    new Attribute("name", AttributeType.STRING)
  )

  private val names = Vector(
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
    "k", "l", "m", "n", "o", "p", "q", "r", "s", "t"
  )

  private def tup(id: Int, name: String): Tuple = {
    val b = Tuple.builder(schema)
    b.add(schema.getAttribute("id"), Int.box(id))
    b.add(schema.getAttribute("name"), name)
    b.build()
  }

  def writeLeftRight(testRoot: Path): Map[PortIdentity, Path] = {
    val leftPath = testRoot.resolve("input_port_0.jsonl")
    val rightPath = testRoot.resolve("input_port_1.jsonl")
    val left = (1 to 25).map(i => tup(i, names((i - 1) % names.size)))
    val right = (10 to 40).map(i => tup(i, names((i - 1) % names.size)))
    TupleIO.writeTuples(leftPath, left.iterator, schema)
    TupleIO.writeTuples(rightPath, right.iterator, schema)
    Map(PortIdentity(0) -> leftPath, PortIdentity(1) -> rightPath)
  }
}

/** Intersect: JVM keeps two `mutable.HashSet[Tuple]` and emits the
  *  intersection in bucket-iteration order. Row order isn't deterministic vs
  *  the pandas `concat + duplicated(keep="first")` path — order policy lives
  *  in [[TransformVerificationRunner.orderInsensitiveOps]]. */
object IntersectTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[IntersectOpDesc]
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) =
    (new IntersectOpDesc(), SetOpFixture.writeLeftRight(testRoot))
}

/** Difference: `leftHashSet.diff(rightHashSet).iterator` — same hash-bucket
  *  order divergence as Intersect. Order policy lives in
  *  [[TransformVerificationRunner.orderInsensitiveOps]]. */
object DifferenceTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[DifferenceOpDesc]
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) =
    (new DifferenceOpDesc(), SetOpFixture.writeLeftRight(testRoot))
}

/** SymmetricDifference: union of the two diffs, hash-set backed on both
  *  sides. Most divergent of the set ops in practice — small fixtures can
  *  accidentally pass, larger ones fail. Order policy lives in
  *  [[TransformVerificationRunner.orderInsensitiveOps]]. */
object SymmetricDifferenceTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SymmetricDifferenceOpDesc]
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) =
    (new SymmetricDifferenceOpDesc(), SetOpFixture.writeLeftRight(testRoot))
}

/** HashJoin INNER on `id`. Build (port 0) and probe (port 1) intentionally
  *  arrive in different id orders so any probe-major / left-major mismatch
  *  between the JVM emit and `pd.merge` shows up. Order policy lives in
  *  [[TransformVerificationRunner.orderInsensitiveOps]]. */
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

/** Aggregate: the harness runs the JVM path (getPhysicalPlan) before
  *  standalone codegen, and getPhysicalPlan mutates `aggregations` via
  *  getFinal, setting attribute := resultAttribute — auto-config's free-form
  *  resultAttribute "1" then leaks into the generated pandas as a column ref
  *  (KeyError). Choosing resultAttribute == attribute makes the mutation a
  *  no-op. Emit-order policy (hash-partition vs first-occurrence) lives in
  *  [[TransformVerificationRunner.orderInsensitiveOps]]. */
object AggregateTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[AggregateOpDesc]
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val desc = new AggregateOpDesc()
    val agg = new AggregationOperation()
    agg.aggFunction = AggregationFunction.SUM
    agg.attribute = "score"
    agg.resultAttribute = "score" // must equal attribute; see scaladoc
    desc.aggregations = List(agg)
    desc.groupByKeys = List("name")
    (desc, CanonicalFixture.writeInputs(testRoot, 1))
  }
}

/** DictionaryMatcher: auto-config autofills the canonical fixture's first
  *  column (`id`, INTEGER) into `attribute`, but DictionaryMatcherOpExec
  *  casts that field to String (ClassCastException). Point it at `name` with
  *  a dictionary matching a strict subset of rows. Map op — both paths keep
  *  input row order, so strict positional comparison holds. */
object DictionaryMatcherTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[DictionaryMatcherOpDesc]
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val desc = new DictionaryMatcherOpDesc()
    desc.dictionary = "alice,bob,zelda" // alice/bob recur in the fixture; zelda never matches
    desc.attribute = "name"
    desc.resultAttribute = "matched"
    desc.matchingType = MatchingType.SCANBASED
    (desc, CanonicalFixture.writeInputs(testRoot, 1))
  }
}

/** Projection: `attributes` carries no @JsonProperty, so the auto-config tier
  *  leaves it empty and ProjectionOpExec rejects the empty list
  *  (Preconditions.checkArgument). Keep two columns, renaming one, to
  *  exercise both select and alias. Map op — strict order holds. */
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

/**
  * Handler for `SortOpDesc`. Writes a tiny 4-row table with one integer and
  * one string column and sorts by the integer descending — touches both
  * column types and exercises the comparator on a non-trivial reordering.
  */
object SortTransformHandler extends TransformHandler {

  override val opDescClass: Class[_ <: LogicalOp] = classOf[SortOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("id", AttributeType.INTEGER),
      new Attribute("name", AttributeType.STRING)
    )

    val rows: Seq[Tuple] = Seq(
      tupleOf(schema, 3, "carol"),
      tupleOf(schema, 1, "alice"),
      tupleOf(schema, 4, "dave"),
      tupleOf(schema, 2, "bob")
    )

    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new SortOpDesc()
    val criterion = new SortCriteriaUnit()
    criterion.attributeName = "id"
    criterion.sortPreference = SortPreference.DESC
    desc.attributes = List(criterion)

    (desc, Map(PortIdentity(0) -> inputPath))
  }

  private def tupleOf(schema: Schema, id: Int, name: String): Tuple = {
    val builder = Tuple.builder(schema)
    builder.add(schema.getAttribute("id"), Int.box(id))
    builder.add(schema.getAttribute("name"), name)
    builder.build()
  }
}

/** BulletChart visualization fixture. Uses two numeric values so the runtime
  * path can render multiple bullet charts while the JSON comparator validates
  * the first Plotly payload. */
object BulletChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[BulletChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(new Attribute("actual", AttributeType.DOUBLE))

    def tup(actual: Double): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("actual"), actual)
      builder.build()
    }

    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, Seq(tup(82.0), tup(91.0)).iterator, schema)

    val steps = new util.ArrayList[BulletChartStepDefinition]()
    steps.add(new BulletChartStepDefinition("0", "70"))
    steps.add(new BulletChartStepDefinition("70", "100"))

    val desc = new BulletChartOpDesc()
    desc.value = "actual"
    desc.deltaReference = "85"
    desc.thresholdValue = "90"
    desc.steps = steps

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** Candlestick visualization fixture with deterministic OHLC rows. */
object CandlestickChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[CandlestickChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("date", AttributeType.STRING),
      new Attribute("open", AttributeType.DOUBLE),
      new Attribute("high", AttributeType.DOUBLE),
      new Attribute("low", AttributeType.DOUBLE),
      new Attribute("close", AttributeType.DOUBLE)
    )

    def tup(date: String, open: Double, high: Double, low: Double, close: Double): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("date"), date)
      builder.add(schema.getAttribute("open"), open)
      builder.add(schema.getAttribute("high"), high)
      builder.add(schema.getAttribute("low"), low)
      builder.add(schema.getAttribute("close"), close)
      builder.build()
    }

    val rows = Seq(
      tup("day-1", 100.0, 110.0, 95.0, 108.0),
      tup("day-2", 108.0, 112.0, 101.0, 104.0),
      tup("day-3", 104.0, 116.0, 103.0, 115.0)
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new CandlestickChartOpDesc()
    desc.date = "date"
    desc.open = "open"
    desc.high = "high"
    desc.low = "low"
    desc.close = "close"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** CarpetPlot visualization fixture using numeric parameter axes. */
object CarpetPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[CarpetPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("a", AttributeType.DOUBLE),
      new Attribute("b", AttributeType.DOUBLE),
      new Attribute("value", AttributeType.DOUBLE)
    )

    def tup(a: Double, b: Double, value: Double): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("a"), a)
      builder.add(schema.getAttribute("b"), b)
      builder.add(schema.getAttribute("value"), value)
      builder.build()
    }

    val rows = Seq(
      tup(0.0, 0.0, 1.0),
      tup(1.0, 0.0, 2.0),
      tup(0.0, 1.0, 3.0),
      tup(1.0, 1.0, 4.0)
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new CarpetPlotOpDesc()
    desc.a = "a"
    desc.b = "b"
    desc.y = "value"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** ChoroplethMap visualization fixture with ISO-3 country codes. */
object ChoroplethMapVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[ChoroplethMapOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("country", AttributeType.STRING),
      new Attribute("score", AttributeType.DOUBLE)
    )

    def tup(country: String, score: Double): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("country"), country)
      builder.add(schema.getAttribute("score"), score)
      builder.build()
    }

    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(
      inputPath,
      Seq(tup("USA", 10.0), tup("CAN", 7.5), tup("MEX", 6.0)).iterator,
      schema
    )

    val desc = new ChoroplethMapOpDesc()
    desc.locations = "country"
    desc.color = "score"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** ContinuousErrorBands fixture with one line and deterministic upper/lower
  * bounds. */
object ContinuousErrorBandsVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[ContinuousErrorBandsOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("x", AttributeType.DOUBLE),
      new Attribute("y", AttributeType.DOUBLE),
      new Attribute("upper", AttributeType.DOUBLE),
      new Attribute("lower", AttributeType.DOUBLE)
    )

    def tup(x: Double, y: Double, upper: Double, lower: Double): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("x"), x)
      builder.add(schema.getAttribute("y"), y)
      builder.add(schema.getAttribute("upper"), upper)
      builder.add(schema.getAttribute("lower"), lower)
      builder.build()
    }

    val rows = Seq(
      tup(1.0, 2.0, 2.4, 1.6),
      tup(2.0, 3.0, 3.6, 2.5),
      tup(3.0, 2.5, 3.0, 2.0),
      tup(4.0, 4.0, 4.8, 3.3)
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val band = new BandConfig()
    band.xValue = "x"
    band.yValue = "y"
    band.yUpper = "upper"
    band.yLower = "lower"
    band.mode = LineMode.LINE_WITH_DOTS
    band.name = "series"
    band.color = "#1f77b4"
    band.fillColor = "rgba(31, 119, 180, 0.2)"

    val bands = new util.ArrayList[BandConfig]()
    bands.add(band)

    val desc = new ContinuousErrorBandsOpDesc()
    desc.xLabel = "X Axis"
    desc.yLabel = "Y Axis"
    desc.bands = bands

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** ContourPlot visualization fixture. The 3x3 grid gives scipy cubic
  * interpolation enough non-collinear points to produce a stable surface. */
object ContourPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[ContourPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("x", AttributeType.DOUBLE),
      new Attribute("y", AttributeType.DOUBLE),
      new Attribute("z", AttributeType.DOUBLE)
    )

    def tup(x: Double, y: Double, z: Double): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("x"), x)
      builder.add(schema.getAttribute("y"), y)
      builder.add(schema.getAttribute("z"), z)
      builder.build()
    }

    val rows =
      for {
        x <- Seq(0.0, 1.0, 2.0)
        y <- Seq(0.0, 1.0, 2.0)
      } yield tup(x, y, x * x + y)
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new ContourPlotOpDesc()
    desc.x = "x"
    desc.y = "y"
    desc.z = "z"
    desc.gridSize = "3"
    desc.connectGaps = true
    desc.coloringMethod = ContourPlotColoringFunction.HEATMAP

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** BarChart visualization fixture. Covers categorical x values, numeric y
  * values, and optional color grouping. */
object BarChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[BarChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("title", AttributeType.STRING),
      new Attribute("runtime_min", AttributeType.INTEGER),
      new Attribute("genre", AttributeType.STRING)
    )

    def tup(title: String, runtime: Int, genre: String): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("title"), title)
      builder.add(schema.getAttribute("runtime_min"), Int.box(runtime))
      builder.add(schema.getAttribute("genre"), genre)
      builder.build()
    }

    val rows = Seq(
      tup("Silent Harbor", 85, "Horror"),
      tup("Neon Skies", 84, "Sci-Fi"),
      tup("The Last Signal", 178, "Drama"),
      tup("Paper Moons", 99, "Action")
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new BarChartOpDesc()
    desc.value = "runtime_min"
    desc.fields = "title"
    desc.categoryColumn = "genre"
    desc.horizontalOrientation = false

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** DotPlot visualization fixture. Uses a string category column with repeated
  * values so the grouped counts in the Plotly payload are stable and
  * non-trivial. */
object DotPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[DotPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(new Attribute("category", AttributeType.STRING))

    def tup(category: String): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("category"), category)
      builder.build()
    }

    val rows = Seq(
      tup("alpha"),
      tup("beta"),
      tup("alpha"),
      tup("gamma"),
      tup("beta"),
      tup("alpha")
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new DotPlotOpDesc()
    desc.countAttribute = "category"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** IcicleChart visualization fixture. Uses a three-level hierarchy and
  * positive numeric values so both Plotly paths produce a stable tree. */
object IcicleChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[IcicleChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("continent", AttributeType.STRING),
      new Attribute("country", AttributeType.STRING),
      new Attribute("city", AttributeType.STRING),
      new Attribute("sales", AttributeType.DOUBLE)
    )

    def tup(continent: String, country: String, city: String, sales: Double): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("continent"), continent)
      builder.add(schema.getAttribute("country"), country)
      builder.add(schema.getAttribute("city"), city)
      builder.add(schema.getAttribute("sales"), sales)
      builder.build()
    }

    val rows = Seq(
      tup("North America", "United States", "Irvine", 12.0),
      tup("North America", "United States", "Seattle", 8.0),
      tup("Europe", "Germany", "Berlin", 7.5),
      tup("Europe", "France", "Paris", 9.5)
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    def hierarchySection(attribute: String): HierarchySection = {
      val section = new HierarchySection()
      section.attributeName = attribute
      section
    }

    val desc = new IcicleChartOpDesc()
    desc.hierarchy = List(
      hierarchySection("continent"),
      hierarchySection("country"),
      hierarchySection("city")
    )
    desc.value = "sales"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** BubbleChart visualization fixture. Covers numeric x/y/size columns and the
  * optional color category path. */
object BubbleChartVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[BubbleChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("x", AttributeType.DOUBLE),
      new Attribute("y", AttributeType.DOUBLE),
      new Attribute("size", AttributeType.DOUBLE),
      new Attribute("category", AttributeType.STRING)
    )

    def tup(x: Double, y: Double, size: Double, category: String): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("x"), x)
      builder.add(schema.getAttribute("y"), y)
      builder.add(schema.getAttribute("size"), size)
      builder.add(schema.getAttribute("category"), category)
      builder.build()
    }

    val rows = Seq(
      tup(1.0, 4.0, 10.0, "alpha"),
      tup(2.0, 3.0, 18.0, "beta"),
      tup(3.0, 2.0, 25.0, "alpha"),
      tup(4.0, 1.0, 12.0, "gamma")
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new BubbleChartOpDesc()
    desc.xValue = "x"
    desc.yValue = "y"
    desc.zValue = "size"
    desc.enableColor = true
    desc.colorCategory = "category"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** Box/Violin Plot visualization fixture. Uses one numeric column with a
  * stable distribution so both the box and violin branches render
  * deterministically. */
object BoxViolinPlotVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[BoxViolinPlotOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(new Attribute("value", AttributeType.DOUBLE))

    def tup(value: Double): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("value"), value)
      builder.build()
    }

    val rows = Seq(
      tup(1.0),
      tup(2.0),
      tup(2.5),
      tup(4.0),
      tup(5.5),
      tup(7.0)
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new BoxViolinPlotOpDesc()
    desc.value = "value"
    desc.quartileType = BoxViolinPlotQuartileFunction.LINEAR
    desc.horizontalOrientation = false
    desc.violinPlot = true

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** ImageVisualizer fixture. Uses deterministic binary payloads; the operator
  * base64-encodes them into img tags. */
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

/** ScatterMatrix visualization fixture. Uses three numeric dimensions and a
  * stable categorical color column. */
object ScatterMatrixVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[ScatterMatrixChartOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("x", AttributeType.DOUBLE),
      new Attribute("y", AttributeType.DOUBLE),
      new Attribute("z", AttributeType.DOUBLE),
      new Attribute("group", AttributeType.STRING)
    )

    def tup(x: Double, y: Double, z: Double, group: String): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("x"), x)
      builder.add(schema.getAttribute("y"), y)
      builder.add(schema.getAttribute("z"), z)
      builder.add(schema.getAttribute("group"), group)
      builder.build()
    }

    val rows = Seq(
      tup(1.0, 4.0, 7.0, "alpha"),
      tup(2.0, 3.0, 6.0, "beta"),
      tup(3.0, 2.0, 5.0, "alpha"),
      tup(4.0, 1.0, 8.0, "gamma"),
      tup(5.0, 5.0, 9.0, "beta")
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new ScatterMatrixChartOpDesc()
    desc.selectedAttributes = List("x", "y", "z")
    desc.color = "group"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}

/** Dendrogram visualization fixture. Four 2-D points in two tight pairs give
  * scipy linkage a stable, non-degenerate hierarchy for Plotly JSON parity. */
object DendrogramVisualizationHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[DendrogramOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("x", AttributeType.DOUBLE),
      new Attribute("y", AttributeType.DOUBLE),
      new Attribute("label", AttributeType.STRING)
    )

    def tup(x: Double, y: Double, label: String): Tuple = {
      val builder = Tuple.builder(schema)
      builder.add(schema.getAttribute("x"), x)
      builder.add(schema.getAttribute("y"), y)
      builder.add(schema.getAttribute("label"), label)
      builder.build()
    }

    val rows = Seq(
      tup(1.0, 1.0, "alpha"),
      tup(1.1, 1.2, "beta"),
      tup(4.0, 4.0, "gamma"),
      tup(4.2, 4.1, "delta")
    )
    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new DendrogramOpDesc()
    desc.xVal = "x"
    desc.yVal = "y"
    desc.labels = "label"

    (desc, Map(PortIdentity(0) -> inputPath))
  }
}
