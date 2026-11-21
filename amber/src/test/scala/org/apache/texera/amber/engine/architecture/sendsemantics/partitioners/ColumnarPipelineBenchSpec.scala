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

package org.apache.texera.amber.engine.architecture.sendsemantics.partitioners

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.serialization.SerializationExtension
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.core.executor.ColumnarResult
import org.apache.texera.amber.core.tuple.{
  Attribute,
  AttributeType,
  Schema,
  SchemaEnforceable,
  Tuple
}
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.common.ambermessage.{VectorFrame, DataFrame}
import org.apache.texera.amber.operator.aggregate.{
  AggregateOpDesc,
  AggregateOpExec,
  AggregationFunction,
  AggregationOperation
}
import org.apache.texera.amber.operator.filter.{
  ComparisonType,
  FilterPredicate,
  SpecializedFilterOpDesc,
  SpecializedFilterOpExec
}
import org.apache.texera.amber.operator.projection.{
  AttributeUnit,
  ProjectionOpDesc,
  ProjectionOpExec
}
import org.apache.texera.amber.util.ArrowUtils
import org.apache.texera.amber.util.JSONUtils.{objectMapper => om}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.util.Random

/**
  * End-to-end cost of a columnar region vs the row path for a realistic three-operator
  * pipeline, project -> filter -> aggregate, with each operator on its own worker so the
  * batch crosses a serialized edge between them. The row path serializes a DataFrame and
  * runs each operator tuple-at-a-time; the columnar path serializes an Arrow VectorFrame
  * and runs each operator's processColumnarBatch (no per-tuple decode), staying columnar
  * across all three hops. Serialization uses the real Pekko/Kryo stack, so these numbers
  * reflect a cross-worker (cross-node) region, which is where the columnar wire is meant to
  * pay off. Run with:
  *   sbt -Dcolumnar.bench=true "WorkflowExecutionService/testOnly *ColumnarPipelineBenchSpec"
  */
class ColumnarPipelineBenchSpec extends AnyFlatSpecLike with BeforeAndAfterAll {

  // Built on first use so a disabled run (the default in CI) spins up no ActorSystem.
  private lazy val system = ActorSystem("ColumnarPipelineBench", AmberRuntime.pekkoConfig)
  private lazy val serde = SerializationExtension(system)
  private val rng = new Random(7)

  // Source schema: a shuffle/group key, a numeric filter column, and two string payload
  // columns the projection drops (so the row path pays to carry them to the projection).
  private val srcSchema: Schema = Schema(
    List(
      new Attribute("key", AttributeType.INTEGER),
      new Attribute("v", AttributeType.DOUBLE),
      new Attribute("p1", AttributeType.STRING),
      new Attribute("p2", AttributeType.STRING)
    )
  )
  // After the projection: only the key and the numeric column remain.
  private val projSchema: Schema =
    Schema(
      List(new Attribute("key", AttributeType.INTEGER), new Attribute("v", AttributeType.DOUBLE))
    )

  private def sourceRows(n: Int): Array[Tuple] =
    Array.tabulate(n) { _ =>
      Tuple
        .builder(srcSchema)
        .addSequentially(
          Array[Any](
            Int.box(rng.nextInt(100)),
            Double.box(rng.nextDouble() * 2 - 1), // roughly half pass v > 0
            rng.alphanumeric.take(16).mkString,
            rng.alphanumeric.take(16).mkString
          )
        )
        .build()
    }

  // ---- operators (one instance each; aggregate is reset per run via open()) ----------

  private lazy val projExec: ProjectionOpExec = {
    val d = new ProjectionOpDesc()
    d.isDrop = false
    d.attributes = List(new AttributeUnit("key", "key"), new AttributeUnit("v", "v"))
    val e = new ProjectionOpExec(om.writeValueAsString(d)); e.open(); e
  }

  private lazy val filterExec: SpecializedFilterOpExec = {
    val d = new SpecializedFilterOpDesc()
    d.predicates = List(new FilterPredicate("v", ComparisonType.GREATER_THAN, "0"))
    val e = new SpecializedFilterOpExec(om.writeValueAsString(d)); e.open(); e
  }

  private val aggJson: String = {
    val d = new AggregateOpDesc()
    val sum = new AggregationOperation()
    sum.aggFunction = AggregationFunction.SUM
    sum.attribute = "v"
    sum.resultAttribute = "sum_v"
    d.aggregations = List(sum)
    d.groupByKeys = List("key")
    om.writeValueAsString(d)
  }
  private lazy val aggExec: AggregateOpExec = new AggregateOpExec(aggJson)

  // ---- timing helpers ---------------------------------------------------------------

  private def median(xs: Seq[Double]): Double = { val s = xs.sorted; s(s.length / 2) }
  private def timeMs(body: => Unit): Double = {
    val t0 = System.nanoTime(); body; (System.nanoTime() - t0) / 1e6
  }
  private val warmups = 3
  private val trials = 7
  private def pct(a: Double, b: Double): String = f"${a / b}%.2fx"

  // Row region: each hop is a Kryo DataFrame round-trip, each operator runs per tuple.
  private def rowRun(rows: Array[Tuple]): Unit = {
    aggExec.open()
    // source -> project edge (carries the full wide rows, payload columns included).
    val srcRows =
      serde.deserialize(serde.serialize(DataFrame(rows)).get, classOf[DataFrame]).get.frame
    val projected = srcRows.map(t =>
      projExec.processTuple(t, 0).next().asInstanceOf[SchemaEnforceable].enforceSchema(projSchema)
    )
    val afterEdge1 =
      serde.deserialize(serde.serialize(DataFrame(projected)).get, classOf[DataFrame]).get.frame
    val filtered = afterEdge1.filter(filterExec.filterFunc)
    val afterEdge2 =
      serde.deserialize(serde.serialize(DataFrame(filtered)).get, classOf[DataFrame]).get.frame
    var i = 0
    while (i < afterEdge2.length) { aggExec.processTuple(afterEdge2(i), 0); i += 1 }
    aggExec.onFinish(0).foreach(_ => ())
  }

  // Columnar region: each hop is a Kryo VectorFrame round-trip, each operator runs
  // processColumnarBatch, and the batch stays Arrow the whole way.
  private def colRun(rows: Array[Tuple]): Unit = {
    aggExec.open()
    def hop(bytes: Array[Byte], rowCount: Int, schema: Schema): Array[Byte] =
      serde
        .deserialize(
          serde.serialize(VectorFrame(bytes, rowCount, schema)).get,
          classOf[VectorFrame]
        )
        .get
        .arrowIpcBytes
    // source -> project
    val srcWire = hop(ArrowUtils.serializeTuples(srcSchema, rows), rows.length, srcSchema)
    val projBytes = projExec.processColumnarBatch(srcWire, 0) match {
      case ColumnarResult.Emit(root) =>
        try ArrowUtils.serializeRoot(root)
        finally root.close()
      case other => fail(s"projection: expected Emit, got $other")
    }
    // project -> filter
    val filterIn = hop(projBytes, rows.length, projSchema)
    val (filterBytes, keptCount) = filterExec.processColumnarBatch(filterIn, 0) match {
      case ColumnarResult.Emit(root) =>
        try (ArrowUtils.serializeRoot(root), root.getRowCount)
        finally root.close()
      case other => fail(s"filter: expected Emit, got $other")
    }
    // filter -> aggregate
    val aggIn = hop(filterBytes, keptCount, projSchema)
    aggExec.processColumnarBatch(aggIn, 0)
    aggExec.onFinish(0).foreach(_ => ())
  }

  private def benchEnabled: Boolean = sys.props.get("columnar.bench").contains("true")

  "a columnar region (project -> filter -> aggregate)" should "beat the row path across serialized edges" in {
    assume(benchEnabled, "set -Dcolumnar.bench=true to run the pipeline benchmark")
    println(
      "\n===== Pipeline project->filter->aggregate, columnar region vs row (median of %d) ====="
        .format(trials)
    )
    println("%-10s | %12s %12s %8s".format("rows", "row ms", "columnar ms", "col/row"))
    for (n <- Seq(2000, 20000, 100000)) {
      val rows = sourceRows(n)
      for (_ <- 0 until warmups) { rowRun(rows); colRun(rows) }
      val rowMs = median((0 until trials).map(_ => timeMs(rowRun(rows))))
      val colMs = median((0 until trials).map(_ => timeMs(colRun(rows))))
      println("%-10d | %12.2f %12.2f %8s".format(n, rowMs, colMs, pct(colMs, rowMs)))
    }
    println("(col/row < 1.00x means the columnar region is faster end to end)\n")
    succeed
  }

  override def afterAll(): Unit = {
    // Only tear down what the benchmark actually built; otherwise the lazy fixtures
    // would be forced into existence just to close them.
    if (benchEnabled) {
      projExec.close(); filterExec.close(); aggExec.close()
      TestKit.shutdownActorSystem(system)
    }
    super.afterAll()
  }
}
