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
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.common.ambermessage.{VectorFrame, DataFrame}
import org.apache.texera.amber.util.ArrowUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import java.sql.Timestamp
import scala.util.Random

/**
  * Measures the per-batch wire cost of the two data-batch formats a cross-worker
  * link can use: the row path (a DataFrame of Tuples, serialized by the real
  * Pekko/Kryo stack) versus the columnar path (an Arrow IPC VectorFrame). For
  * each format it reports sender cost (encode), receiver cost (decode), and the
  * serialized payload size, so the numbers reflect what actually crosses the wire.
  *
  * This is a benchmark, not an assertion of a speedup (same-JVM links never
  * serialize, so the columnar wire's payoff is specifically on cross-worker links;
  * these numbers isolate that serialize/deserialize/size tradeoff). Run with:
  *   WorkflowExecutionService/testOnly *ColumnarWireBenchSpec
  */
class ColumnarWireBenchSpec
    extends TestKit(ActorSystem("ColumnarWireBench", AmberRuntime.pekkoConfig))
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  private val serde = SerializationExtension(system)

  private val rng = new Random(42)

  // ---- workloads -----------------------------------------------------------

  private def narrowNumeric(n: Int): (Schema, Array[Tuple]) = {
    val schema = Schema(
      List(new Attribute("a", AttributeType.INTEGER), new Attribute("b", AttributeType.DOUBLE))
    )
    val rows = Array.tabulate(n) { _ =>
      Tuple
        .builder(schema)
        .addSequentially(Array[Any](Int.box(rng.nextInt()), Double.box(rng.nextDouble())))
        .build()
    }
    (schema, rows)
  }

  private def wideMixed(n: Int): (Schema, Array[Tuple]) = {
    val schema = Schema(
      List(
        new Attribute("i", AttributeType.INTEGER),
        new Attribute("l", AttributeType.LONG),
        new Attribute("d", AttributeType.DOUBLE),
        new Attribute("b", AttributeType.BOOLEAN),
        new Attribute("t", AttributeType.TIMESTAMP),
        new Attribute("s", AttributeType.STRING)
      )
    )
    val rows = Array.tabulate(n) { i =>
      Tuple
        .builder(schema)
        .addSequentially(
          Array[Any](
            Int.box(rng.nextInt()),
            Long.box(rng.nextLong()),
            Double.box(rng.nextDouble()),
            Boolean.box(i % 2 == 0),
            new Timestamp(1_600_000_000_000L + i),
            s"row-$i-${rng.alphanumeric.take(8).mkString}"
          )
        )
        .build()
    }
    (schema, rows)
  }

  private def stringHeavy(n: Int): (Schema, Array[Tuple]) = {
    val schema = Schema(
      List(new Attribute("s1", AttributeType.STRING), new Attribute("s2", AttributeType.STRING))
    )
    val rows = Array.tabulate(n) { _ =>
      Tuple
        .builder(schema)
        .addSequentially(
          Array[Any](rng.alphanumeric.take(32).mkString, rng.alphanumeric.take(32).mkString)
        )
        .build()
    }
    (schema, rows)
  }

  // ---- timing --------------------------------------------------------------

  private def median(xs: Seq[Double]): Double = {
    val s = xs.sorted
    s(s.length / 2)
  }

  private def timeMs(body: => Unit): Double = {
    val t0 = System.nanoTime()
    body
    (System.nanoTime() - t0) / 1e6
  }

  private val warmups = 3
  private val trials = 7

  private case class Result(
      rowEncMs: Double,
      colEncMs: Double,
      rowDecMs: Double,
      colDecMs: Double,
      rowBytes: Int,
      colBytes: Int
  )

  private def measure(schema: Schema, rows: Array[Tuple]): Result = {
    // Row path: Pekko/Kryo over the DataFrame of tuples.
    def rowEncode(): Array[Byte] = serde.serialize(DataFrame(rows)).get
    def rowDecode(bytes: Array[Byte]): AnyRef =
      serde.deserialize(bytes, classOf[DataFrame]).get

    // Columnar path: Arrow-serialize the batch (as NetworkOutputBuffer does), then
    // Pekko/Kryo over the resulting VectorFrame (which just wraps the bytes).
    def colEncode(): Array[Byte] = {
      val arrow = ArrowUtils.serializeTuples(schema, rows)
      serde.serialize(VectorFrame(arrow, rows.length, schema)).get
    }
    def colDecode(bytes: Array[Byte]): AnyRef = {
      val cf = serde.deserialize(bytes, classOf[VectorFrame]).get
      ArrowUtils.deserializeTuples(cf.arrowIpcBytes)
    }

    for (_ <- 0 until warmups) { rowDecode(rowEncode()); colDecode(colEncode()) }

    val rowWire = rowEncode()
    val colWire = colEncode()
    val rowEnc = median((0 until trials).map(_ => timeMs(rowEncode())))
    val colEnc = median((0 until trials).map(_ => timeMs(colEncode())))
    val rowDec = median((0 until trials).map(_ => timeMs(rowDecode(rowWire))))
    val colDec = median((0 until trials).map(_ => timeMs(colDecode(colWire))))
    Result(rowEnc, colEnc, rowDec, colDec, rowWire.length, colWire.length)
  }

  private def pct(col: Double, row: Double): String = f"${col / row}%.2fx"

  // A benchmark, not a correctness gate: skipped unless -Dcolumnar.bench=true, so it
  // does not slow CI. Run it with:
  //   sbt -Dcolumnar.bench=true "WorkflowExecutionService/testOnly *ColumnarWireBenchSpec"
  private def benchEnabled: Boolean = sys.props.get("columnar.bench").contains("true")

  "columnar wire" should "report encode/decode/size vs the row wire" in {
    assume(benchEnabled, "set -Dcolumnar.bench=true to run the wire benchmark")
    val workloads: Seq[(String, Int => (Schema, Array[Tuple]))] = Seq(
      "narrowNumeric" -> narrowNumeric,
      "wideMixed" -> wideMixed,
      "stringHeavy" -> stringHeavy
    )
    val sizes = Seq(2000, 20000, 100000)

    println(
      "\n================ Columnar wire vs row wire (median of %d) ================".format(trials)
    )
    println(
      "%-14s %8s | %10s %10s %6s | %10s %10s %6s | %10s %10s %6s"
        .format(
          "workload",
          "rows",
          "rowEnc ms",
          "colEnc ms",
          "enc",
          "rowDec ms",
          "colDec ms",
          "dec",
          "rowKB",
          "colKB",
          "size"
        )
    )
    for ((name, build) <- workloads; n <- sizes) {
      val (schema, rows) = build(n)
      val r = measure(schema, rows)
      println(
        "%-14s %8d | %10.2f %10.2f %6s | %10.2f %10.2f %6s | %10.1f %10.1f %6s"
          .format(
            name,
            n,
            r.rowEncMs,
            r.colEncMs,
            pct(r.colEncMs, r.rowEncMs),
            r.rowDecMs,
            r.colDecMs,
            pct(r.colDecMs, r.rowDecMs),
            r.rowBytes / 1024.0,
            r.colBytes / 1024.0,
            pct(r.colBytes.toDouble, r.rowBytes.toDouble)
          )
      )
    }
    println("(enc/dec/size columns are columnar / row; < 1.00x means columnar is cheaper)\n")
    succeed
  }

  // The decisive case: a receiver that can VECTORIZE (a filter) skips the
  // decode-to-tuples entirely and reads the Arrow columns directly via
  // processColumnarBatch. This compares the full receive+process cost of the two
  // paths for a filter edge:
  //   row path: Kryo-deserialize the DataFrame, then run the per-tuple predicate.
  //   col path: Kryo-deserialize the VectorFrame, then processColumnarBatch
  //             (reads the Arrow column, builds the survivor mask, no tuple decode).
  "a vectorized receiver (filter)" should "show whether columnar beats row end to end" in {
    assume(benchEnabled, "set -Dcolumnar.bench=true to run the wire benchmark")
    import org.apache.texera.amber.operator.filter.{
      ComparisonType,
      FilterPredicate,
      SpecializedFilterOpExec,
      SpecializedFilterOpDesc
    }
    import org.apache.texera.amber.util.JSONUtils.{objectMapper => om}

    val schema =
      Schema(
        List(new Attribute("v", AttributeType.INTEGER), new Attribute("w", AttributeType.DOUBLE))
      )

    def filterExec(): SpecializedFilterOpExec = {
      val d = new SpecializedFilterOpDesc()
      d.predicates = List(new FilterPredicate("v", ComparisonType.GREATER_THAN, "0"))
      val e = new SpecializedFilterOpExec(om.writeValueAsString(d))
      e.open()
      e
    }

    println("\n========= Filter receive+process: columnar(vectorized) vs row =========")
    println("%-10s | %12s %14s %8s".format("rows", "row recv ms", "col recv ms", "col/row"))
    for (n <- Seq(2000, 20000, 100000)) {
      val rows = Array.tabulate(n)(_ =>
        Tuple
          .builder(schema)
          .addSequentially(
            Array[Any](Int.box(rng.nextInt(200) - 100), Double.box(rng.nextDouble()))
          )
          .build()
      )
      val rowWire = serde.serialize(DataFrame(rows)).get
      val colWire =
        serde.serialize(VectorFrame(ArrowUtils.serializeTuples(schema, rows), n, schema)).get

      val rowExec = filterExec()
      val colExec = filterExec()

      // Row receive: deserialize the batch, then run the predicate per tuple.
      def rowRecv(): Int = {
        val df = serde.deserialize(rowWire, classOf[DataFrame]).get
        var kept = 0
        val fr = df.frame
        var i = 0
        while (i < fr.length) { if (rowExec.filterFunc(fr(i))) kept += 1; i += 1 }
        kept
      }
      // Columnar receive: deserialize the frame, then vectorized filter (no decode).
      def colRecv(): Int = {
        val cf = serde.deserialize(colWire, classOf[VectorFrame]).get
        colExec.processColumnarBatch(cf.arrowIpcBytes, 0) match {
          case org.apache.texera.amber.core.executor.ColumnarResult.Emit(root) =>
            val c = root.getRowCount; root.close(); c
          case _ => 0
        }
      }

      for (_ <- 0 until warmups) { rowRecv(); colRecv() }
      val rowMs = median((0 until trials).map(_ => timeMs(rowRecv())))
      val colMs = median((0 until trials).map(_ => timeMs(colRecv())))
      println("%-10d | %12.2f %14.2f %8s".format(n, rowMs, colMs, pct(colMs, rowMs)))
      rowExec.close(); colExec.close()
    }
    println("(col/row < 1.00x means the vectorized columnar receiver is faster)\n")
    succeed
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }
}
