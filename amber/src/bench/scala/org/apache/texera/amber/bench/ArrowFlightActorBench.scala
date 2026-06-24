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

package org.apache.texera.amber.bench

import org.apache.pekko.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import org.apache.pekko.testkit.TestProbe
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  EmbeddedControlMessageIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{PhysicalLink, PortIdentity}
import org.apache.texera.amber.engine.architecture.common.WorkflowActor.{NetworkAck, NetworkMessage}
import org.apache.texera.amber.engine.architecture.pythonworker.PythonWorkflowWorker
import org.apache.texera.amber.engine.architecture.rpc.controlcommands._
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.ReturnInvocation
import org.apache.texera.amber.engine.architecture.scheduling.config.WorkerConfig
import org.apache.texera.amber.engine.architecture.sendsemantics.partitionings.OneToOnePartitioning
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.common.ambermessage.{DataFrame, WorkflowFIFOMessage}
import org.apache.texera.amber.engine.common.ambermessage.WorkflowMessage.getInMemSize
import org.apache.texera.amber.util.VirtualIdentityUtils

import java.io.PrintWriter
import java.nio.file.{Files, Paths}
import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * End-to-end micro-benchmark of the real Arrow Flight data path through a
  * live PythonWorkflowWorker actor.
  *
  * Each measured config spawns a fresh PythonWorkflowWorker (real Pekko actor,
  * real Python subprocess via texera_run_python_worker.py, real Arrow Flight
  * transport), wires up an identity Python UDF, and times the round-trip of
  * `numBatches` DataFrames send→echo through the actor mailbox.
  *
  * Output (rewritten incrementally after every config so a killed sweep
  * still preserves usable data):
  *   - stdout summary per config
  *   - bench-results/arrow-flight-e2e.csv               (one row per config)
  *   - bench-results/arrow-flight-e2e-throughput.json   (github-action-benchmark customBiggerIsBetter)
  *   - bench-results/arrow-flight-e2e-latency.json      (github-action-benchmark customSmallerIsBetter)
  *
  * Run with:
  *   sbt "WorkflowExecutionService/Test/runMain \
  *     org.apache.texera.amber.bench.ArrowFlightActorBench"
  *
  * Caveat: `Utils.amberHomePath` does a `Files.walk(cwd, 2).findAny` for any
  * dir ending in `amber`. If `.claude/amber/` exists locally, the Python
  * subprocess may end up trying to launch from that path; move it aside for
  * the run, or fix `amberHomePath` upstream.
  */
object ArrowFlightActorBench {

  // ---------------------------------------------------------------------------
  // Identity Python UDF — passes input tuples straight through to output.
  // ---------------------------------------------------------------------------
  private val IdentityPythonCode: String =
    """from pytexera import *
      |
      |class ProcessTupleOperator(UDFOperatorV2):
      |    @overrides
      |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
      |        yield tuple_
      |""".stripMargin

  private val WorkflowId = WorkflowIdentity(1L)
  private val InputPortId = PortIdentity(id = 0, internal = false)
  private val OutputPortId = PortIdentity(id = 0, internal = false)

  // Sweep grid + iteration counts switch on BENCH_MODE so PR / post-merge
  // checks stay around 5 min while scheduled / manual runs do the full
  // 27-config grid that the gh-pages dashboard tracks long-term.
  //   pr   — 3 configs × 20 batches, warmup 5  (~4-5 min in CI)
  //   full — 27 configs × 200 batches, warmup 20  (~40 min in CI)
  // The batchSize=10000 row was dropped from the full grid: its 9 configs
  // (3 schemaWidths x 3 stringLens) ran 30-70 min EACH, pushing the daily
  // run past GitHub's 6 h job ceiling so it timed out before publishing to
  // gh-pages. The remaining 10/100/1000 rows are ~10-1000x cheaper per
  // batch, keeping the full sweep well under an hour.
  // BENCH_NUM_BATCHES, if set, overrides numBatches for the current mode
  // (useful for local smoke).
  private val BenchMode: String = sys.env.getOrElse("BENCH_MODE", "full").toLowerCase

  private case class GridSpec(
      batchSizes: Seq[Int],
      schemaWidths: Seq[Int],
      stringLens: Seq[Int],
      numBatches: Int,
      warmupBatches: Int
  )

  private val grid: GridSpec = BenchMode match {
    case "pr" =>
      GridSpec(
        batchSizes = Seq(10, 100, 1000),
        schemaWidths = Seq(10),
        stringLens = Seq(64),
        numBatches = sys.env.get("BENCH_NUM_BATCHES").map(_.toInt).getOrElse(20),
        warmupBatches = 5
      )
    case _ =>
      GridSpec(
        batchSizes = Seq(10, 100, 1000),
        schemaWidths = Seq(1, 10, 50),
        stringLens = Seq(8, 64, 512),
        numBatches = sys.env.get("BENCH_NUM_BATCHES").map(_.toInt).getOrElse(200),
        warmupBatches = 20
      )
  }

  private val DefaultBatchSizes: Seq[Int] = grid.batchSizes
  private val DefaultSchemaWidths: Seq[Int] = grid.schemaWidths
  private val DefaultStringLens: Seq[Int] = grid.stringLens
  private val DefaultNumBatches: Int = grid.numBatches
  private val WarmupBatches: Int = grid.warmupBatches

  // All artifacts land under bench-results/ so CI can artifact-upload the
  // whole directory uniformly without knowing individual filenames beyond
  // what its publish matrix declares.
  // Conventions for new benches:
  //   bench-results/<bench-name>-throughput.json  → customBiggerIsBetter
  //   bench-results/<bench-name>-latency.json     → customSmallerIsBetter
  //   bench-results/<bench-name>-jmh.json         → tool=jmh
  private val OutDir = Paths.get("bench-results")
  private val CsvOutPath = OutDir.resolve("arrow-flight-e2e.csv")
  // Two JSON files — github-action-benchmark needs distinct
  // customBiggerIsBetter / customSmallerIsBetter inputs since each upload
  // direction is per-`tool` parameter.
  private val ThroughputJsonPath = OutDir.resolve("arrow-flight-e2e-throughput.json")
  private val LatencyJsonPath = OutDir.resolve("arrow-flight-e2e-latency.json")

  // ---------------------------------------------------------------------------
  // Sink actor: stands in for the downstream worker. Auto-acks every
  // NetworkMessage from the worker (otherwise PekkoMessageTransferService
  // throttles after the first unacked reply and the bench stalls), and
  // forwards every received message to the bench probe for inspection.
  // ---------------------------------------------------------------------------
  private class AutoAckSink(forwardTo: ActorRef) extends Actor {
    override def receive: Receive = {
      case msg @ NetworkMessage(id, internal) =>
        sender() ! NetworkAck(id, getInMemSize(internal), 0L)
        forwardTo ! msg
      case other =>
        forwardTo ! other
    }
  }

  private case class BenchConfig(
      configIdx: Int,
      batchSize: Int,
      schemaWidth: Int,
      stringLen: Int,
      numBatches: Int
  )

  private case class BenchResult(
      cfg: BenchConfig,
      totalWallNs: Long,
      totalTuples: Long,
      totalBytesApprox: Long,
      latencyP50Ns: Long,
      latencyP95Ns: Long,
      latencyP99Ns: Long
  ) {
    def tuplesPerSec: Double = totalTuples * 1e9 / totalWallNs
    def mbPerSec: Double = totalBytesApprox * 1e9 / totalWallNs / (1024.0 * 1024.0)
  }

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("arrow-flight-bench", AmberRuntime.pekkoConfig)
    system.actorOf(Props[SingleNodeListener](), "cluster-info")

    val configs: Seq[BenchConfig] = (for {
      sw <- DefaultSchemaWidths
      sl <- DefaultStringLens
      bs <- DefaultBatchSizes
    } yield (sw, sl, bs)).zipWithIndex.map {
      case ((sw, sl, bs), idx) =>
        BenchConfig(
          idx,
          batchSize = bs,
          schemaWidth = sw,
          stringLen = sl,
          numBatches = DefaultNumBatches
        )
    }

    println(s"[bench] sweeping ${configs.size} configurations × ${DefaultNumBatches} batches each")
    // Pre-create output dir + rewrite the result files after every completed
    // config so a killed / timed-out sweep still leaves a usable artifact.
    Files.createDirectories(OutDir)
    val resultsBuf = scala.collection.mutable.ArrayBuffer.empty[BenchResult]
    configs.foreach { cfg =>
      try {
        val r = runConfig(system, cfg)
        resultsBuf += r
        writeCsv(resultsBuf.toSeq)
        writeJsonForGitHubActionBenchmark(resultsBuf.toSeq)
      } catch {
        case t: Throwable =>
          println(s"[bench] FAILED config #${cfg.configIdx} ($cfg): $t")
      }
    }
    printSummary(resultsBuf.toSeq)
    Await.result(system.terminate(), 30.seconds)
  }

  // ---------------------------------------------------------------------------
  // One configuration: spawn fresh worker, run warmup + timed loop, tear down.
  // ---------------------------------------------------------------------------
  private def runConfig(system: ActorSystem, cfg: BenchConfig): BenchResult = {
    val workerId =
      VirtualIdentityUtils.createWorkerIdentity(WorkflowId, "bench", "main", cfg.configIdx)
    val downstreamId =
      VirtualIdentityUtils.createWorkerIdentity(WorkflowId, "benchsink", "main", cfg.configIdx)

    val ctlChannelIn = ChannelIdentity(downstreamId, workerId, isControl = true)
    val dataChannelIn = ChannelIdentity(downstreamId, workerId, isControl = false)
    val dataChannelOut = ChannelIdentity(workerId, downstreamId, isControl = false)

    val probe = TestProbe()(system)
    val sink = system.actorOf(
      Props(new AutoAckSink(probe.ref)),
      name = s"bench-sink-${cfg.configIdx}"
    )
    val worker = system.actorOf(
      PythonWorkflowWorker.props(WorkerConfig(workerId)),
      name = s"bench-worker-${cfg.configIdx}"
    )

    println(s"\n[bench] config #${cfg.configIdx}: $cfg")

    try {
      val schema = makeSchema(cfg.schemaWidth)
      val schemaMap = schema.getAttributes.map(a => a.getName -> a.getType.name()).toMap

      var ctlSeq: Long = 0L
      var dataSeq: Long = 0L
      var msgId: Long = 0L

      def sendCtl(payload: ControlInvocation): Unit = {
        val fifo = WorkflowFIFOMessage(ctlChannelIn, ctlSeq, payload)
        ctlSeq += 1
        worker.tell(NetworkMessage(msgId, fifo), sink)
        msgId += 1
      }
      def sendOnDataChannel(
          payload: org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessagePayload
      ): Unit = {
        val fifo = WorkflowFIFOMessage(dataChannelIn, dataSeq, payload)
        dataSeq += 1
        worker.tell(NetworkMessage(msgId, fifo), sink)
        msgId += 1
      }

      // -----------------------------------------------------------------------
      // Setup control sequence + StartChannel ECM (see Pass 1 for details).
      // -----------------------------------------------------------------------
      val ctx = AsyncRPCContext(sender = downstreamId, receiver = workerId)
      sendCtl(
        ControlInvocation(
          "InitializeExecutor",
          InitializeExecutorRequest(
            1,
            OpExecWithCode(IdentityPythonCode, "python"),
            isSource = false
          ),
          ctx,
          0L
        )
      )
      sendCtl(
        ControlInvocation(
          "AssignPort",
          AssignPortRequest(InputPortId, input = true, schemaMap, Seq.empty, Seq.empty),
          ctx,
          1L
        )
      )
      sendCtl(
        ControlInvocation(
          "AssignPort",
          AssignPortRequest(OutputPortId, input = false, schemaMap, Seq.empty, Seq.empty),
          ctx,
          2L
        )
      )
      sendCtl(
        ControlInvocation(
          "AddInputChannel",
          AddInputChannelRequest(dataChannelIn, InputPortId),
          ctx,
          3L
        )
      )
      val outLink = PhysicalLink(
        fromOpId = VirtualIdentityUtils.getPhysicalOpId(workerId),
        fromPortId = OutputPortId,
        toOpId = VirtualIdentityUtils.getPhysicalOpId(downstreamId),
        toPortId = InputPortId
      )
      sendCtl(
        ControlInvocation(
          "AddPartitioning",
          AddPartitioningRequest(
            outLink,
            // batch_size = cfg.batchSize keeps the Python-side partitioning
            // buffer aligned with our send size — one Java DataFrame in maps
            // to exactly one Python DataFrame out.
            OneToOnePartitioning(batchSize = cfg.batchSize, channels = Seq(dataChannelOut))
          ),
          ctx,
          4L
        )
      )
      sendCtl(ControlInvocation("OpenExecutor", EmptyRequest(), ctx, 5L))
      sendCtl(ControlInvocation("StartWorker", EmptyRequest(), ctx, 6L))

      waitForReturns(probe, 7, 60.seconds)

      // StartChannel ECM enables data flow on the input channel.
      val startChannelEcm = EmbeddedControlMessage(
        id = EmbeddedControlMessageIdentity("StartChannel"),
        ecmType = EmbeddedControlMessageType.NO_ALIGNMENT,
        scope = Seq.empty,
        commandMapping = Map(
          workerId.name -> ControlInvocation(
            "StartChannel",
            EmptyRequest(),
            AsyncRPCContext(ActorVirtualIdentity(""), ActorVirtualIdentity("")),
            -1L
          )
        )
      )
      sendOnDataChannel(startChannelEcm)
      // Drain the StartChannel-echo the worker forwards downstream so it
      // doesn't show up in the data-loop's measurement window.
      drainNonDataFor(probe, 2.seconds)

      // -----------------------------------------------------------------------
      // Build sample tuples once; reuse across all batches in this config.
      // -----------------------------------------------------------------------
      val sampleBatch: Array[Tuple] = buildBatch(schema, cfg.batchSize, cfg.stringLen)
      val approxBytesPerBatch: Long =
        cfg.batchSize.toLong * cfg.schemaWidth.toLong * cfg.stringLen.toLong

      // Warmup — let JIT settle, Python import any lazy modules.
      var warmedBatches = 0
      while (warmedBatches < WarmupBatches) {
        sendOnDataChannel(DataFrame(sampleBatch))
        if (awaitOneDataFrameEcho(probe, 30.seconds)) warmedBatches += 1
      }

      // -----------------------------------------------------------------------
      // Timed loop — per-batch latency from send to corresponding echo.
      // Because the Python pipeline is FIFO, sending batch i then awaiting
      // exactly one DataFrame echo gives latency_i = receive_i - send_i.
      // -----------------------------------------------------------------------
      val latencies = new Array[Long](cfg.numBatches)
      val totalStart = System.nanoTime()
      var i = 0
      while (i < cfg.numBatches) {
        val t0 = System.nanoTime()
        sendOnDataChannel(DataFrame(sampleBatch))
        if (!awaitOneDataFrameEcho(probe, 60.seconds)) {
          throw new RuntimeException(s"timed out waiting for echo of batch $i")
        }
        latencies(i) = System.nanoTime() - t0
        i += 1
      }
      val totalNs = System.nanoTime() - totalStart

      val totalTuples = cfg.numBatches.toLong * cfg.batchSize.toLong
      val totalBytes = cfg.numBatches.toLong * approxBytesPerBatch
      val result = BenchResult(
        cfg,
        totalWallNs = totalNs,
        totalTuples = totalTuples,
        totalBytesApprox = totalBytes,
        latencyP50Ns = percentile(latencies, 0.50),
        latencyP95Ns = percentile(latencies, 0.95),
        latencyP99Ns = percentile(latencies, 0.99)
      )

      printOne(result)
      result
    } finally {
      worker ! PoisonPill
      sink ! PoisonPill
      // Give the worker a moment to tear down its Python subprocess + flight
      // server cleanly before we move to the next config.
      Thread.sleep(500)
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------
  private def makeSchema(width: Int): Schema = {
    val attrs = (0 until width).map(i => new Attribute(s"col$i", AttributeType.STRING))
    Schema(attrs.toList)
  }

  private def buildBatch(schema: Schema, batchSize: Int, stringLen: Int): Array[Tuple] = {
    val arr = new Array[Tuple](batchSize)
    val sampleVal = "x" * stringLen
    var i = 0
    val attrs = schema.getAttributes
    while (i < batchSize) {
      val b = Tuple.builder(schema)
      var j = 0
      while (j < attrs.size) {
        b.add(attrs(j), sampleVal)
        j += 1
      }
      arr(i) = b.build()
      i += 1
    }
    arr
  }

  private def waitForReturns(probe: TestProbe, n: Int, timeout: FiniteDuration): Unit = {
    val deadline = System.currentTimeMillis() + timeout.toMillis
    var seen = 0
    while (seen < n && System.currentTimeMillis() < deadline) {
      probe.receiveOne(2.seconds) match {
        case NetworkMessage(_, WorkflowFIFOMessage(_, _, _: ReturnInvocation)) =>
          seen += 1
        case _ => // ignore acks + other
      }
    }
    if (seen < n) {
      throw new RuntimeException(s"only $seen/$n control returns within $timeout")
    }
  }

  private def awaitOneDataFrameEcho(probe: TestProbe, timeout: FiniteDuration): Boolean = {
    // Each iteration uses the *remaining* time, not the full timeout — so a
    // flood of ACK / ECM messages can't extend the overall wait beyond the
    // caller's deadline by `timeout` × N.
    val deadline = System.nanoTime() + timeout.toNanos
    while (true) {
      val remainingNs = deadline - System.nanoTime()
      if (remainingNs <= 0) return false
      probe.receiveOne(remainingNs.nanos) match {
        case NetworkMessage(_, WorkflowFIFOMessage(_, _, _: DataFrame)) => return true
        case null                                                       => return false
        case _                                                          => // ignore acks, ECM forwards; loop
      }
    }
    false
  }

  private def drainNonDataFor(probe: TestProbe, dur: FiniteDuration): Unit = {
    val end = System.currentTimeMillis() + dur.toMillis
    while (System.currentTimeMillis() < end) {
      probe.receiveOne(200.millis) match {
        case null => return
        case _    => // discard
      }
    }
  }

  private def percentile(values: Array[Long], q: Double): Long = {
    if (values.isEmpty) 0L
    else {
      val sorted = values.sorted
      val idx = math.min(sorted.length - 1, math.max(0, (sorted.length * q).toInt))
      sorted(idx)
    }
  }

  private def printOne(r: BenchResult): Unit = {
    val ms = r.totalWallNs / 1e6
    println(
      f"  -> total=${ms}%.0fms  tuples/s=${r.tuplesPerSec}%,.0f  MB/s=${r.mbPerSec}%.2f  " +
        f"p50=${r.latencyP50Ns / 1000.0}%.1fus  p95=${r.latencyP95Ns / 1000.0}%.1fus  " +
        f"p99=${r.latencyP99Ns / 1000.0}%.1fus"
    )
  }

  private def writeCsv(results: Seq[BenchResult]): Unit = {
    val pw = new PrintWriter(Files.newBufferedWriter(CsvOutPath))
    try {
      pw.println(
        "config_idx,batch_size,schema_width,string_len,num_batches," +
          "total_ms,total_tuples,total_bytes,tuples_per_sec,mb_per_sec," +
          "lat_p50_us,lat_p95_us,lat_p99_us"
      )
      results.foreach { r =>
        val c = r.cfg
        pw.println(
          List(
            c.configIdx,
            c.batchSize,
            c.schemaWidth,
            c.stringLen,
            c.numBatches,
            f"${r.totalWallNs / 1e6}%.2f",
            r.totalTuples,
            r.totalBytesApprox,
            f"${r.tuplesPerSec}%.0f",
            f"${r.mbPerSec}%.3f",
            f"${r.latencyP50Ns / 1000.0}%.2f",
            f"${r.latencyP95Ns / 1000.0}%.2f",
            f"${r.latencyP99Ns / 1000.0}%.2f"
          ).mkString(",")
        )
      }
    } finally pw.close()
    println(s"\n[bench] wrote ${results.size} rows to ${CsvOutPath.toAbsolutePath}")
  }

  /**
    * Emit two JSON arrays per github-action-benchmark's customBiggerIsBetter
    * (throughput) and customSmallerIsBetter (latency) input formats. Each
    * config produces one throughput entry and three latency entries (p50/p95/
    * p99). Spec: https://github.com/benchmark-action/github-action-benchmark
    */
  private def writeJsonForGitHubActionBenchmark(results: Seq[BenchResult]): Unit = {
    def cfgLabel(c: BenchConfig): String =
      s"bs=${c.batchSize} sw=${c.schemaWidth} sl=${c.stringLen}"

    def jsonEntry(name: String, unit: String, value: Double): String =
      s"""  { "name": ${quoteJson(name)}, "unit": ${quoteJson(unit)}, "value": $value }"""

    val throughputEntries = results.map { r =>
      jsonEntry(s"throughput / ${cfgLabel(r.cfg)}", "tuples/sec", r.tuplesPerSec)
    }
    val latencyEntries = results.flatMap { r =>
      Seq(
        jsonEntry(s"latency p50 / ${cfgLabel(r.cfg)}", "us", r.latencyP50Ns / 1000.0),
        jsonEntry(s"latency p95 / ${cfgLabel(r.cfg)}", "us", r.latencyP95Ns / 1000.0),
        jsonEntry(s"latency p99 / ${cfgLabel(r.cfg)}", "us", r.latencyP99Ns / 1000.0)
      )
    }

    writeJsonArray(ThroughputJsonPath, throughputEntries)
    writeJsonArray(LatencyJsonPath, latencyEntries)
    println(
      s"[bench] wrote ${results.size} throughput entries to ${ThroughputJsonPath.toAbsolutePath}"
    )
    println(
      s"[bench] wrote ${latencyEntries.size} latency entries to ${LatencyJsonPath.toAbsolutePath}"
    )
  }

  private def writeJsonArray(path: java.nio.file.Path, entries: Seq[String]): Unit = {
    val pw = new PrintWriter(Files.newBufferedWriter(path))
    try {
      pw.println("[")
      pw.println(entries.mkString(",\n"))
      pw.println("]")
    } finally pw.close()
  }

  private def quoteJson(s: String): String =
    "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

  private def printSummary(results: Seq[BenchResult]): Unit = {
    println("\n[bench] === summary ===")
    println(
      f"${"#"}%3s  ${"bs"}%5s  ${"sw"}%3s  ${"sl"}%4s  ${"tuples/s"}%10s  ${"MB/s"}%7s  " +
        f"${"p50us"}%8s  ${"p99us"}%8s"
    )
    results.foreach { r =>
      println(
        f"${r.cfg.configIdx}%3d  ${r.cfg.batchSize}%5d  ${r.cfg.schemaWidth}%3d  ${r.cfg.stringLen}%4d  " +
          f"${r.tuplesPerSec}%,10.0f  ${r.mbPerSec}%7.2f  " +
          f"${r.latencyP50Ns / 1000.0}%8.1f  ${r.latencyP99Ns / 1000.0}%8.1f"
      )
    }
  }
}
