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

package org.apache.texera.amber.engine.e2e

import com.twitter.util.{Await, Duration, Promise, Return, Throw}
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorConfig,
  ExecutionStateUpdate,
  FatalError,
  Workflow
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  ConsoleMessage,
  ConsoleMessageType,
  EmptyRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.COMPLETED
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.common.client.AmberClient
import org.apache.texera.amber.engine.e2e.TestUtils._
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.audio.AudioAttributeExtractorOpDesc
import org.apache.texera.amber.operator.source.scan.file.FileScanSourceOpDesc
import org.apache.texera.amber.tags.IntegrationTest
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.common.compiler.model.LogicalLink
import org.apache.texera.web.service.ConsoleMessageProcessor
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpecLike

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets.US_ASCII
import java.nio.file.{Files, Path, Paths}
import java.util.Base64
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.jdk.CollectionConverters._
import scala.util.Using

/** Real compiler, File Scan, Arrow transport and Python worker; fixtures use an isolated test DB. */
@IntegrationTest
class AudioAttributeExtractorIntegrationSpec
    extends TestKit(ActorSystem("AudioAttributeExtractorIntegrationSpec", AmberRuntime.pekkoConfig))
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach {
  private val rate = 22050
  private var executionId = 637620
  private var directory: Path = _
  private var tone: Path = _
  private var clicks: Path = _
  private val toneSamples = Array.tabulate(rate * 5)(i =>
    if (i < rate * 4) 0.5 * math.sin(2 * math.Pi * 440 * i / rate) else 0.0
  )

  override def beforeAll(): Unit = {
    directory =
      Files.createTempDirectory(Files.createDirectories(Paths.get("target")), "audio-e2e-")
    tone = writeWav("tone.wav", toneSamples)
    val samples = Array.fill(rate * 8)(0.0)
    for (beat <- 1 to 15; i <- 0 until math.round(rate * 0.01).toInt) {
      samples(math.round(beat * 0.5 * rate).toInt + i) =
        0.8 * math.sin(2 * math.Pi * 1000 * i / rate) * (1 - i / math.round(rate * 0.01).toDouble)
    }
    clicks = writeWav("clicks.wav", samples)
    initiateTexeraDBForTestCases()
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
  }

  override protected def beforeEach(): Unit = {
    executionId += 1
    setUpWorkflowExecutionData(executionId)
  }

  override protected def afterEach(): Unit = cleanupWorkflowExecutionData(executionId)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    Option(directory).foreach(dir =>
      Using.resource(Files.walk(dir))(_.iterator().asScala.toSeq.reverse.foreach(Files.delete))
    )
  }

  private def writeWav(name: String, samples: Array[Double]): Path = {
    val buffer = ByteBuffer.allocate(44 + samples.length * 2).order(ByteOrder.LITTLE_ENDIAN)
    buffer.put("RIFF".getBytes(US_ASCII)).putInt(36 + samples.length * 2)
    buffer.put("WAVEfmt ".getBytes(US_ASCII)).putInt(16).putShort(1.toShort).putShort(1.toShort)
    buffer.putInt(rate).putInt(rate * 2).putShort(2.toShort).putShort(16.toShort)
    buffer.put("data".getBytes(US_ASCII)).putInt(samples.length * 2)
    samples.foreach(sample => buffer.putShort((sample * 32767).toShort))
    Files.write(directory.resolve(name), buffer.array())
  }

  private def audioWorkflow(
      path: Path,
      mode: String = "rms",
      large: Boolean = false,
      retain: Boolean = false,
      skip: Boolean = false,
      archive: Boolean = false
  ): Workflow = {
    val sourceJson = objectMapper.createObjectNode()
    sourceJson.put("operatorType", "FileScan")
    sourceJson.put("fileName", path.toUri.toString)
    sourceJson.put("attributeType", if (large) "large binary" else "binary")
    sourceJson.put("attributeName", "audio")
    sourceJson.put("outputFileName", true)
    sourceJson.put("extract", archive)
    val source = objectMapper
      .treeToValue(sourceJson, classOf[LogicalOp])
      .asInstanceOf[FileScanSourceOpDesc]
    val audioJson = objectMapper.createObjectNode()
    audioJson.put("operatorType", "AudioAttributeExtractor")
    audioJson.put("audioBytesAttribute", "audio")
    audioJson.put("attribute", mode)
    audioJson.put("retainInputColumns", retain)
    audioJson.put("skipUnreadableFiles", skip)
    val audio = objectMapper
      .treeToValue(audioJson, classOf[LogicalOp])
      .asInstanceOf[AudioAttributeExtractorOpDesc]
    buildWorkflow(
      List(source, audio),
      List(
        LogicalLink(
          source.operatorIdentifier,
          PortIdentity(),
          audio.operatorIdentifier,
          PortIdentity()
        )
      ),
      workflowContext(executionId)
    )
  }

  private def run(
      path: Path,
      mode: String = "rms",
      large: Boolean = false,
      retain: Boolean = false,
      skip: Boolean = false,
      archive: Boolean = false
  ): List[Tuple] = {
    val workflow = audioWorkflow(path, mode, large, retain, skip, archive)
    runWorkflowAndReadTerminalResults(
      system,
      workflow,
      Duration.fromSeconds(90)
    ).values.flatten.toList
  }

  for (
    large <- Seq(false, true); mode <- Seq("waveform", "rms", "zcr", "pitch_hz", "onsets", "beats")
  ) {
    "Audio Attribute Extractor" should s"execute $mode with ${if (large) "large binary"
    else "binary"} transport" in {
      val events = mode == "onsets" || mode == "beats"
      val rows = run(if (events) clicks else tone, mode, large, retain = large).sortBy(
        _.getField[Double]("x")
      )
      assert(rows.size == (if (events) 15 else if (mode == "waveform") 5000 else 212))
      assert(
        rows.head.getSchema.getAttributeNames == (if (large) List("filename", "x", "y", "series")
                                                  else List("x", "y", "series"))
      )
      assert(rows.forall(_.getField[String]("series") == mode))
      assert(
        rows
          .sliding(2)
          .forall(pair => pair.head.getField[Double]("x") < pair.last.getField[Double]("x"))
      )
      if (large)
        assert(
          rows.forall(
            _.getField[String]("filename").endsWith(if (events) "clicks.wav" else "tone.wav")
          )
        )
      if (events) {
        assert(rows.forall(_.getField[Double]("y") == 1.0))
        assert(rows.zipWithIndex.forall {
          case (row, i) => math.abs(row.getField[Double]("x") - (i + 1) * 0.5) < 0.08
        })
      } else if (mode == "waveform") {
        assert(rows.head.getField[Double]("x") < 0.01 && rows.last.getField[Double]("x") > 4.9)
        assert(rows.forall(row => math.abs(row.getField[Double]("y")) <= 0.501))
      } else {
        assert(math.abs(rows.head.getField[Double]("x") - 1024.0 / rate) < 1e-10)
        if (mode == "pitch_hz") {
          assert(rows.exists(_.getField[Any]("y") == null))
          val voiced =
            rows.filter(row => row.getField[Any]("y") != null && row.getField[Double]("x") < 3.85)
          assert(
            voiced.nonEmpty && voiced.forall(row => math.abs(row.getField[Double]("y") - 440) < 4.4)
          )
        } else {
          val pcm = toneSamples.take(2048).map(x => (x * 32767).toShort.toDouble / 32768)
          val expected =
            if (mode == "rms") math.sqrt(pcm.map(x => x * x).sum / pcm.length)
            else
              pcm
                .sliding(2)
                .count(pair => (pair.head < 0) != (pair.last < 0))
                .toDouble / (pcm.length - 1)
          assert(math.abs(rows.head.getField[Double]("y") - expected) < 1e-6)
        }
      }
    }
  }

  for (format <- Seq("flac", "mp3")) {
    it should s"decode the original $format fixture inside the worker" in {
      val data = Files
        .readString(
          Paths.get(s"common/workflow-operator/src/test/resources/audio/tone_440hz_1s.$format.b64"),
          US_ASCII
        )
        .linesIterator
        .filterNot(_.startsWith("#"))
        .mkString
      val rows =
        run(Files.write(directory.resolve(s"tone.$format"), Base64.getDecoder.decode(data)))
      val middle =
        rows.filter(row => row.getField[Double]("x") > 0.1 && row.getField[Double]("x") < 0.8)
      assert(
        middle.nonEmpty && middle.forall(row => math.abs(row.getField[Double]("y") - 0.35) < 0.03)
      )
    }
  }

  it should "fail explicitly on unreadable audio by default" in {
    val bad = Files.write(directory.resolve("unreadable.wav"), "not audio".getBytes(US_ASCII))
    val workflow = audioWorkflow(bad)
    val error = Promise[String]()
    val client = new AmberClient(
      system,
      workflow.context,
      workflow.physicalPlan,
      CoordinatorConfig.default,
      e => error.updateIfEmpty(Throw(e))
    )
    try {
      // Tuple-processing errors are reported to the console and pause the worker;
      // waiting for workflow completion would hide the actual error behind a timeout.
      client.registerCallback[ConsoleMessage](evt => {
        if (evt.msgType == ConsoleMessageType.ERROR)
          error.updateIfEmpty(Return(evt.message))
      })
      client.registerCallback[FatalError](evt => error.updateIfEmpty(Throw(evt.e)))
      client.registerCallback[ExecutionStateUpdate](evt => {
        if (evt.state == COMPLETED)
          error.updateIfEmpty(Throw(new AssertionError("Unreadable audio unexpectedly completed")))
      })
      Await.result(client.coordinatorInterface.startWorkflow(EmptyRequest(), ()))
      val message = Await.result(error, Duration.fromSeconds(90))
      assert(message.contains("Cannot read row 1 (unreadable.wav)"))
      assert(message.contains("not a recognised audio file; expected WAV, MP3 or FLAC"))
      assert(message.contains("Turn on Skip Unreadable Files"))
    } finally {
      client.shutdown()
    }
  }

  it should "skip an unreadable archive member and retain only the valid file's identifier" in {
    val archive = directory.resolve("mixed.zip")
    Using.resource(new ZipOutputStream(Files.newOutputStream(archive))) { out =>
      out.putNextEntry(new ZipEntry("unreadable.wav")); out.write("not audio".getBytes(US_ASCII));
      out.closeEntry()
      out.putNextEntry(new ZipEntry("tone.wav")); out.write(Files.readAllBytes(tone));
      out.closeEntry()
    }
    val rows = run(archive, large = true, retain = true, skip = true, archive = true)
    assert(rows.size == 212 && rows.forall(_.getField[String]("filename") == "tone.wav"))
  }

  it should "keep skipped-file and recovery causes visible in truncated console titles" in {
    val archive = directory.resolve("console.zip")
    val nested = "long-directory/" * 20
    Using.resource(new ZipOutputStream(Files.newOutputStream(archive))) { out =>
      out.putNextEntry(new ZipEntry(s"${nested}録音 🎵 unreadable.wav"))
      out.write("not audio".getBytes(US_ASCII))
      out.closeEntry()
      out.putNextEntry(new ZipEntry(s"${nested}録音 🎵 recovered.wav"))
      out.write(Files.readAllBytes(tone).dropRight(2))
      out.closeEntry()
    }
    val workflow = audioWorkflow(archive, large = true, skip = true, archive = true)
    val messages = new ConcurrentLinkedQueue[ConsoleMessage]()
    val causes = Seq(
      "not a recognised audio file; expected WAV, MP3 or FLAC",
      "cut off: under 1 ms of the 5.0 s"
    )
    val received = Promise[Unit]()
    val completed = Promise[Unit]()
    val client = new AmberClient(
      system,
      workflow.context,
      workflow.physicalPlan,
      CoordinatorConfig.default,
      e => { received.updateIfEmpty(Throw(e)); completed.updateIfEmpty(Throw(e)) }
    )
    try {
      client.registerCallback[ConsoleMessage](evt => {
        if (evt.msgType == ConsoleMessageType.PRINT) {
          messages.add(evt)
          if (causes.forall(cause => messages.iterator().asScala.exists(_.title.contains(cause))))
            received.updateIfEmpty(Return(()))
        } else if (evt.msgType == ConsoleMessageType.ERROR)
          received.updateIfEmpty(Throw(new AssertionError(evt.message)))
      })
      client.registerCallback[FatalError](evt => {
        received.updateIfEmpty(Throw(evt.e)); completed.updateIfEmpty(Throw(evt.e))
      })
      client.registerCallback[ExecutionStateUpdate](evt => {
        if (evt.state == COMPLETED) completed.updateIfEmpty(Return(()))
      })
      Await.result(client.coordinatorInterface.startWorkflow(EmptyRequest(), ()))
      Await.result(received, Duration.fromSeconds(90))
      Await.result(completed, Duration.fromSeconds(90))
      for (cause <- causes) {
        val raw = messages.iterator().asScala.find(_.title.contains(cause)).get
        assert(raw.message.isEmpty) // Printed messages have no expandable detail body.
        val displayed = ConsoleMessageProcessor.processConsoleMessage(raw, 100)
        assert(displayed.title.contains(cause), displayed.title)
      }
    } finally client.shutdown()
  }
}
