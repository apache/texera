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

package org.apache.texera.amber.operator.audio

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorMetadataGenerator
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._
import scala.util.Try

class AudioAttributeExtractorOpDescSpec extends AnyFlatSpec with Matchers {

  private val inputSchema = Schema()
    .add("filename", AttributeType.STRING)
    .add("audio", AttributeType.BINARY)

  private def outputOf(op: AudioAttributeExtractorOpDesc, in: Schema = inputSchema): Schema =
    op.getOutputSchemas(Map(op.operatorInfo.inputPorts.head.id -> in))(
      op.operatorInfo.outputPorts.head.id
    )

  "the JSON schema" should "autofill the audio column and limit it to binary columns" in {
    val schema =
      OperatorMetadataGenerator.generateOperatorJsonSchema(classOf[AudioAttributeExtractorOpDesc])
    schema.at("/properties/audioBytesAttribute/autofill").asText() shouldBe "attributeName"
    schema
      .at("/attributeTypeRules/audioBytesAttribute/enum")
      .elements()
      .asScala
      .map(_.asText())
      .toList shouldBe
      List("binary", "large_binary")
  }

  it should "list the six attributes and default to rms" in {
    val schema =
      OperatorMetadataGenerator.generateOperatorJsonSchema(classOf[AudioAttributeExtractorOpDesc])
    val attr = schema.at("/properties/attribute")
    attr.at("/enum").elements().asScala.map(_.asText()).toList shouldBe
      List("waveform", "rms", "zcr", "pitch_hz", "onsets", "beats")
    attr.at("/default").asText() shouldBe "rms"
  }

  it should "hide frame and hop length for waveform, onsets and beats" in {
    val schema =
      OperatorMetadataGenerator.generateOperatorJsonSchema(classOf[AudioAttributeExtractorOpDesc])
    for (field <- Seq("frameLength", "hopLength")) {
      val p = schema.at(s"/properties/$field")
      p.at("/hideTarget").asText() shouldBe "attribute"
      p.at("/hideType").asText() shouldBe "regex"
      val hidden = p.at("/hideExpectedValue").asText().r
      AudioAttribute.values().map(_.getName).filter(hidden.findFirstIn(_).isDefined).toSet shouldBe
        Set("waveform", "onsets", "beats")
    }
    schema.at("/properties/maxPoints/hideTarget").isMissingNode shouldBe true
  }

  it should "stop on unreadable files unless told to skip them" in {
    val schema =
      OperatorMetadataGenerator.generateOperatorJsonSchema(classOf[AudioAttributeExtractorOpDesc])
    schema.at("/properties/skipUnreadableFiles/default").asBoolean() shouldBe false
    new AudioAttributeExtractorOpDesc().skipUnreadableFiles shouldBe false
  }

  it should "show each number's own limits in the form, the ones the compiler checks" in {
    val schema =
      OperatorMetadataGenerator.generateOperatorJsonSchema(classOf[AudioAttributeExtractorOpDesc])
    def bound(field: String, key: String): Option[Int] =
      Some(schema.at(s"/properties/$field/$key")).filterNot(_.isMissingNode).map(_.asInt())
    // Each setter changes only what that field's own limit checks.
    val fields = Seq[(String, Int => AudioAttributeExtractorOpDesc => Unit)](
      "targetSampleRate" -> (v => _.targetSampleRate = v),
      "frameLength" -> (v => o => { o.frameLength = v; o.hopLength = v }),
      "hopLength" -> (v => _.hopLength = v),
      "maxPoints" -> (v => _.maxPoints = v),
      "maxDurationSeconds" -> (v => _.maxDurationSeconds = v)
    )
    fields.map { case (f, _) => f -> (bound(f, "minimum"), bound(f, "maximum")) }.toMap shouldBe
      Map(
        "targetSampleRate" -> (Some(1000), Some(192000)),
        "frameLength" -> (Some(64), Some(65536)),
        "hopLength" -> (Some(1), None), // at most Frame Length, checked when compiling
        "maxPoints" -> (Some(1), Some(AudioAttributeExtractorOpDesc.MaxPoints)),
        "maxDurationSeconds" -> (Some(1), None) // bounded together with Sample Rate
      )
    def compiles(set: AudioAttributeExtractorOpDesc => Unit): Boolean = {
      val op = new AudioAttributeExtractorOpDesc()
      op.audioBytesAttribute = "audio"
      op.maxDurationSeconds = 1 // keeps the limits that combine settings out of the way
      set(op)
      Try(outputOf(op)).isSuccess
    }
    for {
      (f, set) <- fields
      (edge, past) <- Seq(
        bound(f, "minimum").map(v => (v, v - 1)),
        bound(f, "maximum").map(v => (v, v + 1))
      ).flatten
    } withClue(s"$f at $edge and $past: ") {
      compiles(set(edge)) shouldBe true
      compiles(set(past)) shouldBe false
    }
  }

  "the output schema" should "be x, y, series by default" in {
    val op = new AudioAttributeExtractorOpDesc()
    op.audioBytesAttribute = "audio"
    outputOf(op).getAttributes.map(a => a.getName -> a.getType) shouldBe
      List(
        "x" -> AttributeType.DOUBLE,
        "y" -> AttributeType.DOUBLE,
        "series" -> AttributeType.STRING
      )
  }

  it should "carry the other input columns but never the audio bytes" in {
    val op = new AudioAttributeExtractorOpDesc()
    op.audioBytesAttribute = "audio"
    op.retainInputColumns = true
    outputOf(op).getAttributeNames shouldBe List("filename", "x", "y", "series")
  }

  it should "refuse to duplicate an input column named like an output column" in {
    val op = new AudioAttributeExtractorOpDesc()
    op.audioBytesAttribute = "audio"
    op.retainInputColumns = true
    val clash = Schema().add("x", AttributeType.INTEGER).add("audio", AttributeType.BINARY)
    val e = intercept[IllegalArgumentException](outputOf(op, clash))
    e.getMessage should include("duplicate x")
  }

  it should "reject an empty, missing or non-binary audio column at compile time" in {
    val op = new AudioAttributeExtractorOpDesc()
    intercept[IllegalArgumentException](
      outputOf(op)
    ).getMessage shouldBe "Audio Column cannot be empty"
    op.audioBytesAttribute = "nope"
    intercept[IllegalArgumentException](outputOf(op)).getMessage should include(
      "'nope' is not in the input"
    )
    op.audioBytesAttribute = "filename"
    val e = intercept[IllegalArgumentException](outputOf(op))
    e.getMessage should include("'filename' is string, but must be binary or large binary")
  }

  it should "exclude the audio column even when its spelling differs in case" in {
    // Schema lookups ignore case, so "audio" selects a column named "Audio".
    val op = new AudioAttributeExtractorOpDesc()
    op.audioBytesAttribute = "audio"
    op.retainInputColumns = true
    val in = Schema().add("Audio", AttributeType.BINARY).add("name", AttributeType.STRING)
    outputOf(op, in).getAttributeNames shouldBe List("name", "x", "y", "series")
  }

  it should "explain a missing attribute or input schema instead of failing on null" in {
    val op = new AudioAttributeExtractorOpDesc()
    op.audioBytesAttribute = "audio"
    op.attribute = null
    intercept[IllegalArgumentException](op.generatePythonCode()).getMessage should include(
      "Attribute"
    )
    op.attribute = AudioAttribute.RMS
    intercept[IllegalArgumentException](op.getOutputSchemas(Map.empty)).getMessage should include(
      "input"
    )
  }

  it should "accept a large binary audio column" in {
    val op = new AudioAttributeExtractorOpDesc()
    op.audioBytesAttribute = "audio"
    val large = Schema().add("audio", AttributeType.LARGE_BINARY)
    outputOf(op, large).getAttributeNames shouldBe List("x", "y", "series")
  }

  "the settings" should "be checked at compile time, before any code runs" in {
    def error(configure: AudioAttributeExtractorOpDesc => Unit): String = {
      val op = new AudioAttributeExtractorOpDesc()
      op.audioBytesAttribute = "audio"
      configure(op)
      intercept[IllegalArgumentException](outputOf(op)).getMessage
    }
    error(o => { o.frameLength = 256; o.hopLength = 512 }) should include("Hop Length")
    error(_.targetSampleRate = 500) should include("Sample Rate")
    error(_.maxPoints = 0) should include("Max Points")
  }

  it should "reject a pitch window too short to hold the lowest pitch" in {
    val op = new AudioAttributeExtractorOpDesc()
    op.audioBytesAttribute = "audio"
    op.attribute = AudioAttribute.PITCH_HZ
    op.targetSampleRate = 192000
    op.maxDurationSeconds = 60
    op.frameLength = 64
    op.hopLength = 32
    val e = intercept[IllegalArgumentException](outputOf(op))
    e.getMessage should include("Frame Length must be at least 3206 samples")
    op.frameLength = 4096
    op.hopLength = 1024
    outputOf(op).getAttributeNames shouldBe List("x", "y", "series")
  }

  it should "bound the samples one file may decode to" in {
    val op = new AudioAttributeExtractorOpDesc()
    op.audioBytesAttribute = "audio"
    op.targetSampleRate = 192000 // 600 s at 192 kHz is 115 million samples
    intercept[IllegalArgumentException](outputOf(op)).getMessage should include(
      "the limit is 40,000,000"
    )
    op.maxDurationSeconds = 200
    outputOf(op).getAttributeNames should contain("x")
  }

  it should "bound the frame work of small hops, but not where hops are not used" in {
    val op = new AudioAttributeExtractorOpDesc()
    op.audioBytesAttribute = "audio"
    op.hopLength = 1
    intercept[IllegalArgumentException](outputOf(op)).getMessage should include(
      "Raise Hop Length"
    )
    for (a <- Seq(AudioAttribute.WAVEFORM, AudioAttribute.ONSETS, AudioAttribute.BEATS)) {
      op.attribute = a
      withClue(s"${a.getName}: ") { outputOf(op).getAttributeNames should contain("x") }
    }
  }

  it should "bound the analysis steps of beats, which keep every step's spectrum" in {
    // Onsets and beats take about 43 steps per second at any sample rate, so at low
    // rates the samples limit alone would allow hours of steps.
    def compiles(attribute: AudioAttribute, rate: Int, seconds: Int): Try[Schema] = {
      val op = new AudioAttributeExtractorOpDesc()
      op.audioBytesAttribute = "audio"
      op.attribute = attribute
      op.targetSampleRate = rate
      op.maxDurationSeconds = seconds
      Try(outputOf(op))
    }
    compiles(AudioAttribute.BEATS, 1000, 40000).failed.get.getMessage should include(
      "For beats at 1000 Hz, Max Duration can be at most 2,299 s"
    )
    compiles(AudioAttribute.BEATS, 1000, 2299).isSuccess shouldBe true
    compiles(AudioAttribute.BEATS, 1000, 2300).isFailure shouldBe true
    // Onsets keep only one value per step, and the samples limit binds first at 22,050 Hz.
    compiles(AudioAttribute.ONSETS, 1000, 40000).isSuccess shouldBe true
    compiles(AudioAttribute.BEATS, 22050, 1814).isSuccess shouldBe true
  }

  it should "refuse to generate code for invalid settings" in {
    val op = new AudioAttributeExtractorOpDesc()
    op.audioBytesAttribute = "audio"
    op.frameLength = 256
    op.hopLength = 512
    intercept[IllegalArgumentException](op.generatePythonCode()).getMessage should include(
      "Hop Length"
    )
  }

  it should "round-trip through JSON as a LogicalOp" in {
    val json =
      """{"operatorType":"AudioAttributeExtractor","operatorID":"a1","audioBytesAttribute":"audio","attribute":"pitch_hz"}"""
    val op =
      objectMapper.readValue(json, classOf[LogicalOp]).asInstanceOf[AudioAttributeExtractorOpDesc]
    op.attribute shouldBe AudioAttribute.PITCH_HZ
    op.audioBytesAttribute shouldBe "audio"
    op.hopLength shouldBe 512
  }
}
