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

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.operator.tags.IntegrationTest
import org.scalatest.Tag
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
  * Executes the generated operator Python against synthesised audio whose answers
  * are known (tone frequencies, click times, a fixed tempo), plus bad, damaged and
  * cut-off inputs. Needs numpy and scipy (and miniaudio for MP3/FLAC), so every test
  * is tagged for the amber-integration job, which installs them.
  */
class AudioAttributeExtractorBehaviorSpec extends AnyFlatSpec with Matchers {
  private val mapper = new ObjectMapper()

  private lazy val pythonExe: Option[String] = {
    def fromConfig: Option[String] =
      Try(ConfigFactory.parseResources("udf.conf").resolve()).toOption
        .orElse(Try(ConfigFactory.load()).toOption)
        .flatMap(c => Try(c.getConfig("python").getString("path")).toOption)
        .map(_.trim)
        .filter(_.nonEmpty)
    def isRunnable(exe: String): Boolean =
      Try(new ProcessBuilder(exe, "--version").redirectErrorStream(true).start()).toOption
        .exists { p =>
          if (!p.waitFor(5, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
          else p.exitValue() == 0
        }
    (fromConfig.toList ++ List("python3", "python")).distinct.find(isRunnable)
  }

  private def resource(path: String): String = {
    val src = scala.io.Source.fromInputStream(getClass.getResourceAsStream(path), "UTF-8")
    try src.mkString
    finally src.close()
  }

  private lazy val probe: Path = {
    val file = Files.createTempFile("audio_extractor_probe", ".py")
    Files.write(file, resource("/python/audio_extractor_probe.py").getBytes(StandardCharsets.UTF_8))
    file.toFile.deleteOnExit()
    file
  }

  // The license header sits on `#` lines; the rest is the base64 payload.
  private def fixture(name: String): String =
    resource(s"/audio/$name.b64").linesIterator
      .filterNot(_.startsWith("#"))
      .mkString
      .replaceAll("\\s", "")

  private val NeedsPythonPackages = Tag(classOf[IntegrationTest].getName)

  // In amber-integration the packages are installed on purpose, so a missing one is a
  // defect there; on a developer's machine it is only a local-setup fact.
  private val provisioned = sys.env.get("AMBER_TEST_FILTER").contains("integration-only")
  private def missing(message: String): Nothing =
    if (provisioned) fail(message) else cancel(message)

  private def op(
      attribute: AudioAttribute,
      configure: AudioAttributeExtractorOpDesc => Unit = _ => ()
  ): AudioAttributeExtractorOpDesc = {
    val o = new AudioAttributeExtractorOpDesc()
    o.audioBytesAttribute = "audio"
    o.attribute = attribute
    configure(o)
    o
  }
  private def skipping(attribute: AudioAttribute): AudioAttributeExtractorOpDesc =
    op(attribute, _.skipUnreadableFiles = true)

  private def tone(freq: Double, secs: Double, extra: (String, Any)*): ObjectNode = {
    val n = mapper.createObjectNode().put("kind", "tone").put("freq", freq).put("secs", secs)
    extra.foreach {
      case (k, v: Int)    => n.put(k, v)
      case (k, v: Double) => n.put(k, v)
      case (k, v: Seq[_]) =>
        val a = n.putArray(k)
        v.foreach(e => a.add(e.asInstanceOf[Long]))
      case (k, v) => n.put(k, v.toString)
    }
    n
  }
  private def kind(k: String): ObjectNode = mapper.createObjectNode().put("kind", k)
  private def raw(b64: String): ObjectNode = kind("raw").put("b64", b64)
  private def clicks(secs: Double, at: Seq[Double]): ObjectNode = {
    val n = kind("clicks").put("secs", secs)
    val times = n.putArray("times")
    at.foreach(t => times.add(t))
    n
  }

  private def run(o: AudioAttributeExtractorOpDesc, cases: (String, ObjectNode)*) =
    execute(o, mayRaise = false, cases)

  /** Runs every case through one operator instance; returns (results by name, close prints, miniaudio?). */
  private def execute(
      o: AudioAttributeExtractorOpDesc,
      mayRaise: Boolean,
      cases: Seq[(String, ObjectNode)]
  ): (Map[String, JsonNode], Seq[String], Boolean) = {
    val exe = pythonExe.getOrElse(missing("no python interpreter available to run the probe"))
    val request = mapper.createObjectNode()
    request.put("source", o.generatePythonCode())
    val arr = request.putArray("cases")
    cases.foreach {
      case (name, input) => arr.addObject().put("name", name).set[JsonNode]("input", input)
    }
    val outFile = Files.createTempFile("audio_probe_out", ".json")
    val errFile = Files.createTempFile("audio_probe_err", ".txt")
    try {
      val process = new ProcessBuilder(exe, probe.toString)
        .redirectOutput(outFile.toFile)
        .redirectError(errFile.toFile)
        .start()
      process.getOutputStream.write(mapper.writeValueAsBytes(request))
      process.getOutputStream.close()
      if (!process.waitFor(120, TimeUnit.SECONDS)) {
        process.destroyForcibly(); fail("probe timed out")
      }
      val out = new String(Files.readAllBytes(outFile), StandardCharsets.UTF_8)
      val err = new String(Files.readAllBytes(errFile), StandardCharsets.UTF_8)
      // A missing package makes the probe exit early, so look for it before the exit code.
      "No module named '(numpy|scipy)'".r.findFirstMatchIn(err).foreach { m =>
        missing(s"${m.group(1)} is not installed for $exe")
      }
      withClue(s"probe stderr: $err\n") { process.exitValue() shouldBe 0 }
      val tree = mapper.readTree(out)
      val results =
        tree.get("results").elements().asScala.map(r => r.get("name").asText() -> r).toMap
      if (!mayRaise)
        results.values.foreach(r =>
          withClue(s"case ${r.get("name")}: ${r.get("raised")}") { r.has("raised") shouldBe false }
        )
      (
        results,
        tree.get("close_prints").elements().asScala.map(_.asText()).toSeq,
        tree.get("miniaudio").asBoolean()
      )
    } finally {
      Files.deleteIfExists(outFile)
      Files.deleteIfExists(errFile)
    }
  }

  private def ys(r: JsonNode): Seq[Option[Double]] =
    r.get("rows")
      .elements()
      .asScala
      .map(row => Option(row.get("y")).filterNot(_.isNull).map(_.asDouble()))
      .toSeq
  private def xs(r: JsonNode): Seq[Double] =
    r.get("rows").elements().asScala.map(_.get("x").asDouble()).toSeq
  private def prints(r: JsonNode): Seq[String] =
    r.get("prints").elements().asScala.map(_.asText()).toSeq
  private def median(v: Seq[Double]): Double = { val s = v.sorted; s(s.size / 2) }

  "waveform" should "keep the signal's peaks within its amplitude, in time order" taggedAs NeedsPythonPackages in {
    val (r, _, _) = run(op(AudioAttribute.WAVEFORM), "tone" -> tone(440, 0.5))
    val y = ys(r("tone")).flatten
    y.size shouldBe 5000
    y.map(math.abs).max should (be >= 0.49 and be <= 0.51)
    xs(r("tone")) shouldBe sorted
    xs(r("tone")).last should be < 0.5
  }

  "rms" should "be 0.354 for a sine of amplitude 0.5" taggedAs NeedsPythonPackages in {
    val (r, _, _) = run(op(AudioAttribute.RMS), "tone" -> tone(440, 2.0))
    median(ys(r("tone")).flatten) shouldBe 0.3536 +- 0.01
  }

  "zcr" should "be twice the frequency over the sample rate" taggedAs NeedsPythonPackages in {
    val (r, _, _) = run(op(AudioAttribute.ZCR), "tone" -> tone(441, 2.0))
    median(ys(r("tone")).flatten) shouldBe (2 * 441.0 / 22050) +- 0.002
  }

  "pitch_hz" should "match pure tones within 1 % and stay empty on silence and noise" taggedAs NeedsPythonPackages in {
    val tones = Seq(110.0, 220.0, 440.0, 880.0).map(f => s"t$f" -> tone(f, 1.5))
    val (r, _, _) = run(
      op(AudioAttribute.PITCH_HZ),
      tones ++ Seq("silence" -> kind("silence"), "noise" -> kind("noise")): _*
    )
    Seq(110.0, 220.0, 440.0, 880.0).foreach { f =>
      withClue(s"$f Hz: ") { median(ys(r(s"t$f")).flatten) shouldBe f +- f * 0.01 }
    }
    ys(r("silence")).flatten shouldBe empty
    ys(r("noise")).count(_.isDefined).toDouble / ys(r("noise")).size should be < 0.1
  }

  it should "cover 60 to 1000 Hz and report nothing outside it" taggedAs NeedsPythonPackages in {
    // Above 1000 Hz the period is shorter than the lag search, which would otherwise
    // land on a multiple of it: 1100 Hz read as 550 Hz, 2000 Hz as 1000 Hz.
    val above = Seq(1030.0, 1100.0, 2000.0, 3000.0)
    val freqs = Seq(55.0, 60.0, 1000.0, 1020.0) ++ above
    val clickTrack = "clicks" -> clicks(5.0, (0 until 10).map(0.25 + 0.5 * _)) // 1000 Hz bursts
    val (r, _, _) =
      run(op(AudioAttribute.PITCH_HZ), freqs.map(f => s"t$f" -> tone(f, 2.0)) :+ clickTrack: _*)
    for (f <- Seq(60.0, 1000.0)) withClue(s"$f Hz: ") {
      ys(r(s"t$f")).forall(_.isDefined) shouldBe true
      median(ys(r(s"t$f")).flatten) shouldBe f +- f * 0.01
    }
    for (f <- 55.0 +: above) withClue(s"$f Hz: ") { ys(r(s"t$f")).flatten shouldBe empty }
    // Estimates near an edge stray past it (a 60 Hz tone reads 59.7 Hz, the short clicks up
    // to 1008 Hz), and a note just above the range is kept: all of them report the edge.
    for (name <- freqs.map(f => s"t$f") :+ clickTrack._1; y <- ys(r(name)).flatten)
      withClue(s"$name: ") { y should (be >= 60.0 and be <= 1000.0) }
    ys(r("t1020.0")).flatten.toSet shouldBe Set(1000.0)
  }

  "onsets" should "land within 30 ms of each click and find nothing in silence" taggedAs NeedsPythonPackages in {
    val at = Seq(0.5, 1.0, 1.7, 2.2)
    val (r, _, _) =
      run(op(AudioAttribute.ONSETS), "clicks" -> clicks(3.0, at), "silence" -> kind("silence"))
    val found = xs(r("clicks"))
    found.size shouldBe at.size
    at.zip(found).foreach { case (want, got) => got shouldBe want +- 0.03 }
    ys(r("clicks")).flatten.toSet shouldBe Set(1.0)
    xs(r("silence")) shouldBe empty
  }

  it should "find nothing in steady noise" taggedAs NeedsPythonPackages in {
    val (r, _, _) = run(op(AudioAttribute.ONSETS), "noise" -> kind("noise").put("secs", 10.0))
    xs(r("noise")) shouldBe empty
  }

  "beats" should "follow a 120 BPM click track" taggedAs NeedsPythonPackages in {
    val (r, _, _) =
      run(op(AudioAttribute.BEATS), "click track" -> clicks(12.0, (0 until 24).map(0.25 + 0.5 * _)))
    val beats = xs(r("click track"))
    beats.size should be >= 20
    median(beats.zip(beats.tail).map { case (a, b) => b - a }) shouldBe 0.5 +- 0.02
  }

  it should "find no beats in silence or steady noise" taggedAs NeedsPythonPackages in {
    val (r, _, _) = run(
      op(AudioAttribute.BEATS),
      "silence" -> kind("silence").put("secs", 10.0),
      "noise" -> kind("noise").put("secs", 10.0)
    )
    xs(r("silence")) shouldBe empty
    xs(r("noise")) shouldBe empty
  }

  it should "ignore the hidden frame and hop settings" taggedAs NeedsPythonPackages in {
    val track = clicks(12.0, (0 until 24).map(0.25 + 0.5 * _))
    val (plain, _, _) = run(op(AudioAttribute.BEATS), "t" -> track)
    val (odd, _, _) =
      run(op(AudioAttribute.BEATS, o => { o.frameLength = 2049; o.hopLength = 1 }), "t" -> track)
    xs(odd("t")) shouldBe xs(plain("t"))
  }

  "decoding" should "give the same rms for every WAV sample width, channel layout and rate" taggedAs NeedsPythonPackages in {
    val layouts = Seq(
      "u8" -> tone(440, 1.0, "width" -> 1),
      "s16" -> tone(440, 1.0),
      "s24" -> tone(440, 1.0, "width" -> 3),
      "s32" -> tone(440, 1.0, "width" -> 4),
      "stereo" -> tone(440, 1.0, "channels" -> 2),
      "8 kHz" -> tone(440, 1.0, "sr" -> 8000),
      "48 kHz" -> tone(440, 1.0, "sr" -> 48000)
    )
    val (r, _, _) = run(op(AudioAttribute.RMS), layouts: _*)
    layouts.foreach {
      case (name, _) => withClue(s"$name: ") { median(ys(r(name)).flatten) shouldBe 0.3536 +- 0.01 }
    }
  }

  it should "read the MP3 and FLAC fixtures as a 440 Hz tone" taggedAs NeedsPythonPackages in {
    val formats = Seq("mp3", "flac")
    val (r, _, hasMiniaudio) =
      run(op(AudioAttribute.PITCH_HZ), formats.map(f => f -> raw(fixture(s"tone_440hz_1s.$f"))): _*)
    if (!hasMiniaudio) missing("miniaudio is not installed for the probe interpreter")
    formats.foreach(f => withClue(s"$f: ") { median(ys(r(f)).flatten) shouldBe 440.0 +- 4.4 })
  }

  it should "refuse Ogg Vorbis without handing it to a decoder" taggedAs NeedsPythonPackages in {
    // Corrupted Ogg Vorbis files can abort the process inside miniaudio's bundled
    // stb_vorbis (found by fuzzing), so Ogg is rejected before any decoder runs.
    val (r, _, _) = run(skipping(AudioAttribute.RMS), "ogg" -> raw(fixture("tone_440hz_1s.ogg")))
    r("ogg").get("rows").size() shouldBe 0
    prints(r("ogg")).mkString should include("Ogg Vorbis audio is not supported")
  }

  it should "size its buffers from the bytes present, not from a damaged header" taggedAs NeedsPythonPackages in {
    val (r, _, _) = run(
      op(AudioAttribute.RMS),
      // A WAV written to a pipe: both sizes 0xFFFFFFFF, meaning "unknown".
      "piped" -> tone(440, 1.0, "sizes" -> Seq(0xffffffffL, 0xffffffffL)),
      // Sizes damaged to about 2 GB, on one second of audio.
      "damaged" -> tone(440, 1.0, "sizes" -> Seq(0x80000022L, 0x7ffffffeL))
    )
    r("piped").get("rows").size() shouldBe 40
    prints(r("piped")) shouldBe empty
    r("damaged").get("rows").size() shouldBe 40
    prints(r("damaged")).mkString should include("cut off")
    for (name <- Seq("piped", "damaged"))
      withClue(s"$name: ") { r(name).get("largest_buffer").asLong() should be < (10L << 20) }
  }

  it should "warn when the decoder stops part-way, and analyse what it read" taggedAs NeedsPythonPackages in {
    val (r, _, _) =
      run(op(AudioAttribute.RMS), "stops" -> tone(440, 4.0, "inject" -> "decoder_error"))
    r("stops").get("rows").size() should be > 0
    prints(r("stops")).mkString should include("decoding stopped after 3.0 s")
  }

  it should "never hide a bug in its own processing, even when skipping files" taggedAs NeedsPythonPackages in {
    val (r, _, _) = execute(
      skipping(AudioAttribute.RMS),
      mayRaise = true,
      Seq("bug" -> tone(440, 4.0, "inject" -> "processing_error"))
    )
    r("bug").get("raised").asText() should include("RuntimeError: injected processing bug")
  }

  it should "resample without SciPy in bounded memory, approximately, and say so once" taggedAs NeedsPythonPackages in {
    val (exact, _, _) = run(op(AudioAttribute.RMS), "t" -> tone(440, 10.0, "sr" -> 384000))
    val (approx, _, _) =
      run(op(AudioAttribute.RMS), "t" -> tone(440, 10.0, "sr" -> 384000, "inject" -> "no_scipy"))
    ys(approx("t")).size shouldBe ys(exact("t")).size
    ys(approx("t")).flatten.zip(ys(exact("t")).flatten).foreach {
      case (a, e) => a shouldBe e +- 0.01
    }
    prints(approx("t")) shouldBe Seq("SciPy is not installed, so audio is resampled approximately")
    // Holding the file at 384 kHz would take 15 MB; at 22,050 Hz it is under 1 MB.
    approx("t").get("largest_buffer").asLong() should be < (2L << 20)
  }

  it should "upsample in bounded memory, with SciPy or without" taggedAs NeedsPythonPackages in {
    // 30 s at 1,000 Hz becomes 5.76 million samples at 192,000 Hz, 23 MB as float32.
    // Resampled in one piece, the temporary arrays would take two to five times that.
    val o =
      op(AudioAttribute.WAVEFORM, o => { o.targetSampleRate = 192000; o.maxDurationSeconds = 60 })
    val output = 30L * 192000 * 4
    for (inject <- Seq(None, Some("no_scipy"))) withClue(s"${inject.getOrElse("scipy")}: ") {
      val extras = Seq[(String, Any)]("sr" -> 1000, "trace_memory" -> "true") ++
        inject.map(i => "inject" -> i)
      val (r, _, _) = run(o, "t" -> tone(100, 30.0, extras: _*))
      r("t").get("rows").size() shouldBe 5000
      r("t").get("peak_memory").asLong() should be < output * 3 / 2
    }
  }

  "retainInputColumns" should "carry the other columns but never the audio bytes" taggedAs NeedsPythonPackages in {
    val (kept, _, _) =
      run(op(AudioAttribute.RMS, _.retainInputColumns = true), "a" -> tone(440, 0.5))
    val (plain, _, _) = run(op(AudioAttribute.RMS), "a" -> tone(440, 0.5))
    kept("a").get("rows").get(0).fieldNames().asScala.toSeq shouldBe Seq(
      "filename",
      "x",
      "y",
      "series"
    )
    plain("a").get("rows").get(0).fieldNames().asScala.toSeq shouldBe Seq("x", "y", "series")
  }

  it should "find the audio column when its spelling differs in case" taggedAs NeedsPythonPackages in {
    // Texera schemas ignore case, so compiling accepts "AUDIO" for a column named "audio".
    val (r, _, _) = run(
      op(AudioAttribute.RMS, o => { o.audioBytesAttribute = "AUDIO"; o.retainInputColumns = true }),
      "a" -> tone(440, 0.5)
    )
    r("a").get("rows").get(0).fieldNames().asScala.toSeq shouldBe Seq(
      "filename",
      "x",
      "y",
      "series"
    )
  }

  private val badInputs = {
    val m4aHead = java.util.Base64.getEncoder
      .encodeToString(Array[Byte](0, 0, 0, 32) ++ "ftypM4A ".getBytes ++ new Array[Byte](64))
    val adtsHead = java.util.Base64.getEncoder
      .encodeToString(
        Array(0xff, 0xf1, 0x50, 0x80, 0x02, 0x1f, 0xfc).map(_.toByte) ++ new Array[Byte](64)
      )
    val wavHeader = java.util.Base64.getEncoder
      .encodeToString("RIFF".getBytes ++ Array[Byte](36, 0, 0, 0) ++ "WAVE".getBytes)
    Seq(
      ("null", kind("none"), "the audio cell is empty (null)"),
      ("empty", raw(""), "the audio file is empty (0 bytes)"),
      ("text", kind("text").put("text", "not audio"), "the audio column holds text, not binary"),
      (
        "csv bytes",
        raw(java.util.Base64.getEncoder.encodeToString("id,name\n1,a\n".getBytes)),
        "not a recognised audio file"
      ),
      ("m4a", raw(m4aHead), "M4A/MP4 (AAC) audio is not supported"),
      // ADTS AAC shares MP3's frame sync; only the layer bits tell them apart.
      ("adts aac", raw(adtsHead), "AAC (ADTS) audio is not supported"),
      ("wav header only", raw(wavHeader), "WAV file could not be read")
    )
  }

  "bad input" should "stop the run with an error naming the file and the cause" taggedAs NeedsPythonPackages in {
    for ((name, input, reason) <- badInputs) {
      val (r, _, _) = execute(op(AudioAttribute.RMS), mayRaise = true, Seq(name -> input))
      withClue(s"$name: ") {
        r(name).get("raised").asText() should (include(
          s"Cannot read row 1 ($name)"
        ) and include(reason) and include("Turn on Skip Unreadable Files"))
      }
    }
  }

  it should "be skipped with a console message when Skip Unreadable Files is on" taggedAs NeedsPythonPackages in {
    val (r, _, _) = run(skipping(AudioAttribute.RMS), badInputs.map(b => b._1 -> b._2): _*)
    for ((name, _, reason) <- badInputs) withClue(s"$name: ") {
      r(name).get("rows").size() shouldBe 0
      prints(r(name)).mkString should (include("Skipped row") and include(
        s"($name)"
      ) and include(reason))
    }
  }

  "a cut-off WAV" should "still produce rows, with a warning" taggedAs NeedsPythonPackages in {
    val (r, _, _) = run(
      op(AudioAttribute.RMS),
      "half" -> tone(440, 2.0, "keep" -> 0.5),
      "100 bytes short" -> tone(440, 1.0, "drop" -> 100),
      "1 sample short" -> tone(440, 1.0, "drop" -> 2)
    )
    r("half").get("rows").size() should be > 0
    prints(r("half")).mkString should include(
      "cut off: 1.0 s of the 2.0 s its header declares is missing"
    )
    prints(r("100 bytes short")).mkString should include("2 ms of the 1.0 s")
    prints(r("1 sample short")).mkString should include("under 1 ms of the 1.0 s")
  }

  "Max Duration" should "refuse longer files, even slightly longer, before decoding" taggedAs NeedsPythonPackages in {
    val (r, _, _) = run(
      op(AudioAttribute.RMS, o => { o.maxDurationSeconds = 1; o.skipUnreadableFiles = true }),
      "long" -> tone(440, 3.0),
      "slightly long" -> tone(440, 1.2),
      "just over" -> tone(440, 1.01),
      "exactly" -> tone(440, 1.0)
    )
    for (name <- Seq("long", "slightly long", "just over"))
      withClue(s"$name: ") { r(name).get("rows").size() shouldBe 0 }
    prints(r("long")).mkString should include("audio is 3.0 s long; the limit is 1 s")
    prints(r("slightly long")).mkString should include("audio is 1.2 s long; the limit is 1 s")
    // Shown with enough decimals not to read as the limit itself.
    prints(r("just over")).mkString should include("audio is 1.01 s long; the limit is 1 s")
    r("exactly").get("rows").size() shouldBe 40
  }

  "Max Points" should "cap the rows per file" taggedAs NeedsPythonPackages in {
    val (r, _, _) = run(op(AudioAttribute.RMS, _.maxPoints = 10), "tone" -> tone(440, 5.0))
    r("tone").get("rows").size() shouldBe 10
  }

  it should "keep events from the whole file and say how many were left out" taggedAs NeedsPythonPackages in {
    val (r, _, _) = run(
      op(AudioAttribute.ONSETS, _.maxPoints = 10),
      "clicks" -> clicks(10.0, (0 until 40).map(0.125 + 0.25 * _))
    )
    val found = xs(r("clicks"))
    found.size shouldBe 10
    found.last - found.head should be > 9.0
    prints(r("clicks")).mkString should include("showing 10 of 40 onsets")
  }

  "frame and hop length" should "set the measurement grid" taggedAs NeedsPythonPackages in {
    val (coarse, _, _) = run(op(AudioAttribute.RMS), "tone" -> tone(440, 2.0))
    val (fine, _, _) = run(
      op(AudioAttribute.RMS, o => { o.frameLength = 1024; o.hopLength = 256 }),
      "tone" -> tone(440, 2.0)
    )
    coarse("tone").get("rows").size() shouldBe 83 // 1 + (44100 - 2048) / 512
    fine("tone").get("rows").size() shouldBe 169 // 1 + (44100 - 1024) / 256
  }

  "a large binary cell" should "be read through its stream" taggedAs NeedsPythonPackages in {
    val (r, _, _) =
      run(op(AudioAttribute.RMS), "large" -> kind("large").set[ObjectNode]("inner", tone(440, 1.0)))
    median(ys(r("large")).flatten) shouldBe 0.3536 +- 0.01
  }

  it should "skip a missing stored file, but stop on any other storage failure" taggedAs NeedsPythonPackages in {
    val (missing, _, _) =
      run(skipping(AudioAttribute.RMS), "gone" -> kind("large").put("storage_error", "NoSuchKey"))
    missing("gone").get("rows").size() shouldBe 0
    prints(missing("gone")).mkString should include("the stored file is missing (NoSuchKey)")
    // Credentials or configuration are not one file's problem: never skip those.
    val (denied, _, _) = execute(
      skipping(AudioAttribute.RMS),
      mayRaise = true,
      Seq("denied" -> kind("large").put("storage_error", "InvalidAccessKeyId"))
    )
    denied("denied").get("raised").asText() should include("ClientError")
  }

  it should "be refused, unread, past the size limit" taggedAs NeedsPythonPackages in {
    val (r, _, _) = run(skipping(AudioAttribute.RMS), "huge" -> kind("large"))
    prints(r("huge")).mkString should include("larger than the 256 MB limit")
  }

  /** What Texera's console shows of a printed line: past 100 bytes, 97 characters and "...". */
  private def shown(title: String): String =
    if (title.getBytes(StandardCharsets.UTF_8).length > 100) title.take(97) + "..." else title

  "the console" should "show ten file messages in all, skips and warnings alike, then the totals" taggedAs NeedsPythonPackages in {
    val files = (1 to 3).map(i => s"bad $i" -> raw("")) ++
      (1 to 12).map(i => s"cut $i" -> tone(440, 1.0, "keep" -> 0.6))
    val (r, closing, _) = run(skipping(AudioAttribute.RMS), files: _*)
    files.map { case (name, _) => prints(r(name)).size }.sum shouldBe 10
    closing shouldBe Seq(
      "3 rows skipped and 12 rows with warnings in total; the first 10 messages are listed above"
    )
    // Ten messages or fewer need no total.
    val (few, fewClosing, _) = run(skipping(AudioAttribute.RMS), files.take(4): _*)
    files.take(4).map { case (name, _) => prints(few(name)).size }.sum shouldBe 4
    fewClosing shouldBe empty
  }

  it should "keep the cause and both ends of a long file name in view, in its own case" taggedAs NeedsPythonPackages in {
    val unicode = "長い名前の録音ファイル" * 6 + ".wav"
    val ascii = "Field-Recording_" + "x" * 70 + ".WAV"
    val csv = raw(java.util.Base64.getEncoder.encodeToString("id,name\n1,a\n".getBytes))
    val (r, _, _) = run(
      skipping(AudioAttribute.RMS),
      unicode -> csv,
      ascii -> tone(440, 2.0, "keep" -> 0.5)
    )
    val skipped = shown(prints(r(unicode)).head)
    skipped should (include("not a recognised audio file") and include(unicode.take(11)) and
      include(unicode.takeRight(10)))
    val warned = shown(prints(r(ascii)).head)
    warned should (startWith("Row 2 (Field-Recor") and include(".WAV)") and
      include("cut off: 1.0 s of the 2.0 s"))
  }
}
