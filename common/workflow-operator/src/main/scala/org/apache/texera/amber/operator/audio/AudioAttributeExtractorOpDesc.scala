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

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaString,
  JsonSchemaTitle
}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.audio.codegen.{
  BeatTrackerCodegen,
  DecodingCodegen,
  FeaturesCodegen
}
import org.apache.texera.amber.operator.metadata.annotations.{AutofillAttributeName, HideAnnotation}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext

// Decodes each audio file and measures one attribute over time. The analysis is Python kept in
// the codegen package; generatePythonCode adds the imports, the dispatch on Attribute and the
// per-tuple operator. The audio column must hold the file's bytes: File Scan with attribute
// type "binary", or "large binary" for files stored outside the tuple.
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "audioBytesAttribute": { "enum": ["binary", "large_binary"] }
  }
}
""")
class AudioAttributeExtractorOpDesc extends PythonOperatorDescriptor {
  import AudioAttributeExtractorOpDesc._

  @JsonProperty(value = "audioBytesAttribute", required = true)
  @JsonSchemaTitle("Audio Column")
  @JsonPropertyDescription("Binary column holding the audio file (WAV, MP3 or FLAC)")
  @AutofillAttributeName
  var audioBytesAttribute: EncodableString = ""

  @JsonProperty(value = "attribute", required = true, defaultValue = "rms")
  @JsonSchemaTitle("Attribute")
  @JsonPropertyDescription("The property of the sound to measure")
  var attribute: AudioAttribute = AudioAttribute.RMS

  @JsonProperty(value = "targetSampleRate", required = true, defaultValue = "22050")
  @JsonSchemaTitle("Sample Rate (Hz)")
  @JsonPropertyDescription("Audio is resampled to this rate before analysis")
  @JsonSchemaInject(json = """{"minimum": 1000, "maximum": 192000}""")
  var targetSampleRate: Int = 22050

  @JsonProperty(value = "frameLength", required = true, defaultValue = "2048")
  @JsonSchemaTitle("Frame Length (samples)")
  @JsonPropertyDescription("Window size for each measurement")
  @JsonSchemaInject(
    json = """{"minimum": 64, "maximum": 65536}""",
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "attribute"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.regex),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = FixedGridAttributes)
    )
  )
  var frameLength: Int = 2048

  @JsonProperty(value = "hopLength", required = true, defaultValue = "512")
  @JsonSchemaTitle("Hop Length (samples)")
  @JsonPropertyDescription("Step between measurements")
  @JsonSchemaInject(
    json = """{"minimum": 1}""",
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "attribute"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.regex),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = FixedGridAttributes)
    )
  )
  var hopLength: Int = 512

  @JsonProperty(value = "maxPoints", required = true, defaultValue = "5000")
  @JsonSchemaTitle("Max Points")
  @JsonPropertyDescription("Upper bound on output rows per audio file")
  @JsonSchemaInject(json = """{"minimum": 1, "maximum": 100000}""")
  var maxPoints: Int = 5000

  @JsonProperty(value = "maxDurationSeconds", required = true, defaultValue = "600")
  @JsonSchemaTitle("Max Duration (s)")
  @JsonPropertyDescription("Longer files are not decoded")
  @JsonSchemaInject(json = """{"minimum": 1}""")
  var maxDurationSeconds: Int = 600

  @JsonProperty(value = "retainInputColumns", defaultValue = "false")
  @JsonSchemaTitle("Retain Input Columns")
  @JsonPropertyDescription("Copy the other input columns (not the audio) onto every output row")
  var retainInputColumns: Boolean = false

  @JsonProperty(value = "skipUnreadableFiles", defaultValue = "false")
  @JsonSchemaTitle("Skip Unreadable Files")
  @JsonPropertyDescription(
    "Skip audio that cannot be read, with a console message, instead of stopping with an error"
  )
  var skipUnreadableFiles: Boolean = false

  private val outputColumns = List("x", "y", "series")

  private def invalid(message: String): Nothing = throw new IllegalArgumentException(message)

  /** Settings that would crash the analysis or exhaust a worker's memory or time. */
  private def validateSettings(): Unit = {
    if (attribute == null)
      invalid("Attribute must be waveform, rms, zcr, pitch_hz, onsets or beats")
    if (targetSampleRate < 1000 || targetSampleRate > 192000)
      invalid("Sample Rate must be between 1000 and 192000 Hz")
    if (maxPoints < 1 || maxPoints > MaxPoints)
      invalid(f"Max Points must be between 1 and $MaxPoints%,d")
    if (maxDurationSeconds < 1) invalid("Max Duration must be at least 1 second")
    val samples = maxDurationSeconds.toLong * targetSampleRate
    if (samples > MaxSamples)
      invalid(
        f"Max Duration × Sample Rate allows $samples%,d samples per file; the limit is $MaxSamples%,d " +
          "(30 minutes at 22,050 Hz). Lower Max Duration or Sample Rate"
      )
    if (attribute == AudioAttribute.BEATS) {
      // Beats keep a spectrum for every step of their fixed grid, about 43 steps a
      // second at any rate, so a low rate cannot buy a longer file.
      val hop = math.max(8, math.round(targetSampleRate * 512.0 / 22050).toInt)
      if (samples / hop + 1 > MaxBeatSteps)
        invalid(
          f"For beats at $targetSampleRate Hz, Max Duration can be at most " +
            f"${(MaxBeatSteps * hop - 1) / targetSampleRate}%,d s ($MaxBeatSteps%,d analysis steps per file)"
        )
    }
    // Waveform has no frames, and onsets and beats use a fixed step of their own.
    if (FrameAttributes.contains(attribute)) {
      if (frameLength < 64 || frameLength > 65536)
        invalid("Frame Length must be between 64 and 65536")
      if (hopLength < 1 || hopLength > frameLength)
        invalid("Hop Length must be between 1 and Frame Length")
      val work = (samples / hopLength + 1) * frameLength
      if (work > MaxFrameWork)
        invalid(
          f"Hop Length $hopLength with Frame Length $frameLength analyses up to $work%,d samples per file " +
            f"(the limit is $MaxFrameWork%,d). Raise Hop Length, or lower Max Duration or Sample Rate"
        )
      val minPitchFrame = targetSampleRate / MinPitchHz + 6
      if (attribute == AudioAttribute.PITCH_HZ && frameLength < minPitchFrame)
        invalid(
          s"For pitch_hz at $targetSampleRate Hz, Frame Length must be at least $minPitchFrame " +
            s"samples, to hold one period of the lowest pitch ($MinPitchHz Hz)"
        )
    }
  }

  override def generatePythonCode(): String = {
    validateSettings()
    val attr = "\"" + attribute.getName + "\""
    val retain = if (retainInputColumns) "True" else "False"
    val skip = if (skipUnreadableFiles) "True" else "False"
    pyb"""from pytexera import *
       |import io
       |import math
       |import wave
       |
       |import numpy as np
       |
       |try:
       |    import miniaudio
       |except ImportError:  # WAV-only mode
       |    miniaudio = None
       |
       |try:
       |    from scipy.signal import resample_poly
       |except ImportError:  # fall back to a cruder resampler
       |    resample_poly = None
       |
       |try:
       |    from botocore.exceptions import ClientError as StorageError
       |except ImportError:  # no S3 client installed, so no storage errors to tell apart
       |    StorageError = ()
       |
       |
       |class AudioInputError(ValueError):
       |    # Bad input the user can fix: raised with a message naming the cause.
       |
       |    pass
       |
       |
       |${DecodingCodegen.python}
       |
       |
       |${FeaturesCodegen.python}
       |
       |
       |${BeatTrackerCodegen.python}
       |
       |
       |# ---------------------------------------------------------------- entry point
       |
       |
       |def extract(value, attribute, target_sr=22050, frame_length=2048, hop=512, max_points=5000, max_seconds=1800):
       |    # -> (x seconds, y, warnings).
       |    x, sr, warnings = decode(value, target_sr, max_seconds)
       |    if attribute == "waveform":
       |        t, y = waveform(x, sr, max_points)
       |    elif attribute == "rms":
       |        t, y = cap_points(*rms(x, sr, frame_length, hop), max_points, "mean")
       |    elif attribute == "zcr":
       |        t, y = cap_points(*zcr(x, sr, frame_length, hop), max_points, "mean")
       |    elif attribute == "pitch_hz":
       |        t, y = cap_points(*pitch(x, sr, frame_length, hop), max_points, "nanmedian")
       |    elif attribute in ("onsets", "beats"):
       |        t = onsets(x, sr) if attribute == "onsets" else beat_times(x, sr)[1]
       |        t = t[t < len(x) / sr]  # a clip shorter than one frame is padded; nothing past its end
       |        t, cut = spread(t, max_points, attribute)
       |        warnings += cut
       |        y = np.ones(len(t))
       |    else:
       |        raise AudioInputError(f"unknown attribute {attribute!r}")
       |    return t, y, warnings
       |
       |
       |class ProcessTupleOperator(UDFOperatorV2):
       |
       |    def open(self):
       |        self.row = 0
       |        self.skipped = 0
       |        self.warned = 0
       |        self.messages = 0
       |
       |    def name(self, tuple_, column):
       |        # The console shows about 100 characters of a message, so the file is named
       |        # briefly: its name from File Scan's "Include Filename" column if present, with
       |        # a long one cut in the middle so the cause that follows stays in view.
       |        name = tuple_.as_dict().get("filename")
       |        name = name.rstrip("/").rsplit("/", 1)[-1] if isinstance(name, str) and name else column
       |        return name if len(name) <= 24 else name[:11] + "..." + name[-10:]
       |
       |    def report(self, message):
       |        # A batch of bad files must not flood the console: ten messages, then totals.
       |        self.messages += 1
       |        if self.messages <= 10:
       |            print(message)
       |
       |    @overrides
       |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
       |        self.row += 1
       |        if self.row == 1 and resample_poly is None:
       |            print("SciPy is not installed, so audio is resampled approximately")
       |        column = $audioBytesAttribute
       |        names = tuple_.get_field_names()
       |        if column not in names:
       |            # Schemas ignore case, so the setting may be spelled differently from the column.
       |            column = next((n for n in names if n.lower() == column.lower()), column)
       |        value = tuple_[column]
       |        try:
       |            if isinstance(value, largebinary):
       |                value = read_large_binary(value, LargeBinaryInputStream)
       |            xs, ys, warnings = extract(value, $attr, $targetSampleRate, $frameLength, $hopLength, $maxPoints, $maxDurationSeconds)
       |        except AudioInputError as e:
       |            if not $skip:
       |                raise AudioInputError(
       |                    f"Cannot read row {self.row} ({self.name(tuple_, column)}): {e}. "
       |                    "Turn on Skip Unreadable Files to skip such files and continue"
       |                ) from None
       |            # One bad file must not stop the rest: skip it and say why.
       |            self.skipped += 1
       |            self.report(f"Skipped row {self.row} ({self.name(tuple_, column)}): {e}")
       |            return
       |        if warnings:
       |            self.warned += 1
       |        for w in warnings:
       |            self.report(f"Row {self.row} ({self.name(tuple_, column)}): {w}")
       |        carried = {}
       |        if $retain:
       |            carried = {k: v for k, v in tuple_.as_dict().items() if k != column}
       |        for xv, yv in zip(xs.tolist(), ys.tolist()):
       |            row = dict(carried)
       |            row["x"] = float(xv)
       |            row["y"] = None if math.isnan(yv) else float(yv)
       |            row["series"] = $attr
       |            yield row
       |
       |    def close(self):
       |        if self.messages > 10:
       |            totals = []
       |            for count, what in ((self.skipped, "skipped"), (self.warned, "with warnings")):
       |                if count:
       |                    totals.append(f"{count} {'row' if count == 1 else 'rows'} {what}")
       |            print(f"{' and '.join(totals)} in total; the first 10 messages are listed above")""".encode
  }

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    validateSettings()
    val input = inputSchemas.getOrElse(
      operatorInfo.inputPorts.head.id,
      invalid("Audio Attribute Extractor needs an input; connect it to File Scan")
    )
    if (audioBytesAttribute == null || audioBytesAttribute.trim.isEmpty)
      invalid("Audio Column cannot be empty")
    if (!input.containsAttribute(audioBytesAttribute))
      invalid(s"Audio Column '$audioBytesAttribute' is not in the input")
    // Schema lookups ignore case: use the column's own spelling from here on.
    val audio = input.getAttribute(audioBytesAttribute)
    val kind = audio.getType
    if (kind != AttributeType.BINARY && kind != AttributeType.LARGE_BINARY)
      invalid(
        s"Audio Column '$audioBytesAttribute' is ${kind.getName}, but must be binary or large binary; " +
          "set File Scan's attribute type to binary"
      )
    val carried =
      if (retainInputColumns)
        input.getAttributes.filterNot(_.getName == audio.getName).foldLeft(Schema())(_ add _)
      else Schema()
    val clash = outputColumns.filter(carried.containsAttribute)
    if (clash.nonEmpty)
      invalid(
        s"Retain Input Columns would duplicate ${clash.mkString(", ")}; rename those input columns first"
      )
    Map(
      operatorInfo.outputPorts.head.id -> carried
        .add("x", AttributeType.DOUBLE)
        .add("y", AttributeType.DOUBLE)
        .add("series", AttributeType.STRING)
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Audio Attribute Extractor",
      "Measure waveform, loudness, noisiness, pitch, onsets or beats over time from audio files",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )
}

object AudioAttributeExtractorOpDesc {

  /** Samples one file may decode to (Max Duration × Sample Rate); bounds memory. */
  val MaxSamples: Long = 40000000L

  /** Samples summed over all frames of one file; bounds time for small hops. */
  val MaxFrameWork: Long = 500000000L

  /** Steps of the beat grid per file; each keeps a 128-band spectrum (about 51 MB in all). */
  val MaxBeatSteps = 100000

  val MaxPoints = 100000

  /** The lowest pitch pitch_hz reports, matching fmin in [[codegen.FeaturesCodegen]]. */
  val MinPitchHz = 60

  /** Attributes measured on Frame Length / Hop Length frames. */
  val FrameAttributes: Set[AudioAttribute] =
    Set(AudioAttribute.RMS, AudioAttribute.ZCR, AudioAttribute.PITCH_HZ)

  /** Attributes that hide Frame Length and Hop Length. */
  final val FixedGridAttributes = "^waveform$|^onsets$|^beats$"
}
