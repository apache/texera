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

package org.apache.texera.amber.operator.source.scan.text

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaString,
  JsonSchemaTitle
}
import org.apache.texera.amber.operator.StandaloneHelpers
import org.apache.texera.amber.operator.metadata.annotations.HideAnnotation
import org.apache.texera.amber.operator.source.scan.FileAttributeType

/**
  * TextSourceOpDesc is a trait holding commonly used properties and functions used for variations of text input processing
  * Create new, identical limit and offset fields with additional annotations to make hideable binary attributes
  * and strings that are in SingleTuple mode will always read the entire input, so limit / offset are disabled in these cases
  */
trait TextSourceOpDesc {
  @JsonProperty(defaultValue = "string", required = true)
  @JsonSchemaTitle("Attribute Type")
  @JsonPropertyDescription(
    "The output field type. Each line becomes a separate tuple, except for " +
      "'single string' / 'binary' / 'large binary', where the entire text becomes one tuple."
  )
  var attributeType: FileAttributeType = FileAttributeType.STRING

  @JsonProperty(defaultValue = "line", required = true)
  @JsonSchemaTitle("Attribute Name")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  var attributeName: String = "line"

  // Named explicitly so reflection can see it. These are the row-window knobs the
  // text sources actually read — the ones they inherit are ignored — but they carried
  // no @JsonProperty, which leaves them invisible to anything walking the config.
  @JsonProperty
  @JsonSchemaTitle("Limit (lines)")
  @JsonDeserialize(contentAs = classOf[Int])
  @JsonPropertyDescription(
    "Maximum number of lines to output. Leave empty to read all lines. " +
      "(Ignored when reading the whole file as one tuple.)"
  )
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "attributeType"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.regex),
      new JsonSchemaString(
        path = HideAnnotation.hideExpectedValue,
        value = "^binary$|^single string$"
      )
    )
  )
  var fileScanLimit: Option[Int] = None

  @JsonProperty
  @JsonSchemaTitle("Offset (lines)")
  @JsonPropertyDescription(
    "Number of lines to skip from the start before reading. " +
      "(Ignored when reading the whole file as one tuple.)"
  )
  @JsonDeserialize(contentAs = classOf[Int])
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "attributeType"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.regex),
      new JsonSchemaString(
        path = HideAnnotation.hideExpectedValue,
        value = "^binary$|^single string$"
      )
    )
  )
  var fileScanOffset: Option[Int] = None
}

object TextSourceOpDesc {

  /**
    * The line-to-number rules the engine uses, as Python.
    *
    * `parseField` trims the line and reads it with `toInt`, `toLong` or
    * `toDouble`. `int()` and `float()` disagree both ways: they read "1_0",
    * "nan", "inf" and an INTEGER past 32 bits, which end the engine's run, and
    * `float()` refuses "2d" and "0x1p3", which the engine reads. `trim` drops
    * only the characters up to U+0020, so a no-break space stays and fails.
    *
    *  - `_texera_java_trim`: `String.trim`, dropping the characters up to U+0020.
    *  - `_texera_parse_int`: `Integer.parseInt` or `Long.parseLong`, a sign and digits in range.
    *  - `_texera_parse_double`: `Double.parseDouble`'s grammar, read by `_texera_java_double`.
    */
  val NumberParsers: String =
    """def _texera_java_trim(line):
      |    return line.strip("".join(chr(i) for i in range(33)))
      |
      |
      |def _texera_parse_int(line, bits):
      |    import re
      |
      |    text = _texera_java_trim(line)
      |    if not re.fullmatch(r"[+-]?\d+", text):
      |        raise ValueError("For input string: " + repr(text))
      |    value = int(text)
      |    if not -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
      |        raise ValueError("For input string: " + repr(text))
      |    return value
      |
      |
      |def _texera_parse_double(line):
      |    import re
      |
      |    text = _texera_java_trim(line)
      |    if not re.fullmatch(
      |        r"[+-]?(NaN|Infinity|"
      |        r"((\d+\.?\d*|\.\d+)([eE][+-]?\d+)?"
      |        r"|0[xX]([0-9a-fA-F]+\.?|[0-9a-fA-F]*\.[0-9a-fA-F]+)[pP][+-]?\d+)[fFdD]?)",
      |        text,
      |        re.ASCII,
      |    ):
      |        raise ValueError("For input string: " + repr(text))
      |    return _texera_java_double(text)""".stripMargin

  /**
    * The line-to-boolean rule the engine uses, as Python.
    *
    * `parseField` reads "true" and "false" in any case, then falls back to an
    * integer and calls only 1 true. Anything else ends the run. Comparing the
    * lowercased line to "true" read 1 as false and let text the engine refuses
    * through as a row of false.
    */
  val BooleanParser: String =
    """def _texera_parse_bool(line):
      |    text = _texera_java_trim(line)
      |    lowered = text.lower()
      |    if lowered == "true":
      |        return True
      |    if lowered == "false":
      |        return False
      |    return _texera_parse_int(text, 32) == 1""".stripMargin

  /** The call the per-line cast makes, over the loop variable the readers share. */
  val BooleanParserCall: String = "_texera_parse_bool(l)"

  /** A TIMESTAMP line is kept as text and parsed once the column is built, as
    * CSV and JSONL parse theirs. `pd.Timestamp` kept the nanoseconds and the
    * offset that the engine's DateParserUtils does not.
    */
  def parseTimestampColumn(attributeType: FileAttributeType, colLit: String): Seq[String] =
    if (attributeType == FileAttributeType.TIMESTAMP)
      Seq(s"out1df[$colLit] = _texera_text_to_timestamp(out1df[$colLit])")
    else Seq.empty

  /** The helpers the Python above calls for this attribute type. */
  def lineHelpers(attributeType: FileAttributeType): Seq[String] =
    attributeType match {
      case FileAttributeType.INTEGER   => Seq(NumberParsers)
      case FileAttributeType.LONG      => Seq(NumberParsers)
      case FileAttributeType.DOUBLE    => Seq(StandaloneHelpers.AttributeCasts, NumberParsers)
      case FileAttributeType.BOOLEAN   => Seq(NumberParsers, BooleanParser)
      case FileAttributeType.TIMESTAMP => Seq(StandaloneHelpers.AttributeCasts)
      case _                           => Seq.empty
    }
}
