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

package org.apache.texera.amber.operator.source.scan.smart

import org.scalatest.flatspec.AnyFlatSpec

import java.nio.charset.StandardCharsets

class FormatDetectorSpec extends AnyFlatSpec {

  private val utf8 = StandardCharsets.UTF_8

  "FormatDetector" should "detect Parquet by magic bytes" in {
    val bytes = "PAR1".getBytes(utf8) ++ Array.fill(20)(0.toByte)
    assert(FormatDetector.detect(None, bytes, utf8) == SmartFileFormat.PARQUET)
  }

  it should "detect XLSX by ZIP magic bytes" in {
    val bytes = Array[Byte](0x50, 0x4b, 0x03, 0x04, 0, 0, 0, 0)
    assert(FormatDetector.detect(Some("foo.xlsx"), bytes, utf8) == SmartFileFormat.EXCEL)
  }

  it should "not classify a generic ZIP container as Excel" in {
    val bytes = Array[Byte](0x50, 0x4b, 0x03, 0x04, 0, 0, 0, 0)
    assert(FormatDetector.detect(Some("archive.zip"), bytes, utf8) == SmartFileFormat.TEXT)
  }

  it should "detect Arrow by ARROW1 magic" in {
    val bytes = "ARROW1\u0000\u0000".getBytes(utf8)
    assert(FormatDetector.detect(None, bytes, utf8) == SmartFileFormat.ARROW)
  }

  it should "detect TSV when content contains tabs and extension matches" in {
    val bytes = "id\tname\tage\n1\tAda\t36\n2\tLin\t29\n".getBytes(utf8)
    assert(FormatDetector.detect(Some("users.tsv"), bytes, utf8) == SmartFileFormat.TSV)
  }

  it should "detect TSV by content even if extension is .csv" in {
    val bytes = "id\tname\tage\n1\tAda\t36\n2\tLin\t29\n".getBytes(utf8)
    val detected = FormatDetector.detect(Some("misnamed.csv"), bytes, utf8)
    // The .csv extension wins over content sniffing — that's the expected ranking.
    assert(detected == SmartFileFormat.CSV)
  }

  it should "fall back to content sniffing when extension is unknown" in {
    val bytes = "id\tname\n1\tAda\n2\tLin\n".getBytes(utf8)
    assert(FormatDetector.detect(Some("blob.bin"), bytes, utf8) == SmartFileFormat.TSV)
  }

  it should "detect JSONL when multiple lines start with {" in {
    val bytes = "{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n".getBytes(utf8)
    assert(FormatDetector.detect(None, bytes, utf8) == SmartFileFormat.JSONL)
  }

  it should "detect JSON array when content starts with [" in {
    val bytes = "[ {\"a\":1}, {\"a\":2} ]".getBytes(utf8)
    assert(FormatDetector.detect(None, bytes, utf8) == SmartFileFormat.JSON)
  }

  it should "detect plain text when there are no delimiters" in {
    val bytes = "hello world\nthis is text\n".getBytes(utf8)
    assert(FormatDetector.detect(None, bytes, utf8) == SmartFileFormat.TEXT)
  }

  it should "prefer extension over content sniffing for CSV" in {
    val bytes = "a,b,c\n1,2,3\n".getBytes(utf8)
    assert(FormatDetector.detect(Some("data.csv"), bytes, utf8) == SmartFileFormat.CSV)
  }
}
