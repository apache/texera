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

package org.apache.texera.amber.operator.source.scan.csv

import com.univocity.parsers.common.TextParsingException
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import org.scalatest.flatspec.AnyFlatSpec

import java.io.StringReader

/**
  * Verifies the column-overflow translation in [[CSVScanSourceOpExec.parseNextRow]]
  * — the path that turns a deep Univocity stack trace into a single-sentence message
  * the workflow user can act on.
  */
class CSVScanSourceOpExecSpec extends AnyFlatSpec {

  private def parserWithMaxColumns(max: Int): CsvParser = {
    val settings = new CsvParserSettings()
    settings.setMaxColumns(max)
    settings.setMaxCharsPerColumn(-1)
    new CsvParser(settings)
  }

  "parseNextRow" should "return the parsed row when the input is within the column limit" in {
    val parser = parserWithMaxColumns(10)
    parser.beginParsing(new StringReader("a,b,c\n"))

    val row = CSVScanSourceOpExec.parseNextRow(parser, 10)

    assert(row.toSeq == Seq("a", "b", "c"))
  }

  it should "return null at end of input (so the iterator can terminate cleanly)" in {
    val parser = parserWithMaxColumns(10)
    parser.beginParsing(new StringReader(""))

    assert(CSVScanSourceOpExec.parseNextRow(parser, 10) == null)
  }

  it should "translate a column-overflow TextParsingException into a clear user message" in {
    val maxColumns = 2
    val parser = parserWithMaxColumns(maxColumns)
    parser.beginParsing(new StringReader("a,b,c,d,e\n"))

    val ex = intercept[RuntimeException] {
      CSVScanSourceOpExec.parseNextRow(parser, maxColumns)
    }

    // The message must mention the configured limit so the user knows what was hit.
    assert(ex.getMessage.contains(maxColumns.toString))
    assert(ex.getMessage.toLowerCase.contains("max columns"))
    assert(ex.getMessage.toLowerCase.contains("exceeded"))
    // The original Univocity exception is preserved as the cause so developers
    // can still inspect the underlying parser state if needed.
    assert(ex.getCause.isInstanceOf[TextParsingException])
  }

  "isColumnOverflow" should "detect AIOOBE causes from Java 8's plain-integer message" in {
    val cause = new ArrayIndexOutOfBoundsException("5")
    val ex = new TextParsingException(null, "wrapper", cause)
    assert(CSVScanSourceOpExec.isColumnOverflow(ex, maxColumns = 5))
    assert(!CSVScanSourceOpExec.isColumnOverflow(ex, maxColumns = 6))
  }

  it should "detect AIOOBE causes from Java 9+'s 'Index N out of bounds for length M' message" in {
    val cause = new ArrayIndexOutOfBoundsException("Index 5 out of bounds for length 5")
    val ex = new TextParsingException(null, "wrapper", cause)
    assert(CSVScanSourceOpExec.isColumnOverflow(ex, maxColumns = 5))
    assert(!CSVScanSourceOpExec.isColumnOverflow(ex, maxColumns = 6))
  }

  it should "ignore TextParsingExceptions whose cause is unrelated" in {
    val unrelated = new TextParsingException(null, "Some other parsing problem")
    val withDifferentCause =
      new TextParsingException(null, "wrapper", new IllegalStateException("nope"))
    assert(!CSVScanSourceOpExec.isColumnOverflow(unrelated, maxColumns = 5))
    assert(!CSVScanSourceOpExec.isColumnOverflow(withDifferentCause, maxColumns = 5))
  }

  it should "ignore an AIOOBE whose message cannot be parsed as an index" in {
    val unparseable = new ArrayIndexOutOfBoundsException("something went wrong")
    val ex = new TextParsingException(null, "wrapper", unparseable)
    assert(!CSVScanSourceOpExec.isColumnOverflow(ex, maxColumns = 5))
  }

  "columnOverflowMessage" should "include the configured maximum so the user knows the current limit" in {
    val msg = CSVScanSourceOpExec.columnOverflowMessage(750)
    assert(msg.contains("750"))
    assert(msg.toLowerCase.contains("max columns"))
    assert(msg.toLowerCase.contains("exceeded"))
  }
}
