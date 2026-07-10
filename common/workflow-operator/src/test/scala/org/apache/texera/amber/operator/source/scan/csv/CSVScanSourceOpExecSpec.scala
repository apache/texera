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
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.io.StringReader
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

/**
  * Verifies the column-overflow translation in [[CSVScanSourceOpExec.parseNextRow]]
  * — the path that turns a deep Univocity stack trace into a single-sentence message
  * the workflow user can act on — and the instance-side open()/produceTuple()/close()
  * scan loop driven over real temp CSV files.
  */
class CSVScanSourceOpExecSpec extends AnyFlatSpec with BeforeAndAfterAll {

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

  // ---------------------------------------------------------------------------
  // Instance-side scan loop: open() -> produceTuple() -> close() over temp CSVs.
  // ---------------------------------------------------------------------------

  private var tempFiles: List[Path] = Nil

  override def afterAll(): Unit = {
    tempFiles.foreach(Files.deleteIfExists)
    super.afterAll()
  }

  /** Writes `content` to a fresh temp .csv and returns its path (tracked for cleanup). */
  private def writeTempCsv(content: String): Path = {
    val path = Files.createTempFile("csv-scan-exec-spec", ".csv")
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
    tempFiles = path :: tempFiles
    path
  }

  /**
    * Builds a CSVScanSourceOpExec over `path`. The descriptor MUST have a custom
    * delimiter and a resolved file URI *before* the exec is constructed: the
    * constructor eagerly computes the schema via desc.sourceSchema(), which needs
    * both to return a non-null schema.
    */
  private def execOver(
      path: Path,
      hasHeader: Boolean,
      offset: Option[Int] = None,
      limit: Option[Int] = None
  ): CSVScanSourceOpExec = {
    val desc = new CSVScanSourceOpDesc()
    desc.customDelimiter = Some(",")
    desc.hasHeader = hasHeader
    desc.offset = offset
    desc.limit = limit
    desc.setResolvedFileName(URI.create(path.toUri.toString))
    new CSVScanSourceOpExec(objectMapper.writeValueAsString(desc))
  }

  "CSVScanSourceOpExec" should "scan a header CSV and emit one tuple per data row" in {
    val exec = execOver(writeTempCsv("a,b\n1,x\n2,y\n"), hasHeader = true)
    exec.open()
    val tuples =
      try exec.produceTuple().toList
      finally exec.close()

    assert(tuples.size == 2)
    val schema = exec.desc.sourceSchema()
    assert(schema.getAttributeNames.toSet == Set("a", "b"))
  }

  it should "honor offset and limit, emitting only the requested window" in {
    // 5 data rows; drop the first (offset=1), then take 2 (limit=2) -> rows 2 and 3.
    val exec =
      execOver(
        writeTempCsv("a,b\n1,x\n2,y\n3,z\n4,p\n5,q\n"),
        hasHeader = true,
        offset = Some(1),
        limit = Some(2)
      )
    exec.open()
    val tuples =
      try exec.produceTuple().toList
      finally exec.close()

    assert(tuples.size == 2)
    val firstCol = tuples.map(_.getFields(0).toString)
    assert(firstCol == List("2", "3"))
  }

  it should "fail loudly when a row cannot be parsed into the inferred schema" in {
    // No header, so every line is data. Type inference samples only the first
    // INFER_READ_LIMIT (=100) rows; here they are all integers, so the single
    // column (auto-named "column-1") is inferred as INTEGER. Row 101 holds a
    // non-integer ("oops"), which does not match the inferred schema. The scan
    // must abort loudly on that row (surfacing to the UI via
    // DataProcessor.handleExecutorException) rather than silently dropping it.
    val content = (1 to 100).mkString("\n") + "\noops\n"
    val exec = execOver(writeTempCsv(content), hasHeader = false)
    exec.open()
    val ex = intercept[RuntimeException] {
      try exec.produceTuple().toList
      finally exec.close()
    }

    // The message must lead with the essentials — row number, offending value,
    // column name, expected type — then the actionable fix.
    assert(ex.getMessage.startsWith("Row 101: value")) // 1-based row of the bad value
    assert(ex.getMessage.contains("'oops'")) // the offending value
    assert(ex.getMessage.contains("'column-1'")) // the offending column's name
    assert(ex.getMessage.contains("cannot be read as"))
    assert(ex.getMessage.contains("INTEGER")) // the inferred/expected type
    assert(ex.getMessage.contains("clean the data before scanning")) // actionable fix
    // The original parse exception is preserved as the cause for debugging.
    assert(ex.getCause != null)
  }
}
