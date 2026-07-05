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

  it should "silently drop rows that cannot be parsed into the inferred schema" in {
    // No header, so every line is data. The schema is inferred from the first
    // `limit` rows only (INFER_READ_LIMIT is capped by limit); those are integers,
    // so the column is inferred as INTEGER. `offset` then shifts the output window
    // past that inference sample onto a row whose value ("oops") is not an integer,
    // so parseFields throws and produceTuple filters that row out instead of failing.
    val exec =
      execOver(
        writeTempCsv("1\n2\noops\n3\n4\n"),
        hasHeader = false,
        offset = Some(2),
        limit = Some(2)
      )
    exec.open()
    val tuples =
      try exec.produceTuple().toList
      finally exec.close()

    // Output window is rows 3,4,5 ("oops","3","4"); the bad row is skipped, so we
    // get the two good ones rather than an exception. Count is below the raw 5 rows.
    assert(tuples.size == 2)
    assert(tuples.map(_.getFields(0).toString) == List("3", "4"))
  }
}
