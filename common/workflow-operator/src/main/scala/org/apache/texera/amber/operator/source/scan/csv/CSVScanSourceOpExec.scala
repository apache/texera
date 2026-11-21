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
import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.texera.amber.core.executor.SourceOperatorExecutor
import org.apache.texera.amber.core.storage.DocumentFactory
import org.apache.texera.amber.core.tuple.{AttributeTypeUtils, Schema, TupleLike}
import org.apache.texera.amber.operator.source.scan.{ScanRowParseError, SkippedRowReporter}
import org.apache.texera.amber.util.ArrowUtils
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.dao.SiteSettings

import java.io.InputStreamReader
import java.net.URI
import scala.collection.immutable.ArraySeq
import scala.util.Try

class CSVScanSourceOpExec private[csv] (descString: String) extends SourceOperatorExecutor {
  val desc: CSVScanSourceOpDesc = objectMapper.readValue(descString, classOf[CSVScanSourceOpDesc])
  var inputReader: InputStreamReader = _
  var parser: CsvParser = _
  var nextRow: Array[String] = _
  var numRowGenerated = 0
  private var maxColumns: Int = CSVScanSourceOpExec.DEFAULT_MAX_COLUMNS
  private val schema: Schema = desc.sourceSchema()
  private val skippedRows = new SkippedRowReporter()

  override def getWarnings: Seq[String] = skippedRows.warnings

  override def produceTuple(): Iterator[TupleLike] = {

    val rowIterator = new Iterator[Array[String]] {
      override def hasNext: Boolean = {
        if (nextRow != null) {
          return true
        }
        nextRow = CSVScanSourceOpExec.parseNextRow(parser, maxColumns)
        nextRow != null
      }

      override def next(): Array[String] = {
        val ret = nextRow
        numRowGenerated += 1
        nextRow = null
        ret
      }
    }

    var tupleIterator = rowIterator
      .drop(desc.windowOffset)
      .map(row => {
        try {
          TupleLike(
            ArraySeq.unsafeWrapArray(
              AttributeTypeUtils.parseFields(row.asInstanceOf[Array[Any]], schema)
            ): _*
          )
        } catch {
          case e: Throwable =>
            // Skip the unparsable row but surface it as a warning instead of
            // dropping it silently. numRowGenerated is the 1-based data-row number.
            skippedRows.record(
              ScanRowParseError
                .skipWarning(row.toSeq, schema, desc.inferSampleSize, Some(numRowGenerated), e)
            )
            null
        }
      })
      .filter(t => t != null)

    if (desc.windowLimit.isDefined) tupleIterator = tupleIterator.take(desc.windowLimit.get)

    tupleIterator
  }

  // Columnar produce: parse each CSV row's typed values straight into Arrow vectors (the same
  // AttributeTypeUtils.parseFields + per-column setCell the row path's values go through), so no
  // per-row Tuple (and no Tuple.inMemSize deepSizeOf walk) is built. Rows are packed into bounded
  // Arrow batches (COLUMNAR_BATCH_ROWS each) so the batch streams and pauses at a batch boundary.
  // An unparsable row is skipped and reported, exactly as on the row path. Roots are allocated in
  // the shared allocator so a handed-off batch outlives this exec. Falls back to the row path
  // (None) when a scan window (offset/limit) is set.
  override def produceColumnarBatch(
      outputSchema: Schema
  ): Option[Iterator[VectorSchemaRoot]] = {
    if (desc.windowOffset > 0 || desc.windowLimit.isDefined) return None
    val arrowSchema = ArrowUtils.fromTexeraSchema(schema)
    val numCols = schema.getAttributes.length
    val batchSize = CSVScanSourceOpExec.COLUMNAR_BATCH_ROWS
    Some(new Iterator[VectorSchemaRoot] {
      private var pending: Array[String] = CSVScanSourceOpExec.parseNextRow(parser, maxColumns)
      override def hasNext: Boolean = pending != null
      override def next(): VectorSchemaRoot = {
        val root = VectorSchemaRoot.create(arrowSchema, ArrowUtils.sharedAllocator)
        root.allocateNew()
        var r = 0
        while (r < batchSize && pending != null) {
          val row = pending
          numRowGenerated += 1
          pending = CSVScanSourceOpExec.parseNextRow(parser, maxColumns)
          try {
            val values = AttributeTypeUtils.parseFields(row.asInstanceOf[Array[Any]], schema)
            var c = 0
            while (c < numCols) {
              ArrowUtils.setCell(root.getVector(c), r, values(c).asInstanceOf[AnyRef]); c += 1
            }
            r += 1
          } catch {
            case e: Throwable =>
              skippedRows.record(
                ScanRowParseError
                  .skipWarning(row.toSeq, schema, desc.inferSampleSize, Some(numRowGenerated), e)
              )
          }
        }
        root.setRowCount(r)
        root
      }
    })
  }

  override def open(): Unit = {
    inputReader = new InputStreamReader(
      DocumentFactory.openReadonlyDocument(new URI(desc.fileName.get)).asInputStream(),
      desc.fileEncoding.getCharset
    )

    val csvFormat = new CsvFormat()
    csvFormat.setDelimiter(desc.customDelimiter.get.charAt(0))
    csvFormat.setLineSeparator("\n")
    csvFormat.setComment(
      '\u0000'
    ) // disable skipping lines starting with # (default comment character)
    val csvSetting = new CsvParserSettings()
    csvSetting.setMaxCharsPerColumn(-1)
    maxColumns = CSVScanSourceOpExec.getMaxColumns
    csvSetting.setMaxColumns(maxColumns)
    csvSetting.setFormat(csvFormat)
    csvSetting.setHeaderExtractionEnabled(desc.hasHeader)

    parser = new CsvParser(csvSetting)
    parser.beginParsing(inputReader)
  }

  override def close(): Unit = {
    if (parser != null) {
      parser.stopParsing()
    }
    if (inputReader != null) {
      inputReader.close()
    }
  }
}

object CSVScanSourceOpExec {
  val DEFAULT_MAX_COLUMNS = 512

  // Rows per Arrow batch on the columnar path: large enough to amortize vectorized processing,
  // small enough to stream and to bound a pause to one batch.
  val COLUMNAR_BATCH_ROWS = 10000

  def getMaxColumns: Int =
    SiteSettings.getInt("csv_parser_max_columns", DEFAULT_MAX_COLUMNS)

  /**
    * Wraps `parser.parseNext()` so a column-count overflow is reported to the user
    * as a clear instruction rather than a deep Univocity stack trace. Other parser
    * failures are rethrown unchanged.
    *
    * The thrown RuntimeException's message bubbles up through DataProcessor.handleExecutorException
    * and becomes the title of the console message that drives the top-of-page toast.
    */
  def parseNextRow(parser: CsvParser, maxColumns: Int): Array[String] = {
    try parser.parseNext()
    catch {
      case e: TextParsingException if isColumnOverflow(e, maxColumns) =>
        throw new RuntimeException(columnOverflowMessage(maxColumns), e)
    }
  }

  private[csv] def isColumnOverflow(e: TextParsingException, maxColumns: Int): Boolean =
    Option(e.getCause)
      .collect { case aioobe: ArrayIndexOutOfBoundsException => aioobe }
      .exists(aioobe => aioobeIndex(aioobe).exists(_ == maxColumns))

  private def aioobeIndex(aioobe: ArrayIndexOutOfBoundsException): Option[Int] = {
    val msg = Option(aioobe.getMessage).getOrElse("")
    Try(msg.trim.toInt).toOption.orElse {
      raw"Index (\d+) out of bounds".r.findFirstMatchIn(msg).map(_.group(1).toInt)
    }
  }

  private[csv] def columnOverflowMessage(maxColumns: Int): String =
    s"Max columns of $maxColumns exceeded."
}
