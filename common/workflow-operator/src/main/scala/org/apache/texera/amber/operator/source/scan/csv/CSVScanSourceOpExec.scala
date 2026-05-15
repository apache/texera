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

import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import org.apache.texera.amber.core.executor.SourceOperatorExecutor
import org.apache.texera.amber.core.storage.DocumentFactory
import org.apache.texera.amber.core.tuple.{AttributeTypeUtils, Schema, TupleLike}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.io.InputStreamReader
import java.net.URI
import scala.collection.immutable.ArraySeq

class CSVScanSourceOpExec private[csv] (descString: String) extends SourceOperatorExecutor {
  val desc: CSVScanSourceOpDesc = objectMapper.readValue(descString, classOf[CSVScanSourceOpDesc])
  var inputReader: InputStreamReader = _
  var parser: CsvParser = _
  var nextRow: Array[String] = _
  var numRowGenerated = 0
  private val schema: Schema = desc.sourceSchema()
  // When lineage tracking is on, `schema` carries an extra __lineage_origin_row
  // column appended after the data columns. `dataSchema` is the column shape
  // seen by AttributeTypeUtils.parseFields (must match the raw row's cell count);
  // the lineage value is appended after parsing.
  private val dataSchema: Schema =
    if (desc.trackLineage) schema.remove(CSVScanSourceOpDesc.LineageOriginRowColumn)
    else schema

  override def produceTuple(): Iterator[TupleLike] = {

    val rowIterator = new Iterator[Array[String]] {
      override def hasNext: Boolean = {
        if (nextRow != null) {
          return true
        }
        nextRow = parser.parseNext()
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
      .drop(desc.offset.getOrElse(0))
      .map(row => {
        try {
          val parsed =
            AttributeTypeUtils.parseFields(row.asInstanceOf[Array[Any]], dataSchema)
          val fields: Array[Any] =
            // 1-indexed source row within the parsed CSV body (post-header, post-offset
            // applied transparently via the underlying parser's progress counter).
            if (desc.trackLineage) parsed :+ numRowGenerated.toLong
            else parsed
          TupleLike(ArraySeq.unsafeWrapArray(fields): _*)
        } catch {
          case _: Throwable => null
        }
      })
      .filter(t => t != null)

    if (desc.limit.isDefined) tupleIterator = tupleIterator.take(desc.limit.get)

    tupleIterator
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
