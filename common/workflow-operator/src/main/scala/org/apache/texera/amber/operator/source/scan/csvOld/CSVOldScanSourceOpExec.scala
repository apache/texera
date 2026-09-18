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

package org.apache.texera.amber.operator.source.scan.csvOld

import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import org.apache.texera.amber.core.executor.SourceOperatorExecutor
import org.apache.texera.amber.core.storage.DocumentFactory
import org.apache.texera.amber.core.tuple.{Attribute, AttributeTypeUtils, Schema, TupleLike}
import org.apache.texera.amber.operator.source.scan.{ScanRowParseError, SkippedRowReporter}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.net.URI
import scala.collection.compat.immutable.ArraySeq

class CSVOldScanSourceOpExec private[csvOld] (
    descString: String
) extends SourceOperatorExecutor {
  val desc: CSVOldScanSourceOpDesc =
    objectMapper.readValue(descString, classOf[CSVOldScanSourceOpDesc])
  var reader: CSVReader = _
  var rows: Iterator[Seq[String]] = _
  val schema: Schema = desc.sourceSchema()
  private val skippedRows = new SkippedRowReporter()

  override def getWarnings: Seq[String] = skippedRows.warnings

  override def produceTuple(): Iterator[TupleLike] = {

    val tuples = rows.zipWithIndex
      .map {
        case (fields, index) =>
          try {
            val parsedFields: Array[Any] = AttributeTypeUtils.parseFields(
              fields.toArray,
              schema.getAttributes
                .map((attr: Attribute) => attr.getType)
                .toArray
            )
            TupleLike(ArraySeq.unsafeWrapArray(parsedFields): _*)
          } catch {
            case e: Throwable =>
              // Skip the unparsable row but surface it as a warning instead of
              // dropping it silently. `rows` already dropped header and offset, so
              // the absolute 1-based data-row number adds the offset back.
              skippedRows.record(
                ScanRowParseError.skipWarning(
                  fields,
                  schema,
                  desc.inferSampleSize,
                  Some(desc.offset.getOrElse(0) + index + 1),
                  e
                )
              )
              null
          }
      }
      .filter(tuple => tuple != null)

    if (desc.limit.isDefined)
      tuples.take(desc.limit.get)
    else {
      tuples
    }
  }

  override def open(): Unit = {
    implicit object CustomFormat extends DefaultCSVFormat {
      override val delimiter: Char = desc.customDelimiter.get.charAt(0)
    }
    val filePath = DocumentFactory.openReadonlyDocument(new URI(desc.fileName.get)).asFile().toPath
    reader = CSVReader.open(filePath.toString, desc.fileEncoding.getCharset.name())(CustomFormat)
    // skip line if this worker reads the start of a file, and the file has a header line
    val startOffset = desc.offset.getOrElse(0) + (if (desc.hasHeader) 1 else 0)
    rows = reader.iterator.drop(startOffset)
  }

  override def close(): Unit = {
    if (reader != null) {
      reader.close()
    }
  }
}
