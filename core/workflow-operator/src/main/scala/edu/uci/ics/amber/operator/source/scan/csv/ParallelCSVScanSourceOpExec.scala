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

package edu.uci.ics.amber.operator.source.scan.csv

import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeTypeUtils, TupleLike}
import edu.uci.ics.amber.operator.source.BufferedBlockReader
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import org.tukaani.xz.SeekableFileInputStream

import java.net.URI
import java.util
import java.util.stream.{IntStream, Stream}
import scala.collection.compat.immutable.ArraySeq

class ParallelCSVScanSourceOpExec private[csv] (
    descString: String,
    idx: Int = 0,
    workerCount: Int = 1
) extends SourceOperatorExecutor {
  val desc: ParallelCSVScanSourceOpDesc =
    objectMapper.readValue(descString, classOf[ParallelCSVScanSourceOpDesc])
  private var reader: BufferedBlockReader = _
  private val schema = desc.sourceSchema()

  override def produceTuple(): Iterator[TupleLike] =
    new Iterator[TupleLike]() {
      override def hasNext: Boolean = reader.hasNext

      override def next(): TupleLike = {

        try {
          // obtain String representation of each field
          // a null value will present if omit in between fields, e.g., ['hello', null, 'world']
          val line = reader.readLine
          if (line == null) {
            return null
          }
          var fields: Array[AnyRef] = line.toArray

          if (fields == null || util.Arrays.stream(fields).noneMatch(s => s != null)) {
            // discard tuple if it's null or it only contains null
            // which means it will always discard Tuple(null) from readLine()
            return null
          }

          // however the null values won't present if omitted in the end, we need to match nulls.
          if (fields.length != schema.getAttributes.size)
            fields = Stream
              .concat(
                util.Arrays.stream(fields),
                IntStream
                  .range(0, schema.getAttributes.size - fields.length)
                  .mapToObj((_: Int) => null)
              )
              .toArray()
          // parse Strings into inferred AttributeTypes
          val parsedFields: Array[Any] = AttributeTypeUtils.parseFields(
            fields.asInstanceOf[Array[Any]],
            schema.getAttributes
              .map((attr: Attribute) => attr.getType)
              .toArray
          )
          TupleLike(ArraySeq.unsafeWrapArray(parsedFields): _*)
        } catch {
          case _: Throwable => null
        }
      }

    }.filter(tuple => tuple != null)

  override def open(): Unit = {
    // here, the stream requires to be seekable, so datasetFileDesc creates a temp file here
    // TODO: consider a better way
    val file = DocumentFactory.openReadonlyDocument(new URI(desc.fileName.get)).asFile()
    val totalBytes: Long = file.length()
    // TODO: add support for limit
    // TODO: add support for offset
    val startOffset: Long = totalBytes / workerCount * idx
    val endOffset: Long =
      if (idx != workerCount - 1) totalBytes / workerCount * (idx + 1) else totalBytes

    val stream = new SeekableFileInputStream(file)

    stream.seek(startOffset)
    reader = new BufferedBlockReader(
      stream,
      endOffset - startOffset,
      desc.customDelimiter.get.charAt(0),
      null
    )
    // skip line if this worker reads from middle of a file
    if (startOffset > 0) reader.readLine
    // skip line if this worker reads the start of a file, and the file has a header line
    if (startOffset == 0 && desc.hasHeader) reader.readLine
  }

  override def close(): Unit = reader.close()

}
