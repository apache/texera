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

package edu.uci.ics.amber.operator.source.scan.json

import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.tuple.AttributeTypeUtils.parseField
import edu.uci.ics.amber.core.tuple.TupleLike
import edu.uci.ics.amber.operator.source.scan.json.JSONUtil.JSONToMap
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.{Failure, Success, Try}

class JSONLScanSourceOpExec private[json] (
    descString: String,
    idx: Int = 0,
    workerCount: Int = 1
) extends SourceOperatorExecutor {
  private val desc: JSONLScanSourceOpDesc =
    objectMapper.readValue(descString, classOf[JSONLScanSourceOpDesc])
  private var rows: Iterator[String] = _
  private var reader: BufferedReader = _
  private val schema = desc.sourceSchema()

  override def produceTuple(): Iterator[TupleLike] = {
    rows.flatMap { line =>
      Try {
        val data = JSONToMap(objectMapper.readTree(line), desc.flatten).withDefaultValue(null)
        val fields = schema.getAttributeNames.map { fieldName =>
          parseField(data(fieldName), schema.getAttribute(fieldName).getType)
        }
        TupleLike(fields: _*)
      } match {
        case Success(tuple) => Some(tuple)
        case Failure(_)     => None
      }
    }
  }

  override def open(): Unit = {
    val stream = DocumentFactory.openReadonlyDocument(new URI(desc.fileName.get)).asInputStream()
    // count lines and partition the task to each worker
    reader = new BufferedReader(
      new InputStreamReader(stream, desc.fileEncoding.getCharset)
    )
    val offsetValue = desc.offset.getOrElse(0)
    var lines = reader.lines().iterator().asScala.drop(offsetValue)
    if (desc.limit.isDefined) lines = lines.take(desc.limit.get)
    val (it1, it2) = lines.duplicate
    val count: Int = it1.map(_ => 1).sum

    val startOffset: Int = offsetValue + count / workerCount * idx
    val endOffset: Int =
      offsetValue + (if (idx != workerCount - 1) count / workerCount * (idx + 1)
                     else count)

    rows = it2.iterator.slice(startOffset, endOffset)
  }

  override def close(): Unit = reader.close()

}
