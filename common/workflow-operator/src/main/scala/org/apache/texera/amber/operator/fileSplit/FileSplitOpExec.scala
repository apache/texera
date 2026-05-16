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

package org.apache.texera.amber.operator.fileSplit

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.JSONUtils.objectMapper

import scala.collection.mutable

class FileSplitOpExec(descString: String) extends OperatorExecutor {
  private val desc: FileSplitOpDesc = objectMapper.readValue(descString, classOf[FileSplitOpDesc])
  private val fileToPort = mutable.LinkedHashMap.empty[String, PortIdentity]
  private var fileAttribute: String = _
  private var outputPortCount: Int = _

  override def open(): Unit = {
    outputPortCount = desc.operatorInfo.outputPorts.length
    require(outputPortCount > 0, "File Split requires at least one output port")
  }

  override def processTupleMultiPort(
      tuple: Tuple,
      port: Int
  ): Iterator[(TupleLike, Option[PortIdentity])] = {
    if (fileAttribute == null) {
      fileAttribute = desc.resolveFileAttribute(tuple.getSchema)
    }
    val sourceFile = Option(tuple.getField[String](fileAttribute)).getOrElse(
      throw new IllegalArgumentException(s"File Split column '$fileAttribute' cannot be null")
    )
    val outputPort = fileToPort.getOrElseUpdate(
      sourceFile,
      PortIdentity(fileToPort.size % outputPortCount)
    )
    Iterator.single((tuple, Some(outputPort)))
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = ???
}
