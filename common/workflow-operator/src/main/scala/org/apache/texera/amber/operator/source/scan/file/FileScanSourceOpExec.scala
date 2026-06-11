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

package org.apache.texera.amber.operator.source.scan.file

import org.apache.texera.amber.core.executor.SourceOperatorExecutor
import org.apache.texera.amber.core.tuple.TupleLike
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.io.IOException

class FileScanSourceOpExec private[scan] (
    descString: String
) extends SourceOperatorExecutor {
  private val desc: FileScanSourceOpDesc =
    objectMapper.readValue(descString, classOf[FileScanSourceOpDesc])

  @throws[IOException]
  override def produceTuple(): Iterator[TupleLike] = {
    FileScanUtils.createTuplesFromFile(
      fileName = desc.fileName.get,
      attributeType = desc.attributeType,
      fileEncoding = desc.fileEncoding,
      extract = desc.extract,
      outputFileName = desc.outputFileName,
      fileScanOffset = desc.fileScanOffset,
      fileScanLimit = desc.fileScanLimit
    )
  }
}
