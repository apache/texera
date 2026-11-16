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

import org.apache.texera.amber.core.storage.FileResolver
import org.apache.texera.amber.core.executor.SourceOperatorExecutor
import org.apache.texera.amber.core.tuple.TupleLike
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.io.IOException
import java.net.URI
import java.nio.file.{Files, Paths}

class FileScanSourceOpExec private[scan] (
    descString: String
) extends SourceOperatorExecutor {
  private val desc: FileScanSourceOpDesc =
    objectMapper.readValue(descString, classOf[FileScanSourceOpDesc])

  @throws[IOException]
  override def produceTuple(): Iterator[TupleLike] = {
    // A name that a control variable bound at run time, such as "$file", is resolved here; a
    // name the compiler already resolved stays as it is.
    val fileName = desc.fileName.get
    FileScanUtils.createTuplesFromFile(
      fileName = locate(fileName).toASCIIString,
      displayFileName = fileName,
      attributeType = desc.attributeType,
      fileEncoding = desc.encoding,
      extract = desc.extract,
      outputFileName = desc.outputFileName,
      fileScanOffset = desc.fileScanOffset,
      fileScanLimit = desc.fileScanLimit
    )
  }

  /**
    * Resolves the file name to a URI the way FileScanOpExec resolves a name it reads on the
    * worker: a resolved URI as it is, then a local path, then a dataset or model path.
    */
  private def locate(fileName: String): URI =
    if (FileResolver.isFileResolved(fileName)) {
      new URI(fileName)
    } else {
      val localPath = Paths.get(fileName)
      if (Files.exists(localPath)) localPath.toUri
      else FileResolver.resolveVersionedResource(fileName)
    }
}
