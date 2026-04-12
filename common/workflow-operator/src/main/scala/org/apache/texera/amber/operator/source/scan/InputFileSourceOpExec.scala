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

package org.apache.texera.amber.operator.source.scan

import org.apache.texera.amber.core.executor.SourceOperatorExecutor
import org.apache.texera.amber.core.storage.FileResolver
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.core.workflow.PortIdentity

import scala.collection.mutable.ArrayBuffer

trait InputFileSourceOpExec extends SourceOperatorExecutor {
  private val inputFileNames = ArrayBuffer.empty[String]

  override def processTupleMultiPort(
      tuple: Tuple,
      port: Int
  ): Iterator[(TupleLike, Option[PortIdentity])] = {
    processTuple(tuple, port).map(tupleLike => (tupleLike, None))
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    inputFileNames += extractFileName(tuple)
    Iterator.empty
  }

  protected def resolvedInputFileNames: Seq[String] = {
    if (inputFileNames.isEmpty) {
      throw new IllegalStateException("No input file is available for this source operator.")
    }

    inputFileNames.toSeq.map(fileName =>
      if (FileResolver.isFileResolved(fileName)) {
        fileName
      } else {
        FileResolver.resolve(fileName).toASCIIString
      }
    )
  }

  protected def resolvedInputFileName: String = {
    val fileNames = resolvedInputFileNames
    if (fileNames.size > 1) {
      throw new IllegalStateException("This source operator accepts only one input filename.")
    }
    fileNames.head
  }

  private def extractFileName(tuple: Tuple): String = {
    if (tuple.getSchema.containsAttribute("filename")) {
      return tuple.getField[String]("filename")
    }

    val stringFields = tuple.getFields.collect { case value: String => value }
    if (stringFields.size == 1) {
      return stringFields.head
    }

    tuple.getFields.headOption match {
      case Some(value: String) => value
      case _ =>
        throw new IllegalArgumentException(
          "The filename input port expects a tuple containing a filename string."
        )
    }
  }
}
