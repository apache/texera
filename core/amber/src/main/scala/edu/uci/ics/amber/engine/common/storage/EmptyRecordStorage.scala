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

package edu.uci.ics.amber.engine.common.storage

import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage.{
  SequentialRecordReader,
  SequentialRecordWriter
}
import org.apache.commons.io.input.NullInputStream
import org.apache.hadoop.io.IOUtils.NullOutputStream

import java.io.{DataInputStream, DataOutputStream}
import scala.reflect.ClassTag

class EmptyRecordStorage[T >: Null <: AnyRef: ClassTag] extends SequentialRecordStorage[T] {
  override def getWriter(fileName: String): SequentialRecordWriter[T] = {
    new SequentialRecordWriter(
      new DataOutputStream(
        new NullOutputStream()
      )
    )
  }

  override def getReader(fileName: String): SequentialRecordReader[T] = {
    new SequentialRecordReader(() =>
      new DataInputStream(
        new NullInputStream()
      )
    )
  }

  override def deleteStorage(): Unit = {
    // empty
  }

  override def containsFolder(folderName: String): Boolean = false
}
