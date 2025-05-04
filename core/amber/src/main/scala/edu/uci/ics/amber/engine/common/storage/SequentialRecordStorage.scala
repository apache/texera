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

import com.esotericsoftware.kryo.io.{Input, Output}
import edu.uci.ics.amber.engine.common.AmberRuntime
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage.{
  SequentialRecordReader,
  SequentialRecordWriter
}

import java.io.{DataInputStream, DataOutputStream}
import java.net.URI
import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}

object SequentialRecordStorage {

  // For debugging purpose only
  def fetchAllRecords[T >: Null <: AnyRef](
      storage: SequentialRecordStorage[T],
      logFileName: String
  ): Iterable[T] = {
    val reader = storage.getReader(logFileName)
    val recordIter = reader.mkRecordIterator()
    val buffer = new ArrayBuffer[T]()
    while (recordIter.hasNext) {
      buffer.append(recordIter.next())
    }
    buffer
  }

  class SequentialRecordWriter[T >: Null <: AnyRef](outputStream: DataOutputStream) {
    lazy val output = new Output(outputStream)
    def writeRecord(obj: T): Unit = {
      val bytes = AmberRuntime.serde.serialize(obj).get
      output.writeInt(bytes.length)
      output.write(bytes)
    }
    def flush(): Unit = {
      output.flush()
    }
    def close(): Unit = {
      output.close()
    }
  }

  class SequentialRecordReader[T >: Null <: AnyRef: ClassTag](
      inputStreamGen: () => DataInputStream
  ) {
    val clazz = classTag[T].runtimeClass.asInstanceOf[Class[T]]

    def mkRecordIterator(): Iterator[T] = {
      lazy val input = new Input(inputStreamGen())
      new Iterator[T] {
        var record: T = internalNext()
        private def internalNext(): T = {
          try {
            val len = input.readInt()
            val bytes = input.readBytes(len)
            AmberRuntime.serde.deserialize(bytes, clazz).get
          } catch {
            case e: Throwable =>
              input.close()
              null
          }
        }
        override def next(): T = {
          val currentRecord = record
          record = internalNext()
          currentRecord
        }
        override def hasNext: Boolean = record != null
      }
    }
  }

  def getStorage[T >: Null <: AnyRef: ClassTag](
      storageLocation: Option[URI]
  ): SequentialRecordStorage[T] = {
    storageLocation match {
      case Some(location) =>
        if (location.getScheme.toLowerCase == "hdfs") {
          new HDFSRecordStorage(location) // hdfs lib supports r/w operations
        } else {
          new VFSRecordStorage(location)
        }
      case None => new EmptyRecordStorage()
    }
  }
}

/**
  * Sequential record storage is designed to do read/write for sequential generic data. It represents
  * a one-level folder (no nesting) which contains a list of files. Files are identified by a unique
  * file name string.
  *
  * Key Features:
  *   - Allows for the sequential writing and reading of records of a generic type `T`.
  *     It utilizes Kryo serialization for efficient binary storage of records.
  *   - The class assumes a sequential access pattern to the data. It is not optimized for random
  *     access or querying specific records without reading sequentially.
  * Usage:
  *   - To use `SequentialRecordStorage`, one must extend this abstract class and implement the
  *     methods for creating record readers and writers. Implementations can customize how and
  *     where the data is stored and retrieved.
  *   - The `SequentialRecordWriter` and `SequentialRecordReader` inner classes provide the
  *     functionality for writing to and reading from the storage.
  *
  * @tparam T The type of records that this storage system will handle.
  */
abstract class SequentialRecordStorage[T >: Null <: AnyRef] {

  def getWriter(fileName: String): SequentialRecordWriter[T]

  def getReader(fileName: String): SequentialRecordReader[T]

  def deleteStorage(): Unit

  def containsFolder(folderName: String): Boolean
}
