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

import com.typesafe.scalalogging.LazyLogging
import SequentialRecordStorage.{SequentialRecordReader, SequentialRecordWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI
import scala.reflect.ClassTag

class HDFSRecordStorage[T >: Null <: AnyRef: ClassTag](hdfsLogFolderURI: URI)
    extends SequentialRecordStorage[T]
    with LazyLogging {

  // only support hdfs uris
  assert(hdfsLogFolderURI.getScheme.toLowerCase == "hdfs")

  private var fileSystem: FileSystem = _
  private val fsConf = new Configuration()
  // configuration for HDFS
  fsConf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "false")
  fileSystem = FileSystem.get(hdfsLogFolderURI, fsConf)
  fileSystem.setWriteChecksum(false)
  fileSystem.setVerifyChecksum(false)

  private val folderPath =
    Path.mergePaths(fileSystem.getWorkingDirectory, new Path(hdfsLogFolderURI.getPath))

  if (!fileSystem.exists(folderPath)) {
    fileSystem.mkdirs(folderPath)
  }

  override def getWriter(fileName: String): SequentialRecordWriter[T] = {
    new SequentialRecordWriter(fileSystem.create(folderPath.suffix("/" + fileName)))
  }

  override def getReader(fileName: String): SequentialRecordReader[T] = {
    val path = folderPath.suffix("/" + fileName)
    if (fileSystem.exists(path)) {
      new SequentialRecordReader(() => fileSystem.open(path))
    } else {
      new EmptyRecordStorage[T]().getReader(fileName)
    }
  }

  override def deleteStorage(): Unit = {
    // delete the entire log folder if exists
    if (fileSystem.exists(folderPath)) {
      fileSystem.delete(folderPath, true)
    }
  }

  override def containsFolder(folderName: String): Boolean = {
    val path = folderPath.suffix("/" + folderName)
    fileSystem.exists(path) && fileSystem.getFileStatus(path).isDirectory
  }
}
