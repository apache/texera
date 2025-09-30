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
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage.{
  SequentialRecordReader,
  SequentialRecordWriter
}
import org.apache.commons.vfs2.{FileObject, FileSystemManager, VFS}

import java.io.{DataInputStream, DataOutputStream}
import java.net.URI
import scala.reflect.ClassTag

class VFSRecordStorage[T >: Null <: AnyRef: ClassTag](vfsLogFolderURI: URI)
    extends SequentialRecordStorage[T]
    with LazyLogging {

  private val fs: FileSystemManager = VFS.getManager
  private val folder: FileObject = fs.resolveFile(vfsLogFolderURI)

  if (!folder.exists()) {
    folder.createFolder()
  }

  override def getWriter(fileName: String): SequentialRecordStorage.SequentialRecordWriter[T] = {
    val file = folder.resolveFile(fileName)
    file.createFile()
    val outputStream = file.getContent.getOutputStream
    new SequentialRecordWriter(new DataOutputStream(outputStream))
  }

  override def getReader(fileName: String): SequentialRecordStorage.SequentialRecordReader[T] = {
    new SequentialRecordReader(() => {
      val inputStream = folder.resolveFile(fileName).getContent.getInputStream
      new DataInputStream(inputStream)
    })
  }

  override def deleteStorage(): Unit = {
    folder.deleteAll()
  }

  override def containsFolder(folderName: String): Boolean = {
    val fileObj = folder.getChild(folderName)
    fileObj != null && fileObj.exists() && fileObj.isFolder
  }
}
