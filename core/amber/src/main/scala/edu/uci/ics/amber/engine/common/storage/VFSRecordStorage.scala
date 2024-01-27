package edu.uci.ics.amber.engine.common.storage

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage.{
  SequentialRecordReader,
  SequentialRecordWriter
}
import org.apache.commons.vfs2.{FileObject, FileSystemManager, VFS}

import java.io.{DataInputStream, DataOutputStream}
import java.net.URI

class VFSRecordStorage[T >: Null <: AnyRef](vfsLogFolderURI: URI)
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
}
