package edu.uci.ics.amber.engine.common.storage

import com.typesafe.scalalogging.LazyLogging
import SequentialRecordStorage.{SequentialRecordReader, SequentialRecordWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI

class HDFSRecordStorage[T >: Null <: AnyRef](hdfsLogFolderURI: URI)
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

}
