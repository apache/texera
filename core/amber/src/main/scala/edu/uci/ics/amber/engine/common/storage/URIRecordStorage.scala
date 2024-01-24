package edu.uci.ics.amber.engine.common.storage

import com.typesafe.scalalogging.LazyLogging
import SequentialRecordStorage.{SequentialRecordReader, SequentialRecordWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI

import java.net.URI
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

class URIRecordStorage[T >: Null <: AnyRef](logFolderURI: URI)
    extends SequentialRecordStorage[T]
    with LazyLogging {
  private var fileSystem: FileSystem = _
  private val fsConf = new Configuration()
  // configuration for HDFS
  fsConf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "false")
  // configuration for disabling SUCCESS files
  fsConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
  try {
    fileSystem = FileSystem.get(logFolderURI, fsConf) // Supports various URI schemes
    fileSystem.setWriteChecksum(false)
    fileSystem.setVerifyChecksum(false)
  } catch {
    case e: Exception =>
      logger.warn("Caught error during creating file system", e)
  }

  private val folderPath =
    Path.mergePaths(fileSystem.getWorkingDirectory, new Path(logFolderURI.getPath))

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
