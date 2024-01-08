package edu.uci.ics.amber.engine.architecture.logreplay.storage

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.logreplay.storage.ReplayLogStorage.{
  ReplayLogReader,
  ReplayLogWriter
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI

import java.net.URI
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

class URILogStorage(logFolderURI: URI) extends ReplayLogStorage with LazyLogging {
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

  override def getWriter(logFileName: String): ReplayLogWriter = {
    new ReplayLogWriter(fileSystem.create(folderPath.suffix("/" + logFileName)))
  }

  override def getReader(logFileName: String): ReplayLogReader = {
    val path = folderPath.suffix("/" + logFileName)
    if (fileSystem.exists(path)) {
      new ReplayLogReader(() => fileSystem.open(path))
    } else {
      new EmptyLogStorage().getReader(logFileName)
    }
  }

  override def deleteStorage(): Unit = {
    // delete the entire log folder if exists
    if (fileSystem.exists(folderPath)) {
      fileSystem.delete(folderPath, true)
    }
  }

}
