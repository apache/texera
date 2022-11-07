package edu.uci.ics.amber.engine.architecture.logging.storage

import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.{
  DeterminantLogReader,
  DeterminantLogWriter
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{DataInputStream, InputStream}
import java.net.URI

class HDFSLogStorage(name: String, hdfsIP: String) extends DeterminantLogStorage {
  var hdfs: FileSystem = _
  val hdfsConf = new Configuration
  hdfsConf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "false")
  try {
    hdfs = FileSystem.get(new URI(hdfsIP), hdfsConf)
  } catch {
    case e: Exception =>
      e.printStackTrace()
  }
  private val recoveryLogFolder: Path = new Path("/recovery-logs")
  if (!hdfs.exists(recoveryLogFolder)) {
    hdfs.mkdirs(recoveryLogFolder)
  }

  private def getLogPath(isTempLog: Boolean): Path = {
    new Path("/recovery-logs/" + name + ".logfile" + (if (isTempLog) {
                                                        ".tmp"
                                                      } else { "" }))
  }

  override def getWriter(isTempLog: Boolean): DeterminantLogWriter = {
    new DeterminantLogWriter {
      override lazy protected val outputStream = {
        hdfs.append(getLogPath(isTempLog))
      }
    }
  }

  override def getReader: DeterminantLogReader = {
    val path = getLogPath(false)
    if (hdfs.exists(path)) {
      new DeterminantLogReader {
        override protected val inputStream = hdfs.open(path)
      }
    } else {
      new EmptyLogStorage().getReader
    }
  }

  override def deleteLog(): Unit = {
    // delete temp log if exists
    val tempLogPath = getLogPath(true)
    if (hdfs.exists(tempLogPath)) {
      hdfs.delete(tempLogPath, false)
    }
    // delete log if exists
    val path = getLogPath(false)
    if (hdfs.exists(path)) {
      hdfs.delete(path, false)
    }
  }

  override def swapTempLog(): Unit = {
    val tempLogPath = getLogPath(true)
    val path = getLogPath(false)
    if (hdfs.exists(path)) {
      hdfs.delete(path, false)
    }
    if (hdfs.exists(tempLogPath)) {
      hdfs.rename(tempLogPath, path)
    }
  }
}
