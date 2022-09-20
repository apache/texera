package edu.uci.ics.amber.engine.architecture.logging.storage

import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.{DeterminantLogReader, DeterminantLogWriter, EmptyLogReader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

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

  private def getLogPath(attempt: Int):Path = {
    new Path("/recovery-logs/" + name +"-"+attempt+ ".logfile")
  }

  override def getWriter(attempt:Int): DeterminantLogWriter = {
    new DeterminantLogWriter {
      override lazy protected val outputStream = {
        hdfs.create(getLogPath(attempt))
      }
    }
  }

  override def getReader(attempt:Int): DeterminantLogReader = {
    val path = getLogPath(attempt)
    if(hdfs.exists(path)) {
      new DeterminantLogReader {
        override protected val inputStream = hdfs.open(path)
      }
    }else{
      null
    }
  }

  override def deleteLog(attempt: Int): Unit = {
    val path = getLogPath(attempt)
    if (hdfs.exists(path)) {
      hdfs.delete(path, false)
    }
  }
}
