package edu.uci.ics.amber.engine.architecture.logging.storage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.InputStream
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
  private val recoveryLogPath: Path = new Path("/recovery-logs/" + name + ".logfile")
  if (!hdfs.exists(recoveryLogPath)) {
    hdfs.createNewFile(recoveryLogPath)
  }

  override def getWriter: DeterminantLogWriter = {
    new DeterminantLogWriter {
      val outputStream = hdfs.append(recoveryLogPath)
      override def writeLogRecord(payload: Array[Byte]): Unit = outputStream.write(payload)

      override def flush(): Unit = outputStream.flush()

      override def close(): Unit = outputStream.close()
    }
  }

  override def getReader: InputStream = {
    hdfs.open(recoveryLogPath)
  }
}
