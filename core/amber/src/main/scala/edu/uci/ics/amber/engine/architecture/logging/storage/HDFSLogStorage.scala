package edu.uci.ics.amber.engine.architecture.logging.storage

import com.esotericsoftware.kryo.io.Output
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.DeterminantLogWriter
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
      val output = new Output(hdfs.append(recoveryLogPath))
      override def writeLogRecord(obj: AnyRef): Unit = ser.writeObject(output, obj)

      override def flush(): Unit = output.flush()

      override def close(): Unit = output.close()
    }
  }

  override def deleteLog(): Unit = {
    if (hdfs.exists(recoveryLogPath)) {
      hdfs.delete(recoveryLogPath, false)
    }
  }
}
