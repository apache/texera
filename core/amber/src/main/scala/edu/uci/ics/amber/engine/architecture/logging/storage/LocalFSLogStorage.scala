package edu.uci.ics.amber.engine.architecture.logging.storage

import com.esotericsoftware.kryo.io.Output
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.DeterminantLogWriter

import java.io.InputStream
import java.nio.file.{Files, Path, Paths}

class LocalFSLogStorage(name: String) extends DeterminantLogStorage {

  private val recoveryLogFolder: Path = Paths.get("").resolve("recovery-logs")
  private val filePath = recoveryLogFolder.resolve(name + ".logfile")
  if (!Files.exists(recoveryLogFolder)) {
    Files.createDirectory(recoveryLogFolder)
  }
  if (!Files.exists(filePath)) {
    Files.createFile(filePath)
  }

  override def getWriter: DeterminantLogWriter = {
    new DeterminantLogWriter {
      private val output = new Output(Files.newOutputStream(filePath))

      override def writeLogRecord(obj: AnyRef): Unit = {
        ser.writeObject(output, obj)
      }

      override def flush(): Unit = output.flush()

      override def close(): Unit = output.close()
    }
  }

  override def deleteLog(): Unit = {
    if (Files.exists(filePath)) {
      Files.delete(filePath)
    }
  }
}
