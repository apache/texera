package edu.uci.ics.amber.engine.architecture.logging.storage

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
      private val out = Files.newOutputStream(filePath)

      override def writeLogRecord(payload: Array[Byte]): Unit = {
        out.write(payload)
      }

      override def flush(): Unit = out.flush()

      override def close(): Unit = out.close()
    }
  }

  override def getReader: InputStream = {
    Files.newInputStream(filePath)
  }

  override def deleteLog(): Unit = {
    if (Files.exists(filePath)) {
      Files.delete(filePath)
    }
  }
}
