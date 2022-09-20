package edu.uci.ics.amber.engine.architecture.logging.storage

import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.{DeterminantLogReader, DeterminantLogWriter, EmptyLogReader}

import java.io.{DataInputStream, DataOutputStream}
import java.nio.file.{Files, Path, Paths}

class LocalFSLogStorage(name: String, attempt: Int) extends DeterminantLogStorage(attempt) {

  private val recoveryLogFolder: Path = Paths.get("").resolve("recovery-logs")
  if (!Files.exists(recoveryLogFolder)) {
    Files.createDirectory(recoveryLogFolder)
  }

  private def getLogPath(attempt: Int):Path = {
    recoveryLogFolder.resolve(name +"-"+attempt+ ".logfile")
  }

  override def getWriter(attempt: Int): DeterminantLogWriter = {
    new DeterminantLogWriter {
      override lazy protected val outputStream = {
        new DataOutputStream(Files.newOutputStream(getLogPath(attempt)))
      }
    }
  }

  override def getReader(attempt: Int): DeterminantLogReader = {
    val path = getLogPath(attempt)
    if(Files.exists(path)){
      new DeterminantLogReader {
        override protected val inputStream = new DataInputStream(Files.newInputStream(path))
      }
    }else{
      null
    }
  }

  override def deleteLog(attempt: Int): Unit = {
    val path = getLogPath(attempt)
    if (Files.exists(path)) {
      Files.delete(path)
    }
  }
}
