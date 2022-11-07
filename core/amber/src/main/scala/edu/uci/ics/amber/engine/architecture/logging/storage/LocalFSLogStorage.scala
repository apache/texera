package edu.uci.ics.amber.engine.architecture.logging.storage

import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.{
  DeterminantLogReader,
  DeterminantLogWriter
}
import edu.uci.ics.amber.engine.architecture.recovery.RecordIterator

import java.io.{DataInputStream, DataOutputStream}
import java.nio.file.{
  CopyOption,
  Files,
  OpenOption,
  Path,
  Paths,
  StandardCopyOption,
  StandardOpenOption
}

class LocalFSLogStorage(name: String) extends DeterminantLogStorage {

  private val recoveryLogFolder: Path = Paths.get("").resolve("recovery-logs")
  if (!Files.exists(recoveryLogFolder)) {
    Files.createDirectory(recoveryLogFolder)
  }

  private def getLogPath(isTempLog: Boolean): Path = {
    recoveryLogFolder.resolve(name + ".logfile" + (if (isTempLog) { ".tmp" }
                                                   else { "" }))
  }

  override def getWriter(isTempLog: Boolean): DeterminantLogWriter = {
    new DeterminantLogWriter {
      override lazy protected val outputStream = {
        new DataOutputStream(
          Files.newOutputStream(
            getLogPath(isTempLog),
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND
          )
        )
      }
    }
  }

  override def getReader: DeterminantLogReader = {
    val path = getLogPath(false)
    if (Files.exists(path)) {
      new DeterminantLogReader {
        override protected val inputStream = new DataInputStream(Files.newInputStream(path))
      }
    } else {
      // we do not throw exception here because every worker
      // will try to read log during startup.
      // TODO: We need a way to distinguish between 1)trying to recover 2)need to recover but no log file
      new EmptyLogStorage().getReader
    }
  }

  override def deleteLog(): Unit = {
    // delete temp log if exists
    val tempLogPath = getLogPath(true)
    if (Files.exists(tempLogPath)) {
      Files.delete(tempLogPath)
    }
    val path = getLogPath(false)
    if (Files.exists(path)) {
      Files.delete(path)
    }
  }

  override def swapTempLog(): Unit = {
    val tempLogPath = getLogPath(true)
    val path = getLogPath(false)
    if (Files.exists(tempLogPath)) {
      Files.move(
        tempLogPath,
        path,
        StandardCopyOption.REPLACE_EXISTING,
        StandardCopyOption.ATOMIC_MOVE
      )
    }
  }
}
