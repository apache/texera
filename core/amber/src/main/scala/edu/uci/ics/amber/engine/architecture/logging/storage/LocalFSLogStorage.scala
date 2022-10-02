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

  private def getLogPath: Path = {
    recoveryLogFolder.resolve(name + ".logfile")
  }

  override def getWriter: DeterminantLogWriter = {
    new DeterminantLogWriter {
      override lazy protected val outputStream = {
        new DataOutputStream(
          Files.newOutputStream(
            getLogPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND
          )
        )
      }
    }
  }

  override def getReader: DeterminantLogReader = {
    val path = getLogPath
    if (Files.exists(path)) {
      new DeterminantLogReader {
        override protected val inputStream = new DataInputStream(Files.newInputStream(path))
      }
    } else {
      new EmptyLogStorage().getReader
    }
  }

  override def deleteLog(): Unit = {
    val path = getLogPath
    if (Files.exists(path)) {
      Files.delete(path)
    }
  }

  override def cleanPartiallyWrittenLogFile(): Unit = {
    var tmpPath = getLogPath
    tmpPath = tmpPath.resolveSibling(tmpPath.getFileName+".tmp")
    copyReadableLogRecords(new DeterminantLogWriter {
      override lazy protected val outputStream = {
        new DataOutputStream(
          Files.newOutputStream(
            tmpPath,
            StandardOpenOption.CREATE
          )
        )
      }
    })
    Files.move(
      tmpPath,
      getLogPath,
      StandardCopyOption.REPLACE_EXISTING,
      StandardCopyOption.ATOMIC_MOVE
    )
  }
}
