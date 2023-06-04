package edu.uci.ics.amber.engine.architecture.logging.storage

import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.{
  DeterminantLogReader,
  DeterminantLogWriter
}

import java.io.{DataInputStream, DataOutputStream}
import java.nio.file.{Files, Path, Paths, StandardCopyOption, StandardOpenOption}

class LocalFSLogStorage(name: String) extends DeterminantLogStorage {

  private val recoveryLogFolder: Path = Paths.get("").resolve("recovery-logs")
  if (!Files.exists(recoveryLogFolder)) {
    Files.createDirectory(recoveryLogFolder)
  }

  private def getLogPath: Path = {
    recoveryLogFolder.resolve(name + ".logfile")
  }

  override def getWriter: DeterminantLogWriter = {
    new DeterminantLogWriter(
      new DataOutputStream(
        Files.newOutputStream(
          getLogPath,
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND
        )
      )
    )
  }

  override def getReader: DeterminantLogReader = {
    val path = getLogPath
    if (Files.exists(path)) {
      new DeterminantLogReader(() => new DataInputStream(Files.newInputStream(path)))
    } else {
      // we do not throw exception here because every worker
      // will try to read log during startup.
      // TODO: We need a way to distinguish between 1)trying to recover 2)need to recover but no log file
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
    tmpPath = tmpPath.resolveSibling(tmpPath.getFileName + ".tmp")
    copyReadableLogRecords(
      new DeterminantLogWriter(
        new DataOutputStream(
          Files.newOutputStream(
            tmpPath,
            StandardOpenOption.CREATE
          )
        )
      )
    )
    Files.move(
      tmpPath,
      getLogPath,
      StandardCopyOption.REPLACE_EXISTING,
      StandardCopyOption.ATOMIC_MOVE
    )
  }

  override def isLogAvailableForRead: Boolean = {
    if (Files.exists(getLogPath)) {
      Files.isReadable(getLogPath) && Files.size(getLogPath) > 0
    } else {
      false
    }
  }
}
