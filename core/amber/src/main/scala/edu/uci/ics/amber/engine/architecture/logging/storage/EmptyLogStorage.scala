package edu.uci.ics.amber.engine.architecture.logging.storage

import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.{
  DeterminantLogReader,
  DeterminantLogWriter
}
import org.apache.commons.io.input.NullInputStream
import org.apache.hadoop.io.IOUtils.NullOutputStream

import java.io.{DataInputStream, DataOutputStream}

class EmptyLogStorage extends DeterminantLogStorage {
  override def getWriter: DeterminantLogWriter = {
    new DeterminantLogWriter(
      new DataOutputStream(
        new NullOutputStream()
      )
    )
  }

  override def getReader: DeterminantLogReader = {
    new DeterminantLogReader(() =>
      new DataInputStream(
        new NullInputStream()
      )
    )
  }

  override def deleteLog(): Unit = {
    // empty
  }

  override def cleanPartiallyWrittenLogFile(): Unit = {
    // empty
  }

  override def isLogAvailableForRead: Boolean = false
}
