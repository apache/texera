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
    new DeterminantLogWriter {
      override protected val outputStream: DataOutputStream = new DataOutputStream(
        new NullOutputStream()
      )
    }
  }

  override def getReader: DeterminantLogReader = {
    new DeterminantLogReader {
      override protected val inputStream: DataInputStream = new DataInputStream(
        new NullInputStream()
      )
    }
  }

  override def deleteLog(): Unit = {
    // empty
  }

  override def cleanPartiallyWrittenLogFile(): Unit = {
    // empty
  }
}
