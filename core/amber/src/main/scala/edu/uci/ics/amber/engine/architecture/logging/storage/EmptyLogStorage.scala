package edu.uci.ics.amber.engine.architecture.logging.storage

import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.{DeterminantLogReader, DeterminantLogWriter}

import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}

class EmptyLogStorage extends DeterminantLogStorage {
  override def getWriter(isTempLog: Boolean): DeterminantLogWriter = {
    new DeterminantLogWriter {
      override protected val outputStream: DataOutputStream = new DataOutputStream(OutputStream.nullOutputStream())
    }
  }

  override def getReader: DeterminantLogReader = {
    new DeterminantLogReader {
      override protected val inputStream: DataInputStream = new DataInputStream(InputStream.nullInputStream())
    }
  }

  override def deleteLog(): Unit = {
    // empty
  }

  override def swapTempLog(): Unit = {
    // empty
  }
}
