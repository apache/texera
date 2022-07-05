package edu.uci.ics.amber.engine.architecture.logging.storage

import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.DeterminantLogWriter

import java.io.InputStream

object DeterminantLogStorage {
  abstract class DeterminantLogWriter {
    def writeLogRecord(payload: Array[Byte]): Unit
    def flush(): Unit
    def close(): Unit
  }
}

abstract class DeterminantLogStorage {

  def getWriter: DeterminantLogWriter

  def getReader: InputStream

  def deleteLog(): Unit

}
