package edu.uci.ics.amber.engine.architecture.logging.storage

import java.io.InputStream

abstract class DeterminantLogStorage {

  abstract class DeterminantLogWriter {
    def writeLogRecord(payload: Array[Byte]): Unit
    def flush(): Unit
    def close(): Unit
  }

  def getWriter: DeterminantLogWriter

  def getReader: InputStream

  def deleteLog(): Unit

}
