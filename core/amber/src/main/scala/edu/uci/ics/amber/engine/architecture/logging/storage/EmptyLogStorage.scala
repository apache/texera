package edu.uci.ics.amber.engine.architecture.logging.storage

class EmptyLogStorage extends DeterminantLogStorage {
  override def getWriter(isTempLog: Boolean): DeterminantLogStorage.DeterminantLogWriter = null

  override def getReader: DeterminantLogStorage.DeterminantLogReader = null

  override def deleteLog(): Unit = {
    // empty
  }

  override def swapTempLog(): Unit = {
    // empty
  }
}
