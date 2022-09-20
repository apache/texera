package edu.uci.ics.amber.engine.architecture.logging.storage

class EmptyLogStorage extends DeterminantLogStorage {
  override def getWriter(attempt: Int): DeterminantLogStorage.DeterminantLogWriter = null

  override def getReader(attempt: Int): DeterminantLogStorage.DeterminantLogReader = null

  override def deleteLog(attempt: Int): Unit = {
    // empty
  }
}
