package edu.uci.ics.amber.engine.recovery

import scala.collection.mutable

class DPLogManager(logStorage: LogStorage[Long]) extends RecoveryComponent {
  private val correlatedSeq = logStorage.load().to[mutable.Queue]

  checkIfCompleted()

  def persistCurrentDataCursor(cur: Long): Unit = {
    logStorage.persistElement(cur)
  }

  def isCurrentCorrelated(cur: Long): Boolean = {
    correlatedSeq.nonEmpty && correlatedSeq.head == cur
  }

  def advanceCursor(): Unit = {
    correlatedSeq.dequeue()
    checkIfCompleted()
  }

  @inline
  private[this] def checkIfCompleted(): Unit = {
    if (correlatedSeq.isEmpty && isRecovering) {
      setRecoveryCompleted()
    }
  }
}
