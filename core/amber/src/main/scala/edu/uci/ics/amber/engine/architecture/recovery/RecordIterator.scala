package edu.uci.ics.amber.engine.architecture.recovery

import edu.uci.ics.amber.engine.architecture.logging.InMemDeterminant
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.DeterminantLogReader

class RecordIterator(logReader: DeterminantLogReader) {

  private var stop = logReader == null
  private var head: InMemDeterminant = _

  def peek(): InMemDeterminant = {
    if (!stop && head == null) {
      readNext()
    }
    head
  }

  def isEmpty: Boolean = {
    if (!stop && head == null) {
      readNext()
    }
    stop
  }

  def readNext(): Unit = {
    try {
      head = logReader.readLogRecord()
    } catch {
      case exception: Exception =>
        logReader.close()
        stop = true
    }
  }

}
