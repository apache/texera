package edu.uci.ics.amber.engine.architecture.recovery

import edu.uci.ics.amber.engine.architecture.logging.{InMemDeterminant, StepDelta}
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.DeterminantLogReader

class RecordIterator(logReader: DeterminantLogReader) {

  private var stop = false
  private var current: InMemDeterminant = _
  private var temp: InMemDeterminant = _

  def peek(): InMemDeterminant = {
    if (current == null) {
      readNext()
    }
    current
  }

  def isEmpty: Boolean = {
    if (current == null) {
      readNext()
    }
    stop
  }

  def readNext(): Unit = {
    if (temp != null) {
      current = temp
      temp = null
    } else {
      current = logReader.readLogRecord()
      if (current == null) {
        stop = true
      } else if (current != null && current.isInstanceOf[StepDelta]) {
        temp = logReader.readLogRecord()
        while (temp != null && temp.isInstanceOf[StepDelta]) {
          current = StepDelta(
            current.asInstanceOf[StepDelta].steps + temp.asInstanceOf[StepDelta].steps
          )
          temp = logReader.readLogRecord()
        }
      }
    }
//    current = logReader.readLogRecord()
//    if (current == null) {
//      stop = true
//    }
  }

}
