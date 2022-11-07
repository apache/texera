package edu.uci.ics.amber.engine.architecture.logging

import scala.collection.mutable

class DeterminantLoggerImpl extends DeterminantLogger {

  private val tempLogs = mutable.ArrayBuffer[InMemDeterminant]()
  private var step = 0L

  def stepIncrement(): Unit = {
    step += 1
  }

  def logDeterminant(inMemDeterminant: InMemDeterminant): Unit = {
    pushStepDelta()
    tempLogs.append(inMemDeterminant)
  }

  def drainCurrentLogRecords(): Array[InMemDeterminant] = {
    pushStepDelta()
    val result = tempLogs.toArray
    tempLogs.clear()
    result
  }

  private def pushStepDelta(): Unit = {
    if (step == 0L) {
      return
    }
    tempLogs.append(StepDelta(step))
    step = 0L
  }

}
