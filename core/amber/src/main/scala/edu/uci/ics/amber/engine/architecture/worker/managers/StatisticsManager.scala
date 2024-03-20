package edu.uci.ics.amber.engine.architecture.worker.managers

import edu.uci.ics.amber.engine.architecture.worker.statistics.{WorkerState, WorkerStatistics}
import edu.uci.ics.amber.engine.common.SinkOperatorExecutor
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor

class StatisticsManager {
  // DataProcessor
  private var inputTupleCount: Long = 0
  private var outputTupleCount: Long = 0
  private var dataProcessingTime: Long = 0
  private var totalExecutionTime: Long = 0
  private var workerStartTime: Long = 0

  // AmberProcessor
  private var controlProcessingTime: Long = 0

  def getStatistics(workerState: WorkerState, operator: OperatorExecutor): WorkerStatistics = {
    // sink operator doesn't output to downstream so internal count is 0
    // but for user-friendliness we show its input count as output count
    val displayOut = operator match {
      case sink: SinkOperatorExecutor =>
        inputTupleCount
      case _ =>
        outputTupleCount
    }
    WorkerStatistics(
      workerState,
      inputTupleCount,
      displayOut,
      dataProcessingTime,
      controlProcessingTime,
      totalExecutionTime - dataProcessingTime - controlProcessingTime
    )
  }

  def getInputTupleCount: Long = inputTupleCount

  def getOutputTupleCount: Long = outputTupleCount

  def increaseInputTupleCount(): Unit = {
    inputTupleCount += 1
  }

  def increaseOutputTupleCount(): Unit = {
    outputTupleCount += 1
  }

  def increaseDataProcessingTime(time: Long): Unit = {
    dataProcessingTime += time
  }

  def increaseControlProcessingTime(time: Long): Unit = {
    controlProcessingTime += time
  }

  def updateTotalExecutionTime(time: Long): Unit = {
    totalExecutionTime = time - workerStartTime
  }

  def initializeWorkerStartTime(time: Long): Unit = {
    workerStartTime = time
  }
}
