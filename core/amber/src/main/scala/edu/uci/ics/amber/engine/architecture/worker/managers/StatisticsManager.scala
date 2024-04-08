package edu.uci.ics.amber.engine.architecture.worker.managers

import edu.uci.ics.amber.engine.architecture.worker.statistics.{
  PortTupleCountMapping,
  WorkerStatistics
}
import edu.uci.ics.amber.engine.common.SinkOperatorExecutor
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor

import scala.collection.mutable

class StatisticsManager {
  // DataProcessor
  private var inputTupleCount: mutable.Map[PortIdentity, Long] = mutable.Map()
  private var outputTupleCount: mutable.Map[PortIdentity, Long] = mutable.Map()
  private var dataProcessingTime: Long = 0
  private var totalExecutionTime: Long = 0
  private var workerStartTime: Long = 0

  // AmberProcessor
  private var controlProcessingTime: Long = 0

  def getStatistics(operator: OperatorExecutor): WorkerStatistics = {
    // sink operator doesn't output to downstream so internal count is 0
    // but for user-friendliness we show its input count as output count
    val displayOut = operator match {
      case sink: SinkOperatorExecutor =>
        inputTupleCount
      case _ =>
        outputTupleCount
    }
    WorkerStatistics(
      inputTupleCount.map {
        case (portId, tupleCount) => new PortTupleCountMapping(Option(portId), tupleCount)
      }.toSeq,
      displayOut.map {
        case (portId, tupleCount) => new PortTupleCountMapping(Option(portId), tupleCount)
      }.toSeq,
      dataProcessingTime,
      controlProcessingTime,
      totalExecutionTime - dataProcessingTime - controlProcessingTime
    )
  }

  def getInputTupleCount: Long = inputTupleCount.values.sum

  def getOutputTupleCount: Long = outputTupleCount.values.sum

  def increaseInputTupleCount(portId: PortIdentity): Unit = {
    inputTupleCount.getOrElseUpdate(portId, 0)
    inputTupleCount(portId) += 1
  }

  def increaseOutputTupleCount(portId: PortIdentity): Unit = {
    outputTupleCount.getOrElseUpdate(portId, 0)
    outputTupleCount(portId) += 1
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
