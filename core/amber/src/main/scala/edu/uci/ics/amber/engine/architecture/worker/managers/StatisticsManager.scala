package edu.uci.ics.amber.engine.architecture.worker.managers

import edu.uci.ics.amber.core.executor.{OperatorExecutor, SinkOperatorExecutor}
import edu.uci.ics.amber.engine.architecture.worker.statistics.{PortTupleMetrics, WorkerStatistics}
import edu.uci.ics.amber.core.workflow.PortIdentity

import scala.collection.mutable

class StatisticsManager {
  // DataProcessor
  private val inputTupleCount = mutable.Map.empty[PortIdentity, Long].withDefaultValue(0L)
  private val inputTupleSize = mutable.Map.empty[PortIdentity, Long].withDefaultValue(0L)
  private val outputTupleCount = mutable.Map.empty[PortIdentity, Long].withDefaultValue(0L)
  private val outputTupleSize = mutable.Map.empty[PortIdentity, Long].withDefaultValue(0L)
  private var dataProcessingTime: Long = 0L
  private var totalExecutionTime: Long = 0L
  private var workerStartTime: Long = 0L

  // AmberProcessor
  private var controlProcessingTime: Long = 0L

  def getStatistics(operator: OperatorExecutor): WorkerStatistics = {
    val (userFriendlyOutputTupleCount, userFriendlyOutputTupleSize) = operator match {
      case _: SinkOperatorExecutor => (inputTupleCount, inputTupleSize)
      case _                       => (outputTupleCount, outputTupleSize)
    }

    WorkerStatistics(
      inputTupleCount.map {
        case (portId, tupleCount) => PortTupleMetrics(portId, tupleCount)
      }.toSeq,
      inputTupleSize.map { case (portId, tupleSize) => PortTupleMetrics(portId, tupleSize) }.toSeq,
      userFriendlyOutputTupleCount.map {
        case (portId, tupleCount) => PortTupleMetrics(portId, tupleCount)
      }.toSeq,
      userFriendlyOutputTupleSize.map {
        case (portId, tupleSize) => PortTupleMetrics(portId, tupleSize)
      }.toSeq,
      dataProcessingTime,
      controlProcessingTime,
      totalExecutionTime - dataProcessingTime - controlProcessingTime
    )
  }

  def getInputTupleCount: Long = inputTupleCount.values.sum

  def getOutputTupleCount: Long = outputTupleCount.values.sum

  def increaseInputTupleCount(portId: PortIdentity): Unit = {
    inputTupleCount(portId) += 1
  }

  def increaseInputTupleSize(portId: PortIdentity, size: Long): Unit = {
    inputTupleSize(portId) += size
  }

  def increaseOutputTupleCount(portId: PortIdentity): Unit = {
    outputTupleCount(portId) += 1
  }

  def increaseOutputTupleSize(portId: PortIdentity, size: Long): Unit = {
    outputTupleSize(portId) += size
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
