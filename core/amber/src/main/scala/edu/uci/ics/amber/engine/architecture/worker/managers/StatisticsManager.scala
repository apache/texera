package edu.uci.ics.amber.engine.architecture.worker.managers

import edu.uci.ics.amber.core.executor.{OperatorExecutor, SinkOperatorExecutor}
import edu.uci.ics.amber.engine.architecture.worker.statistics.{
  PortTupleMetricsMapping,
  TupleMetrics,
  WorkerStatistics
}
import edu.uci.ics.amber.core.workflow.PortIdentity

import scala.collection.mutable

class StatisticsManager {
  // DataProcessor
  private val inputStatistics: mutable.Map[PortIdentity, (Long, Long)] =
    mutable.Map.empty.withDefaultValue((0L, 0L))
  private val outputStatistics: mutable.Map[PortIdentity, (Long, Long)] =
    mutable.Map.empty.withDefaultValue((0L, 0L))
  private var dataProcessingTime: Long = 0L
  private var totalExecutionTime: Long = 0L
  private var workerStartTime: Long = 0L

  // AmberProcessor
  private var controlProcessingTime: Long = 0L

  /**
    * Retrieves the current statistics for the operator.
    * @param operator the operator executor
    * @return a WorkerStatistics object containing the statistics
    */
  def getStatistics(operator: OperatorExecutor): WorkerStatistics = {
    val userFriendlyOutputStatistics = operator match {
      case _: SinkOperatorExecutor => inputStatistics
      case _                       => outputStatistics
    }

    WorkerStatistics(
      inputStatistics.map {
        case (portId, (tupleCount, tupleSize)) =>
          PortTupleMetricsMapping(portId, TupleMetrics(tupleCount, tupleSize))
      }.toSeq,
      userFriendlyOutputStatistics.map {
        case (portId, (tupleCount, tupleSize)) =>
          PortTupleMetricsMapping(portId, TupleMetrics(tupleCount, tupleSize))
      }.toSeq,
      dataProcessingTime,
      controlProcessingTime,
      totalExecutionTime - dataProcessingTime - controlProcessingTime
    )
  }

  /**
    * Calculates the total number of input tuples.
    * @return the total input tuple count
    */
  def getInputTupleCount: Long = inputStatistics.values.map(_._1).sum

  /**
    * Calculates the total number of output tuples.
    * @return the total output tuple count
    */
  def getOutputTupleCount: Long = outputStatistics.values.map(_._1).sum

  /**
    * Increases the input statistics for a given port.
    * @param portId the port identity
    * @param size the size of the tuple
    */
  def increaseInputStatistics(portId: PortIdentity, size: Long): Unit = {
    require(size >= 0, "Tuple size must be non-negative")
    val (count, totalSize) = inputStatistics(portId)
    inputStatistics.update(portId, (count + 1, totalSize + size))
  }

  /**
    * Increases the output statistics for a given port.
    * @param portId the port identity
    * @param size the size of the tuple
    */
  def increaseOutputStatistics(portId: PortIdentity, size: Long): Unit = {
    require(size >= 0, "Tuple size must be non-negative")
    val (count, totalSize) = outputStatistics(portId)
    outputStatistics.update(portId, (count + 1, totalSize + size))
  }

  /**
    * Increases the data processing time.
    * @param time the time to add
    */
  def increaseDataProcessingTime(time: Long): Unit = {
    require(time >= 0, "Time must be non-negative")
    dataProcessingTime += time
  }

  /**
    * Increases the control processing time.
    * @param time the time to add
    */
  def increaseControlProcessingTime(time: Long): Unit = {
    require(time >= 0, "Time must be non-negative")
    controlProcessingTime += time
  }

  /**
    * Updates the total execution time.
    * @param time the current time
    */
  def updateTotalExecutionTime(time: Long): Unit = {
    require(
      time >= workerStartTime,
      "Current time must be greater than or equal to worker start time"
    )
    totalExecutionTime = time - workerStartTime
  }

  /**
    * Initializes the worker start time.
    * @param time the start time
    */
  def initializeWorkerStartTime(time: Long): Unit = {
    workerStartTime = time
  }
}
