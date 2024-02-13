package edu.uci.ics.amber.engine.architecture.worker.managers

class StatisticsManager {
  private var inputTupleCount: Long = 0
  private var outputTupleCount: Long = 0
  private var dataProcessingTime: Long = 0
  private var controlProcessingTime: Long = 0
  private var totalExecutionTime: Long = 0
  private var workerStartTime: Long = 0

  def getStatistics: (Long, Long, Long, Long, Long) = {
    (
      inputTupleCount,
      outputTupleCount,
      dataProcessingTime,
      controlProcessingTime,
      totalExecutionTime - dataProcessingTime - controlProcessingTime // Idle Time
    )
  }

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
    totalExecutionTime = time
  }

  def initializeWorkerStartTime(time: Long): Unit = {
    workerStartTime = time
  }
}
