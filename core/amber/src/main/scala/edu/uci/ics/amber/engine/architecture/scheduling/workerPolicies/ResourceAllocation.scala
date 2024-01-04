package edu.uci.ics.amber.engine.architecture.scheduling.workerPolicies

import edu.uci.ics.amber.engine.architecture.scheduling.Region
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity

import scala.math._

object ResourceAllocation {
  def apply(region: Region): ResourceAllocation = {
    ResourceAllocation(region)
  }
}

case class ResourceAllocation(region: Region) {

  private def physicalOpIds = region.physicalOpIds.filter(_.logicalOpId.id.contains("PythonUDF"))

  private def historicalRuntimeStats = new HistoricalRuntimeStats()

  private def avgHistoryPerOperator(operatorIdentity: OperatorIdentity): (Double, Double) = {
    val executionHistory = historicalRuntimeStats.getHistoricalRuntimeStatsForOperator(operatorIdentity.id)
    if (executionHistory.isEmpty) {
      return (0, 0)
    }
    val filteredList = executionHistory.filter(stats =>
      Option(stats.getInputTupleCnt).exists(_.intValue() != 0) &&
        Option(stats.getOutputTupleCnt).exists(_.intValue() != 0) &&
        Option(stats.getStatus).contains(1.toByte)
    )

    val avgInputTupleCnt = if (filteredList.nonEmpty) {
      filteredList.map(_.getInputTupleCnt.intValue).sum.toDouble / filteredList.size
    } else 0

    val avgOutputTupleCnt = if (filteredList.nonEmpty) {
      filteredList.map(_.getOutputTupleCnt.intValue).sum.toDouble / filteredList.size
    } else 0

    (avgInputTupleCnt, avgOutputTupleCnt)
  }

  private def avgHistory(): List[Double] = {
    physicalOpIds.map(op => avgHistoryPerOperator(op.logicalOpId)._2).toList
  }

  private def allocateWorkers(speeds: List[Double], totNumWorkers: Int): List[Int] = {
    var numWorkers: List[Int] = speeds.map(speed => max(1, speed * floor(totNumWorkers / speeds.sum)).intValue())
    var restNumWorkers: Int = totNumWorkers - numWorkers.sum
    if (speeds.sum <= totNumWorkers) {
      if (restNumWorkers < numWorkers.length) return numWorkers
      numWorkers.foreach(x => x + 1)
      restNumWorkers = restNumWorkers - numWorkers.length
    }
    for (_ <- 1 to restNumWorkers) {
      val j = numWorkers.indices.minBy(j => numWorkers(j) * 1 / speeds(j))
      numWorkers = numWorkers.updated(j, numWorkers(j) + 1)
    }
    numWorkers
  }

  def allocation(executionClusterInfo: ExecutionClusterInfo): (List[Int], Double) = {
    val speeds: List[Double] = avgHistory().map(throughPut => if (throughPut != 0) 1 / throughPut else 1)
    val numWorkers = allocateWorkers(speeds, executionClusterInfo.getRegionExecutionClusterInfo(region))
    (numWorkers, (speeds zip numWorkers).map { case (speed, numWorker) => speed * numWorker }.max)
  }
}