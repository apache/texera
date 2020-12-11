package edu.uci.ics.texera.workflow.operators.util

import scala.collection.mutable

object OperatorDescriptorUtils {

  /**
    * Tries to equally partition a integer goal into n total number of workers.
    * In the case that the goal is not a multiple of worker count,
    * this function tries to spread out the remainder evenly to the workers.
    *
    * @param goal total goal to reach for all workers
    * @param totalNumWorkers total number of workers
    * @return a list which size is equal to totalNumWorkers, each number is the goal assigned for that worker index
    */
  def equallyPartitionGoal(goal: Int, totalNumWorkers: Int): List[Int] = {
    val goalPerWorker =
      mutable.ArrayBuffer.fill(totalNumWorkers)(goal / totalNumWorkers) // integer division
    // divide up the remainder, give 1 to the first n workers
    for (worker <- 0 until goal % totalNumWorkers) {
      goalPerWorker(worker) = goalPerWorker(worker) + 1
    }
    goalPerWorker.toList
  }

}
