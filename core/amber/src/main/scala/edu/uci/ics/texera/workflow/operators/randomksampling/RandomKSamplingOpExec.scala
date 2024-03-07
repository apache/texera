package edu.uci.ics.texera.workflow.operators.randomksampling

import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpExec

import scala.util.Random

class RandomKSamplingOpExec(percentage: Int, worker: Int, seedFunc: Int => Int)
    extends FilterOpExec {
  val rand: Random = new Random(seedFunc(worker))
  setFilterFunc(_ => (percentage / 100.0) >= rand.nextDouble())
}
