package edu.uci.ics.amber.operator.randomksampling

import edu.uci.ics.amber.operator.filter.FilterOpExec
import scala.util.Random

class RandomKSamplingOpExec(percentage: Int, worker: Int, seedFunc: Int => Int)
    extends FilterOpExec {
  val rand: Random = new Random(seedFunc(worker))
  setFilterFunc(_ => (percentage / 100.0) >= rand.nextDouble())
}
