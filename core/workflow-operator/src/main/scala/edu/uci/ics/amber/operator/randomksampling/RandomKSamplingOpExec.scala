package edu.uci.ics.amber.operator.randomksampling

import edu.uci.ics.amber.operator.filter.FilterOpExec
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import scala.util.Random

class RandomKSamplingOpExec(descString: String, idx: Int, workerCount: Int) extends FilterOpExec {
  private val desc: RandomKSamplingOpDesc =
    objectMapper.readValue(descString, classOf[RandomKSamplingOpDesc])

  val rand: Random = new Random(workerCount)
  setFilterFunc(_ => (desc.percentage / 100.0) >= rand.nextDouble())
}
