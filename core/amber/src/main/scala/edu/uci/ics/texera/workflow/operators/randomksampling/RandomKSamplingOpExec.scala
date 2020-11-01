package edu.uci.ics.texera.workflow.operators.randomksampling

import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpExec
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.util.Random

class RandomKSamplingOpExec(val actor: Int, val opDesc: RandomKSamplingOpDesc) extends FilterOpExec {
  val rand: Random = new Random(opDesc.getSeed(actor))
  setFilterFunc(_ => (opDesc.percentage / 100.0) >= rand.nextDouble())
}
