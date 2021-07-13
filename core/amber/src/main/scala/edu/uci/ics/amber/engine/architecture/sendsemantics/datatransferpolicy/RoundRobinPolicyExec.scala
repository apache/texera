package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.tuple.ITuple

case class RoundRobinPolicyExec(policy: RoundRobinPolicy) extends ParallelBatchingPolicyExec {
  var roundRobinIndex = 0

  override def selectBatchingIndex(tuple: ITuple): Int = {
    roundRobinIndex = (roundRobinIndex + 1) % policy.receivers.length
    roundRobinIndex
  }
}
