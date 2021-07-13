package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.tuple.ITuple

case class HashBasedShufflePolicyExec(policy: HashBasedShufflePolicy)
    extends ParallelBatchingPolicyExec() {
  override def selectBatchingIndex(tuple: ITuple): Int = {
    val numBuckets = policy.receivers.length
    (policy.hashFunc(tuple) % numBuckets + numBuckets) % numBuckets
  }
}
