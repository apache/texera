package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, EndOfUpstream}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

class HashBasedShufflePolicy(
    policyTag: LinkIdentity,
    batchSize: Int,
    val hashFunc: ITuple => Int,
    receivers: Array[ActorVirtualIdentity]
) extends ParallelBatchingPolicy(policyTag, batchSize, receivers) {

  override def selectBatchingIndex(tuple: ITuple): Int = {
    val numBuckets = receivers.length
    (hashFunc(tuple) % numBuckets + numBuckets) % numBuckets
  }
}
