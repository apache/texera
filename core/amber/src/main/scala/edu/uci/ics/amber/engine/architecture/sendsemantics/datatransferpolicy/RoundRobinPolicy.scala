package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, EndOfUpstream}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

class RoundRobinPolicy(
    policyTag: LinkIdentity,
    batchSize: Int,
    receivers: Array[ActorVirtualIdentity]
) extends ParallelBatchingPolicy(policyTag, batchSize, receivers) {
  var roundRobinIndex = 0

  override def selectBatchingIndex(tuple: ITuple): Int = {
    roundRobinIndex = (roundRobinIndex + 1) % receivers.length
    roundRobinIndex
  }
}
