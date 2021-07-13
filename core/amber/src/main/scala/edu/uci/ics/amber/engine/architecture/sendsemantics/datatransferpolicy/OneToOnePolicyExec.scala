package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, EndOfUpstream}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity

import scala.collection.mutable.ArrayBuffer



case class OneToOnePolicyExec(override val policy: OneToOnePolicy)
    extends DataSendingPolicyExec {
  var batch: Array[ITuple] = new Array[ITuple](policy.batchSize)
  var currentSize = 0

  assert(policy.receivers.length == 1)

  override def addTupleToBatch(
      tuple: ITuple
  ): Option[(ActorVirtualIdentity, DataPayload)] = {
    batch(currentSize) = tuple
    currentSize += 1
    if (currentSize == policy.batchSize) {
      currentSize = 0
      val retBatch = batch
      batch = new Array[ITuple](policy.batchSize)
      return Some((policy.receivers(0), DataFrame(retBatch)))
    }
    None
  }

  override def noMore(): Array[(ActorVirtualIdentity, DataPayload)] = {
    val ret = new ArrayBuffer[(ActorVirtualIdentity, DataPayload)]
    if (currentSize > 0) {
      ret.append((policy.receivers(0), DataFrame(batch.slice(0, currentSize))))
    }
    ret.append((policy.receivers(0), EndOfUpstream()))
    ret.toArray
  }

  override def reset(): Unit = {
    batch = new Array[ITuple](policy.batchSize)
    currentSize = 0
  }
}
