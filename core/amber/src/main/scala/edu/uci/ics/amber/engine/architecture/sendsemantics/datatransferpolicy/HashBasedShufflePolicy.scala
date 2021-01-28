package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.ambertag.LinkTag
import edu.uci.ics.amber.engine.common.tuple.ITuple
import akka.actor.{ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{DataFrame, EndOfUpstream}
import edu.uci.ics.amber.engine.common.ambermessage.neo.DataPayload
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

/**
  * Notes for later:
  *
  * I will keep a heavy hitter list (top k). When asked to share load, I will start checking the incoming tuple to see if it
  * is a heavy hitter. If yes, then I will send it to the free or skewed in round robin manner. But if not heavy hitter, it will
  * always go to the skewed.
  * @param policyTag
  * @param batchSize
  * @param hashFunc
  * @param receivers
  */

class HashBasedShufflePolicy(
    policyTag: LinkTag,
    batchSize: Int,
    val hashFunc: ITuple => Int,
    receivers: Array[ActorVirtualIdentity]
) extends DataSendingPolicy(policyTag, batchSize, receivers) {
  val numBuckets = receivers.length

  // buckets once decided will remain same because we are not changing the number of workers in Join
  var bucketsToReceivers = new mutable.HashMap[Int, ArrayBuffer[ActorVirtualIdentity]]()
  var currReceiverIdxInBucket = new mutable.HashMap[Int, Int]()
  var receiverToBatch = new mutable.HashMap[ActorVirtualIdentity, Array[ITuple]]()
  var receiverToCurrBatchSize = new mutable.HashMap[ActorVirtualIdentity, Int]()

  initializeInternalState(receivers)

  override def noMore(): Array[(ActorVirtualIdentity, DataPayload)] = {
    val receiversAndBatches = new ArrayBuffer[(ActorVirtualIdentity, DataPayload)]
    for ((receiver, currSize) <- receiverToCurrBatchSize) {
      if (currSize > 0) {
        receiversAndBatches.append(
          (receiver, DataFrame(receiverToBatch(receiver).slice(0, currSize)))
        )
      }
      receiversAndBatches.append((receiver, EndOfUpstream()))
    }
    receiversAndBatches.toArray
  }

  private def getReceiverForBucket(bucket: Int): ActorVirtualIdentity = {
    val receiver = bucketsToReceivers(bucket)(currReceiverIdxInBucket(bucket))
    var nextReceiverIdx = currReceiverIdxInBucket(bucket) + 1
    if (nextReceiverIdx >= bucketsToReceivers(bucket).size) {
      nextReceiverIdx = 0
    }
    currReceiverIdxInBucket(bucket) = nextReceiverIdx
    receiver
  }

  override def addTupleToBatch(
      tuple: ITuple
  ): Option[(ActorVirtualIdentity, DataPayload)] = {
    val index = (hashFunc(tuple) % numBuckets + numBuckets) % numBuckets
    val receiver = getReceiverForBucket(index)
    receiverToBatch(receiver)(receiverToCurrBatchSize(receiver)) = tuple
    receiverToCurrBatchSize(receiver) += 1
    if (receiverToCurrBatchSize(receiver) == batchSize) {
      receiverToCurrBatchSize(receiver) = 0
      val retBatch = receiverToBatch(receiver)
      receiverToBatch(receiver) = new Array[ITuple](batchSize)
      return Some((receiver, DataFrame(retBatch)))
    }
    None
  }

  override def reset(): Unit = {
    initializeInternalState(receivers)
  }

  private[this] def initializeInternalState(_receivers: Array[ActorVirtualIdentity]): Unit = {
    for (i <- 0 until numBuckets) {
      bucketsToReceivers(i) = ArrayBuffer[ActorVirtualIdentity](receivers(i))
      currReceiverIdxInBucket(i) = 0
      receiverToBatch(_receivers(i)) = new Array[ITuple](batchSize)
      receiverToCurrBatchSize(_receivers(i)) = 0
    }
  }
}
