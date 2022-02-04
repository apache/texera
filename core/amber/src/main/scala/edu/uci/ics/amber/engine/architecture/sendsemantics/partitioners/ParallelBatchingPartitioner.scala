package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, EndOfUpstream}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

abstract class ParallelBatchingPartitioner(batchSize: Int, receivers: Seq[ActorVirtualIdentity])
    extends Partitioner {

  // A bucket corresponds to a partition. When Reshape is not enabled, a bucket has just one receiver.
  // Reshape divides a skewed partition onto multiple workers. So, with Reshape, a bucket can have
  // multiple receivers. First receiver in the bucket is the original receiver for that partition.
  val numBuckets = receivers.length
  var bucketsToReceivers = new mutable.HashMap[Int, ArrayBuffer[ActorVirtualIdentity]]()
  var receiverToBatch = new mutable.HashMap[ActorVirtualIdentity, Array[ITuple]]()
  var receiverToCurrBatchSize = new mutable.HashMap[ActorVirtualIdentity, Int]()
  val samplingSize =
    2000 // For every `samplingSize` tuples, record the tuple count to each receiver.
  var tupleIndexForSampling = 0 // goes from 0 to `samplingSize` and then resets to 0
  var receiverToWorkloadSamples = new mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]()
  @volatile var currentSampleCollectionIndex =
    0 // the index in `receiverToWorkloadSamples` array where sample is being recorded for a receiver
  var maxSamples = 500
  initializeInternalState(receivers)

  def selectBatchingIndex(tuple: ITuple): Int

  def getDefaultReceiverForBucket(bucket: Int): ActorVirtualIdentity =
    bucketsToReceivers(bucket)(0)

  /**
    * Used to return the workload samples to the controller.
    */
  def getWorkloadHistory(): mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]] = {
    if (Constants.reshapeSkewHandlingEnabled) {
      val collectedTillNow = new mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]()
      receiverToWorkloadSamples.keys.foreach(rec => {
        collectedTillNow(rec) = new ArrayBuffer[Long]()
        for (i <- 0 to receiverToWorkloadSamples(rec).size - 1) {
          collectedTillNow(rec).append(receiverToWorkloadSamples(rec)(i))
        }
      })

      currentSampleCollectionIndex = 0
      for (i <- 0 until numBuckets) {
        receiverToWorkloadSamples(receivers(i)) = ArrayBuffer[Long](0)
      }
      collectedTillNow
    } else {
      new mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]
    }
  }

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

  override def addTupleToBatch(
      tuple: ITuple
  ): Option[(ActorVirtualIdentity, DataPayload)] = {
    val index = selectBatchingIndex(tuple)
    if (Constants.reshapeSkewHandlingEnabled) {
      // the first receiver in a bucket is the actual receiver for the partition
      // when there is no mitigation done by Reshape
      val storedSamples = receiverToWorkloadSamples(bucketsToReceivers(index)(0))
      storedSamples(currentSampleCollectionIndex) = storedSamples(currentSampleCollectionIndex) + 1
      tupleIndexForSampling += 1
      if (tupleIndexForSampling % samplingSize == 0) {
        if (currentSampleCollectionIndex >= maxSamples - 1) {
          // Maximum number of samples have been collected.
          // Write over the older samples
          currentSampleCollectionIndex = 0
        } else {
          if (storedSamples.size < maxSamples) {
            receiverToWorkloadSamples.keys.foreach(rec => {
              receiverToWorkloadSamples(rec).append(0)
            })
          }
          currentSampleCollectionIndex += 1
        }
        // Set to 0 before starting new sample
        receiverToWorkloadSamples.keys.foreach(rec =>
          receiverToWorkloadSamples(rec)(currentSampleCollectionIndex) = 0
        )
        tupleIndexForSampling = 0
      }
    }

    val receiver: ActorVirtualIdentity = getDefaultReceiverForBucket(index)
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

  private[this] def initializeInternalState(_receivers: Seq[ActorVirtualIdentity]): Unit = {
    for (i <- 0 until numBuckets) {
      bucketsToReceivers(i) = ArrayBuffer[ActorVirtualIdentity](receivers(i))
      receiverToWorkloadSamples(_receivers(i)) = ArrayBuffer[Long](0)
      receiverToBatch(_receivers(i)) = new Array[ITuple](batchSize)
      receiverToCurrBatchSize(_receivers(i)) = 0
    }
  }

}
