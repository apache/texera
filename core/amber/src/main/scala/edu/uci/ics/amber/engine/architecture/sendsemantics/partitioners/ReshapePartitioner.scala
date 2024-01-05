package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * The reshape partitioner wraps a regular partitioner (round-robin, hash, or range)
  *   and provides alternative routing decisions based on reshape skew handling.
  *
  * For each tuple, the reshape partitioner invokes the wrapped partitioner
  *   to get a virtual "bucket index". Reshape builds another routing table that maps the virtual
  *   "bucket index" to the actual receivers, which might be different from the original receivers.
  */
class ReshapePartitioner(partitioner: Partitioner) extends Partitioner {

  override def getBucketIndex(tuple: ITuple): Iterator[Int] = {
    val it = partitioner.getBucketIndex(tuple)
    it.map(bucketIndex =>
      originalReceiverIndexMapping(recordSampleAndGetReceiverForReshape(bucketIndex))
    )
  }

  override def allReceivers: Seq[ActorVirtualIdentity] = partitioner.allReceivers

  val receivers: Seq[ActorVirtualIdentity] = partitioner.allReceivers
  val originalReceiverIndexMapping: Map[ActorVirtualIdentity, Int] = receivers.zipWithIndex.toMap

  // A bucket corresponds to a partition. When Reshape is not enabled, a bucket has just one receiver.
  // Reshape divides a skewed partition onto multiple workers. So, with Reshape, a bucket can have
  // multiple receivers. First receiver in the bucket is the original receiver for that partition.
  val numBuckets = receivers.length
  var bucketsToReceivers = new mutable.HashMap[Int, ArrayBuffer[ActorVirtualIdentity]]()
  var bucketsToSharingEnabled =
    new mutable.HashMap[Int, Boolean]() // Buckets to whether its inputs is redirected to helper.
  var bucketsToRedirectRatio =
    new mutable.HashMap[
      Int,
      (Long, Long, Long)
    ]() // How much input is to be redirected from the skewed worker input. The triplet corresponds to the
  // following - position index, numerator, denominator. Out of every `denominator` tuples in the bucket,
  // `numerator` tuples are redirected to the numerator. The `position index` is used to track how many tuples
  // have been redirected.

  val samplingSize =
    AmberConfig.reshapeWorkloadSampleSize // For every `samplingSize` tuples, record the tuple count to each receiver.
  var tupleIndexForSampling = 0 // goes from 0 to `samplingSize` and then resets to 0
  var receiverToWorkloadSamples = new mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]()
  @volatile var currentSampleCollectionIndex =
    0 // the index in `receiverToWorkloadSamples` array where sample is being recorded for a receiver
  var maxSamples = AmberConfig.reshapeMaxWorkloadSamplesInWorker

  initializeInternalState(receivers)

  def getDefaultReceiverForBucket(bucket: Int): ActorVirtualIdentity =
    bucketsToReceivers(bucket)(0)

  /**
    * For a bucket in which the input is shared, this function chooses the receiver worker for
    * the next input tuple
    */
  def getAndIncrementReceiverForBucket(bucket: Int): ActorVirtualIdentity = {
    var receiver: ActorVirtualIdentity = null
    val receiverCandidates = bucketsToReceivers(bucket)
    val redirectRatio: (Long, Long, Long) = bucketsToRedirectRatio.getOrElse(bucket, (0L, 0L, 0L))

    if (receiverCandidates.size > 1 && bucketsToSharingEnabled(bucket)) {
      if (bucketsToRedirectRatio.contains(bucket) && redirectRatio._1 <= redirectRatio._2) {
        // redirect to helper
        receiver = receiverCandidates(1)
      } else {
        // keep at skewed
        receiver = receiverCandidates(0)
      }
      if (bucketsToRedirectRatio.contains(bucket)) {
        if (redirectRatio._1 + 1L > redirectRatio._3) {
          bucketsToRedirectRatio(bucket) = (1L, redirectRatio._2, redirectRatio._3)
        } else {
          bucketsToRedirectRatio(bucket) =
            (redirectRatio._1 + 1L, redirectRatio._2, redirectRatio._3)
        }
      }
    } else {
      // sharing not enabled
      receiver = receiverCandidates(0)
    }

    receiver
  }

  /**
    * This is used to redirect a specific portion of the input of the skewed worker
    * to the helper worker in the first and second phase of mitigation. Out of every
    * `tuplesToRedirectDenominator` input tuples to the skewed worker, `tuplesToRedirectNumerator`
    * tuples are redirected to the helper worker.
    */
  def addReceiverToBucket(
      defaultRecId: ActorVirtualIdentity,
      helperRecId: ActorVirtualIdentity,
      tuplesToRedirectNumerator: Long,
      tuplesToRedirectDenominator: Long
  ): Boolean = {
    var defaultBucket: Int = -1
    bucketsToReceivers.keys.foreach(b => {
      if (bucketsToReceivers(b)(0) == defaultRecId) {
        defaultBucket = b
      }
    })
    if (defaultBucket == -1) {
      return false
    }
    if (!bucketsToReceivers(defaultBucket).contains(helperRecId)) {
      bucketsToReceivers(defaultBucket).append(helperRecId)
    }
    bucketsToRedirectRatio(defaultBucket) =
      (1, tuplesToRedirectNumerator, tuplesToRedirectDenominator)
    bucketsToSharingEnabled(defaultBucket) = true

    true
  }

  def removeReceiverFromBucket(
      defaultRecId: ActorVirtualIdentity,
      helperRecIdToRemove: ActorVirtualIdentity
  ): Boolean = {
    var defaultBucket: Int = -1
    bucketsToReceivers.keys.foreach(b => {
      if (bucketsToReceivers(b)(0) == defaultRecId) {
        defaultBucket = b
      }
    })
    if (defaultBucket == -1) {
      return false
    }
    bucketsToSharingEnabled(defaultBucket) = false
    true
  }

  /**
    * Used to return the workload samples to the controller.
    */
  def getWorkloadHistory(): Map[ActorVirtualIdentity, List[Long]] = {
    if (AmberConfig.reshapeSkewHandlingEnabled) {
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
      collectedTillNow.mapValues(_.toList).toMap
    } else {
      Map[ActorVirtualIdentity, List[Long]]()
    }
  }

  /**
    * Record the sample when the input is received for a bucket
    */
  def recordSample(bucket: Int): Unit = {
    // the first receiver in a bucket is the actual receiver for the partition
    // when there is no mitigation done by Reshape
    val storedSamples = receiverToWorkloadSamples(bucketsToReceivers(bucket)(0))
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

  def recordSampleAndGetReceiverForReshape(bucket: Int): ActorVirtualIdentity = {
    recordSample(bucket)
    if (bucketsToSharingEnabled.contains(bucket) && bucketsToSharingEnabled(bucket)) {
      // if skew mitigation (either first or second phase) is happening
      getAndIncrementReceiverForBucket(bucket)
    } else {
      getDefaultReceiverForBucket(bucket)
    }
  }

  private[this] def initializeInternalState(_receivers: Seq[ActorVirtualIdentity]): Unit = {
    for (i <- 0 until numBuckets) {
      bucketsToReceivers(i) = ArrayBuffer[ActorVirtualIdentity](receivers(i))
      receiverToWorkloadSamples(_receivers(i)) = ArrayBuffer[Long](0)
    }
  }

}
