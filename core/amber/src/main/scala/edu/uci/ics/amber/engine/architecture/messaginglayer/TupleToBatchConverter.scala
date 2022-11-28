package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners.{
  HashBasedShufflePartitioner,
  OneToOnePartitioner,
  ParallelBatchingPartitioner,
  Partitioner,
  RangeBasedShufflePartitioner,
  RoundRobinPartitioner
}
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings._
import edu.uci.ics.amber.engine.common.ambermessage.DataPayload
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

import scala.Function.tupled
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** This class is a container of all the transfer partitioners.
  *
  * @param selfID         ActorVirtualIdentity of self.
  * @param dataOutputPort DataOutputPort
  */
class TupleToBatchConverter(
    selfID: ActorVirtualIdentity,
    dataOutputPort: NetworkOutputPort[DataPayload]
) {
  private val partitioners = mutable.HashMap[LinkIdentity, Partitioner]()

  /**
    * Used to return the workload samples of the next operator's workers to the controller.
    */
  def getWorkloadHistory(): List[Map[ActorVirtualIdentity, List[Long]]] = {
    val allDownstreamSamples =
      new ArrayBuffer[Map[ActorVirtualIdentity, List[Long]]]()
    partitioners.values.foreach(partitioner => {
      if (partitioner.isInstanceOf[ParallelBatchingPartitioner]) {
        // Reshape only needs samples from workers that shuffle data across nodes
        allDownstreamSamples.append(
          partitioner.asInstanceOf[ParallelBatchingPartitioner].getWorkloadHistory()
        )
      }
    })
    allDownstreamSamples.toList
  }

  /**
    * Used by Reshape to share the input of skewed worker with the helper worker.
    * For every `tuplesToRedirectDenominator` tuples in the partition of the skewed
    * worker, `tuplesToRedirectNumerator` tuples will be redirected to the helper.
    */
  def sharePartition(
      skewedReceiverId: ActorVirtualIdentity,
      helperReceiverId: ActorVirtualIdentity,
      tuplesToRedirectNumerator: Long,
      tuplesToRedirectDenominator: Long
  ): Boolean = {
    var success = false
    // There can be many downstream operators that this worker sends data
    // to. The `skewedReceiverId` and `helperReceiverId` correspond to just
    // one of the operators. So, as long as the workers are found and the partition
    // is shared in one of the `partiotioners`, we return success.
    partitioners.values.foreach(partitioner => {
      if (partitioner.isInstanceOf[ParallelBatchingPartitioner]) {
        val receiversFound = partitioner
          .asInstanceOf[ParallelBatchingPartitioner]
          .addReceiverToBucket(
            skewedReceiverId,
            helperReceiverId,
            tuplesToRedirectNumerator,
            tuplesToRedirectDenominator
          )
        success = success | receiversFound
      }
    })
    success
  }

  /**
    * Used by Reshape to temporarily pause the mitigation if the helper worker gets
    * too overloaded.
    */
  def pauseSkewMitigation(
      skewedReceiverId: ActorVirtualIdentity,
      helperReceiverId: ActorVirtualIdentity
  ): Boolean = {
    var success = false
    // There can be many downstream operators that this worker sends data
    // to. The `skewedReceiverId` and `helperReceiverId` correspond to just
    // one of the operators. So, as long as the workers are found and the partition
    // is shared in one of the `partiotioners`, we return success.
    partitioners.values.foreach(partitioner => {
      if (partitioner.isInstanceOf[ParallelBatchingPartitioner]) {
        val receiversFound = partitioner
          .asInstanceOf[ParallelBatchingPartitioner]
          .removeReceiverFromBucket(
            skewedReceiverId,
            helperReceiverId
          )
        success = success | receiversFound
      }
    })
    success
  }

  /**
    * Add down stream operator and its corresponding Partitioner.
    * @param partitioning Partitioning, describes how and whom to send to.
    */
  def addPartitionerWithPartitioning(tag: LinkIdentity, partitioning: Partitioning): Unit = {

    // create a corresponding partitioner for the given partitioning
    val partitioner = partitioning match {
      case oneToOnePartitioning: OneToOnePartitioning => OneToOnePartitioner(oneToOnePartitioning)
      case roundRobinPartitioning: RoundRobinPartitioning =>
        RoundRobinPartitioner(roundRobinPartitioning)
      case hashBasedShufflePartitioning: HashBasedShufflePartitioning =>
        HashBasedShufflePartitioner(hashBasedShufflePartitioning)
      case rangeBasedShufflePartitioning: RangeBasedShufflePartitioning =>
        RangeBasedShufflePartitioner(rangeBasedShufflePartitioning)
      case _ => throw new RuntimeException(s"partitioning $partitioning not supported")
    }

    // update the existing partitioners.
    partitioners.update(tag, partitioner)

  }

  /**
    * Push one tuple to the downstream, will be batched by each transfer partitioning.
    * Should ONLY be called by DataProcessor.
    * @param tuple ITuple to be passed.
    */
  def passTupleToDownstream(
      tuple: ITuple,
      outputPort: Option[LinkIdentity] = Option.empty
  ): Unit = {
    // find the corresponding partitioner based on output port
    val outputPortPartitioners: Iterable[Partitioner] =
      if (outputPort.isEmpty)
        partitioners.values
      else
        List(
          partitioners.getOrElse(
            outputPort.get,
            throw new RuntimeException("output port not found")
          )
        )

    outputPortPartitioners.foreach(partitioner =>
      partitioner.addTupleToBatch(tuple) foreach tupled((to, batch) =>
        dataOutputPort.sendTo(to, batch)
      )
    )
  }

  /* Old API: for compatibility */
  @deprecated
  def resetPolicies(): Unit = {
    partitioners.values.foreach(_.reset())
  }

  /**
    * Send the last batch and EOU marker to all down streams
    */
  def emitEndOfUpstream(): Unit = {
    partitioners.values.foreach(partitioner =>
      partitioner.noMore() foreach tupled((to, batch) => dataOutputPort.sendTo(to, batch))
    )
  }

}
