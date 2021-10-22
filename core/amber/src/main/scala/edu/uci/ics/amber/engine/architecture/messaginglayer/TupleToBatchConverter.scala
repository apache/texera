package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners.{
  HashBasedShufflePartitioner,
  OneToOnePartitioner,
  Partitioner,
  RoundRobinPartitioner
}
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings._
import edu.uci.ics.amber.engine.common.ambermessage.DataPayload
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

import scala.Function.tupled
import scala.collection.mutable

/** This class is a container of all the transfer partitioners.
  * @param selfID ActorVirtualIdentity of self.
  * @param dataOutputPort DataOutputPort
  */
class TupleToBatchConverter(
    selfID: ActorVirtualIdentity,
    dataOutputPort: NetworkOutputPort[DataPayload]
) {
  private val partitioners = mutable.HashMap[LinkIdentity, Partitioner]()

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
  def passTupleToDownstream(tuple: ITuple): Unit = {
    partitioners.valuesIterator.foreach(partitioner =>
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
