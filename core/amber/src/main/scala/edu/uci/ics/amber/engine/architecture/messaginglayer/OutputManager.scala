package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.messaginglayer.OutputManager.{
  DPOutputIterator,
  getBatchSize,
  toPartitioner
}
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners._
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings._
import edu.uci.ics.amber.engine.architecture.worker.DataProcessor.{FinalizeExecutor, FinalizePort}
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.tuple.amber.{SchemaEnforceable, TupleLike}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.engine.common.workflow.{PhysicalLink, PortIdentity}
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

import scala.collection.mutable

object OutputManager {

  final case class FlushNetworkBuffer() extends ControlCommand[Unit]

  // create a corresponding partitioner for the given partitioning policy
  def toPartitioner(partitioning: Partitioning, actorId: ActorVirtualIdentity): Partitioner = {
    val partitioner = partitioning match {
      case oneToOnePartitioning: OneToOnePartitioning =>
        OneToOnePartitioner(oneToOnePartitioning, actorId)
      case roundRobinPartitioning: RoundRobinPartitioning =>
        RoundRobinPartitioner(roundRobinPartitioning)
      case hashBasedShufflePartitioning: HashBasedShufflePartitioning =>
        HashBasedShufflePartitioner(hashBasedShufflePartitioning)
      case rangeBasedShufflePartitioning: RangeBasedShufflePartitioning =>
        RangeBasedShufflePartitioner(rangeBasedShufflePartitioning)
      case broadcastPartitioning: BroadcastPartitioning =>
        BroadcastPartitioner(broadcastPartitioning)
      case _ => throw new RuntimeException(s"partitioning $partitioning not supported")
    }
    partitioner
  }

  def getBatchSize(partitioning: Partitioning): Int = {
    partitioning match {
      case p: OneToOnePartitioning          => p.batchSize
      case p: RoundRobinPartitioning        => p.batchSize
      case p: HashBasedShufflePartitioning  => p.batchSize
      case p: RangeBasedShufflePartitioning => p.batchSize
      case p: BroadcastPartitioning         => p.batchSize
      case _                                => throw new RuntimeException(s"partitioning $partitioning not supported")
    }
  }

  class DPOutputIterator extends Iterator[(TupleLike, Option[PortIdentity])] {
    val queue = new mutable.ListBuffer[(TupleLike, Option[PortIdentity])]
    @transient var outputIter: Iterator[(TupleLike, Option[PortIdentity])] = Iterator.empty

    def setTupleOutput(outputIter: Iterator[(TupleLike, Option[PortIdentity])]): Unit = {
      if (outputIter != null) {
        this.outputIter = outputIter
      } else {
        this.outputIter = Iterator.empty
      }
    }

    override def hasNext: Boolean = outputIter.hasNext || queue.nonEmpty

    override def next(): (TupleLike, Option[PortIdentity]) = {
      if (outputIter.hasNext) {
        outputIter.next()
      } else {
        queue.remove(0)
      }
    }

    def appendSpecialTupleToEnd(tuple: TupleLike): Unit = {
      queue.append((tuple, None))
    }
  }
}

/** This class is a container of all the transfer partitioners.
  *
  * @param actorId         ActorVirtualIdentity of self.
  * @param outputGateway DataOutputPort
  */
class OutputManager(
    val actorId: ActorVirtualIdentity,
    outputGateway: NetworkOutputGateway
) extends AmberLogging {

  val outputIterator: DPOutputIterator = new DPOutputIterator()
  private val partitioners: mutable.Map[PhysicalLink, Partitioner] =
    mutable.HashMap[PhysicalLink, Partitioner]()

  private val ports: mutable.HashMap[PortIdentity, WorkerPort] = mutable.HashMap()

  private val networkOutputBuffers =
    mutable.HashMap[(PhysicalLink, ActorVirtualIdentity), NetworkOutputBuffer]()

  /**
    * Add down stream operator and its corresponding Partitioner.
    * @param partitioning Partitioning, describes how and whom to send to.
    */
  def addPartitionerWithPartitioning(
      link: PhysicalLink,
      partitioning: Partitioning
  ): Unit = {
    val partitioner = toPartitioner(partitioning, actorId)
    partitioners.update(link, partitioner)
    partitioner.allReceivers.foreach(receiver => {
      val buffer = new NetworkOutputBuffer(receiver, outputGateway, getBatchSize(partitioning))
      networkOutputBuffers.update((link, receiver), buffer)
      outputGateway.addOutputChannel(ChannelIdentity(actorId, receiver, isControl = false))
    })
  }

  /**
    * Push one tuple to the downstream, will be batched by each transfer partitioning.
    * Should ONLY be called by DataProcessor.
    * @param tupleLike TupleLike to be passed.
    * @param outputPortId Optionally specifies the output port from which the tuple should be emitted.
    *                     If None, the tuple is broadcast to all output ports.
    */
  def passTupleToDownstream(
      tupleLike: SchemaEnforceable,
      outputPortId: Option[PortIdentity] = None
  ): Unit = {
    (outputPortId match {
      case Some(portId) => partitioners.filter(_._1.fromPortId == portId) // send to a specific port
      case None         => partitioners // send to all ports
    }).foreach {
      case (link, partitioner) =>
        // Enforce schema based on the port's schema
        val tuple = tupleLike.enforceSchema(getPort(link.fromPortId).schema)
        partitioner.getBucketIndex(tuple).foreach { bucketIndex =>
          networkOutputBuffers((link, partitioner.allReceivers(bucketIndex))).addTuple(tuple)
        }
    }
  }

  /**
    * Flushes the network output buffers based on the specified set of physical links.
    *
    * This method flushes the buffers associated with the network output. If the 'onlyFor' parameter
    * is specified with a set of 'PhysicalLink's, only the buffers corresponding to those links are flushed.
    * If 'onlyFor' is None, all network output buffers are flushed.
    *
    * @param onlyFor An optional set of 'ChannelID' indicating the specific buffers to flush.
    *                If None, all buffers are flushed. Default value is None.
    */
  def flush(onlyFor: Option[Set[ChannelIdentity]] = None): Unit = {
    val buffersToFlush = onlyFor match {
      case Some(channelIds) =>
        networkOutputBuffers
          .filter(out => {
            val channel = ChannelIdentity(actorId, out._1._2, isControl = false)
            channelIds.contains(channel)
          })
          .values
      case None => networkOutputBuffers.values
    }
    buffersToFlush.foreach(_.flush())
  }

  /**
    * Send the last batch and EOU marker to all down streams
    */
  def emitEndOfUpstream(): Unit = {
    // flush all network buffers of this operator, emit end marker to network
    networkOutputBuffers.foreach(kv => {
      kv._2.flush()
      kv._2.noMore()
    })
  }

  def addPort(portId: PortIdentity, schema: Schema): Unit = {
    // each port can only be added and initialized once.
    if (this.ports.contains(portId)) {
      return
    }
    this.ports(portId) = WorkerPort(schema)

  }

  def getPort(portId: PortIdentity): WorkerPort = ports(portId)

  def hasUnfinishedOutput: Boolean = outputIterator.hasNext

  def finalizeOutput(): Unit = {
    this.ports.keys
      .foreach(outputPortId =>
        outputIterator.appendSpecialTupleToEnd(FinalizePort(outputPortId, input = false))
      )
    outputIterator.appendSpecialTupleToEnd(FinalizeExecutor())
  }

  def getSingleOutputPortIdentity: PortIdentity = {
    assert(ports.size == 1)
    ports.head._1
  }

}
