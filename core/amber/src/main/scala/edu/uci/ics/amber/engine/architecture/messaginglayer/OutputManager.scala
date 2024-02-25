package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.messaginglayer.OutputManager.{
  getBatchSize,
  toPartitioner
}
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners._
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings._
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.tuple.amber.{MapTupleLike, SchemaEnforceable, SeqTupleLike}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import org.jooq.exception.MappingException

import scala.collection.mutable

object OutputManager {

  final case class FlushNetworkBuffer() extends ControlCommand[Unit]

  // create a corresponding partitioner for the given partitioning policy
  def toPartitioner(partitioning: Partitioning): Partitioner = {
    val partitioner = partitioning match {
      case oneToOnePartitioning: OneToOnePartitioning => OneToOnePartitioner(oneToOnePartitioning)
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
}

/** This class is a container of all the transfer partitioners.
  *
  * @param selfID         ActorVirtualIdentity of self.
  * @param dataOutputPort DataOutputPort
  */
class OutputManager(
    selfID: ActorVirtualIdentity,
    dataOutputPort: NetworkOutputGateway
) {

  val partitioners = mutable.HashMap[PhysicalLink, Partitioner]()

  val networkOutputBuffers =
    mutable.HashMap[(PhysicalLink, ActorVirtualIdentity), NetworkOutputBuffer]()

  /**
    * Add down stream operator and its corresponding Partitioner.
    * @param partitioning Partitioning, describes how and whom to send to.
    */
  def addPartitionerWithPartitioning(
      link: PhysicalLink,
      partitioning: Partitioning
  ): Unit = {
    val partitioner = toPartitioner(partitioning)
    partitioners.update(link, partitioner)
    partitioner.allReceivers.foreach(receiver => {
      val buffer = new NetworkOutputBuffer(receiver, dataOutputPort, getBatchSize(partitioning))
      networkOutputBuffers.update((link, receiver), buffer)
      dataOutputPort.addOutputChannel(ChannelIdentity(selfID, receiver, isControl = false))
    })
  }

  /**
    * Push one tuple to the downstream, will be batched by each transfer partitioning.
    * Should ONLY be called by DataProcessor.
    * @param tupleLike TupleLike to be passed.
    */
  def passTupleToDownstream(
      tupleLike: SchemaEnforceable,
      outputLink: PhysicalLink,
      schema: Schema
  ): Unit = {
    val partitioner =
      partitioners.getOrElse(outputLink, throw new MappingException("output port not found"))
    val outputTuple: Tuple = enforceSchema(tupleLike, schema)
    partitioner
      .getBucketIndex(outputTuple)
      .foreach(bucketIndex => {
        val destActor = partitioner.allReceivers(bucketIndex)
        networkOutputBuffers((outputLink, destActor)).addTuple(outputTuple)
      })
  }

  /**
    * Transforms a TupleLike object to a Tuple that conforms to a given Schema.
    *
    * @param tupleLike The TupleLike object to be transformed.
    * @param schema The Schema to which the tupleLike object must conform.
    * @return A Tuple that matches the specified schema.
    * @throws RuntimeException if the tupleLike object type is unsupported or invalid for schema enforcement.
    */
  private def enforceSchema(tupleLike: SchemaEnforceable, schema: Schema): Tuple = {
    tupleLike match {
      case tTuple: Tuple =>
        assert(
          tTuple.getSchema == schema,
          s"output tuple schema does not match the expected schema! " +
            s"output schema: ${tTuple.getSchema}, " +
            s"expected schema: $schema"
        )
        tTuple
      case map: MapTupleLike =>
        buildTupleWithSchema(map, schema)
      case seq: SeqTupleLike =>
        buildTupleWithSchema(seq, schema)
      case _ =>
        throw new RuntimeException("invalid tuple type, cannot enforce schema")
    }
  }

  /**
    * Constructs a `Tuple` object based on a given schema and a map of field mappings.
    *
    * This method iterates over the field mappings provided by the `tupleLike` object, adding each field to the `Tuple` builder
    * based on the corresponding attribute in the `schema`. The `schema` defines the structure and types of fields allowed in the `Tuple`.
    *
    * @param tupleLike The source of field mappings, where each entry maps a field name to its value.
    * @param schema    The schema defining the structure and types of the `Tuple` to be built.
    * @return A `Tuple` instance that matches the provided schema and contains the data from `tupleLike`.
    */
  private def buildTupleWithSchema(tupleLike: MapTupleLike, schema: Schema): Tuple = {
    val builder = Tuple.newBuilder(schema)
    tupleLike.fieldMappings.foreach {
      case (name, value) =>
        builder.add(schema.getAttribute(name), value)
    }
    builder.build()
  }

  /**
    * Constructs a Tuple object from a sequence of field values
    * according to the specified schema. It asserts that the number
    * of provided fields matches the schema's requirement, every
    * field must also satisfy the field type.
    *
    * @param tupleLike Sequence of field values.
    * @param schema Schema for Tuple construction.
    * @return Tuple constructed according to the schema.
    */
  private def buildTupleWithSchema(tupleLike: SeqTupleLike, schema: Schema): Tuple = {
    val attributes = schema.getAttributes
    val builder = Tuple.newBuilder(schema)
    tupleLike.fields.zipWithIndex.foreach {
      case (value, i) =>
        builder.add(attributes.get(i), value)
    }
    builder.build()
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
            val channel = ChannelIdentity(selfID, out._1._2, isControl = false)
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

}
