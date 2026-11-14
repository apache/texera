/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.engine.architecture.messaginglayer

import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.storage.{DocumentFactory, VFSURIFactory}
import org.apache.texera.amber.core.storage.model.BufferedItemWriter
import org.apache.texera.amber.core.tuple._
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalLink, PortIdentity}
import org.apache.texera.amber.engine.architecture.messaginglayer.OutputManager.{
  DPOutputIterator,
  getBatchSize,
  toPartitioner
}
import org.apache.texera.amber.engine.architecture.sendsemantics.partitioners._
import org.apache.texera.amber.engine.architecture.sendsemantics.partitionings._
import org.apache.texera.amber.engine.architecture.worker.managers.{
  OutputPortStorageWriterThread,
  PortStorageWriterTerminateSignal
}
import org.apache.texera.amber.engine.common.AmberLogging
import org.apache.texera.amber.engine.common.ambermessage.VectorFrame
import org.apache.texera.amber.util.{ArrowUtils, VirtualIdentityUtils}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.VectorSchemaRoot

import java.net.URI
import scala.collection.mutable

object OutputManager {

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
  * @param actorId       ActorVirtualIdentity of self.
  * @param outputGateway DataOutputPort
  */
class OutputManager(
    val actorId: ActorVirtualIdentity,
    outputGateway: NetworkOutputGateway
) extends AmberLogging {

  val outputIterator: DPOutputIterator = new DPOutputIterator()

  // Set by the worker: whether a receiver worker is co-located (same ActorSystem). A local
  // hop never serializes, so the columnar wire is pure overhead there and that edge stays
  // on rows. None = not yet known (treated as remote). Wired into each buffer's locality
  // check when the partitioner is added.
  var isLocalReceiver: ActorVirtualIdentity => Option[Boolean] = _ => None
  private val partitioners: mutable.Map[PhysicalLink, Partitioner] =
    mutable.HashMap[PhysicalLink, Partitioner]()

  private val ports: mutable.HashMap[PortIdentity, WorkerPort] = mutable.HashMap()

  private val networkOutputBuffers =
    mutable.HashMap[(PhysicalLink, ActorVirtualIdentity), NetworkOutputBuffer]()

  private val outputPortResultWriterThreads
      : mutable.HashMap[PortIdentity, OutputPortStorageWriterThread] =
    mutable.HashMap()

  private val stateWriterThreads: mutable.HashMap[PortIdentity, OutputPortStorageWriterThread] =
    mutable.HashMap()

  /**
    * Add down stream operator and its corresponding Partitioner.
    *
    * @param partitioning Partitioning, describes how and whom to send to.
    */
  def addPartitionerWithPartitioning(
      link: PhysicalLink,
      partitioning: Partitioning,
      // Per-edge columnar-wire decision from the planner: ship this edge's batches as
      // Arrow (VectorFrame) rather than rows. Defaults off so the row wire is unchanged.
      columnar: Boolean = false
  ): Unit = {
    val partitioner = toPartitioner(partitioning, actorId)
    partitioners.update(link, partitioner)
    partitioner.allReceivers.foreach(receiver => {
      val buffer =
        new NetworkOutputBuffer(receiver, outputGateway, getBatchSize(partitioning), columnar)
      // A local (same-JVM) receiver keeps the edge on rows regardless of the planner flag.
      buffer.localityCheck = () => isLocalReceiver(receiver)
      networkOutputBuffers.update((link, receiver), buffer)
      outputGateway.addOutputChannel(ChannelIdentity(actorId, receiver, isControl = false))
    })
  }

  /**
    * Push one tuple to the downstream, will be batched by each transfer partitioning.
    * Should ONLY be called by DataProcessor.
    *
    * @param tuple    TupleLike to be passed.
    * @param outputPortId Optionally specifies the output port from which the tuple should be emitted.
    *                     If None, the tuple is broadcast to all output ports.
    */
  def passTupleToDownstream(
      tuple: Tuple,
      outputPortId: Option[PortIdentity] = None
  ): Unit = {
    (outputPortId match {
      case Some(portId) => partitioners.filter(_._1.fromPortId == portId) // send to a specific port
      case None         => partitioners // send to all ports
    }).foreach {
      case (link, partitioner) =>
        partitioner.getBucketIndex(tuple).foreach { bucketIndex =>
          networkOutputBuffers((link, partitioner.allReceivers(bucketIndex))).addTuple(tuple)
        }
    }
  }

  /**
    * Provisional (phase 1): turn the columnar output path on or off for every current
    * edge. Phase 2 replaces this blanket switch with a per-edge decision made by the
    * planner (local vs cross-node, and whether the receiver consumes columns).
    */
  def setColumnarOutput(enabled: Boolean): Unit =
    networkOutputBuffers.values.foreach(_.columnar = enabled)

  // Backs the per-bucket roots the columnar shuffle materializes; each is freed right
  // after it is serialized, so this allocator's outstanding bytes return to zero.
  private lazy val columnarAllocator: BufferAllocator = new RootAllocator()

  /**
    * Columnar output path (no round-trip): ship an operator's Arrow result batch straight
    * downstream without decoding it to tuples and re-encoding it. Eligible only when the
    * output port needs no storage and every edge on it is in columnar mode. A
    * non-splitting edge (one-to-one, broadcast) sends the whole batch, serialized once, to
    * each receiver. A splitting edge (hash/range/round-robin) assigns each row a bucket
    * through the same getBucketIndex the row path uses (reading only the key columns into a
    * partial tuple), gathers each bucket's rows in Arrow, and sends one VectorFrame per
    * non-empty bucket. Bucket assignment is therefore identical to the row path. Does NOT
    * close `root` (caller owns it).
    *
    * @return Some((portId, totalSerializedByteCount)) if it shipped the batch (caller
    *         records output statistics), or None if the port is not eligible (fall back).
    */
  def tryEmitColumnarBatch(
      root: VectorSchemaRoot,
      outputPortId: Option[PortIdentity]
  ): Option[(PortIdentity, Long)] = {
    if (outputPortId.isEmpty && ports.size != 1) return None
    val portId = outputPortId.getOrElse(ports.head._1)
    val links = partitioners.filter(_._1.fromPortId == portId).toSeq
    val eligible =
      links.nonEmpty &&
        !outputPortResultWriterThreads.contains(portId) &&
        links.forall {
          case (link, partitioner) =>
            // The no-decode fast path runs only when every receiver is remote-columnar. A
            // local (or mixed) edge falls back to the row path, whose per-receiver flush
            // already emits rows to local receivers and Arrow to remote ones.
            partitioner.allReceivers.forall(r =>
              networkOutputBuffers.get((link, r)).exists(_.remoteColumnar)
            )
        }
    if (!eligible) return None
    val schema = getPort(portId).schema
    // The executor's Arrow schema must match the declared output port schema; if not,
    // fall back so the row path's enforceSchema can adjust it.
    if (ArrowUtils.toTexeraSchema(root.getSchema) != schema) return None
    Some((portId, links.map { case (link, p) => emitColumnarToLink(root, schema, link, p) }.sum))
  }

  // Ship `root` to one link's receivers in Arrow, splitting per bucket if the partitioner
  // splits. Returns the total serialized bytes put on the wire for this link.
  private def emitColumnarToLink(
      root: VectorSchemaRoot,
      schema: Schema,
      link: PhysicalLink,
      partitioner: Partitioner
  ): Long = {
    val receivers = partitioner.allReceivers
    if (!partitioner.columnarSplits) {
      // One-to-one / broadcast: the whole batch goes to each receiver; serialize once.
      val bytes = ArrowUtils.serializeRoot(root)
      val frame = VectorFrame(bytes, root.getRowCount, schema)
      receivers.foreach(r => networkOutputBuffers((link, r)).sendVectorFrame(frame))
      bytes.length.toLong
    } else {
      // Splitting shuffle: assign each row a bucket, then gather and send per bucket.
      val n = root.getRowCount
      val rowsPerBucket = Array.fill(receivers.length)(new mutable.ArrayBuffer[Int]())
      // Build only the columns getBucketIndex needs into a partial tuple per row.
      val keyCols =
        if (partitioner.requiresFullTuple) schema.getAttributes.map(_.getName)
        else partitioner.bucketKeyColumns
      val keySchema = Schema(keyCols.map(schema.getAttribute).toList)
      val keyIdx =
        if (keyCols.isEmpty) Array.empty[Int] else ArrowUtils.projectionIndices(root, keyCols)
      val keyTypes = keyCols.map(schema.getAttribute(_).getType).toArray
      val vectors = root.getFieldVectors
      var i = 0
      while (i < n) {
        val keyValues = new Array[Any](keyIdx.length)
        var c = 0
        while (c < keyIdx.length) {
          keyValues(c) = ArrowUtils.getCell(vectors.get(keyIdx(c)), i, keyTypes(c)); c += 1
        }
        partitioner.getBucketIndex(Tuple(keySchema, keyValues)).foreach(b => rowsPerBucket(b) += i)
        i += 1
      }
      var bytes = 0L
      var b = 0
      while (b < receivers.length) {
        val rows = rowsPerBucket(b)
        if (rows.nonEmpty) {
          val bucketRoot = ArrowUtils.gather(root, rows.toArray, columnarAllocator)
          try {
            val frameBytes = ArrowUtils.serializeRoot(bucketRoot)
            networkOutputBuffers((link, receivers(b)))
              .sendVectorFrame(VectorFrame(frameBytes, rows.length, schema))
            bytes += frameBytes.length.toLong
          } finally bucketRoot.close()
        }
        b += 1
      }
      bytes
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
    * Emit a State to every network buffer and (if configured) the state
    * storage. `loopCounter` / `loopStartId` are the loop envelope riding
    * alongside the State (see `StateFrame`); a JVM hop inside a loop body
    * passes the incoming envelope through unchanged, while a Scala-originated
    * state (start/end-channel handlers) uses the "no loop" defaults.
    */
  def emitState(state: State, loopCounter: Long = 0L, loopStartId: String = ""): Unit = {
    networkOutputBuffers.foreach(kv => kv._2.sendState(state, loopCounter, loopStartId))
    saveStateToStorageIfNeeded(state, loopCounter, loopStartId)
  }

  def addPort(portId: PortIdentity, schema: Schema, storageURIBaseOption: Option[URI]): Unit = {
    // each port can only be added and initialized once.
    if (this.ports.contains(portId)) {
      return
    }
    this.ports(portId) = WorkerPort(schema)

    // if a storage URI base is provided, set up storage writer threads
    storageURIBaseOption match {
      case Some(portBaseURI) => setupOutputStorageWriterThread(portId, portBaseURI)
      case None              => // No need to add a writer
    }
  }

  /**
    * Optionally write the tuple to storage if the specified output port is determined by the scheduler to need storage.
    * This method is not blocking because a separate thread is used to flush the tuple to storage in batch.
    *
    * @param tuple TupleLike to be written to storage.
    * @param outputPortId If not specified, the tuple will be written to all output ports that need storage.
    */
  def saveTupleToStorageIfNeeded(
      tuple: Tuple,
      outputPortId: Option[PortIdentity] = None
  ): Unit = {
    (outputPortId match {
      case Some(portId) =>
        this.outputPortResultWriterThreads.get(portId) match {
          case Some(_) => this.outputPortResultWriterThreads.filter(_._1 == portId)
          case None    => Map.empty
        }
      case None => this.outputPortResultWriterThreads
    }).foreach({
      case (portId, writerThread) =>
        // write to storage in a separate thread
        writerThread.queue.put(Left(tuple))
    })
  }

  private def saveStateToStorageIfNeeded(
      state: State,
      loopCounter: Long,
      loopStartId: String
  ): Unit = {
    // The same state row is fanned out to every output port's state
    // table. This mirrors the broadcast-to-all-workers behavior on the
    // emit side: state is shared context, not per-key data, so every
    // downstream operator (and every worker reading the materialization)
    // needs the full set. The loop envelope is materialized as its own
    // columns so the downstream reader can rebuild it.
    stateWriterThreads.values.foreach(_.queue.put(Left(state.toTuple(loopCounter, loopStartId))))
  }

  /**
    * Singal the port storage writer to flush the remaining buffer and wait for commits to finish so that
    * the output port is properly completed. If the output port does not need storage, no action will be done.
    *
    * If the writer thread captured a failure (e.g., iceberg commit retries
    * exhausted), re-throw it here so the DP thread surfaces a FatalError
    * to the coordinator via pekko's supervisor strategy. Otherwise the worker
    * would announce port completion as if the result was durably written.
    */
  def closeOutputStorageWriterIfNeeded(outputPortId: PortIdentity): Unit = {
    this.outputPortResultWriterThreads.get(outputPortId) match {
      case Some(writerThread) =>
        // Non-blocking call
        writerThread.queue.put(Right(PortStorageWriterTerminateSignal))
        // Blocking call
        writerThread.join()
        writerThread.getFailure.foreach(throw _)
      case None =>
    }
    this.stateWriterThreads.remove(outputPortId).foreach { writerThread =>
      writerThread.queue.put(Right(PortStorageWriterTerminateSignal))
      writerThread.join()
      writerThread.getFailure.foreach(throw _)
    }
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

  /**
    * This method is only used for ensuring correct region execution. Some operators may have input port dependency
    * relationships, for which we currently use a two-phase region execution scheme.  (See `RegionExecutionManager`
    * for details.)
    * This logic will only be executed when the worker is part of an `executingDependeePort` region-execution phase.
    * We currently assume that in this phase the operator (worker) will not output any data, hence no output ports.
    * However we still need to keep this worker open for the next `executingNonDependeePort` phase.
    *
    * @return Whether this worker currently does not have any output port.
    */
  def isMissingOutputPort: Boolean = {
    this.ports.isEmpty
  }

  def getSingleOutputPortIdentity: PortIdentity = {
    assert(ports.size == 1, "expect 1 output port, got " + ports.size)
    ports.head._1
  }

  private def setupOutputStorageWriterThread(portId: PortIdentity, portBaseURI: URI): Unit = {
    val bufferedItemWriter = DocumentFactory
      .openDocument(VFSURIFactory.resultURI(portBaseURI))
      ._1
      .writer(
        VirtualIdentityUtils
          .getWorkerIndex(actorId)
          .getOrElse(
            throw new IllegalStateException(
              s"Expected worker actor id for output storage writer, got: ${actorId.name}"
            )
          )
          .toString
      )
      .asInstanceOf[BufferedItemWriter[Tuple]]
    val writerThread = new OutputPortStorageWriterThread(bufferedItemWriter)
    this.outputPortResultWriterThreads(portId) = writerThread
    writerThread.start()

    // The state document is provisioned alongside the result document
    // by RegionExecutionManager, so it is always present.
    val stateWriter = DocumentFactory
      .openDocument(VFSURIFactory.stateURI(portBaseURI))
      ._1
      .writer(VirtualIdentityUtils.getWorkerIndex(actorId).toString)
      .asInstanceOf[BufferedItemWriter[Tuple]]
    val stateWriterThread = new OutputPortStorageWriterThread(stateWriter)
    this.stateWriterThreads(portId) = stateWriterThread
    stateWriterThread.start()
  }

}
