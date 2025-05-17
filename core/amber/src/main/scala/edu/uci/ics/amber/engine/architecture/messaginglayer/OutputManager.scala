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

package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.core.marker.Marker
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.storage.model.BufferedItemWriter
import edu.uci.ics.amber.core.tuple._
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.core.workflow.{PhysicalLink, PortIdentity}
import edu.uci.ics.amber.engine.architecture.messaginglayer.OutputManager.{
  DPOutputIterator,
  getBatchSize,
  toPartitioner
}
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners._
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings._
import edu.uci.ics.amber.engine.architecture.worker.managers.{
  OutputPortResultWriterThread,
  PortStorageWriterTerminateSignal
}
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.util.VirtualIdentityUtils

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
  private val partitioners: mutable.Map[PhysicalLink, Partitioner] =
    mutable.HashMap[PhysicalLink, Partitioner]()

  private val ports: mutable.HashMap[PortIdentity, WorkerPort] = mutable.HashMap()

  private val networkOutputBuffers =
    mutable.HashMap[(PhysicalLink, ActorVirtualIdentity), NetworkOutputBuffer]()

  private val outputPortResultWriterThreads
      : mutable.HashMap[PortIdentity, OutputPortResultWriterThread] =
    mutable.HashMap()

  /**
    * Add down stream operator and its corresponding Partitioner.
    *
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

  def emitMarker(marker: Marker): Unit = {
    networkOutputBuffers.foreach(kv => kv._2.sendMarker(marker))
  }

  def addPort(portId: PortIdentity, schema: Schema, storageURIOption: Option[URI]): Unit = {
    // each port can only be added and initialized once.
    if (this.ports.contains(portId)) {
      return
    }
    this.ports(portId) = WorkerPort(schema)

    // if a storage URI is provided, set up a storage writer thread
    storageURIOption match {
      case Some(storageUri) => setupOutputStorageWriterThread(portId, storageUri)
      case None             => // No need to add a writer
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

  /**
    * Singal the port storage writer to flush the remaining buffer and wait for commits to finish so that
    * the output port is properly completed. If the output port does not need storage, no action will be done.
    */
  def closeOutputStorageWriterIfNeeded(outputPortId: PortIdentity): Unit = {
    this.outputPortResultWriterThreads.get(outputPortId) match {
      case Some(writerThread) =>
        // Non-blocking call
        writerThread.queue.put(Right(PortStorageWriterTerminateSignal))
        // Blocking call
        this.outputPortResultWriterThreads.values.foreach(writerThread => writerThread.join())
      case None =>
    }

  }

  def getPort(portId: PortIdentity): WorkerPort = ports(portId)

  def hasUnfinishedOutput: Boolean = outputIterator.hasNext

  def finalizeOutput(): Unit = {
    if (this.ports.isEmpty) {
      // An operator with an input-port dependency relationship currently belongs to two regions R1->R2.
      // In a previous design (before #3312), the depender port and the output port of this operator
      // also belongs to R1, so the completion of the dependee port in R1 does not trigger finalizeOutput on the worker.
      // After #3312, R1 contains ONLY the dependee input port and no output ports, so the completion of the dependee
      // input port will trigger finalizeOutput and indicate R1 is completed, causing the workers of this operator to
      // be closed prematurely.
      // This additional check ensures the workers of such an operator is not finalized in R1 as it needs to remain open
      // when R2 is scheduled to execute: when a worker does not have any output ports (this will ONLY be true
      // for the workers of this operator in R1 as we no longer have sinks operators), this worker needs to remain open.
      // When the workers of this operator is executed again in R2, the output port will be assigned, and this check
      // will pass.
      // TODO: Remove after implementation of a cleaner design of enforcing input port dependencies that does not allow
      // a worker to belong to two regions.
      return
    }
    this.ports.keys
      .foreach(outputPortId =>
        outputIterator.appendSpecialTupleToEnd(FinalizePort(outputPortId, input = false))
      )
    outputIterator.appendSpecialTupleToEnd(FinalizeExecutor())
  }

  def getSingleOutputPortIdentity: PortIdentity = {
    assert(ports.size == 1, "expect 1 output port, got " + ports.size)
    ports.head._1
  }

  private def setupOutputStorageWriterThread(portId: PortIdentity, storageUri: URI): Unit = {
    val bufferedItemWriter = DocumentFactory
      .openDocument(storageUri)
      ._1
      .writer(VirtualIdentityUtils.getWorkerIndex(actorId).toString)
      .asInstanceOf[BufferedItemWriter[Tuple]]
    val writerThread = new OutputPortResultWriterThread(bufferedItemWriter)
    this.outputPortResultWriterThreads(portId) = writerThread
    writerThread.start()
  }

}
