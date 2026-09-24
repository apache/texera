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

package org.apache.texera.amber.engine.architecture.worker.managers

import org.apache.texera.common.config.ApplicationConfig
import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.storage.model.BufferedItemWriter
import org.apache.texera.amber.core.storage.{DocumentFactory, VFSURIFactory}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  EmbeddedControlMessageIdentity,
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{GlobalPortIdentity, PortIdentity}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.EmbeddedControlMessageType.{
  NO_ALIGNMENT,
  PORT_ALIGNMENT
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.EmbeddedControlMessage
import org.apache.texera.amber.engine.architecture.sendsemantics.partitionings.{
  BroadcastPartitioning,
  HashBasedShufflePartitioning,
  Partitioning
}
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.{
  DPInputQueueElement,
  FIFOMessageElement
}
import org.apache.texera.amber.engine.common.ambermessage.{
  DataFrame,
  StateFrame,
  WorkflowFIFOMessage
}
import org.apache.texera.amber.util.VirtualIdentityUtils.getFromActorIdForInputPortStorage
import org.scalatest.flatspec.AnyFlatSpec

import java.net.URI
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.ArrayBuffer

/**
  * `InputPortMaterializationReaderThread` replays a materialized upstream output into a worker's
  * input queue, standing in for the upstream worker's output manager: it brackets the replay with
  * the same START_CHANNEL / END_CHANNEL embedded control messages a live upstream would send, and
  * it applies the upstream link's partitioning itself, because the materialization holds the whole
  * port's output and only the slice addressed to this worker may be delivered.
  *
  * The two documents it reads are real Iceberg tables written through `DocumentFactory` on the
  * ambient catalog, the same way `SyncExecutionResourceSpec` / `ExecutionResultServiceSpec` set up
  * their result tables — there is no seam to inject storage through, and a fixture that stubbed the
  * documents would not exercise `run` at all. Each test hangs off its own operator id, because an
  * Iceberg table's identity derives from its URI path and sbt runs amber's suites in one unforked
  * JVM.
  *
  * `run()` is invoked directly rather than through `start()`: the production caller
  * (`InputManager.startInputPortReaderThreads`) starts a real thread, but the body is what is under
  * test and running it inline makes the queue contents observable without a join/poll race.
  */
class InputPortMaterializationReaderThreadSpec extends AnyFlatSpec {

  /** The worker whose input queue is being filled — i.e. the slice owner the partitioner is asked about. */
  private val workerId: ActorVirtualIdentity = ActorVirtualIdentity("Worker:WF1-reader-main-0")

  /** A second worker on the same link, so "routed to me" is distinguishable from "routed to anyone". */
  private val siblingWorkerId: ActorVirtualIdentity =
    ActorVirtualIdentity("Worker:WF1-reader-main-1")

  private val schema: Schema =
    Schema().add("id", AttributeType.INTEGER).add("name", AttributeType.STRING)

  private def tuple(id: Int): Tuple =
    Tuple.builder(schema).addSequentially(Array(Int.box(id), s"row-$id")).build()

  /**
    * Workflow / execution ids that no other suite uses, so the tables these tests create cannot
    * collide with another suite's in the shared catalog.
    */
  private val workflowId = WorkflowIdentity(97311L)
  private val executionId = ExecutionIdentity(97312L)

  private def baseURI(operatorId: String): URI =
    VFSURIFactory.createPortBaseURI(
      workflowId,
      executionId,
      GlobalPortIdentity(
        PhysicalOpIdentity(OperatorIdentity(s"ipmrt-$operatorId"), "main"),
        PortIdentity(),
        input = false
      )
    )

  private def write(uri: URI, docSchema: Schema, rows: Seq[Tuple]): Unit = {
    val writer = DocumentFactory
      .createDocument(uri, docSchema)
      .writer(s"ipmrt-${uri.getPath}")
      .asInstanceOf[BufferedItemWriter[Tuple]]
    writer.open()
    rows.foreach(writer.putOne)
    writer.close()
  }

  /**
    * Materializes one port: the state table and the result table the reader opens in that order.
    * Both must exist — `RegionExecutionManager` creates the pair together — so even a
    * states-only or tuples-only case writes both tables.
    *
    * @param states (user state, loop counter, LoopStart id) triples, written as state rows.
    */
  private def materialize(
      operatorId: String,
      tuples: Seq[Tuple] = Seq.empty,
      states: Seq[(State, Long, String)] = Seq.empty
  ): URI = {
    val base = baseURI(operatorId)
    write(
      VFSURIFactory.stateURI(base),
      State.schema,
      states.map { case (state, counter, startId) => state.toTuple(counter, startId) }
    )
    write(VFSURIFactory.resultURI(base), schema, tuples)
    base
  }

  /** Broadcast keeps every tuple, so it isolates the replay from the partitioner filter. */
  private def broadcastTo(receivers: ActorVirtualIdentity*): Partitioning =
    BroadcastPartitioning(
      batchSize = 1,
      channels = receivers.map(ChannelIdentity(workerId, _, isControl = false))
    )

  private def hashShuffleTo(receivers: ActorVirtualIdentity*): Partitioning =
    HashBasedShufflePartitioning(
      batchSize = 1,
      channels = receivers.map(ChannelIdentity(workerId, _, isControl = false)),
      hashAttributeNames = Seq("id")
    )

  /**
    * Runs the reader inline and returns everything it enqueued, in order, together with the reader
    * itself so its completion flag can be read.
    */
  private def replay(
      uri: URI,
      partitioning: Partitioning,
      readerFor: ActorVirtualIdentity = workerId
  ): (InputPortMaterializationReaderThread, Seq[WorkflowFIFOMessage]) = {
    val queue = new LinkedBlockingQueue[DPInputQueueElement]()
    val reader =
      new InputPortMaterializationReaderThread(uri, queue, readerFor, partitioning)
    reader.run()
    (reader, drain(queue))
  }

  private def drain(queue: LinkedBlockingQueue[DPInputQueueElement]): Seq[WorkflowFIFOMessage] = {
    val drained = ArrayBuffer[WorkflowFIFOMessage]()
    var next = queue.poll()
    while (next != null) {
      drained += next.asInstanceOf[FIFOMessageElement].msg
      next = queue.poll()
    }
    drained.toSeq
  }

  private def ecmIds(messages: Seq[WorkflowFIFOMessage]): Seq[EmbeddedControlMessageIdentity] =
    messages.collect { case WorkflowFIFOMessage(_, _, ecm: EmbeddedControlMessage) => ecm.id }

  private def dataFrames(messages: Seq[WorkflowFIFOMessage]): Seq[DataFrame] =
    messages.collect { case WorkflowFIFOMessage(_, _, frame: DataFrame) => frame }

  private def stateFrames(messages: Seq[WorkflowFIFOMessage]): Seq[StateFrame] =
    messages.collect { case WorkflowFIFOMessage(_, _, frame: StateFrame) => frame }

  private def deliveredIds(messages: Seq[WorkflowFIFOMessage]): Seq[Int] =
    dataFrames(messages).flatMap(_.frame.toSeq).map(_.getField[Int]("id"))

  behavior of "InputPortMaterializationReaderThread"

  it should "bracket the replay with START_CHANNEL and END_CHANNEL and mark itself finished" in {
    val uri = materialize("bracket", tuples = Seq(tuple(1)))
    val (reader, messages) = replay(uri, broadcastTo(workerId))

    // The markers are the first and last things on the channel: a downstream input port that saw
    // data before START_CHANNEL, or END_CHANNEL before the last batch, would mis-order the port.
    assert(
      ecmIds(messages) == Seq(
        EmbeddedControlMessageIdentity("StartChannel"),
        EmbeddedControlMessageIdentity("EndChannel")
      )
    )
    assert(messages.head.payload.isInstanceOf[EmbeddedControlMessage])
    assert(messages.last.payload.isInstanceOf[EmbeddedControlMessage])
    // START is unaligned (it only opens the channel) while END must align the port, which is what
    // lets the worker decide the port is complete.
    val Seq(start, end) = messages.collect {
      case WorkflowFIFOMessage(_, _, ecm: EmbeddedControlMessage) => ecm
    }
    assert(start.ecmType == NO_ALIGNMENT)
    assert(end.ecmType == PORT_ALIGNMENT)
    assert(reader.finished)
  }

  it should "not report itself finished before the replay runs" in {
    val queue = new LinkedBlockingQueue[DPInputQueueElement]()
    val reader = new InputPortMaterializationReaderThread(
      baseURI("unstarted"),
      queue,
      workerId,
      broadcastTo(workerId)
    )

    assert(!reader.finished)
    assert(queue.isEmpty)
  }

  it should "send every message on one channel from a URI-derived sender, numbered from zero" in {
    val uri =
      materialize("channel", tuples = Seq(tuple(1)), states = Seq((State(Map("k" -> 1)), 0L, "")))
    val (_, messages) = replay(uri, broadcastTo(workerId))

    // The dummy sender is derived from the URI *and* the receiving worker, so two workers reading
    // the same materialization do not share a channel (and neither collides with a real upstream).
    val expectedChannel = ChannelIdentity(
      getFromActorIdForInputPortStorage(uri.toString, workerId),
      workerId,
      isControl = false
    )
    assert(messages.map(_.channelId).distinct == Seq(expectedChannel))
    // FIFO ordering downstream is by sequence number, so they must be dense and start at 0.
    assert(messages.map(_.sequenceNumber) == messages.indices.map(_.toLong))
  }

  it should "replay states ahead of tuples, carrying each row's loop envelope" in {
    // Distinct counters and ids per state, so a reader that reused one row's envelope for the
    // other, or defaulted to the "no loop" values, would show up.
    val states = Seq(
      (State(Map("phase" -> "a")), 7L, "loop-start-A"),
      (State(Map("phase" -> "b")), 9L, "loop-start-B")
    )
    val uri = materialize("states", tuples = Seq(tuple(1), tuple(2)), states = states)

    val (_, messages) = replay(uri, broadcastTo(workerId))

    // Downstream operators need their state set up before the tuples arrive, so every state
    // precedes every tuple regardless of how the two tables were written.
    val lastState = messages.lastIndexWhere(_.payload.isInstanceOf[StateFrame])
    val firstData = messages.indexWhere(_.payload.isInstanceOf[DataFrame])
    assert(lastState >= 0 && firstData >= 0 && lastState < firstData)
    assert(
      stateFrames(messages).map(frame =>
        (frame.frame.values("phase"), frame.loopCounter, frame.loopStartId)
      ) == Seq(("a", 7L, "loop-start-A"), ("b", 9L, "loop-start-B"))
    )
  }

  it should "emit no state frame when the port materialized no state" in {
    val uri = materialize("nostate", tuples = Seq(tuple(1)))

    val (_, messages) = replay(uri, broadcastTo(workerId))

    assert(stateFrames(messages).isEmpty)
    assert(deliveredIds(messages) == Seq(1))
  }

  it should "deliver every tuple broadcast to it, in materialization order, in one batch" in {
    val ids = 1 to 5
    val uri = materialize("broadcast", tuples = ids.map(tuple))

    val (_, messages) = replay(uri, broadcastTo(workerId, siblingWorkerId))

    // Under the batch size, everything leaves in the single trailing flush.
    assert(dataFrames(messages).size == 1)
    assert(deliveredIds(messages) == ids)
  }

  it should "drop every tuple the partitioning routes to another worker, but still bracket the channel" in {
    val uri = materialize("elsewhere", tuples = (1 to 5).map(tuple))

    // Broadcast to the sibling only: `allReceivers` never contains this worker, so the membership
    // test fails for every tuple.
    val (reader, messages) = replay(uri, broadcastTo(siblingWorkerId))

    assert(deliveredIds(messages).isEmpty)
    assert(dataFrames(messages).isEmpty)
    // An empty slice is still a complete channel — the port would never finish otherwise.
    assert(
      ecmIds(messages) == Seq(
        EmbeddedControlMessageIdentity("StartChannel"),
        EmbeddedControlMessageIdentity("EndChannel")
      )
    )
    assert(reader.finished)
  }

  it should "split the materialization between the workers of a hash-shuffled link" in {
    val ids = 1 to 20
    val uri = materialize("shuffle", tuples = ids.map(tuple))
    val partitioning = hashShuffleTo(workerId, siblingWorkerId)

    val (_, mine) = replay(uri, partitioning)
    val (_, siblings) = replay(uri, partitioning, readerFor = siblingWorkerId)

    // Each worker's reader takes exactly its own buckets: together they cover the materialization
    // once. A filter that ignored the partitioner would duplicate every tuple; one that dropped
    // the `exists` would deliver none.
    assert(deliveredIds(mine).nonEmpty)
    assert(deliveredIds(siblings).nonEmpty)
    assert(deliveredIds(mine).intersect(deliveredIds(siblings)).isEmpty)
    assert((deliveredIds(mine) ++ deliveredIds(siblings)).sorted == ids)
  }

  it should "cut a full batch as soon as the buffer reaches the transfer batch size" in {
    // One tuple past a full batch, so the split is observable and the remainder is not itself a
    // full batch: a reader that only flushed at the end would produce a single oversized frame.
    val batchSize = ApplicationConfig.defaultDataTransferBatchSize
    val ids = 1 to (batchSize + 1)
    val uri = materialize("batching", tuples = ids.map(tuple))

    val (_, messages) = replay(uri, broadcastTo(workerId))

    assert(dataFrames(messages).map(_.frame.length) == Seq(batchSize, 1))
    assert(deliveredIds(messages) == ids)
  }

  it should "wrap a storage failure in a RuntimeException that keeps the cause" in {
    // Nothing was ever materialized at this URI, so opening the state document fails. The reader
    // runs on its own thread with no supervisor, so it re-throws rather than swallowing.
    val uri = baseURI("missing")
    val queue = new LinkedBlockingQueue[DPInputQueueElement]()
    val reader =
      new InputPortMaterializationReaderThread(uri, queue, workerId, broadcastTo(workerId))

    val failure = intercept[RuntimeException] {
      reader.run()
    }

    assert(failure.getMessage.startsWith("Error reading input port materializations: "))
    assert(failure.getCause != null)
    assert(failure.getMessage.endsWith(failure.getCause.getMessage))
    // START_CHANNEL is emitted before the try, so the marker is already on the queue...
    assert(ecmIds(drain(queue)) == Seq(EmbeddedControlMessageIdentity("StartChannel")))
    // ...but the channel never completes, which is what stops the port being called complete.
    assert(!reader.finished)
  }
}
