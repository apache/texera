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

import com.softwaremill.macwire.wire
import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple, TupleLike}
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import org.apache.texera.amber.core.workflow.{PhysicalLink, PortIdentity}
import org.apache.texera.amber.engine.architecture.sendsemantics.partitioners.{
  BroadcastPartitioner,
  HashBasedShufflePartitioner,
  OneToOnePartitioner,
  RangeBasedShufflePartitioner,
  RoundRobinPartitioner
}
import org.apache.texera.amber.engine.architecture.sendsemantics.partitionings.{
  BroadcastPartitioning,
  HashBasedShufflePartitioning,
  OneToOnePartitioning,
  Partitioning,
  RangeBasedShufflePartitioning,
  RoundRobinPartitioning
}
import org.apache.texera.amber.engine.architecture.messaginglayer.OutputManager.{
  getBatchSize,
  toPartitioner
}
import org.apache.texera.amber.engine.common.ambermessage._
import org.apache.texera.amber.util.ArrowUtils
import org.apache.arrow.memory.RootAllocator
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.ArrayBuffer

class OutputManagerSpec extends AnyFlatSpec with MockFactory {
  private val mockHandler =
    mock[WorkflowFIFOMessage => Unit]
  private val identifier = ActorVirtualIdentity("batch producer mock")
  private val mockDataOutputPort = // scalafix:ok; need it for wiring purpose
    new NetworkOutputGateway(identifier, mockHandler)
  var counter: Int = 0
  val schema: Schema = Schema()
    .add("field1", AttributeType.INTEGER)
    .add("field2", AttributeType.INTEGER)
    .add("field3", AttributeType.INTEGER)
    .add("field4", AttributeType.INTEGER)
    .add("field5", AttributeType.STRING)
    .add("field6", AttributeType.DOUBLE)

  def physicalOpId(): PhysicalOpIdentity = {
    counter += 1
    PhysicalOpIdentity(OperatorIdentity("" + counter), "" + counter)
  }

  def mkDataMessage(
      to: ActorVirtualIdentity,
      from: ActorVirtualIdentity,
      seq: Long,
      payload: DataPayload
  ): WorkflowFIFOMessage = {
    WorkflowFIFOMessage(ChannelIdentity(from, to, isControl = false), seq, payload)
  }

  private def sampleTuple(): Tuple =
    TupleLike(1, 2, 3, 4, "5", 9.8).enforceSchema(schema)

  private def channelTo(receiver: ActorVirtualIdentity): ChannelIdentity =
    ChannelIdentity(identifier, receiver, isControl = false)

  "OutputManager" should "aggregate tuples and output" in {
    val outputManager: OutputManager = wire[OutputManager]
    val mockPortId = PortIdentity()
    outputManager.addPort(mockPortId, schema, None)

    val tuples = Array.fill(21)(
      TupleLike(1, 2, 3, 4, "5", 9.8).enforceSchema(schema)
    )
    val fakeID = ActorVirtualIdentity("testReceiver")
    inSequence {
      (mockHandler.apply _).expects(
        mkDataMessage(fakeID, identifier, 0, DataFrame(tuples.slice(0, 10)))
      )
      (mockHandler.apply _).expects(
        mkDataMessage(fakeID, identifier, 1, DataFrame(tuples.slice(10, 20)))
      )
      (mockHandler.apply _).expects(
        mkDataMessage(fakeID, identifier, 2, DataFrame(tuples.slice(20, 21)))
      )
    }
    val fakeLink = PhysicalLink(physicalOpId(), mockPortId, physicalOpId(), mockPortId)
    val fakeReceiver =
      Array[ChannelIdentity](ChannelIdentity(identifier, fakeID, isControl = false))

    outputManager.addPartitionerWithPartitioning(
      fakeLink,
      OneToOnePartitioning(10, fakeReceiver.toSeq)
    )
    tuples.foreach { t =>
      outputManager.passTupleToDownstream(TupleLike(t.getFields).enforceSchema(schema), None)
    }
    outputManager.flush()
  }

  // -- OutputManager.toPartitioner / getBatchSize --------------------------

  "OutputManager.toPartitioner" should "map OneToOnePartitioning to OneToOnePartitioner" in {
    val partitioning = OneToOnePartitioning(11, Seq(channelTo(ActorVirtualIdentity("o2o-rec"))))
    assert(toPartitioner(partitioning, identifier).isInstanceOf[OneToOnePartitioner])
    assert(getBatchSize(partitioning) == 11)
  }

  it should "map RoundRobinPartitioning to RoundRobinPartitioner" in {
    val partitioning = RoundRobinPartitioning(12, Seq(channelTo(ActorVirtualIdentity("rr-rec"))))
    assert(toPartitioner(partitioning, identifier).isInstanceOf[RoundRobinPartitioner])
    assert(getBatchSize(partitioning) == 12)
  }

  it should "map HashBasedShufflePartitioning to HashBasedShufflePartitioner" in {
    val partitioning = HashBasedShufflePartitioning(
      13,
      Seq(channelTo(ActorVirtualIdentity("hash-rec"))),
      Seq("field1")
    )
    assert(toPartitioner(partitioning, identifier).isInstanceOf[HashBasedShufflePartitioner])
    assert(getBatchSize(partitioning) == 13)
  }

  it should "map RangeBasedShufflePartitioning to RangeBasedShufflePartitioner" in {
    val partitioning = RangeBasedShufflePartitioning(
      14,
      Seq(channelTo(ActorVirtualIdentity("range-rec"))),
      Seq("field1"),
      0,
      100
    )
    assert(toPartitioner(partitioning, identifier).isInstanceOf[RangeBasedShufflePartitioner])
    assert(getBatchSize(partitioning) == 14)
  }

  it should "map BroadcastPartitioning to BroadcastPartitioner" in {
    val partitioning = BroadcastPartitioning(15, Seq(channelTo(ActorVirtualIdentity("bc-rec"))))
    assert(toPartitioner(partitioning, identifier).isInstanceOf[BroadcastPartitioner])
    assert(getBatchSize(partitioning) == 15)
  }

  it should "throw for an unsupported partitioning" in {
    // Partitioning.Empty is a valid Partitioning that is none of the five
    // concrete cases, so it exercises the `_ =>` default of both methods.
    val unsupported: Partitioning = Partitioning.Empty
    assertThrows[RuntimeException](toPartitioner(unsupported, identifier))
    assertThrows[RuntimeException](getBatchSize(unsupported))
  }

  // -- passTupleToDownstream port routing ----------------------------------

  "passTupleToDownstream" should "route only to the partitioner whose fromPortId matches" in {
    val outputManager: OutputManager = wire[OutputManager]
    val portA = PortIdentity(1)
    val portB = PortIdentity(2)
    outputManager.addPort(portA, schema, None)
    outputManager.addPort(portB, schema, None)

    val recA = ActorVirtualIdentity("route-recA")
    val recB = ActorVirtualIdentity("route-recB")
    val linkA = PhysicalLink(physicalOpId(), portA, physicalOpId(), portA)
    val linkB = PhysicalLink(physicalOpId(), portB, physicalOpId(), portB)

    outputManager.addPartitionerWithPartitioning(
      linkA,
      OneToOnePartitioning(10, Seq(channelTo(recA)))
    )
    outputManager.addPartitionerWithPartitioning(
      linkB,
      OneToOnePartitioning(10, Seq(channelTo(recB)))
    )

    val tuple = sampleTuple()
    // Only recA's buffer should receive the tuple; recB gets nothing.
    (mockHandler.apply _).expects(
      mkDataMessage(recA, identifier, 0, DataFrame(Array(tuple)))
    )
    outputManager.passTupleToDownstream(tuple, Some(portA))
    outputManager.flush()
  }

  it should "broadcast to all partitioners when no port is specified" in {
    val outputManager: OutputManager = wire[OutputManager]
    val portA = PortIdentity(3)
    val portB = PortIdentity(4)
    outputManager.addPort(portA, schema, None)
    outputManager.addPort(portB, schema, None)

    val recA = ActorVirtualIdentity("bcast-recA")
    val recB = ActorVirtualIdentity("bcast-recB")
    val linkA = PhysicalLink(physicalOpId(), portA, physicalOpId(), portA)
    val linkB = PhysicalLink(physicalOpId(), portB, physicalOpId(), portB)

    outputManager.addPartitionerWithPartitioning(
      linkA,
      OneToOnePartitioning(10, Seq(channelTo(recA)))
    )
    outputManager.addPartitionerWithPartitioning(
      linkB,
      OneToOnePartitioning(10, Seq(channelTo(recB)))
    )

    val tuple = sampleTuple()
    inAnyOrder {
      (mockHandler.apply _).expects(mkDataMessage(recA, identifier, 0, DataFrame(Array(tuple))))
      (mockHandler.apply _).expects(mkDataMessage(recB, identifier, 0, DataFrame(Array(tuple))))
    }
    outputManager.passTupleToDownstream(tuple, None)
    outputManager.flush()
  }

  // -- flush(onlyFor) ------------------------------------------------------

  "flush" should "flush only the buffers matching the given channel ids" in {
    val outputManager: OutputManager = wire[OutputManager]
    val portA = PortIdentity(5)
    val portB = PortIdentity(6)
    outputManager.addPort(portA, schema, None)
    outputManager.addPort(portB, schema, None)

    val recA = ActorVirtualIdentity("flush-recA")
    val recB = ActorVirtualIdentity("flush-recB")
    val linkA = PhysicalLink(physicalOpId(), portA, physicalOpId(), portA)
    val linkB = PhysicalLink(physicalOpId(), portB, physicalOpId(), portB)

    // batchSize large enough that tuples stay buffered until flush.
    outputManager.addPartitionerWithPartitioning(
      linkA,
      OneToOnePartitioning(100, Seq(channelTo(recA)))
    )
    outputManager.addPartitionerWithPartitioning(
      linkB,
      OneToOnePartitioning(100, Seq(channelTo(recB)))
    )

    val tuple = sampleTuple()
    outputManager.passTupleToDownstream(tuple, None) // buffers both A and B

    // Only recA's buffer should be flushed.
    (mockHandler.apply _).expects(mkDataMessage(recA, identifier, 0, DataFrame(Array(tuple))))
    outputManager.flush(Some(Set(channelTo(recA))))

    // The remaining recB buffer is flushed here.
    (mockHandler.apply _).expects(mkDataMessage(recB, identifier, 0, DataFrame(Array(tuple))))
    outputManager.flush()
  }

  // -- emitState -----------------------------------------------------------

  "emitState" should "fan the state out to every network buffer" in {
    val outputManager: OutputManager = wire[OutputManager]
    val portA = PortIdentity(7)
    val portB = PortIdentity(8)
    outputManager.addPort(portA, schema, None)
    outputManager.addPort(portB, schema, None)

    val recA = ActorVirtualIdentity("state-recA")
    val recB = ActorVirtualIdentity("state-recB")
    val linkA = PhysicalLink(physicalOpId(), portA, physicalOpId(), portA)
    val linkB = PhysicalLink(physicalOpId(), portB, physicalOpId(), portB)

    outputManager.addPartitionerWithPartitioning(
      linkA,
      OneToOnePartitioning(10, Seq(channelTo(recA)))
    )
    outputManager.addPartitionerWithPartitioning(
      linkB,
      OneToOnePartitioning(10, Seq(channelTo(recB)))
    )

    val state = State(Map("k" -> 1))
    // Each buffer's sendState flushes (no-op, empty), sends a StateFrame, then
    // flushes again (no-op). With no writer threads configured,
    // saveStateToStorageIfNeeded is a no-op over the empty writer map.
    inAnyOrder {
      (mockHandler.apply _).expects(
        WorkflowFIFOMessage(channelTo(recA), 0, StateFrame(state))
      )
      (mockHandler.apply _).expects(
        WorkflowFIFOMessage(channelTo(recB), 0, StateFrame(state))
      )
    }
    outputManager.emitState(state)
  }

  it should "carry the loop envelope onto every emitted StateFrame" in {
    // The loop envelope (loop_counter / loop_start_id) is owned by the Python
    // runtime; a JVM hop only forwards it. emitState must stamp the incoming
    // envelope onto the outgoing frames (and the storage write -- covered by
    // State.toTuple in StateSpec), not reset it to the no-loop defaults.
    val outputManager: OutputManager = wire[OutputManager]
    val portA = PortIdentity(7)
    outputManager.addPort(portA, schema, None)
    // Fresh receiver: the spec-level gateway tracks sequence numbers per
    // channel, so reusing another test's receiver would start at seq 1.
    val rec = ActorVirtualIdentity("state-recEnvelope")
    outputManager.addPartitionerWithPartitioning(
      PhysicalLink(physicalOpId(), portA, physicalOpId(), portA),
      OneToOnePartitioning(10, Seq(channelTo(rec)))
    )

    val state = State(Map("k" -> 1))
    (mockHandler.apply _).expects(
      WorkflowFIFOMessage(channelTo(rec), 0, StateFrame(state, 2L, "outer-loop"))
    )
    outputManager.emitState(state, loopCounter = 2L, loopStartId = "outer-loop")
  }

  // -- addPort / storage no-ops / getSingleOutputPortIdentity --------------

  "addPort" should "be idempotent for a duplicate port id" in {
    val outputManager: OutputManager = wire[OutputManager]
    val portId = PortIdentity(9)
    outputManager.addPort(portId, schema, None)
    val firstPort = outputManager.getPort(portId)
    // A second addPort with the same id must early-return without replacing.
    outputManager.addPort(portId, Schema().add("other", AttributeType.STRING), None)
    assert(outputManager.getPort(portId) eq firstPort)
    assert(outputManager.getPort(portId).schema == schema)
  }

  it should "create no storage writer when the storage URI base is None" in {
    val outputManager: OutputManager = wire[OutputManager]
    val portId = PortIdentity(10)
    // None storage base means no writer thread is set up; the tuple- and
    // state-storage paths below must therefore be no-ops.
    outputManager.addPort(portId, schema, None)
    val tuple = sampleTuple()
    // No exception, no handler interaction: empty writer maps.
    outputManager.saveTupleToStorageIfNeeded(tuple, Some(portId))
    outputManager.saveTupleToStorageIfNeeded(tuple, None)
    outputManager.emitState(State(Map("k" -> 2)))
  }

  "saveTupleToStorageIfNeeded" should "be a no-op over an empty writer map" in {
    val outputManager: OutputManager = wire[OutputManager]
    val portId = PortIdentity(11)
    outputManager.addPort(portId, schema, None)
    val tuple = sampleTuple()
    // Some(portId) with no writer -> Map.empty branch; None -> whole (empty) map.
    outputManager.saveTupleToStorageIfNeeded(tuple, Some(portId))
    outputManager.saveTupleToStorageIfNeeded(tuple, Some(PortIdentity(999)))
    outputManager.saveTupleToStorageIfNeeded(tuple, None)
  }

  "closeOutputStorageWriterIfNeeded" should "do nothing when the port has no writer" in {
    val outputManager: OutputManager = wire[OutputManager]
    val portId = PortIdentity(12)
    outputManager.addPort(portId, schema, None)
    // No writer thread was created, so both the result-writer and state-writer
    // branches are the None/absent case: no exception, no join.
    outputManager.closeOutputStorageWriterIfNeeded(portId)
    outputManager.closeOutputStorageWriterIfNeeded(PortIdentity(998))
  }

  // -- tryEmitColumnarBatch (phase 1: no-round-trip columnar output) --------

  private def capturingOutputManager(): (OutputManager, ArrayBuffer[WorkflowFIFOMessage]) = {
    val sent = ArrayBuffer[WorkflowFIFOMessage]()
    val om =
      new OutputManager(identifier, new NetworkOutputGateway(identifier, m => { sent += m; () }))
    (om, sent)
  }

  private def emitThrough(
      om: OutputManager,
      rows: Array[Tuple]
  ): Option[(PortIdentity, Long)] = {
    val alloc = new RootAllocator()
    try {
      ArrowUtils.deserializeRootFold(ArrowUtils.serializeTuples(schema, rows), alloc)(root =>
        om.tryEmitColumnarBatch(root, None)
      )
    } finally alloc.close()
  }

  private def rowWith(v: Int): Tuple = TupleLike(v, 2, 3, 4, "5", 9.8).enforceSchema(schema)

  // Emit a batch columnar through a fresh OutputManager carrying `partitioning`, then
  // return, per receiver, the rows that receiver actually got (decoded from the Arrow
  // frames put on the wire).
  private def columnarShuffle(
      portId: PortIdentity,
      partitioning: Partitioning,
      rows: Array[Tuple]
  ): Map[ActorVirtualIdentity, List[Tuple]] = {
    val (om, sent) = capturingOutputManager()
    om.addPort(portId, schema, None)
    om.addPartitionerWithPartitioning(
      PhysicalLink(physicalOpId(), portId, physicalOpId(), portId),
      partitioning,
      columnar = true
    )
    emitThrough(om, rows)
    sent
      .groupBy(_.channelId.toWorkerId)
      .map {
        case (receiver, msgs) =>
          receiver -> msgs
            .flatMap(m =>
              ArrowUtils.deserializeTuples(m.payload.asInstanceOf[VectorFrame].arrowIpcBytes)
            )
            .toList
      }
  }

  // Per receiver, the rows the row-path partitioner would assign, in order.
  private def rowPathBuckets(
      partitioner: org.apache.texera.amber.engine.architecture.sendsemantics.partitioners.Partitioner,
      receivers: Seq[ActorVirtualIdentity],
      rows: Array[Tuple]
  ): Map[ActorVirtualIdentity, List[Tuple]] =
    rows.toList
      .flatMap(t => partitioner.getBucketIndex(t).map(b => receivers(b) -> t))
      .groupBy(_._1)
      .map { case (r, pairs) => r -> pairs.map(_._2) }

  "tryEmitColumnarBatch" should "ship a one-to-one columnar edge as a single VectorFrame" in {
    val (om, sent) = capturingOutputManager()
    val portId = PortIdentity(20)
    om.addPort(portId, schema, None)
    om.addPartitionerWithPartitioning(
      PhysicalLink(physicalOpId(), portId, physicalOpId(), portId),
      OneToOnePartitioning(1000, Seq(channelTo(ActorVirtualIdentity("o2o-col-rec"))))
    )
    om.setColumnarOutput(true)

    val rows = Array(sampleTuple(), sampleTuple(), sampleTuple())
    val result = emitThrough(om, rows)

    assert(result.exists(_._1 == portId))
    assert(sent.size == 1)
    val cf = sent.head.payload.asInstanceOf[VectorFrame]
    assert(cf.rowCount == 3)
    assert(ArrowUtils.deserializeTuples(cf.arrowIpcBytes).toList == rows.toList)
  }

  it should "hash-shuffle rows to the same buckets the row partitioner would" in {
    val receivers = (0 until 3).map(i => ActorVirtualIdentity(s"hash-r$i"))
    val partitioning =
      HashBasedShufflePartitioning(1000, receivers.map(channelTo), Seq("field1"))
    val rows = (0 until 50).map(rowWith).toArray
    val got = columnarShuffle(PortIdentity(30), partitioning, rows)
    val expected = rowPathBuckets(HashBasedShufflePartitioner(partitioning), receivers, rows)
    // No row lost or duplicated, and each receiver gets exactly the row-path bucket.
    assert(got.values.map(_.size).sum == rows.length)
    receivers.foreach(r => assert(got.getOrElse(r, Nil) == expected.getOrElse(r, Nil)))
  }

  it should "range-shuffle rows to the same buckets the row partitioner would" in {
    val receivers = (0 until 3).map(i => ActorVirtualIdentity(s"range-r$i"))
    val partitioning =
      RangeBasedShufflePartitioning(1000, receivers.map(channelTo), Seq("field1"), 0, 49)
    val rows = (0 until 50).map(rowWith).toArray
    val got = columnarShuffle(PortIdentity(31), partitioning, rows)
    val expected = rowPathBuckets(RangeBasedShufflePartitioner(partitioning), receivers, rows)
    assert(got.values.map(_.size).sum == rows.length)
    receivers.foreach(r => assert(got.getOrElse(r, Nil) == expected.getOrElse(r, Nil)))
  }

  it should "round-robin rows to the same buckets the row partitioner would (counter parity)" in {
    val receivers = (0 until 3).map(i => ActorVirtualIdentity(s"rr-r$i"))
    val partitioning = RoundRobinPartitioning(1000, receivers.map(channelTo))
    val rows = (0 until 10).map(rowWith).toArray
    val got = columnarShuffle(PortIdentity(32), partitioning, rows)
    // A fresh row-path partitioner advances the same counter over the same row order.
    val expected = rowPathBuckets(RoundRobinPartitioner(partitioning), receivers, rows)
    assert(got.values.map(_.size).sum == rows.length)
    receivers.foreach(r => assert(got.getOrElse(r, Nil) == expected.getOrElse(r, Nil)))
  }

  it should "broadcast the whole batch to every receiver" in {
    val receivers = (0 until 3).map(i => ActorVirtualIdentity(s"bc-r$i"))
    val partitioning = BroadcastPartitioning(1000, receivers.map(channelTo))
    val rows = (0 until 5).map(rowWith).toArray
    val got = columnarShuffle(PortIdentity(33), partitioning, rows)
    receivers.foreach(r => assert(got.getOrElse(r, Nil) == rows.toList))
  }

  // -- local columnar: a local columnar-capable edge hands off a live Arrow root ----

  "local columnar" should "hand a local one-to-one columnar edge a live root, not rows" in {
    val (om, sent) = capturingOutputManager()
    val rec = ActorVirtualIdentity("local-o2o")
    val portId = PortIdentity(40)
    om.addPort(portId, schema, None)
    om.addPartitionerWithPartitioning(
      PhysicalLink(physicalOpId(), portId, physicalOpId(), portId),
      OneToOnePartitioning(1, Seq(channelTo(rec))),
      columnar = true
    )
    om.isLocalReceiver = id => if (id == rec) Some(true) else None
    om.passTupleToDownstream(sampleTuple(), Some(portId))
    // columnar is on and the receiver is local, so the batch is handed off as a live
    // Arrow root (zero-copy), letting the co-located receiver process columns.
    assert(sent.size == 1)
    assert(sent.head.payload.isInstanceOf[LocalColumnarBatch])
  }

  it should "take the no-decode path for both local and remote, as a live root vs Arrow bytes" in {
    def payloadsFor(local: Boolean): (Boolean, Seq[Any]) = {
      val (om, sent) = capturingOutputManager()
      val rec = ActorVirtualIdentity(s"fast-$local")
      val portId = PortIdentity(if (local) 41 else 42)
      om.addPort(portId, schema, None)
      om.addPartitionerWithPartitioning(
        PhysicalLink(physicalOpId(), portId, physicalOpId(), portId),
        OneToOnePartitioning(1000, Seq(channelTo(rec))),
        columnar = true
      )
      om.isLocalReceiver = _ => Some(local)
      (emitThrough(om, Array(sampleTuple())).isDefined, sent.map(_.payload).toSeq)
    }
    val (localTaken, localPayloads) = payloadsFor(local = true)
    val (remoteTaken, remotePayloads) = payloadsFor(local = false)
    assert(localTaken && remoteTaken) // no-decode path taken for both, neither decodes to rows
    assert(localPayloads.forall(_.isInstanceOf[LocalColumnarBatch])) // local -> live root
    assert(remotePayloads.forall(_.isInstanceOf[VectorFrame])) // remote -> Arrow bytes
    localPayloads.foreach(_.asInstanceOf[LocalColumnarBatch].root.close())
  }

  it should "in the row path, send a local root to a local receiver and Arrow to a remote one on a mixed shuffle" in {
    val local = ActorVirtualIdentity("mix-local")
    val remote = ActorVirtualIdentity("mix-remote")
    val (om, sent) = capturingOutputManager()
    val portId = PortIdentity(43)
    om.addPort(portId, schema, None)
    om.addPartitionerWithPartitioning(
      PhysicalLink(physicalOpId(), portId, physicalOpId(), portId),
      // batchSize 1 so each tuple flushes to its bucket immediately.
      HashBasedShufflePartitioning(1, Seq(channelTo(local), channelTo(remote)), Seq("field1")),
      columnar = true
    )
    om.isLocalReceiver = id => Some(id == local)
    (0 until 40).foreach(v => om.passTupleToDownstream(rowWith(v), Some(portId)))
    om.flush()
    val byReceiver = sent.groupBy(_.channelId.toWorkerId)
    // Both buckets got data: the local one as a live Arrow root (zero-copy), the remote
    // one as VectorFrames.
    assert(byReceiver.get(local).exists(_.nonEmpty))
    assert(byReceiver.get(remote).exists(_.nonEmpty))
    assert(byReceiver(local).forall(_.payload.isInstanceOf[LocalColumnarBatch]))
    assert(byReceiver(remote).forall(_.payload.isInstanceOf[VectorFrame]))
  }

  it should "in the fast path, ship Arrow to remote buckets and a live root to local buckets on a mixed shuffle" in {
    val local = ActorVirtualIdentity("c-local")
    val remote = ActorVirtualIdentity("c-remote")
    val (om, sent) = capturingOutputManager()
    val portId = PortIdentity(44)
    om.addPort(portId, schema, None)
    val partitioning =
      HashBasedShufflePartitioning(1000, Seq(channelTo(local), channelTo(remote)), Seq("field1"))
    om.addPartitionerWithPartitioning(
      PhysicalLink(physicalOpId(), portId, physicalOpId(), portId),
      partitioning,
      columnar = true
    )
    om.isLocalReceiver = id => Some(id == local)

    val rows = (0 until 40).map(rowWith).toArray
    assert(emitThrough(om, rows).isDefined) // the fast path runs for every columnar receiver

    val byReceiver = sent.groupBy(_.channelId.toWorkerId)
    // The local bucket arrives as a live Arrow root (zero-copy), the remote bucket as Arrow bytes.
    assert(byReceiver(local).forall(_.payload.isInstanceOf[LocalColumnarBatch]))
    assert(byReceiver(remote).forall(_.payload.isInstanceOf[VectorFrame]))
    // Both buckets hold exactly the rows the row partitioner assigns, nothing lost.
    val got: Map[ActorVirtualIdentity, List[Tuple]] = byReceiver.map {
      case (r, msgs) =>
        r -> msgs.flatMap {
          case m if m.payload.isInstanceOf[VectorFrame] =>
            ArrowUtils.deserializeTuples(m.payload.asInstanceOf[VectorFrame].arrowIpcBytes).toList
          case m =>
            val b = m.payload.asInstanceOf[LocalColumnarBatch]
            try ArrowUtils.tuplesOfRoot(b.root).toList
            finally b.root.close()
        }.toList
    }
    val expected =
      rowPathBuckets(HashBasedShufflePartitioner(partitioning), Seq(local, remote), rows)
    assert(got.values.map(_.size).sum == rows.length)
    assert(got.getOrElse(local, Nil) == expected.getOrElse(local, Nil))
    assert(got.getOrElse(remote, Nil) == expected.getOrElse(remote, Nil))
  }

  it should "decline when columnar output is off" in {
    val (om, sent) = capturingOutputManager()
    val portId = PortIdentity(22)
    om.addPort(portId, schema, None)
    om.addPartitionerWithPartitioning(
      PhysicalLink(physicalOpId(), portId, physicalOpId(), portId),
      OneToOnePartitioning(1000, Seq(channelTo(ActorVirtualIdentity("off-rec"))))
    )
    // Columnar output left off.
    assert(emitThrough(om, Array(sampleTuple())).isEmpty)
    assert(sent.isEmpty)
  }

  "addPartitionerWithPartitioning" should "make the edge's buffers columnar when told to" in {
    val (om, sent) = capturingOutputManager()
    val portId = PortIdentity(23)
    om.addPort(portId, schema, None)
    om.addPartitionerWithPartitioning(
      PhysicalLink(physicalOpId(), portId, physicalOpId(), portId),
      // batchSize 1 so the single tuple auto-flushes.
      OneToOnePartitioning(1, Seq(channelTo(ActorVirtualIdentity("col-edge-rec")))),
      columnar = true
    )
    om.passTupleToDownstream(sampleTuple(), Some(portId))
    assert(sent.size == 1)
    assert(sent.head.payload.isInstanceOf[VectorFrame])
  }

  it should "keep the edge's buffers on rows by default" in {
    val (om, sent) = capturingOutputManager()
    val portId = PortIdentity(24)
    om.addPort(portId, schema, None)
    om.addPartitionerWithPartitioning(
      PhysicalLink(physicalOpId(), portId, physicalOpId(), portId),
      OneToOnePartitioning(1, Seq(channelTo(ActorVirtualIdentity("row-edge-rec"))))
    )
    om.passTupleToDownstream(sampleTuple(), Some(portId))
    assert(sent.size == 1)
    assert(sent.head.payload.isInstanceOf[DataFrame])
  }

  // -- tryEmitLocalColumnarRoot (phase 5b: local zero-copy, live root by reference) -----

  // Build an owned root the test manages (deserializeRootFold would close it), run the
  // live-emit, and clean up. Returns the result and the captured messages.
  private def liveEmit(
      om: OutputManager,
      sent: ArrayBuffer[WorkflowFIFOMessage],
      rows: Array[Tuple]
  )(check: (Option[(PortIdentity, Long)], ArrayBuffer[WorkflowFIFOMessage]) => Unit): Unit = {
    val alloc = new RootAllocator()
    val root =
      ArrowUtils.deserializeRootOwned(ArrowUtils.serializeTuples(schema, rows), alloc)
    try check(om.tryEmitLocalColumnarRoot(root, None), sent)
    finally { root.close(); alloc.close() }
  }

  private def oneToOneCapable(
      om: OutputManager,
      portId: PortIdentity,
      rec: ActorVirtualIdentity
  ): Unit = {
    om.addPort(portId, schema, None)
    om.addPartitionerWithPartitioning(
      PhysicalLink(physicalOpId(), portId, physicalOpId(), portId),
      OneToOnePartitioning(1000, Seq(channelTo(rec))),
      columnar = true
    )
  }

  "tryEmitLocalColumnarRoot" should "hand a live root to a local one-to-one columnar receiver" in {
    val (om, sent) = capturingOutputManager()
    val rec = ActorVirtualIdentity("live-rec")
    oneToOneCapable(om, PortIdentity(50), rec)
    om.isLocalReceiver = _ => Some(true)
    liveEmit(om, sent, (0 until 10).map(rowWith).toArray) { (res, msgs) =>
      assert(res.exists(_._1 == PortIdentity(50)))
      assert(msgs.size == 1)
      val batch = msgs.head.payload.asInstanceOf[LocalColumnarBatch]
      assert(batch.rowCount == 10)
      assert(ArrowUtils.tuplesOfRoot(batch.root).length == 10)
    }
  }

  it should "decline a remote receiver (that hop serializes, so it is not zero-copy)" in {
    val (om, sent) = capturingOutputManager()
    oneToOneCapable(om, PortIdentity(51), ActorVirtualIdentity("remote-rec"))
    om.isLocalReceiver = _ => Some(false)
    liveEmit(om, sent, Array(sampleTuple())) { (res, msgs) =>
      assert(res.isEmpty)
      assert(msgs.isEmpty)
    }
  }

  it should "decline when the receiver is not columnar-capable" in {
    val (om, sent) = capturingOutputManager()
    val rec = ActorVirtualIdentity("local-rowonly")
    om.addPort(PortIdentity(52), schema, None)
    om.addPartitionerWithPartitioning(
      PhysicalLink(physicalOpId(), PortIdentity(52), physicalOpId(), PortIdentity(52)),
      OneToOnePartitioning(1000, Seq(channelTo(rec)))
      // columnar defaults to false: receiver cannot consume columns
    )
    om.isLocalReceiver = _ => Some(true)
    liveEmit(om, sent, Array(sampleTuple())) { (res, msgs) =>
      assert(res.isEmpty)
      assert(msgs.isEmpty)
    }
  }

  it should "decline a splitting shuffle (more than one consumer)" in {
    val (om, sent) = capturingOutputManager()
    om.addPort(PortIdentity(53), schema, None)
    om.addPartitionerWithPartitioning(
      PhysicalLink(physicalOpId(), PortIdentity(53), physicalOpId(), PortIdentity(53)),
      HashBasedShufflePartitioning(
        1000,
        Seq(channelTo(ActorVirtualIdentity("a")), channelTo(ActorVirtualIdentity("b"))),
        Seq("field1")
      ),
      columnar = true
    )
    om.isLocalReceiver = _ => Some(true)
    liveEmit(om, sent, Array(sampleTuple())) { (res, msgs) =>
      assert(res.isEmpty)
      assert(msgs.isEmpty)
    }
  }

  "getSingleOutputPortIdentity" should "return the sole port when exactly one is present" in {
    val outputManager: OutputManager = wire[OutputManager]
    val portId = PortIdentity(13)
    outputManager.addPort(portId, schema, None)
    assert(outputManager.getSingleOutputPortIdentity == portId)
  }

  it should "assert when there is more than one output port" in {
    val outputManager: OutputManager = wire[OutputManager]
    outputManager.addPort(PortIdentity(14), schema, None)
    outputManager.addPort(PortIdentity(15), schema, None)
    assertThrows[AssertionError](outputManager.getSingleOutputPortIdentity)
  }

}
