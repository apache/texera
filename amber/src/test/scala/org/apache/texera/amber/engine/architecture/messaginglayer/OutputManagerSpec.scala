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
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

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
