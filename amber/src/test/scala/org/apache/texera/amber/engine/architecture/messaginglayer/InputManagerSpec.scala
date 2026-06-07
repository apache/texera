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

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.architecture.sendsemantics.partitionings.Partitioning
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.DPInputQueueElement
import org.scalatest.flatspec.AnyFlatSpec

import java.net.URI
import java.util.concurrent.LinkedBlockingQueue

class InputManagerSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------

  private val actorId: ActorVirtualIdentity = ActorVirtualIdentity("worker-1")

  private def emptyQueue(): LinkedBlockingQueue[DPInputQueueElement] =
    new LinkedBlockingQueue[DPInputQueueElement]()

  private def freshManager: InputManager = new InputManager(actorId, emptyQueue())

  // A one-field schema is enough to build real Tuple instances for the batch tests.
  private val intAttr = new Attribute("v", AttributeType.INTEGER)
  private val schema: Schema = Schema().add(intAttr)
  private def tuple(value: Int): Tuple =
    Tuple.builder(schema).add(intAttr, Integer.valueOf(value)).build()

  private def channelId(from: String, to: String): ChannelIdentity =
    ChannelIdentity(ActorVirtualIdentity(from), ActorVirtualIdentity(to), isControl = false)

  // ---------------------------------------------------------------------------
  // getAllPorts / addPort / getPort
  // ---------------------------------------------------------------------------

  "InputManager.getAllPorts (fresh)" should "be empty" in {
    val mgr = freshManager
    assert(mgr.getAllPorts.isEmpty)
  }

  "InputManager.addPort" should
    "register a fresh WorkerPort under the supplied portId (no reader threads when URI list is empty)" in {
    val mgr = freshManager
    val portId = PortIdentity(0)
    mgr.addPort(portId, schema, urisToRead = List.empty, partitionings = List.empty)
    assert(mgr.getAllPorts == Set(portId))
    val port = mgr.getPort(portId)
    assert(port.schema == schema)
    assert(!port.completed)
    assert(mgr.getInputPortReaderThreads.isEmpty)
  }

  it should "be a no-op when called a second time for the same portId (first call wins)" in {
    // Pin the "each port can only be added and initialized once" comment in
    // the production code: the second addPort returns early WITHOUT
    // overwriting the existing WorkerPort. We prove this by mutating the
    // first port's `completed` flag, then re-adding, then checking the
    // mutation survives — if the second call had replaced the WorkerPort,
    // `completed` would revert to its case-class default `false`.
    val mgr = freshManager
    val portId = PortIdentity(0)
    mgr.addPort(portId, schema, urisToRead = List.empty, partitionings = List.empty)
    val firstPort = mgr.getPort(portId)
    firstPort.completed = true
    mgr.addPort(portId, schema, urisToRead = List.empty, partitionings = List.empty)
    assert(mgr.getPort(portId) eq firstPort, "second addPort must not replace the WorkerPort")
    assert(mgr.getPort(portId).completed, "second addPort must not reset the existing port's state")
  }

  it should
    "throw AssertionError when urisToRead.size does not match partitionings.size" in {
    val mgr = freshManager
    val portId = PortIdentity(0)
    // 1 URI but 0 partitionings — sizes diverge.
    assertThrows[AssertionError] {
      mgr.addPort(
        portId,
        schema,
        urisToRead = List(new URI("file:///nowhere")),
        partitionings = List.empty[Partitioning]
      )
    }
  }

  "InputManager.getPort" should "return the WorkerPort previously registered by addPort" in {
    val mgr = freshManager
    val portId = PortIdentity(7)
    mgr.addPort(portId, schema, urisToRead = List.empty, partitionings = List.empty)
    val a = mgr.getPort(portId)
    val b = mgr.getPort(portId)
    assert(a eq b, "getPort must be a pure lookup — no fresh WorkerPort each call")
  }

  "InputManager.getInputPortReaderThreads" should
    "be empty when no port has been added with URIs" in {
    val mgr = freshManager
    mgr.addPort(PortIdentity(0), schema, urisToRead = List.empty, partitionings = List.empty)
    mgr.addPort(PortIdentity(1), schema, urisToRead = List.empty, partitionings = List.empty)
    assert(mgr.getInputPortReaderThreads.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // Batch cursor — initBatch, hasUnfinishedInput, getNextTuple, getCurrentTuple
  // ---------------------------------------------------------------------------

  "InputManager.hasUnfinishedInput (fresh)" should "be false when no batch has been initialized" in {
    val mgr = freshManager
    assert(!mgr.hasUnfinishedInput)
  }

  "InputManager.getCurrentTuple (fresh)" should "return null when no batch has been initialized" in {
    val mgr = freshManager
    assert(mgr.getCurrentTuple == null)
  }

  "InputManager.initBatch" should
    "swap in a new batch, reset the cursor, and surface the channel id" in {
    val mgr = freshManager
    val cid = channelId("upstream", "worker-1")
    mgr.initBatch(cid, Array(tuple(10), tuple(20), tuple(30)))
    assert(mgr.currentChannelId == cid)
    assert(mgr.hasUnfinishedInput)
    // Prove the cursor was reset to before-first: the FIRST `getNextTuple`
    // returns the first element of the batch (idx 0), not idx 1 or later.
    // If `initBatch` had not reset `currentInputIdx` back to -1, the
    // first `getNextTuple` would skip past element 0.
    assert(mgr.getNextTuple == tuple(10))
  }

  it should "drive getNextTuple to yield elements in order and complete after the last one" in {
    val mgr = freshManager
    val cid = channelId("upstream", "worker-1")
    val batch = Array(tuple(10), tuple(20), tuple(30))
    mgr.initBatch(cid, batch)
    assert(mgr.hasUnfinishedInput)
    assert(mgr.getNextTuple == tuple(10))
    assert(mgr.getCurrentTuple == tuple(10))
    assert(mgr.hasUnfinishedInput)
    assert(mgr.getNextTuple == tuple(20))
    assert(mgr.getCurrentTuple == tuple(20))
    assert(mgr.hasUnfinishedInput)
    assert(mgr.getNextTuple == tuple(30))
    assert(mgr.getCurrentTuple == tuple(30))
    assert(!mgr.hasUnfinishedInput)
  }

  it should "allow a second initBatch to replace the existing batch and restart the cursor" in {
    val mgr = freshManager
    val cid = channelId("upstream", "worker-1")
    mgr.initBatch(cid, Array(tuple(1), tuple(2)))
    assert(mgr.getNextTuple == tuple(1))
    val cid2 = channelId("upstream-2", "worker-1")
    mgr.initBatch(cid2, Array(tuple(7), tuple(8), tuple(9)))
    assert(mgr.currentChannelId == cid2)
    assert(mgr.hasUnfinishedInput)
    assert(mgr.getNextTuple == tuple(7))
  }

  "InputManager.getCurrentTuple (empty batch)" should "return null" in {
    // Production code path: `inputBatch.isEmpty` → return `null`. Pin the
    // edge case explicitly so a regression that defaulted to throwing
    // (or returning a sentinel tuple) would surface.
    val mgr = freshManager
    mgr.initBatch(channelId("u", "w"), Array.empty[Tuple])
    assert(mgr.getCurrentTuple == null)
  }

  "InputManager.hasUnfinishedInput (empty batch)" should "be false (no elements remain)" in {
    val mgr = freshManager
    mgr.initBatch(channelId("u", "w"), Array.empty[Tuple])
    assert(!mgr.hasUnfinishedInput)
  }

  // ---------------------------------------------------------------------------
  // isPortCompleted — non-materialized path
  // ---------------------------------------------------------------------------
  //
  // For a port with no attached InputPortMaterializationReaderThread,
  // `isPortCompleted` reads through to `WorkerPort.completed`.

  "InputManager.isPortCompleted (non-materialized)" should
    "return false for a freshly-added port and true once WorkerPort.completed is set" in {
    val mgr = freshManager
    val portId = PortIdentity(0)
    mgr.addPort(portId, schema, urisToRead = List.empty, partitionings = List.empty)
    assert(!mgr.isPortCompleted(portId))
    mgr.getPort(portId).completed = true
    assert(mgr.isPortCompleted(portId))
  }

  // ---------------------------------------------------------------------------
  // startInputPortReaderThreads — no-op when nothing is registered
  // ---------------------------------------------------------------------------

  "InputManager.startInputPortReaderThreads (no reader threads)" should
    "be a safe no-op" in {
    val mgr = freshManager
    mgr.addPort(PortIdentity(0), schema, urisToRead = List.empty, partitionings = List.empty)
    mgr.startInputPortReaderThreads() // must not throw
    succeed
  }
}
