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

import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.engine.common.ambermessage.{DataFrame, WorkflowFIFOMessage}
import org.apache.texera.amber.engine.common.virtualidentity.util.SELF
import org.scalatest.flatspec.AnyFlatSpec

class NetworkOutputGatewaySpec extends AnyFlatSpec {

  private val actorId = ActorVirtualIdentity("self")
  private val other = ActorVirtualIdentity("other")

  private def newFixture()
      : (NetworkOutputGateway, scala.collection.mutable.ArrayBuffer[WorkflowFIFOMessage]) = {
    val buf = scala.collection.mutable.ArrayBuffer[WorkflowFIFOMessage]()
    val g = new NetworkOutputGateway(actorId, msg => { buf += msg; () })
    (g, buf)
  }

  "network output gateway" should "rewrite a SELF-targeted channel's toWorkerId to actorId and increment sequence numbers" in {
    val (g, buf) = newFixture()
    val channelId = ChannelIdentity(other, SELF, isControl = false)
    val payload = DataFrame(Array.empty[Tuple])

    g.sendTo(channelId, payload)
    g.sendTo(channelId, payload)

    assert(buf.size == 2)
    // toWorkerId == SELF is folded to actorId, other fields preserved
    val expectedChannel = ChannelIdentity(other, actorId, isControl = false)
    assert(buf(0).channelId == expectedChannel)
    assert(buf(1).channelId == expectedChannel)
    // sequence number starts at 0 and increments to 1 on the second send
    assert(buf(0).sequenceNumber == 0L)
    assert(buf(1).sequenceNumber == 1L)
  }

  "network output gateway" should "pass a non-SELF channel through verbatim" in {
    val (g, buf) = newFixture()
    val channelId = ChannelIdentity(actorId, other, isControl = false)
    val payload = DataFrame(Array.empty[Tuple])

    g.sendTo(channelId, payload)

    assert(buf.size == 1)
    assert(buf(0).channelId == channelId)
    assert(buf(0).sequenceNumber == 0L)
  }

  "network output gateway" should "fold a SELF receiver to actorId on the data channel via the ActorVirtualIdentity overload" in {
    val (g, buf) = newFixture()

    g.sendTo(SELF, DataFrame(Array.empty[Tuple]))

    assert(buf.size == 1)
    assert(buf(0).channelId == ChannelIdentity(actorId, actorId, isControl = false))
    assert(buf(0).sequenceNumber == 0L)
  }

  "network output gateway" should "keep a non-SELF receiver as-is on the data channel via the ActorVirtualIdentity overload" in {
    val (g, buf) = newFixture()

    g.sendTo(other, DataFrame(Array.empty[Tuple]))

    assert(buf.size == 1)
    assert(buf(0).channelId == ChannelIdentity(actorId, other, isControl = false))
    assert(buf(0).sequenceNumber == 0L)
  }

  "network output gateway" should "reflect per-channel sequence numbers in getFIFOState" in {
    val (g, _) = newFixture()
    val channelId = ChannelIdentity(actorId, other, isControl = false)
    val payload = DataFrame(Array.empty[Tuple])

    g.sendTo(channelId, payload)
    g.sendTo(channelId, payload)

    assert(g.getFIFOState.get(channelId).contains(2L))
  }

}
