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

import edu.uci.ics.amber.core.tuple.{AttributeType, Schema, TupleLike}
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, WorkflowFIFOMessage}
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class NetworkInputGatewaySpec extends AnyFlatSpec with MockFactory {

  private val fakeReceiverID = ActorVirtualIdentity("testReceiver")
  private val fakeSenderID = ActorVirtualIdentity("testSender")
  private val channelId = ChannelIdentity(fakeSenderID, fakeReceiverID, isControl = false)
  private val payloads = (0 until 4).map { i =>
    DataFrame(
      Array(
        TupleLike(i) enforceSchema Schema().add("field1", AttributeType.INTEGER)
      )
    )
  }.toArray
  private val messages = (0 until 4).map { i =>
    WorkflowFIFOMessage(channelId, i, payloads(i))
  }.toArray

  "network input port" should "output payload in FIFO order" in {
    val inputPort = new NetworkInputGateway(fakeReceiverID)
    Array(2, 0, 1, 3).foreach { i =>
      inputPort.getChannel(channelId).acceptMessage(messages(i))
    }

    (0 until 4).foreach { i =>
      val msg = inputPort.getChannel(channelId).take
      assert(msg.sequenceNumber == i)
    }

  }

  "network input port" should "de-duplicate payload" in {
    val inputPort = new NetworkInputGateway(fakeReceiverID)
    Array(2, 2, 2, 2, 2, 2, 0, 1, 1, 3, 3).foreach { i =>
      inputPort.getChannel(channelId).acceptMessage(messages(i))
    }
    (0 until 4).foreach { i =>
      val msg = inputPort.getChannel(channelId).take
      assert(msg.sequenceNumber == i)
    }

    assert(!inputPort.getChannel(channelId).hasMessage)

  }

  "network input port" should "keep unordered messages" in {
    val inputPort = new NetworkInputGateway(fakeReceiverID)
    Array(3, 2, 1).foreach { i =>
      inputPort.getChannel(channelId).acceptMessage(messages(i))
    }
    assert(!inputPort.getChannel(channelId).hasMessage)
    inputPort.getChannel(channelId).acceptMessage(messages(0))
    assert(inputPort.getChannel(channelId).hasMessage)
    (0 until 4).foreach { i =>
      val msg = inputPort.getChannel(channelId).take
      assert(msg.sequenceNumber == i)
    }
    assert(!inputPort.getChannel(channelId).hasMessage)

  }

}
