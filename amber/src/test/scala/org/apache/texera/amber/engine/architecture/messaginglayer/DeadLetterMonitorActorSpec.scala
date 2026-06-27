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

import org.apache.pekko.actor.{ActorSystem, DeadLetter, Props}
import org.apache.pekko.testkit.{ImplicitSender, TestKit, TestProbe}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, TupleLike}
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.engine.architecture.common.WorkflowActor.{
  MessageBecomesDeadLetter,
  NetworkMessage
}
import org.apache.texera.amber.engine.common.ambermessage.{DataFrame, WorkflowFIFOMessage}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration.DurationInt

class DeadLetterMonitorActorSpec
    extends TestKit(ActorSystem("DeadLetterMonitorActorSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  private val channelId =
    ChannelIdentity(
      ActorVirtualIdentity("sender"),
      ActorVirtualIdentity("receiver"),
      isControl = false
    )

  private def aNetworkMessage(): NetworkMessage = {
    val payload = DataFrame(
      Array(TupleLike(1) enforceSchema Schema().add("field1", AttributeType.INTEGER))
    )
    NetworkMessage(0, WorkflowFIFOMessage(channelId, 0, payload))
  }

  "DeadLetterMonitorActor" should "forward MessageBecomesDeadLetter to the original sender for a NetworkMessage dead letter" in {
    val monitor = system.actorOf(Props(new DeadLetterMonitorActor()))
    val originalSender = TestProbe()
    val recipient = TestProbe()
    val message = aNetworkMessage()

    monitor ! DeadLetter(message, originalSender.ref, recipient.ref)

    originalSender.expectMsg(MessageBecomesDeadLetter(message))
  }

  it should "ignore a dead letter whose payload is not a NetworkMessage" in {
    val monitor = system.actorOf(Props(new DeadLetterMonitorActor()))
    val originalSender = TestProbe()
    val recipient = TestProbe()

    monitor ! DeadLetter("not a network message", originalSender.ref, recipient.ref)

    originalSender.expectNoMessage(200.millis)
  }

  it should "ignore messages that are not dead letters" in {
    val monitor = system.actorOf(Props(new DeadLetterMonitorActor()))

    monitor ! "some unrelated message"

    expectNoMessage(200.millis)
  }
}
