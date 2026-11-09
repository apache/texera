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

package org.apache.texera.amber.engine.architecture.worker

import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  EmbeddedControlMessageIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.core.workflow.WorkflowContext.DEFAULT_WORKFLOW_ID
import org.apache.texera.amber.engine.architecture.messaginglayer.{
  InputManager,
  NetworkInputGateway
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  EmbeddedControlMessage,
  EmbeddedControlMessageType
}
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.DPInputQueueElement
import org.apache.texera.amber.util.VirtualIdentityUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.LinkedBlockingQueue

class EmbeddedControlMessageManagerSpec extends AnyFlatSpec with Matchers {

  private val testOpId = PhysicalOpIdentity(OperatorIdentity("testop"), "main")
  private val upstreamOpIdA = PhysicalOpIdentity(OperatorIdentity("senderA"), "main")
  private val upstreamOpIdB = PhysicalOpIdentity(OperatorIdentity("senderB"), "main")

  private val actorId: ActorVirtualIdentity =
    VirtualIdentityUtils.createWorkerIdentity(DEFAULT_WORKFLOW_ID, testOpId, 0)
  private val senderA: ActorVirtualIdentity =
    VirtualIdentityUtils.createWorkerIdentity(DEFAULT_WORKFLOW_ID, upstreamOpIdA, 0)
  private val senderB: ActorVirtualIdentity =
    VirtualIdentityUtils.createWorkerIdentity(DEFAULT_WORKFLOW_ID, upstreamOpIdB, 0)
  // a different worker of the same operator, used as the target of a channel that
  // should be filtered out when it appears in an ECM scope.
  private val otherWorker: ActorVirtualIdentity =
    VirtualIdentityUtils.createWorkerIdentity(DEFAULT_WORKFLOW_ID, testOpId, 1)

  private def mkGateway: NetworkInputGateway = new NetworkInputGateway(actorId)

  private def mkInputManager: InputManager =
    new InputManager(actorId, new LinkedBlockingQueue[DPInputQueueElement]())

  private def mkEcm(
      ecmType: EmbeddedControlMessageType,
      scope: Seq[ChannelIdentity]
  ): EmbeddedControlMessage =
    EmbeddedControlMessage(
      EmbeddedControlMessageIdentity("ecm"),
      ecmType,
      scope,
      Map.empty
    )

  "EmbeddedControlMessageManager" should "align ALL_ALIGNMENT only after every data channel is received" in {
    val gateway = mkGateway
    val inputManager = mkInputManager
    val portId = PortIdentity()

    val chA = ChannelIdentity(senderA, actorId, isControl = false)
    val chB = ChannelIdentity(senderB, actorId, isControl = false)
    gateway.getChannel(chA).setPortId(portId)
    gateway.getChannel(chB).setPortId(portId)

    val mgr = new EmbeddedControlMessageManager(actorId, gateway, inputManager)
    val ecm = mkEcm(EmbeddedControlMessageType.ALL_ALIGNMENT, Seq.empty)

    // first data channel: not yet received from all channels in scope
    mgr.isECMAligned(chA, ecm) shouldBe false
    // second (last) data channel: now aligned
    mgr.isECMAligned(chB, ecm) shouldBe true
  }

  it should "align PORT_ALIGNMENT only after all channels registered on the port are received" in {
    val gateway = mkGateway
    val inputManager = mkInputManager
    val portId = PortIdentity()

    val chA = ChannelIdentity(senderA, actorId, isControl = false)
    val chB = ChannelIdentity(senderB, actorId, isControl = false)
    gateway.getChannel(chA).setPortId(portId)
    gateway.getChannel(chB).setPortId(portId)

    inputManager.addPort(portId, Schema(), List.empty, List.empty)
    inputManager.getPort(portId).channels.add(chA)
    inputManager.getPort(portId).channels.add(chB)

    val mgr = new EmbeddedControlMessageManager(actorId, gateway, inputManager)
    val ecm = mkEcm(EmbeddedControlMessageType.PORT_ALIGNMENT, Seq(chA, chB))

    // first channel on the port received: the port is not fully aligned yet
    mgr.isECMAligned(chA, ecm) shouldBe false
    // second (last) channel on the port received: now aligned
    mgr.isECMAligned(chB, ecm) shouldBe true
  }

  it should "align NO_ALIGNMENT only on the first received ECM" in {
    val gateway = mkGateway
    val inputManager = mkInputManager
    val portId = PortIdentity()

    val chA = ChannelIdentity(senderA, actorId, isControl = false)
    val chB = ChannelIdentity(senderB, actorId, isControl = false)
    gateway.getChannel(chA).setPortId(portId)
    gateway.getChannel(chB).setPortId(portId)

    val mgr = new EmbeddedControlMessageManager(actorId, gateway, inputManager)
    val ecm = mkEcm(EmbeddedControlMessageType.NO_ALIGNMENT, Seq.empty)

    // first ECM triggers
    mgr.isECMAligned(chA, ecm) shouldBe true
    // subsequent ECM does not trigger again
    mgr.isECMAligned(chB, ecm) shouldBe false
  }

  it should "throw IllegalArgumentException for an unsupported ECM type" in {
    val gateway = mkGateway
    val inputManager = mkInputManager
    val portId = PortIdentity()

    val chId = ChannelIdentity(senderA, actorId, isControl = false)
    gateway.getChannel(chId).setPortId(portId)

    val mgr = new EmbeddedControlMessageManager(actorId, gateway, inputManager)
    val ecm = mkEcm(EmbeddedControlMessageType.Unrecognized(-1), Seq.empty)

    assertThrows[IllegalArgumentException] {
      mgr.isECMAligned(chId, ecm)
    }
  }

  it should "restrict the scope to channels targeting this worker when scope is non-empty" in {
    val gateway = mkGateway
    val inputManager = mkInputManager
    val portId = PortIdentity()

    // one scope channel targets this worker; the other targets a different worker
    val inScope = ChannelIdentity(senderA, actorId, isControl = false)
    val foreign = ChannelIdentity(senderB, otherWorker, isControl = false)
    gateway.getChannel(inScope).setPortId(portId)
    gateway.getChannel(foreign).setPortId(portId)

    val mgr = new EmbeddedControlMessageManager(actorId, gateway, inputManager)
    // the scope names both channels, but `foreign` (toWorkerId != actorId) must be filtered
    // out; alignment is therefore reached once only `inScope` has been received.
    val ecm = mkEcm(EmbeddedControlMessageType.ALL_ALIGNMENT, Seq(inScope, foreign))

    mgr.isECMAligned(inScope, ecm) shouldBe true
  }
}
