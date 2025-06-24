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

package edu.uci.ics.amber.engine.architecture.control.utils

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.NetworkAck
import edu.uci.ics.amber.engine.architecture.common.{AmberProcessor, WorkflowActor}
import edu.uci.ics.amber.engine.architecture.control.utils.TrivialControlTester.ControlTesterRPCClient
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkOutputGateway
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.AsyncRPCContext
import edu.uci.ics.amber.engine.architecture.rpc.testerservice.RPCTesterFs2Grpc
import edu.uci.ics.amber.engine.common.CheckpointState
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage.getInMemSize
import edu.uci.ics.amber.engine.common.ambermessage.{
  DirectControlMessagePayload,
  DataPayload,
  WorkflowFIFOMessage
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}

object TrivialControlTester {
  class ControlTesterRPCClient(outputGateway: NetworkOutputGateway, actorId: ActorVirtualIdentity)
      extends AsyncRPCClient(outputGateway, actorId) {
    val getProxy: RPCTesterFs2Grpc[Future, AsyncRPCContext] =
      AsyncRPCClient
        .createProxy[RPCTesterFs2Grpc[Future, AsyncRPCContext]](createPromise, outputGateway)
  }
}

class TrivialControlTester(
    id: ActorVirtualIdentity
) extends WorkflowActor(replayLogConfOpt = None, actorId = id) {
  val ap = new AmberProcessor(
    id,
    {
      case Left(value)  => ???
      case Right(value) => transferService.send(value)
    }
  ) {
    override val asyncRPCClient = new ControlTesterRPCClient(outputGateway, id)
  }
  val initializer =
    new TesterAsyncRPCHandlerInitializer(ap.actorId, ap.asyncRPCClient, ap.asyncRPCServer)

  override def handleInputMessage(id: Long, workflowMsg: WorkflowFIFOMessage): Unit = {
    val channel = ap.inputGateway.getChannel(workflowMsg.channelId)
    channel.acceptMessage(workflowMsg)
    while (channel.isEnabled && channel.hasMessage) {
      val msg = channel.take
      msg.payload match {
        case payload: DirectControlMessagePayload => ap.processDCM(msg.channelId, payload)
        case _: DataPayload                       => ???
        case _                                    => ???
      }
    }
    sender() ! NetworkAck(id, getInMemSize(workflowMsg), getQueuedCredit(workflowMsg.channelId))
  }

  /** flow-control */
  override def getQueuedCredit(channelId: ChannelIdentity): Long = 0L

  override def preStart(): Unit = {
    transferService.initialize()
  }

  override def handleBackpressure(isBackpressured: Boolean): Unit = {}

  override def initState(): Unit = {}

  override def loadFromCheckpoint(chkpt: CheckpointState): Unit = {}
}
