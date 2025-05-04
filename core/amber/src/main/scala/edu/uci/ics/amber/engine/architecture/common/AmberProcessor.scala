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

package edu.uci.ics.amber.engine.architecture.common

import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  InputGateway,
  NetworkInputGateway,
  NetworkOutputGateway
}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ControlInvocation
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.ReturnInvocation
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import edu.uci.ics.amber.engine.architecture.worker.managers.StatisticsManager
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.ambermessage.{ControlPayload, WorkflowFIFOMessage}
import edu.uci.ics.amber.engine.common.rpc.{AsyncRPCClient, AsyncRPCServer}
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}

abstract class AmberProcessor(
    val actorId: ActorVirtualIdentity,
    @transient var outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit
) extends AmberLogging
    with Serializable {

  /** FIFO & exactly once */
  val inputGateway: InputGateway = new NetworkInputGateway(this.actorId)

  // 1. Unified Output
  val outputGateway: NetworkOutputGateway =
    new NetworkOutputGateway(
      this.actorId,
      msg => {
        // done by the same thread
        outputHandler(Right(msg))
      }
    )
  // 2. RPC Layer
  val asyncRPCClient = new AsyncRPCClient(outputGateway, actorId)
  val asyncRPCServer: AsyncRPCServer =
    new AsyncRPCServer(outputGateway, actorId)

  // statistics manager
  val statisticsManager: StatisticsManager = new StatisticsManager()

  def processControlPayload(
      channelId: ChannelIdentity,
      payload: ControlPayload
  ): Unit = {
    val controlProcessingStartTime = System.nanoTime();
    payload match {
      case invocation: ControlInvocation =>
        asyncRPCServer.receive(invocation, channelId.fromWorkerId)
      case ret: ReturnInvocation =>
        asyncRPCClient.logControlReply(ret, channelId)
        asyncRPCClient.fulfillPromise(ret)
    }
    statisticsManager.increaseControlProcessingTime(System.nanoTime() - controlProcessingStartTime)
  }

}
