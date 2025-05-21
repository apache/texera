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

package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.core.WorkflowRuntimeException
import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.marker.{EndOfInputChannel, StartOfInputChannel}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EmptyRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkerStateResponse
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{READY, RUNNING}
import edu.uci.ics.amber.engine.common.ambermessage.MarkerFrame
import edu.uci.ics.amber.engine.common.virtualidentity.util.SOURCE_STARTER_ACTOR
import edu.uci.ics.amber.core.virtualidentity.ChannelIdentity
import edu.uci.ics.amber.core.workflow.PortIdentity

trait StartHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def startWorker(
      request: EmptyRequest,
      ctx: AsyncRPCContext
  ): Future[WorkerStateResponse] = {
    logger.info("Starting the worker.")
    if (dp.executor.isInstanceOf[SourceOperatorExecutor]) {
      dp.stateManager.assertState(READY)
      dp.stateManager.transitTo(RUNNING)
      // for source operator: add a virtual input channel just for kicking off the execution
      val dummyInputPortId = PortIdentity()
      dp.inputManager.addPort(dummyInputPortId, null)
      dp.inputGateway
        .getChannel(ChannelIdentity(SOURCE_STARTER_ACTOR, actorId, isControl = false))
        .setPortId(dummyInputPortId)
      dp.processDataPayload(
        ChannelIdentity(SOURCE_STARTER_ACTOR, dp.actorId, isControl = false),
        MarkerFrame(StartOfInputChannel())
      )
      dp.processDataPayload(
        ChannelIdentity(SOURCE_STARTER_ACTOR, dp.actorId, isControl = false),
        MarkerFrame(EndOfInputChannel())
      )
      WorkerStateResponse(dp.stateManager.getCurrentState)
    } else {
      throw new WorkflowRuntimeException(
        s"non-source worker $actorId received unexpected StartWorker!"
      )
    }
  }

}
