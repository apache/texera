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
import edu.uci.ics.amber.core.tuple.FinalizePort
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EmptyRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.error.ErrorUtils.safely

trait EndChannelHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def endChannel(
      request: EmptyRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    val channelId = dp.inputManager.currentChannelId
    val portId = dp.inputGateway.getChannel(channelId).getPortId
    dp.inputManager.getPort(portId).completed = true
    dp.inputManager.initBatch(channelId, Array.empty)
    try {
      val outputState = dp.executor.produceStateOnFinish(portId.id)
      if (outputState.isDefined) {
        dp.outputManager.emitState(outputState.get)
      }
      dp.outputManager.outputIterator.setTupleOutput(
        dp.executor.onFinishMultiPort(portId.id)
      )
    } catch safely {
      case e =>
        // forward input tuple to the user and pause DP thread
        dp.handleExecutorException(e)
    }

    dp.outputManager.outputIterator.appendSpecialTupleToEnd(
      FinalizePort(portId, input = true)
    )

    if (dp.inputManager.getAllPorts.forall(portId => dp.inputManager.isPortCompleted(portId))) {
      // Need this check for handling input port dependency relationships.
      // See documentation of isMissingOutputPort
      if (!dp.outputManager.isMissingOutputPort) {
        // assuming all the output ports finalize after all input ports are finalized.
        dp.outputManager.finalizeOutput()
      }
    }
    EmptyReturn()
  }
}
