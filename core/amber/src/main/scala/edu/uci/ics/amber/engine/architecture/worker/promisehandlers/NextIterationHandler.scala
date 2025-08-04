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
import edu.uci.ics.amber.core.tuple.FinalizeIteration
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmbeddedControlMessageType.PORT_ALIGNMENT
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmptyRequest,
  EndIterationRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_END_ITERATION
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.operator.loop.LoopStartOpExec

trait NextIterationHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def nextIteration(
      request: EmptyRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    dp.processOnFinish()
    if (dp.executor.asInstanceOf[LoopStartOpExec].checkCondition()) {
      dp.outputManager.finalizeIteration(dp.actorId)
    } else {
      dp.outputManager.finalizeOutput()
    }
    EmptyReturn()
  }
}
