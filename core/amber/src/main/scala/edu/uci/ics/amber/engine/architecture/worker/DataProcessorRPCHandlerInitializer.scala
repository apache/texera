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

package edu.uci.ics.amber.engine.architecture.worker

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  DebugCommandRequest,
  EmptyRequest,
  EvaluatePythonExpressionRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.{EmptyReturn, EvaluatedValue}
import edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceFs2Grpc
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers._
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCHandlerInitializer
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity

class DataProcessorRPCHandlerInitializer(val dp: DataProcessor)
    extends AsyncRPCHandlerInitializer(dp.asyncRPCClient, dp.asyncRPCServer)
    with WorkerServiceFs2Grpc[Future, AsyncRPCContext]
    with AmberLogging
    with InitializeExecutorHandler
    with OpenExecutorHandler
    with PauseHandler
    with AddPartitioningHandler
    with QueryStatisticsHandler
    with ResumeHandler
    with StartHandler
    with AssignPortHandler
    with AddInputChannelHandler
    with FlushNetworkBufferHandler
    with RetrieveStateHandler
    with PrepareCheckpointHandler
    with FinalizeCheckpointHandler {
  val actorId: ActorVirtualIdentity = dp.actorId

  override def debugCommand(
      request: DebugCommandRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = ???

  override def evaluatePythonExpression(
      request: EvaluatePythonExpressionRequest,
      ctx: AsyncRPCContext
  ): Future[EvaluatedValue] = ???

  override def retryCurrentTuple(request: EmptyRequest, ctx: AsyncRPCContext): Future[EmptyReturn] =
    ???

  override def noOperation(request: EmptyRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = ???
}
