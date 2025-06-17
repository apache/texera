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

package edu.uci.ics.amber.engine.architecture.controller

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers._
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.AsyncRPCContext
import edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceFs2Grpc
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCHandlerInitializer
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity

class ControllerAsyncRPCHandlerInitializer(
    val cp: ControllerProcessor
) extends AsyncRPCHandlerInitializer(cp.asyncRPCClient, cp.asyncRPCServer)
    with ControllerServiceFs2Grpc[Future, AsyncRPCContext]
    with AmberLogging
    with LinkWorkersHandler
    with WorkerExecutionCompletedHandler
    with WorkerStateUpdatedHandler
    with PauseHandler
    with QueryWorkerStatisticsHandler
    with ResumeHandler
    with StartWorkflowHandler
    with PortCompletedHandler
    with ConsoleMessageHandler
    with RetryWorkflowHandler
    with EvaluatePythonExpressionHandler
    with DebugCommandHandler
    with TakeGlobalCheckpointHandler
    with EmbeddedControlMessageHandler
    with RetrieveWorkflowStateHandler {
  val actorId: ActorVirtualIdentity = cp.actorId
}
