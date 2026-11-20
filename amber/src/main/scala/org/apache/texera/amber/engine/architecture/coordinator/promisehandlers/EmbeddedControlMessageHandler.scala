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

package org.apache.texera.amber.engine.architecture.coordinator.promisehandlers

import com.twitter.util.Future
import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.engine.architecture.coordinator.CoordinatorAsyncRPCHandlerInitializer
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ControlInvocation,
  PropagateEmbeddedControlMessageRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  ControlReturn,
  PropagateEmbeddedControlMessageResponse
}

trait EmbeddedControlMessageHandler {
  this: CoordinatorAsyncRPCHandlerInitializer =>

  override def propagateEmbeddedControlMessage(
      msg: PropagateEmbeddedControlMessageRequest,
      ctx: AsyncRPCContext
  ): Future[PropagateEmbeddedControlMessageResponse] = {
    // step1: create separate control commands for each target actor.
    val inputSet = msg.targetOps.flatMap { target =>
      cp.workflowExecution.getRunningRegionExecutions
        .map(_.getOperatorExecution(target))
        .flatMap(_.getWorkerIds.map { worker =>
          worker -> createInvocation(msg.methodName, msg.command, worker)
        })
    }
    // step 2: packing all control commands into one compound command.
    val cmdMapping: Map[String, ControlInvocation] = inputSet.iterator.map {
      case (workerId, (control, _)) => (workerId.name, control)
    }.toMap
    val futures: Set[Future[(ActorVirtualIdentity, ControlReturn)]] = inputSet.iterator.map {
      case (workerId, (_, future)) => future.map(ret => (workerId, ret))
    }.toSet

    // step 3 & 4: resolve scope channels and send ECM through source control channels.
    sendEmbeddedControlMessageToSources(
      msg.id,
      msg.ecmType,
      msg.scope.toSet,
      msg.sourceOpToStartProp,
      cmdMapping
    )

    // step 5: wait for the ECM propagation.
    Future.collect(futures.toList).map { ret =>
      cp.logManager.markAsReplayDestination(msg.id)
      PropagateEmbeddedControlMessageResponse(ret.iterator.map(x => (x._1.name, x._2)).toMap)
    }
  }
}
