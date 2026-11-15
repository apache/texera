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

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import org.apache.texera.amber.core.virtualidentity.ChannelIdentity
import org.apache.texera.amber.engine.architecture.coordinator.RuntimeStatisticsPersist
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmptyRequest,
  ControlInvocation => ControlInvocationPayload
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.ReturnInvocation
import org.apache.texera.amber.engine.architecture.rpc.coordinatorservice.CoordinatorServiceGrpc.{
  METHOD_COORDINATOR_INITIATE_QUERY_STATISTICS,
  METHOD_COORDINATOR_STATISTICS_QUERY_DEADLINE,
  METHOD_WORKER_EXECUTION_COMPLETED
}
import org.apache.texera.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_QUERY_STATISTICS
import org.apache.texera.amber.engine.architecture.scheduling.RegionExecutionManagerTestSupport.{
  createSourceOp,
  createWorkerId
}
import org.apache.texera.amber.engine.architecture.scheduling.RegionExecutionManagerTestSupport
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import org.apache.texera.amber.engine.common.virtualidentity.util.{CLIENT, COORDINATOR, SELF}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

/**
  * After a worker completes, the coordinator refreshes its statistics one last time, since the
  * worker may be stopped before the next poll; only once that refresh ends does it report a
  * finished workflow and stop the statistics timers. A refresh that never ends (#6924) would
  * leave the workflow never reported as finished.
  */
class WorkerExecutionCompletedHandlerSpec
    extends TestKit(ActorSystem("WorkerExecutionCompletedHandlerSpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with RegionExecutionManagerTestSupport {

  private val physicalOp = createSourceOp("worker-completed-op")
  private val workerId = createWorkerId(physicalOp)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  private def invocationsNamed(
      captured: Iterable[WorkflowFIFOMessage],
      methodName: String
  ): Seq[WorkflowFIFOMessage] =
    captured.toSeq.filter {
      case WorkflowFIFOMessage(_, _, inv: ControlInvocationPayload) =>
        inv.methodName.equalsIgnoreCase(methodName)
      case _ => false
    }

  "WorkerExecutionCompletedHandler" should "complete its final statistics refresh when the worker never answers" in {
    val f = newCoordinatorInFlight(physicalOp, workerId)
    f.cp.processDCM(
      ChannelIdentity(workerId, COORDINATOR, isControl = true),
      ControlInvocation(
        METHOD_WORKER_EXECUTION_COMPLETED,
        EmptyRequest(),
        AsyncRPCContext(workerId, COORDINATOR),
        1
      )
    )

    // Run the refresh the handler requested of the coordinator itself.
    val Seq(refreshMsg) = invocationsNamed(
      f.captured,
      METHOD_COORDINATOR_INITIATE_QUERY_STATISTICS.getBareMethodName
    )
    val refresh = refreshMsg.payload.asInstanceOf[ControlInvocationPayload]
    f.cp.processDCM(refreshMsg.channelId, refresh)
    assert(invocationsNamed(f.captured, METHOD_QUERY_STATISTICS.getBareMethodName).size == 1)

    // The worker never answers; the refresh's deadline fires instead.
    val deadline = f.actorService.scheduledToSelf.collect {
      case (_, inv: ControlInvocationPayload)
          if inv.methodName.equalsIgnoreCase(
            METHOD_COORDINATOR_STATISTICS_QUERY_DEADLINE.getBareMethodName
          ) =>
        inv
    }
    f.cp.processDCM(ChannelIdentity(SELF, SELF, isControl = true), deadline.head)

    // The refresh ends, forwarding the last known statistics to persistence as well as the UI,
    // and replies, which lets the handler go on to its end-of-workflow check.
    assert(
      f.captured.exists(msg =>
        msg.channelId.toWorkerId == CLIENT &&
          msg.payload.isInstanceOf[RuntimeStatisticsPersist]
      )
    )
    assert(f.captured.exists {
      case WorkflowFIFOMessage(_, _, ret: ReturnInvocation) => ret.commandId == refresh.commandId
      case _                                                => false
    })
  }
}
