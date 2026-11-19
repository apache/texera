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

import com.twitter.util.Duration
import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import org.apache.texera.amber.core.workflow.WorkflowContext.{
  DEFAULT_EXECUTION_ID,
  DEFAULT_WORKFLOW_ID
}
import org.apache.texera.amber.core.workflow.{PhysicalOp, PhysicalPlan, WorkflowContext}
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorAsyncRPCHandlerInitializer,
  CoordinatorConfig,
  CoordinatorProcessor
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.ControlInvocation
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.apache.texera.amber.util.VirtualIdentityUtils

import scala.collection.mutable.ArrayBuffer

/** Shared coordinator processor fixture and control-invocation collectors for promise-handler specs. */
trait CoordinatorMessageFixture {

  protected val awaitTimeout: Duration = Duration.fromSeconds(1)

  protected def mkPhysicalOp(logicalOpId: String, layerName: String = "main"): PhysicalOp =
    PhysicalOp(
      PhysicalOpIdentity(OperatorIdentity(logicalOpId), layerName),
      DEFAULT_WORKFLOW_ID,
      DEFAULT_EXECUTION_ID,
      OpExecInitInfo.Empty
    )

  protected def mkWorkerId(physicalOp: PhysicalOp, index: Int): ActorVirtualIdentity =
    VirtualIdentityUtils.createWorkerIdentity(DEFAULT_WORKFLOW_ID, physicalOp.id, index)

  protected def createCoordinatorFixture(
      initialPlan: Option[PhysicalPlan] = None
  ): (CoordinatorAsyncRPCHandlerInitializer, ArrayBuffer[WorkflowFIFOMessage]) = {
    val sent = ArrayBuffer[WorkflowFIFOMessage]()
    val outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit = {
      case Right(m) => sent += m
      case _        => ()
    }
    val cp = new CoordinatorProcessor(
      new WorkflowContext(),
      CoordinatorConfig(None, None, None, None),
      COORDINATOR,
      outputHandler
    )
    initialPlan.foreach(plan => cp.workflowScheduler.physicalPlan = plan)
    (new CoordinatorAsyncRPCHandlerInitializer(cp), sent)
  }

  protected def controlInvocations(sent: ArrayBuffer[WorkflowFIFOMessage]): Seq[ControlInvocation] =
    sent.toSeq.collect {
      case WorkflowFIFOMessage(_, _, invocation: ControlInvocation) => invocation
    }

  protected def controlInvocationsNamed(
      sent: ArrayBuffer[WorkflowFIFOMessage],
      methodName: String
  ): Seq[ControlInvocation] =
    controlInvocations(sent).filter(_.methodName == methodName)
}
