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

package org.apache.texera.amber.engine.architecture.common

import org.apache.pekko.actor.{Address, Deploy}
import org.apache.pekko.remote.RemoteScope
import org.apache.texera.amber.core.workflow.{PhysicalOp, PreferCoordinator, RoundRobinPreference}
import org.apache.texera.amber.engine.architecture.coordinator.execution.OperatorExecution
import org.apache.texera.amber.engine.architecture.deploysemantics.AddressInfo
import org.apache.texera.amber.engine.architecture.pythonworker.PythonWorkflowWorker
import org.apache.texera.amber.engine.architecture.scheduling.config.OperatorConfig
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.{
  FaultToleranceConfig,
  StateRestoreConfig,
  WorkerReplayInitialization
}
import org.apache.texera.amber.util.VirtualIdentityUtils

object ExecutorDeployment {

  def createWorkers(
      op: PhysicalOp,
      coordinatorActorService: PekkoActorService,
      operatorExecution: OperatorExecution,
      operatorConfig: OperatorConfig,
      stateRestoreConfig: Option[StateRestoreConfig],
      replayLoggingConfig: Option[FaultToleranceConfig]
  ): Unit = {

    val addressInfo = AddressInfo(
      coordinatorActorService.getClusterNodeAddresses,
      coordinatorActorService.self.path.address
    )
    lazy val isPythonBased = op.isPythonBased
    lazy val replayInit = WorkerReplayInitialization(stateRestoreConfig, replayLoggingConfig)
    lazy val preferredAddressFor: Int => Address =
      op.locationPreference.getOrElse(RoundRobinPreference) match {
        case PreferCoordinator =>
          _ => addressInfo.coordinatorAddress
        case RoundRobinPreference =>
          assert(
            addressInfo.allAddresses.nonEmpty,
            "Execution failed to start, no available computation nodes"
          )
          workerIndex => addressInfo.allAddresses(workerIndex % addressInfo.allAddresses.length)
      }

    operatorConfig.workerConfigs.foreach(workerConfig => {
      val workerId = workerConfig.workerId
      val workerIndex = VirtualIdentityUtils
        .getWorkerIndex(workerId)
        .getOrElse(
          throw new IllegalStateException(
            s"Expected worker actor id when deploying executor, got: ${workerId.name}"
          )
        )
      val preferredAddress = preferredAddressFor(workerIndex)

      val workflowWorker = if (isPythonBased) {
        PythonWorkflowWorker.props(workerConfig)
      } else {
        WorkflowWorker.props(workerConfig, replayInit)
      }
      // Note: At this point, we don't know if the actor is fully initialized.
      // Thus, the ActorRef returned from `coordinatorActorService.actorOf` is ignored.
      coordinatorActorService.actorOf(
        workflowWorker.withDeploy(Deploy(scope = RemoteScope(preferredAddress)))
      )
      operatorExecution.initWorkerExecution(workerId)
    })
  }

}
