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

import akka.actor.{Address, Deploy}
import akka.remote.RemoteScope
import edu.uci.ics.amber.core.workflow.{PhysicalOp, PreferController, RoundRobinPreference}
import edu.uci.ics.amber.engine.architecture.controller.execution.OperatorExecution
import edu.uci.ics.amber.engine.architecture.deploysemantics.AddressInfo
import edu.uci.ics.amber.engine.architecture.pythonworker.PythonWorkflowWorker
import edu.uci.ics.amber.engine.architecture.scheduling.config.OperatorConfig
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.{
  FaultToleranceConfig,
  StateRestoreConfig,
  WorkerReplayInitialization
}
import edu.uci.ics.amber.util.VirtualIdentityUtils

object ExecutorDeployment {

  def createWorkers(
      op: PhysicalOp,
      controllerActorService: AkkaActorService,
      operatorExecution: OperatorExecution,
      operatorConfig: OperatorConfig,
      stateRestoreConfig: Option[StateRestoreConfig],
      replayLoggingConfig: Option[FaultToleranceConfig]
  ): Unit = {

    val addressInfo = AddressInfo(
      controllerActorService.getClusterNodeAddresses,
      controllerActorService.self.path.address
    )

    operatorConfig.workerConfigs.foreach(workerConfig => {
      val workerId = workerConfig.workerId
      val workerIndex = VirtualIdentityUtils.getWorkerIndex(workerId)
      val locationPreference = op.locationPreference.getOrElse(RoundRobinPreference)
      val preferredAddress: Address = locationPreference match {
        case PreferController =>
          addressInfo.controllerAddress
        case RoundRobinPreference =>
          assert(
            addressInfo.allAddresses.nonEmpty,
            "Execution failed to start, no available computation nodes"
          )
          addressInfo.allAddresses(workerIndex % addressInfo.allAddresses.length)
      }

      val workflowWorker = if (op.isPythonBased) {
        PythonWorkflowWorker.props(workerConfig)
      } else {
        WorkflowWorker.props(
          workerConfig,
          WorkerReplayInitialization(
            stateRestoreConfig,
            replayLoggingConfig
          )
        )
      }
      // Note: At this point, we don't know if the actor is fully initialized.
      // Thus, the ActorRef returned from `controllerActorService.actorOf` is ignored.
      controllerActorService.actorOf(
        workflowWorker.withDeploy(Deploy(scope = RemoteScope(preferredAddress)))
      )
      operatorExecution.initWorkerExecution(workerId)
    })
  }

}
