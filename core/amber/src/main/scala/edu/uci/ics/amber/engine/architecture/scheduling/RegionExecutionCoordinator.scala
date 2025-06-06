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

package edu.uci.ics.amber.engine.architecture.scheduling

import com.twitter.util.Future
import java.util.concurrent.atomic.AtomicReference
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.storage.VFSURIFactory.decodeURI
import edu.uci.ics.amber.core.workflow.{GlobalPortIdentity, PhysicalLink, PhysicalOp}
import edu.uci.ics.amber.engine.architecture.common.{AkkaActorService, ExecutorDeployment}
import edu.uci.ics.amber.engine.architecture.controller.execution.{
  OperatorExecution,
  WorkflowExecution
}
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerConfig,
  ExecutionStatsUpdate,
  WorkerAssignmentUpdate
}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AssignPortRequest,
  EmptyRequest,
  InitializeExecutorRequest,
  LinkWorkersRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.architecture.scheduling.config.{
  InputPortConfig,
  OperatorConfig,
  OutputPortConfig,
  ResourceConfig
}
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.Partitioning
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource

class RegionExecutionCoordinator(
    region: Region,
    workflowExecution: WorkflowExecution,
    asyncRPCClient: AsyncRPCClient,
    controllerConfig: ControllerConfig,
    actorService: AkkaActorService
) {

  initRegionExecution()

  private sealed trait RegionState
  private case object Unexecuted extends RegionState
  private case object ExecutingDependeePorts extends RegionState
  private case object ExecutingNonDependeePorts extends RegionState

  private val stateRef: AtomicReference[RegionState] =
    new AtomicReference(Unexecuted)

  def isInDependeePhase: Boolean = stateRef.get() == ExecutingDependeePorts

  def execute(): Future[Unit] =
    stateRef.get() match {
      case Unexecuted =>
        if (region.getOperators.exists(_.dependeeInputs.nonEmpty)) {
          stateRef.set(ExecutingDependeePorts)
          executeDependeePorts(actorService)
        } else {
          stateRef.set(ExecutingNonDependeePorts)
          executeNonDependeePorts(actorService)
        }
      case ExecutingDependeePorts =>
        stateRef.set(ExecutingNonDependeePorts)
        executeNonDependeePorts(actorService)
      case ExecutingNonDependeePorts =>
        Future.Unit
    }

  private def executeDependeePorts(actorService: AkkaActorService): Future[Unit] = {
    val ops = region.getOperators.filter(_.dependeeInputs.nonEmpty)

    prepareAndLaunch(
      actorService,
      ops,
      () => assignPortsInternal(region, dependeePhase = true),
      () => Future.value(Seq.empty), // no links in pass-1
      () => sendOpsWithDependeeInputStarts(region)
    )
  }

  private def executeNonDependeePorts(actorService: AkkaActorService): Future[Unit] = {
    // create storage for output ports – original behaviour
    region.resourceConfig.get.portConfigs
      .collect {
        case (id, cfg: OutputPortConfig) => id -> cfg
      }
      .foreach {
        case (pid, cfg) =>
          createOutputPortStorageObjects(Map(pid -> cfg))
      }

    val ops = region.getOperators.filter(_.dependeeInputs.isEmpty)

    prepareAndLaunch(
      actorService,
      ops,
      () => assignPortsInternal(region, dependeePhase = false),
      () => connectChannels(region.getLinks),
      () => sendStarts(region)
    )
  }

  private def prepareAndLaunch(
      actorService: AkkaActorService,
      operatorsToRun: Set[PhysicalOp],
      assignPortLogic: () => Future[Seq[EmptyReturn]],
      connectLinkLogic: () => Future[Seq[EmptyReturn]],
      startWorkerLogic: () => Future[Seq[Unit]]
  ): Future[Unit] = {

    val resourceConfig = region.resourceConfig.get
    val regionExecution = workflowExecution.getRegionExecution(region.id)

    asyncRPCClient.sendToClient(
      ExecutionStatsUpdate(workflowExecution.getAllRegionExecutionsStats)
    )
    asyncRPCClient.sendToClient(
      WorkerAssignmentUpdate(
        operatorsToRun
          .map(_.id)
          .map { pid =>
            pid.logicalOpId.id -> regionExecution
              .getOperatorExecution(pid)
              .getWorkerIds
              .map(_.name)
              .toList
          }
          .toMap
      )
    )
    Future(())
      .flatMap(_ => initExecutors(operatorsToRun, resourceConfig))
      .flatMap(_ => assignPortLogic())
      .flatMap(_ => connectLinkLogic())
      .flatMap(_ => openOperators(operatorsToRun))
      .flatMap(_ => startWorkerLogic())
      .unit
  }

  private def initRegionExecution(): Unit = {
    val resourceConfig = region.resourceConfig.get
    val regionExecution = workflowExecution.getRegionExecution(region.id)

    region.getOperators.foreach { physicalOp =>
      val existOpExecution =
        workflowExecution.getAllRegionExecutions.exists(_.hasOperatorExecution(physicalOp.id))

      val operatorExecution = regionExecution.initOperatorExecution(
        physicalOp.id,
        if (existOpExecution)
          Some(workflowExecution.getLatestOperatorExecution(physicalOp.id))
        else
          None
      )

      if (!existOpExecution) {
        buildOperator(
          actorService,
          physicalOp,
          resourceConfig.operatorConfigs(physicalOp.id),
          operatorExecution
        )
      }
    }
  }

  private def buildOperator(
      actorService: AkkaActorService,
      physicalOp: PhysicalOp,
      operatorConfig: OperatorConfig,
      operatorExecution: OperatorExecution
  ): Unit = {
    ExecutorDeployment.createWorkers(
      physicalOp,
      actorService,
      operatorExecution,
      operatorConfig,
      controllerConfig.stateRestoreConfOpt,
      controllerConfig.faultToleranceConfOpt
    )
  }

  private def initExecutors(
      operators: Set[PhysicalOp],
      resourceConfig: ResourceConfig
  ): Future[Seq[EmptyReturn]] = {
    Future
      .collect(
        operators
          .flatMap(physicalOp => {
            val workerConfigs = resourceConfig.operatorConfigs(physicalOp.id).workerConfigs
            workerConfigs.map(_.workerId).map { workerId =>
              asyncRPCClient.workerInterface.initializeExecutor(
                InitializeExecutorRequest(
                  workerConfigs.length,
                  physicalOp.opExecInitInfo,
                  physicalOp.isSourceOperator
                ),
                asyncRPCClient.mkContext(workerId)
              )
            }
          })
          .toSeq
      )
  }

  /* ---------- unified helpers ---------- */

  /** Consolidated implementation used by both `assignDependeePorts` and `assignPorts`.
   *
   * @param dependeePhase  true  ⇒ handle **dependee** input ports only
   *                       false ⇒ handle **normal** input ports + all output ports
   */
  private def assignPortsInternal(
                                   region: Region,
                                   dependeePhase: Boolean
                                 ): Future[Seq[EmptyReturn]] = {
    val resourceConfig = region.resourceConfig.get
    Future.collect(
      region.getOperators
        .flatMap { physicalOp: PhysicalOp =>
          // assign input ports
          val inputPortMapping = physicalOp.inputPorts
            .filter {
              case (portId, _) =>
                // keep only the ports that belong to the requested phase
                dependeePhase == physicalOp.dependeeInputs.contains(portId)
            }
            .filter {
              // Because of the hack on input dependency, some input ports may not belong to this region.
              case (inputPortId, _) =>
                val globalInputPortId = GlobalPortIdentity(physicalOp.id, inputPortId, input = true)
                region.getPorts.contains(globalInputPortId)
            }
            .flatMap {
              case (inputPortId, (_, _, Right(schema))) =>
                val globalInputPortId = GlobalPortIdentity(physicalOp.id, inputPortId, input = true)
                val (storageURIs, partitionings) =
                  resourceConfig.portConfigs.get(globalInputPortId) match {
                    case Some(cfg: InputPortConfig) =>
                      (cfg.storagePairs.map(_._1.toString), cfg.storagePairs.map(_._2))
                    case _ => (List.empty[String], List.empty[Partitioning])
                  }
                Some(globalInputPortId -> (storageURIs, partitionings, schema))
              case _ => None
            }

          // assign ports (only for non-dependee phase)
          val outputPortMapping =
            if (dependeePhase) Iterable.empty
            else
              physicalOp.outputPorts
                .filter {
                  case (outputPortId, _) =>
                    val globalInputPortId = GlobalPortIdentity(physicalOp.id, outputPortId)
                    region.getPorts.contains(globalInputPortId)
                }
                .flatMap {
                  case (outputPortId, (_, _, Right(schema))) =>
                    val storageURI = resourceConfig.portConfigs
                      .collectFirst {
                        case (gid, cfg: OutputPortConfig)
                          if gid == GlobalPortIdentity(opId = physicalOp.id, portId = outputPortId) =>
                          cfg.storageURI.toString
                      }
                      .getOrElse("")
                    Some(
                  GlobalPortIdentity(physicalOp.id, outputPortId) -> (List(
                    storageURI
                  ), List.empty, schema)
                    )
                  case _ => None
                }

          inputPortMapping ++ outputPortMapping
        }
        // Issue AssignPort control messages to each worker.
        .flatMap {
          case (globalPortId, (storageUris, partitionings, schema)) =>
            resourceConfig.operatorConfigs(globalPortId.opId).workerConfigs.map(_.workerId).map {
              workerId =>
                asyncRPCClient.workerInterface.assignPort(
                  AssignPortRequest(
                    globalPortId.portId,
                    globalPortId.input,
                    schema.toRawSchema,
                    storageUris,
                    partitionings
                  ),
                  asyncRPCClient.mkContext(workerId)
                )
            }
        }
        .toSeq
    )
  }

  private def connectChannels(links: Set[PhysicalLink]): Future[Seq[EmptyReturn]] = {
    Future.collect(
      links.map { link: PhysicalLink =>
        asyncRPCClient.controllerInterface.linkWorkers(
          LinkWorkersRequest(link),
          asyncRPCClient.mkContext(CONTROLLER)
        )
      }.toSeq
    )
  }

  private def openOperators(operators: Set[PhysicalOp]): Future[Seq[EmptyReturn]] = {
    Future
      .collect(
        operators
          .map(_.id)
          .flatMap(opId =>
            workflowExecution.getRegionExecution(region.id).getOperatorExecution(opId).getWorkerIds
          )
          .map { workerId =>
            asyncRPCClient.workerInterface
              .openExecutor(EmptyRequest(), asyncRPCClient.mkContext(workerId))
          }
          .toSeq
      )
  }

  private def sendOpsWithDependeeInputStarts(region: Region): Future[Seq[Unit]] = {
    asyncRPCClient.sendToClient(
      ExecutionStatsUpdate(
        workflowExecution.getAllRegionExecutionsStats
      )
    )
    Future.collect(
      region.getStarterOperators
        .filter(op => op.dependeeInputs.nonEmpty)
        .map(_.id)
        .flatMap { opId =>
          workflowExecution
            .getRegionExecution(region.id)
            .getOperatorExecution(opId)
            .getWorkerIds
            .map { workerId =>
              asyncRPCClient.workerInterface
                .startWorker(EmptyRequest(), asyncRPCClient.mkContext(workerId))
                .map(resp =>
                  // update worker state
                  workflowExecution
                    .getRegionExecution(region.id)
                    .getOperatorExecution(opId)
                    .getWorkerExecution(workerId)
                    .setState(resp.state)
                )
            }
        }
        .toSeq
    )
  }

  private def sendStarts(region: Region): Future[Seq[Unit]] = {
    asyncRPCClient.sendToClient(
      ExecutionStatsUpdate(
        workflowExecution.getAllRegionExecutionsStats
      )
    )
    Future.collect(
      region.getStarterOperators
        .map(_.id)
        .flatMap { opId =>
          workflowExecution
            .getRegionExecution(region.id)
            .getOperatorExecution(opId)
            .getWorkerIds
            .map { workerId =>
              asyncRPCClient.workerInterface
                .startWorker(EmptyRequest(), asyncRPCClient.mkContext(workerId))
                .map(resp =>
                  // update worker state
                  workflowExecution
                    .getRegionExecution(region.id)
                    .getOperatorExecution(opId)
                    .getWorkerExecution(workerId)
                    .setState(resp.state)
                )
            }
        }
        .toSeq
    )
  }

  private def createOutputPortStorageObjects(
      portConfigs: Map[GlobalPortIdentity, OutputPortConfig]
  ): Unit = {
    portConfigs.foreach {
      case (outputPortId, portConfig) =>
        val storageUriToAdd = portConfig.storageURI
        val (_, eid, _, _) = decodeURI(storageUriToAdd)
        val schemaOptional =
          region.getOperator(outputPortId.opId).outputPorts(outputPortId.portId)._3
        val schema =
          schemaOptional.getOrElse(throw new IllegalStateException("Schema is missing"))
        DocumentFactory.createDocument(storageUriToAdd, schema)
        WorkflowExecutionsResource.insertOperatorPortResultUri(
          eid = eid,
          globalPortId = outputPortId,
          uri = storageUriToAdd
        )
    }
  }

}
