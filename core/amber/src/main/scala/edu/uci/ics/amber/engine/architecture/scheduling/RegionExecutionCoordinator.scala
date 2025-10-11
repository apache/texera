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

import akka.pattern.gracefulStop
import com.twitter.util.{Future, Return, Throw}
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.storage.VFSURIFactory.decodeURI
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.core.workflow.{GlobalPortIdentity, PhysicalLink, PhysicalOp}
import edu.uci.ics.amber.engine.architecture.common.{
  AkkaActorRefMappingService,
  AkkaActorService,
  ExecutorDeployment
}
import edu.uci.ics.amber.engine.architecture.controller.execution.{
  OperatorExecution,
  RegionExecution,
  WorkflowExecution
}
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerConfig,
  ExecutionStatsUpdate,
  WorkerAssignmentUpdate
}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands._
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.architecture.scheduling.config.{
  InputPortConfig,
  OperatorConfig,
  OutputPortConfig,
  ResourceConfig
}
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.Partitioning
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.FutureBijection._
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.Duration

/**
  * The executor of a region.
  *
  * We currently use a two-phase execution scheme to handle input-port dependency relationships. This is based on these
  * assumptions:
  *
  *  - We only allow input port dependencies where the input ports of a region can be grouped as two layers, with one
  *    layer of “dependee” ports and another layer of “depender” ports. We do not allow the case where an input port
  *    can both be a dependee and a depender.
  *  - We only allow depender ports to send data to output ports. Depenee input ports cannot send data to output ports.
  *  - All the physical operators must have output ports so that we can use the existence of output ports to decide
  *    whether to `FinalizeExecutor()` for a worker. (See `OutputManager.finalizeOutput()`)
  *
  * Under these assumptions, we can `syncStatusAndTransitionRegionExecutionPhase` for a region in this sequence:
  *
  * 0. `Unexecuted`
  *
  * 1. `ExecutingDependeePortsPhase`: All the dependee input ports are executed first until they complete.
  *    The corresponding workers of those input ports are also started in this phase. No output ports are allowed. If no
  *    dependee ports exist in a region, this first phase will be skipped.
  *
  * 2. `ExecutingNonDependeePortsPhase`: All other ports (non-dependee input ports, output ports) and
  *    their workers are executed. Region completion is indicated by the completion of all the ports when in this phase.
  *
  * 3. `Completed`
  */
class RegionExecutionCoordinator(
    region: Region,
    workflowExecution: WorkflowExecution,
    asyncRPCClient: AsyncRPCClient,
    controllerConfig: ControllerConfig,
    actorService: AkkaActorService,
    actorRefService: AkkaActorRefMappingService
) extends AmberLogging {

  initRegionExecution()

  private sealed trait RegionExecutionPhase
  private case object Unexecuted extends RegionExecutionPhase
  private case object ExecutingDependeePortsPhase extends RegionExecutionPhase
  private case object ExecutingNonDependeePortsPhase extends RegionExecutionPhase
  private case object Completed extends RegionExecutionPhase

  private val currentPhaseRef: AtomicReference[RegionExecutionPhase] = new AtomicReference(
    Unexecuted
  )

  /**
    * Sync the status of `RegionExecution` and transition this coordinator's phase to `Completed` only when the
    * coordinator is currently in `ExecutingNonDependeePortsPhase` and all the ports of this region are completed.
    *
    * Additionally, this method will also terminate all the workers of this region:
    *
    * 1.  An `EndWorker` control message is first sent to all the workers. This will be the last message each worker
    * receives. We wait for all workers have replied to indicate they have finished processing all control messages.
    *
    * 2. Only after all workers have processed all control messages do we send a `gracefulStop` (akka message) to each
    * worker. JVM workers will be terminated by `gracefulStop`. Python proxy workes will also be terminated by
    * `gracefulStop`, whose termination logic will also kill the PVMs.
    */
  private def tryCompleteRegionExecution(): Future[Unit] = {
    // Only `ExecutingNonDependeePortsPhase` can transition to `Completed`
    if (currentPhaseRef.get != ExecutingNonDependeePortsPhase) {
      return Future.Unit
    }

    // Sync the status with RegionExecution
    val regionExecution = workflowExecution.getRegionExecution(region.id)
    if (!regionExecution.isCompleted) {
      return Future.Unit
    }

    // Set this coordinator's status to be completed so that subsequent regions can be started by
    // WorkflowExecutionCoordinator.
    currentPhaseRef.set(Completed)

    // Terminate all the workers in this region.
    terminateWorkers(regionExecution)
  }

  private def terminateWorkers(regionExecution: RegionExecution) = {
    // 1. Send EndWorkers to every worker
    val endWorkerRequests =
      regionExecution.getAllOperatorExecutions.flatMap {
        case (_, opExec) =>
          opExec.getWorkerIds.map { workerId =>
            asyncRPCClient.workerInterface
              .endWorker(EmptyRequest(), asyncRPCClient.mkContext(workerId))
          }
      }.toSeq

    val endWorkerFuture: Future[Unit] =
      Future.collect(endWorkerRequests).unit

    // 2. Send GracefulStops only after 1 has finished
    val gracefulStopRequests: Future[Unit] =
      endWorkerFuture.flatMap { _ =>
        val gracefulStops =
          regionExecution.getAllOperatorExecutions.flatMap {
            case (_, opExec) =>
              opExec.getWorkerIds.map { workerId =>
                val actorRef = actorRefService.getActorRef(workerId)
                // Remove the actorRef so that no other actors can find the worker and send messages.
                actorRefService.removeActorRef(workerId)
                gracefulStop(actorRef, Duration(5, TimeUnit.SECONDS)).asTwitter()
              }
          }.toSeq

        Future.collect(gracefulStops).unit
      }

    // 3. Log whether the kills were successful
    gracefulStopRequests.transform {
      case Return(_) =>
        logger.info(s"Region ${region.id.id} successfully terminated.")
        regionExecution.getAllOperatorExecutions.foreach {
          case (_, opExec) =>
            opExec.getWorkerIds.foreach { workerId =>
              opExec.getWorkerExecution(workerId).update(System.nanoTime(), WorkerState.TERMINATED)
            }
        }
        Future.Unit // propagate success
      case Throw(err) =>
        logger.warn(s"Error when terminating region ${region.id}.")
        Future.exception(err) // propagate failure
    }
  }

  def isCompleted: Boolean = currentPhaseRef.get == Completed

  /**
    * This will sync and transition the region execution phase from one to another depending on its current phase:
    *
    * `Unexecuted` -> `ExecutingDependeePortsPhase` -> `ExecutingNonDependeePortsPhase` -> `Completed`
    */
  def syncStatusAndTransitionRegionExecutionPhase(): Future[Unit] =
    currentPhaseRef.get match {
      case Unexecuted =>
        executeDependeePortPhase()
      case ExecutingDependeePortsPhase =>
        val regionExecution = workflowExecution.getRegionExecution(region.id)
        if (
          region.getOperators.forall { op =>
            val operatorExecution = regionExecution.getOperatorExecution(op.id)
            op.dependeeInputs.forall { dependeePortId =>
              operatorExecution.isInputPortCompleted(dependeePortId)
            }
          }
        ) {
          // All dependee ports are completed. Can proceed with the next phase.
          executeNonDependeePortPhase()
        } else {
          // Some dependee ports are still executing. Continue with this phase.
          Future.Unit
        }
      case ExecutingNonDependeePortsPhase =>
        tryCompleteRegionExecution()
      case Completed =>
        // Already completed, no further action needed.
        Future.Unit
    }

  private def executeDependeePortPhase(): Future[Unit] = {
    currentPhaseRef.set(ExecutingDependeePortsPhase)
    if (!region.getOperators.exists(_.dependeeInputs.nonEmpty)) {
      // Skip to the next phase when there are no dependee input ports
      return syncStatusAndTransitionRegionExecutionPhase()
    }
    val ops = region.getOperators.filter(_.dependeeInputs.nonEmpty)

    launchPhaseExecutionInternal(
      ops,
      () => assignPorts(region, isDependeePhase = true),
      () => Future.value(Seq.empty),
      () => sendStarts(region, isDependeePhase = true)
    )
  }

  private def executeNonDependeePortPhase(): Future[Unit] = {
    currentPhaseRef.set(ExecutingNonDependeePortsPhase)
    // Allocate output port storage objects
    region.resourceConfig.get.portConfigs
      .collect {
        case (id, cfg: OutputPortConfig) => id -> cfg
      }
      .foreach {
        case (pid, cfg) =>
          createOutputPortStorageObjects(Map(pid -> cfg))
      }

    val ops = region.getOperators.filter(_.dependeeInputs.isEmpty)

    launchPhaseExecutionInternal(
      ops,
      () => assignPorts(region, isDependeePhase = false),
      () => connectChannels(region.getLinks),
      () => sendStarts(region, isDependeePhase = false)
    )
  }

  /**
    * Unified logic for launching either of the two phases asynchronously.
    */
  private def launchPhaseExecutionInternal(
      operatorsToRun: Set[PhysicalOp],
      assignPortsLogic: () => Future[Seq[EmptyReturn]],
      connectChannelsLogic: () => Future[Seq[EmptyReturn]],
      startWorkersLogic: () => Future[Seq[Unit]]
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
      .flatMap(_ => assignPortsLogic())
      .flatMap(_ => connectChannelsLogic())
      .flatMap(_ => openOperators(operatorsToRun))
      .flatMap(_ => startWorkersLogic())
      .unit
  }

  /**
    * Initialize the execution states of all the operators in the region, and also create workers for each operator.
    */
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

  private def assignPorts(
      region: Region,
      isDependeePhase: Boolean
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
                isDependeePhase == physicalOp.dependeeInputs.contains(portId)
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

          // Currently an output port uses the same AssignPortRequest as an Input port.
          // However, an output port does not need a list of URIs or partitionings.
          // TODO: Separate AssignPortRequest for Input and Output Ports

          // assign output ports (only for non-dependee phase)
          val outputPortMapping =
            if (isDependeePhase) {
              Iterable.empty
            } else {
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
                            if gid == GlobalPortIdentity(
                              opId = physicalOp.id,
                              portId = outputPortId
                            ) =>
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

  private def sendStarts(
      region: Region,
      isDependeePhase: Boolean
  ): Future[Seq[Unit]] = {
    asyncRPCClient.sendToClient(
      ExecutionStatsUpdate(
        workflowExecution.getAllRegionExecutionsStats
      )
    )
    val allStarterOperators = region.getStarterOperators
    val starterOpsForThisPhase =
      if (isDependeePhase) allStarterOperators.filter(_.dependeeInputs.nonEmpty)
      else allStarterOperators
    Future.collect(
      starterOpsForThisPhase
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
                    .update(System.nanoTime(), resp.state)
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

  override def actorId: ActorVirtualIdentity = CONTROLLER
}
