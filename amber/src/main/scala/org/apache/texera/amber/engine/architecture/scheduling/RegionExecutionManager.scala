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

package org.apache.texera.amber.engine.architecture.scheduling

import org.apache.pekko.pattern.gracefulStop
import com.twitter.util.{Duration => TwitterDuration, Future, JavaTimer, Return, Throw, Timer}
import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.storage.{DocumentFactory, VFSURIFactory}
import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.core.workflow.{GlobalPortIdentity, PhysicalLink, PhysicalOp}
import org.apache.texera.amber.engine.architecture.common.{
  PekkoActorRefMappingService,
  PekkoActorService,
  ExecutorDeployment
}
import org.apache.texera.amber.engine.architecture.coordinator.execution.{
  OperatorExecution,
  RegionExecution,
  WorkflowExecution
}
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorConfig,
  ExecutionStatsUpdate,
  OperatorPortResultUriAvailable,
  RuntimeStatisticsPersist,
  WorkerAssignmentUpdate
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands._
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import org.apache.texera.amber.engine.architecture.scheduling.config.{
  InputPortConfig,
  OperatorConfig,
  OutputPortConfig,
  ResourceConfig
}
import org.apache.texera.amber.engine.architecture.sendsemantics.partitionings.Partitioning
import org.apache.texera.amber.engine.architecture.worker.statistics.WorkerState
import org.apache.texera.amber.engine.common.AmberLogging
import org.apache.texera.amber.engine.common.FutureBijection._
import org.apache.texera.amber.engine.common.rpc.AsyncRPCClient
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.apache.texera.web.SessionState
import org.apache.texera.web.model.websocket.event.RegionStateEvent

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.{Duration => ScalaDuration}

object RegionExecutionManager {

  // Max EndWorker retries before termination fails. ~30s at DefaultKillRetryDelay (200ms).
  private[scheduling] val DefaultMaxTerminationAttempts: Int = 150

  private[scheduling] val DefaultKillRetryDelay: TwitterDuration =
    TwitterDuration.fromMilliseconds(200)
}

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
class RegionExecutionManager(
    region: Region,
    isRestart: Boolean,
    workflowExecution: WorkflowExecution,
    asyncRPCClient: AsyncRPCClient,
    coordinatorConfig: CoordinatorConfig,
    actorService: PekkoActorService,
    actorRefService: PekkoActorRefMappingService,
    maxTerminationAttempts: Int = RegionExecutionManager.DefaultMaxTerminationAttempts,
    killRetryDelay: TwitterDuration = RegionExecutionManager.DefaultKillRetryDelay,
    // Loop-back write addresses (Loop Start logical op id -> its input port's
    // state URI), shipped to every worker in InitializeExecutorRequest. See
    // WorkflowExecutionManager.loopStartStateUris.
    loopStartStateUris: Map[String, String] = Map.empty
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
  private val terminationFutureRef: AtomicReference[Future[Unit]] = new AtomicReference(null)
  private val killRetryTimer: Timer = new JavaTimer(true)

  /**
    * Sync the status of `RegionExecution` and transition this manager's phase to `Completed` only when the
    * manager is currently in `ExecutingNonDependeePortsPhase`, all the ports of this region are completed, and
    * all workers in this region are terminated.
    *
    * Additionally, this method will also terminate all the workers of this region:
    *
    * 1.  An `EndWorker` control message is first sent to all the workers. This will be the last message each worker
    * receives. We wait for all workers have replied to indicate they have finished processing all control messages.
    *
    * 2. Only after all workers have processed all control messages do we send a `gracefulStop` (pekko message) to each
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

    val existingTerminationFuture = terminationFutureRef.get
    if (existingTerminationFuture != null) {
      existingTerminationFuture
    } else {
      val terminationFuture = terminateWorkersWithRetry(regionExecution).flatMap { _ =>
        // Set this manager's status to be completed so that subsequent regions can be started by
        // WorkflowExecutionManager.
        setPhase(Completed)
        Future.Unit
      }
      if (terminationFutureRef.compareAndSet(null, terminationFuture)) {
        terminationFuture
      } else {
        terminationFutureRef.get
      }
    }
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
                // Restarted regions reuse actorId. Remove stale control channels so the
                // coordinator does not reuse old control-message sequence numbers for new workers.
                asyncRPCClient.inputGateway.removeControlChannel(workerId)
                asyncRPCClient.outputGateway.removeControlChannel(workerId)
                gracefulStop(actorRef, ScalaDuration(5, TimeUnit.SECONDS)).asTwitter()
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

  private def terminateWorkersWithRetry(
      regionExecution: RegionExecution,
      attempt: Int = 1
  ): Future[Unit] = {
    terminateWorkers(regionExecution).rescue {
      case err if attempt >= maxTerminationAttempts =>
        val workerIds = regionExecution.getAllOperatorExecutions.flatMap {
          case (_, opExec) => opExec.getWorkerIds
        }.toSeq
        val attemptsLabel = if (attempt == 1) "1 attempt" else s"$attempt attempts"
        logger.error(
          s"Region ${region.id.id} could not be terminated after $attemptsLabel; giving up. " +
            s"Workers still not terminated: ${workerIds.mkString(", ")}.",
          err
        )
        Future.exception(
          new IllegalStateException(
            s"Region ${region.id.id} could not be terminated after $attemptsLabel " +
              s"(workers still not terminated: ${workerIds.mkString(", ")}).",
            err
          )
        )
      case err =>
        logger.warn(
          s"Failed to terminate region ${region.id.id} on attempt $attempt of $maxTerminationAttempts. " +
            s"Retrying in ${killRetryDelay.inMilliseconds} ms.",
          err
        )
        Future
          .sleep(killRetryDelay)(killRetryTimer)
          .flatMap(_ => terminateWorkersWithRetry(regionExecution, attempt + 1))
    }
  }

  def isCompleted: Boolean = currentPhaseRef.get == Completed

  /**
    * Returns the region termination future if termination has been initiated.
    * This is only set by `tryCompleteRegionExecution()`.
    */
  def getTerminationFutureOpt: Option[Future[Unit]] = Option(terminationFutureRef.get)

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
    setPhase(ExecutingDependeePortsPhase)
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
    setPhase(ExecutingNonDependeePortsPhase)
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

    val stats = workflowExecution.getAllRegionExecutionsStats
    asyncRPCClient.sendToClient(ExecutionStatsUpdate(stats))
    asyncRPCClient.sendToClient(RuntimeStatisticsPersist(stats))
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
      actorService: PekkoActorService,
      physicalOp: PhysicalOp,
      operatorConfig: OperatorConfig,
      operatorExecution: OperatorExecution
  ): Unit = {
    ExecutorDeployment.createWorkers(
      physicalOp,
      actorService,
      operatorExecution,
      operatorConfig,
      coordinatorConfig.stateRestoreConfOpt,
      coordinatorConfig.faultToleranceConfOpt
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
                  physicalOp.isSourceOperator,
                  loopStartStateUris
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
                          cfg.storageURIBase.toString
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
        asyncRPCClient.coordinatorInterface.linkWorkers(
          LinkWorkersRequest(link),
          asyncRPCClient.mkContext(COORDINATOR)
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
    val stats = workflowExecution.getAllRegionExecutionsStats
    asyncRPCClient.sendToClient(ExecutionStatsUpdate(stats))
    asyncRPCClient.sendToClient(RuntimeStatisticsPersist(stats))
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
        val portBaseURI = portConfig.storageURIBase
        val resultURI = VFSURIFactory.resultURI(portBaseURI)
        val stateURI = VFSURIFactory.stateURI(portBaseURI)
        val schema =
          region.getOperator(outputPortId.opId).outputPorts(outputPortId.portId)._3 match {
            case Right(resolvedSchema) => resolvedSchema
            case Left(cause)           =>
              // The output port schema failed to resolve (e.g. a dataset the workflow reads is not
              // shared with the running user, making its file and inferred schema unavailable).
              // Surface the underlying cause instead of a generic "Schema is missing" (issue #3546).
              val reason = Option(cause.getMessage).getOrElse(cause.toString)
              logger.error(s"Output schema unavailable for port $outputPortId", cause)
              throw new IllegalStateException(
                s"Failed to resolve the output schema: $reason",
                cause
              )
          }
        // An output port whose storage accumulates across region re-executions
        // (e.g. a LoopEnd port, whose output builds up over the iterations of
        // its own loop) sets `reuseStorage`. When set, the port's existing
        // document is kept and reopened on each re-run; when unset, a fresh one
        // is created. Read per output port -- storage behavior is port-specific.
        // (The inner LoopEnd of a nested loop additionally drops its output
        // once per outer iteration on the Python worker side in
        // MainLoop._process_state_frame, which is orthogonal to this.)
        val reuseStorage =
          region
            .getOperator(outputPortId.opId)
            .outputPorts(outputPortId.portId)
            ._1
            .reuseStorage
        Seq((resultURI, schema), (stateURI, State.schema)).foreach {
          case (uri, sch) =>
            DocumentFactory.createOrReuseDocument(uri, sch, reuseStorage)
        }
        if (!isRestart) {
          asyncRPCClient.sendToClient(
            OperatorPortResultUriAvailable(outputPortId, resultURI)
          )
        }
    }
  }

  private def setPhase(phase: RegionExecutionPhase): Unit = {
    currentPhaseRef.set(phase)
    SessionState.getAllSessionStates.foreach { state =>
      state.send(RegionStateEvent(region.id.id, phase.toString))
    }
  }

  override def actorId: ActorVirtualIdentity = COORDINATOR
}
