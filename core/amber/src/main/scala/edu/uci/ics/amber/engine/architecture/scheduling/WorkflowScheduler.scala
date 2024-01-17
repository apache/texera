package edu.uci.ics.amber.engine.architecture.scheduling

import com.twitter.util.Future
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.common.{AkkaActorRefMappingService, AkkaActorService}
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  WorkerAssignmentUpdate,
  WorkflowStatusUpdate
}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkersHandler.LinkWorkers
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, ExecutionState, Workflow}
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalLink
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.InitializeOperatorLogicHandler.InitializeOperatorLogic
import edu.uci.ics.amber.engine.architecture.scheduling.config.OperatorConfig
import edu.uci.ics.amber.engine.architecture.scheduling.policies.SchedulingPolicy
import edu.uci.ics.amber.engine.architecture.worker.controlcommands.LinkOrdinal
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.OpenOperatorHandler.OpenOperator
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SchedulerTimeSlotEventHandler.SchedulerTimeSlotEvent
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.READY
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  PhysicalLinkIdentity,
  PhysicalOpIdentity
}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class WorkflowScheduler(
    regionsToSchedule: mutable.Buffer[Region],
    executionState: ExecutionState,
    controllerConfig: ControllerConfig,
    asyncRPCClient: AsyncRPCClient
) extends LazyLogging {
  val schedulingPolicy: SchedulingPolicy =
    SchedulingPolicy.createPolicy(
      AmberConfig.schedulingPolicyName,
      regionsToSchedule
    )

  // Since one operator/link(i.e. links within an operator) can belong to multiple regions, we need to keep
  // track of those already built
  private val builtPhysicalOpIds = new mutable.HashSet[PhysicalOpIdentity]()
  private val openedOperators = new mutable.HashSet[PhysicalOpIdentity]()
  private val initializedPythonOperators = new mutable.HashSet[PhysicalOpIdentity]()
  private val activatedLink = new mutable.HashSet[PhysicalLinkIdentity]()

  private val constructingRegions = new mutable.HashSet[RegionIdentity]()
  private val startedRegions = new mutable.HashSet[RegionIdentity]()

  def startWorkflow(
      workflow: Workflow,
      akkaActorRefMappingService: AkkaActorRefMappingService,
      akkaActorService: AkkaActorService
  ): Future[Seq[Unit]] = {
    val nextRegionsToSchedule = schedulingPolicy.startWorkflow(workflow)
    doSchedulingWork(workflow, nextRegionsToSchedule, akkaActorService)
  }

  def onWorkerCompletion(
      workflow: Workflow,
      akkaActorRefMappingService: AkkaActorRefMappingService,
      akkaActorService: AkkaActorService,
      workerId: ActorVirtualIdentity
  ): Future[Seq[Unit]] = {
    val nextRegionsToSchedule =
      schedulingPolicy.onWorkerCompletion(workflow, executionState, workerId)
    doSchedulingWork(workflow, nextRegionsToSchedule, akkaActorService)
  }

  def onLinkCompletion(
      workflow: Workflow,
      akkaActorRefMappingService: AkkaActorRefMappingService,
      akkaActorService: AkkaActorService,
      linkId: PhysicalLinkIdentity
  ): Future[Seq[Unit]] = {
    val nextRegionsToSchedule = schedulingPolicy.onLinkCompletion(workflow, executionState, linkId)
    doSchedulingWork(workflow, nextRegionsToSchedule, akkaActorService)
  }

  def onTimeSlotExpired(
      workflow: Workflow,
      timeExpiredRegions: Set[Region],
      akkaActorRefMappingService: AkkaActorRefMappingService,
      akkaActorService: AkkaActorService
  ): Future[Seq[Unit]] = {
    val nextRegions = schedulingPolicy.onTimeSlotExpired(workflow)
    var regionsToPause: Set[Region] = Set()
    if (nextRegions.nonEmpty) {
      regionsToPause = timeExpiredRegions
    }

    doSchedulingWork(workflow, nextRegions, akkaActorService)
      .flatMap(_ => {
        val pauseFutures = new ArrayBuffer[Future[Unit]]()
        regionsToPause.foreach(stoppingRegion => {
          schedulingPolicy.removeFromRunningRegion(Set(stoppingRegion))
          executionState
            .getAllWorkersOfRegion(stoppingRegion)
            .foreach(wid => {
              pauseFutures.append(
                asyncRPCClient
                  .send(SchedulerTimeSlotEvent(true), wid)
              )
            })
        })
        Future.collect(pauseFutures)
      })
      .map(_ => {
        Seq()
      })
  }

  private def doSchedulingWork(
      workflow: Workflow,
      regions: Set[Region],
      actorService: AkkaActorService
  ): Future[Seq[Unit]] = {
    if (regions.nonEmpty) {
      Future.collect(
        regions.toArray.map(r => scheduleRegion(workflow, r, actorService))
      )
    } else {
      Future(Seq())
    }
  }

  private def constructRegion(
      workflow: Workflow,
      region: Region,
      akkaActorService: AkkaActorService
  ): Unit = {
    val builtOpsInRegion = new mutable.HashSet[PhysicalOpIdentity]()
    var frontier = region.sourcePhysicalOpIds
    while (frontier.nonEmpty) {
      frontier.foreach { (physicalOpId: PhysicalOpIdentity) =>
        if (!builtPhysicalOpIds.contains(physicalOpId)) {
          buildOperator(
            workflow,
            physicalOpId,
            region.config.get.operatorConfigs(physicalOpId),
            akkaActorService
          )
          builtPhysicalOpIds.add(physicalOpId)
        }
        builtOpsInRegion.add(physicalOpId)
      }

      frontier = region.getEffectiveOperators
        .filter(physicalOpId => {
          !builtOpsInRegion.contains(physicalOpId) && workflow.physicalPlan
            .getUpstreamPhysicalOpIds(physicalOpId)
            .intersect(region.physicalOpIds)
            .forall(builtPhysicalOpIds.contains)
        })
    }

  }

  private def buildOperator(
      workflow: Workflow,
      physicalOpId: PhysicalOpIdentity,
      operatorConfig: OperatorConfig,
      controllerActorService: AkkaActorService
  ): Unit = {
    val physicalOp = workflow.physicalPlan.getOperator(physicalOpId)
    val opExecution = executionState.initOperatorState(physicalOpId, operatorConfig)
    physicalOp.build(
      controllerActorService,
      opExecution,
      operatorConfig,
      controllerConfig.workerRestoreConfMapping,
      controllerConfig.workerLoggingConfMapping
    )
  }
  private def initializePythonOperators(region: Region): Future[Seq[Unit]] = {
    val allOperatorsInRegion = region.getEffectiveOperators
    val uninitializedPythonOperators = executionState.filterPythonPhysicalOpIds(
      allOperatorsInRegion.diff(initializedPythonOperators)
    )
    Future
      .collect(
        // initialize python operator code
        executionState
          .getPythonWorkerToOperatorExec(uninitializedPythonOperators)
          .map {
            case (workerId, pythonUDFPhysicalOp) =>
              val inputMappingList = pythonUDFPhysicalOp.inputPortToLinkIdMapping.flatMap {
                case (portIdx, linkIds) => linkIds.map(linkId => LinkOrdinal(linkId, portIdx))
              }.toList
              val outputMappingList = pythonUDFPhysicalOp.outputPortToLinkIdMapping.flatMap {
                case (portIdx, linkIds) => linkIds.map(linkId => LinkOrdinal(linkId, portIdx))
              }.toList
              asyncRPCClient
                .send(
                  InitializeOperatorLogic(
                    pythonUDFPhysicalOp.getPythonCode,
                    pythonUDFPhysicalOp.isSourceOperator,
                    inputMappingList,
                    outputMappingList,
                    pythonUDFPhysicalOp.getOutputSchema
                  ),
                  workerId
                )
          }
          .toSeq
      )
      .onSuccess(_ =>
        uninitializedPythonOperators.foreach(opId => initializedPythonOperators.add(opId))
      )
  }

  private def activateAllLinks(workflow: Workflow, region: Region): Future[Seq[Unit]] = {
    val allOperatorsInRegion = region.getEffectiveOperators
    Future.collect(
      // activate all links
      workflow.physicalPlan.links
        .filter(link => {
          !activatedLink.contains(link.id) &&
            allOperatorsInRegion.contains(link.fromOp.id) &&
            allOperatorsInRegion.contains(link.toOp.id)
        })
        .map { link: PhysicalLink =>
          asyncRPCClient
            .send(LinkWorkers(link.id), CONTROLLER)
            .onSuccess(_ => activatedLink.add(link.id))
        }
        .toSeq
    )
  }

  private def openAllOperators(region: Region): Future[Seq[Unit]] = {
    val allOperatorsInRegion = region.getEffectiveOperators
    val allNotOpenedOperators =
      allOperatorsInRegion.diff(openedOperators)
    Future
      .collect(
        executionState
          .getAllWorkersForOperators(allNotOpenedOperators)
          .map { workerID =>
            asyncRPCClient.send(OpenOperator(), workerID)
          }
          .toSeq
      )
      .onSuccess(_ => allNotOpenedOperators.foreach(opId => openedOperators.add(opId)))
  }

  private def startRegion(workflow: Workflow, region: Region): Future[Seq[Unit]] = {
    val allOperatorsInRegion = region.getEffectiveOperators

    allOperatorsInRegion
      .filter(opId =>
        executionState.getOperatorExecution(opId).getState == WorkflowAggregatedState.UNINITIALIZED
      )
      .foreach(opId => executionState.getOperatorExecution(opId).setAllWorkerState(READY))
    asyncRPCClient.sendToClient(WorkflowStatusUpdate(executionState.getWorkflowStatus))

    val ops = region.sourcePhysicalOpIds
    if (!schedulingPolicy.getRunningRegions.contains(region)) {
      val futures = ops.flatMap { opId =>
        val opExecution = executionState.getOperatorExecution(opId)
        opExecution.getBuiltWorkerIds
          .map(worker =>
            asyncRPCClient
              .send(StartWorker(), worker)
              .map(ret =>
                // update worker state
                opExecution.getWorkerInfo(worker).state = ret
              )
          )
      }.toSeq
      Future.collect(futures)
    } else {
      throw new WorkflowRuntimeException(
        s"Start region called on an already running region: ${region.physicalOpIds.mkString(",")}"
      )
    }
  }

  private def prepareAndStartRegion(
      workflow: Workflow,
      region: Region,
      actorService: AkkaActorService
  ): Future[Unit] = {
    asyncRPCClient.sendToClient(WorkflowStatusUpdate(executionState.getWorkflowStatus))
    asyncRPCClient.sendToClient(
      WorkerAssignmentUpdate(
        region.getEffectiveOperators
          .map(physicalOpId => {
            physicalOpId.logicalOpId.id -> executionState
              .getOperatorExecution(physicalOpId)
              .getBuiltWorkerIds
              .map(_.name)
              .toList
          })
          .toMap
      )
    )
    Future(())
      .flatMap(_ => initializePythonOperators(region))
      .flatMap(_ => activateAllLinks(workflow, region))
      .flatMap(_ => openAllOperators(region))
      .flatMap(_ => startRegion(workflow, region))
      .map(_ => {
        constructingRegions.remove(region.id)
        schedulingPolicy.addToRunningRegions(Set(region), actorService)
        startedRegions.add(region.id)
      })
  }

  private def resumeRegion(
      region: Region,
      actorService: AkkaActorService
  ): Future[Unit] = {
    if (!schedulingPolicy.getRunningRegions.contains(region)) {
      Future
        .collect(
          executionState
            .getAllWorkersOfRegion(region)
            .map(worker =>
              asyncRPCClient
                .send(SchedulerTimeSlotEvent(false), worker)
            )
            .toSeq
        )
        .map { _ =>
          schedulingPolicy.addToRunningRegions(Set(region), actorService)
        }
    } else {
      throw new WorkflowRuntimeException(
        s"Resume region called on an already running region: ${region.physicalOpIds.mkString(",")}"
      )
    }

  }

  private def scheduleRegion(
      workflow: Workflow,
      region: Region,
      actorService: AkkaActorService
  ): Future[Unit] = {
    if (constructingRegions.contains(region.id)) {
      return Future(())
    }
    if (!startedRegions.contains(region.id)) {
      constructingRegions.add(region.id)

      constructRegion(workflow, region, actorService)
      prepareAndStartRegion(workflow, region, actorService).rescue {
        case err: Throwable =>
          // this call may come from client or worker(by execution completed)
          // thus we need to force it to send error to client.
          asyncRPCClient.sendToClient(FatalError(err, None))
          Future.Unit
      }
    } else {
      // region has already been constructed. Just needs to resume
      resumeRegion(region, actorService)
    }

  }

}
