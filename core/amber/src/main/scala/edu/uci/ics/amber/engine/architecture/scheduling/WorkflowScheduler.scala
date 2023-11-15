package edu.uci.ics.amber.engine.architecture.scheduling

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.common.{AkkaActorRefMappingService, AkkaActorService}
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  WorkerAssignmentUpdate,
  WorkflowStatusUpdate
}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkersHandler.LinkWorkers
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerConfig,
  ExecutionState,
  OperatorExecution,
  Workflow
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.linksemantics.LinkStrategy
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.InitializeOperatorLogicHandler.InitializeOperatorLogic
import edu.uci.ics.amber.engine.architecture.scheduling.policies.SchedulingPolicy
import edu.uci.ics.amber.engine.architecture.worker.controlcommands.LinkOrdinal
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.OpenOperatorHandler.OpenOperator
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SchedulerTimeSlotEventHandler.SchedulerTimeSlotEvent
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.READY
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity
}
import edu.uci.ics.amber.engine.common.{Constants, ISourceOperatorExecutor}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState
import edu.uci.ics.texera.workflow.operators.udf.python.PythonUDFOpExecV2

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class WorkflowScheduler(
    regionsToSchedule: mutable.Buffer[PipelinedRegion],
    executionState: ExecutionState,
    controllerConfig: ControllerConfig,
    asyncRPCClient: AsyncRPCClient
) {
  val schedulingPolicy: SchedulingPolicy =
    SchedulingPolicy.createPolicy(
      Constants.schedulingPolicyName,
      regionsToSchedule
    )

  // Since one operator/link(i.e. links within an operator) can belong to multiple regions, we need to keep
  // track of those already built
  private val builtOperators = new mutable.HashSet[LayerIdentity]()
  private val openedOperators = new mutable.HashSet[LayerIdentity]()
  private val initializedPythonOperators = new mutable.HashSet[LayerIdentity]()
  private val activatedLink = new mutable.HashSet[LinkIdentity]()

  private val constructingRegions = new mutable.HashSet[PipelinedRegionIdentity]()
  private val startedRegions = new mutable.HashSet[PipelinedRegionIdentity]()

  def startWorkflow(
      workflow: Workflow,
      akkaActorRefMappingService: AkkaActorRefMappingService,
      akkaActorService: AkkaActorService
  ): Future[Seq[Unit]] = {
    val nextRegionsToSchedule = schedulingPolicy.startWorkflow(workflow)
    doSchedulingWork(workflow, nextRegionsToSchedule, akkaActorRefMappingService, akkaActorService)
  }

  def onWorkerCompletion(
      workflow: Workflow,
      akkaActorRefMappingService: AkkaActorRefMappingService,
      akkaActorService: AkkaActorService,
      workerId: ActorVirtualIdentity
  ): Future[Seq[Unit]] = {
    val nextRegionsToSchedule =
      schedulingPolicy.onWorkerCompletion(workflow, executionState, workerId)
    doSchedulingWork(workflow, nextRegionsToSchedule, akkaActorRefMappingService, akkaActorService)
  }

  def onLinkCompletion(
      workflow: Workflow,
      akkaActorRefMappingService: AkkaActorRefMappingService,
      akkaActorService: AkkaActorService,
      linkId: LinkIdentity
  ): Future[Seq[Unit]] = {
    val nextRegionsToSchedule = schedulingPolicy.onLinkCompletion(workflow, executionState, linkId)
    doSchedulingWork(workflow, nextRegionsToSchedule, akkaActorRefMappingService, akkaActorService)
  }

  def onTimeSlotExpired(
      workflow: Workflow,
      timeExpiredRegions: Set[PipelinedRegion],
      akkaActorRefMappingService: AkkaActorRefMappingService,
      akkaActorService: AkkaActorService
  ): Future[Seq[Unit]] = {
    val nextRegions = schedulingPolicy.onTimeSlotExpired(workflow)
    var regionsToPause: Set[PipelinedRegion] = Set()
    if (nextRegions.nonEmpty) {
      regionsToPause = timeExpiredRegions
    }

    doSchedulingWork(workflow, nextRegions, akkaActorRefMappingService, akkaActorService)
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
      regions: Set[PipelinedRegion],
      akkaActorRefMappingService: AkkaActorRefMappingService,
      actorService: AkkaActorService
  ): Future[Seq[Unit]] = {
    if (regions.nonEmpty) {
      Future.collect(
        regions.toArray.map(r =>
          scheduleRegion(workflow, r, akkaActorRefMappingService, actorService)
        )
      )
    } else {
      Future(Seq())
    }
  }

  private def constructRegion(
      workflow: Workflow,
      region: PipelinedRegion,
      akkaActorRefMappingService: AkkaActorRefMappingService,
      akkaActorService: AkkaActorService
  ): Unit = {
    val builtOpsInRegion = new mutable.HashSet[LayerIdentity]()
    var frontier: Iterable[LayerIdentity] = workflow.getSourcesOfRegion(region)
    while (frontier.nonEmpty) {
      frontier.foreach { (op: LayerIdentity) =>
        val prev: Array[(LayerIdentity, OpExecConfig)] =
          workflow.physicalPlan
            .getUpstream(op)
            .filter(upStreamOp =>
              builtOperators.contains(upStreamOp) && region.getOperators().contains(upStreamOp)
            )
            .map(upStreamOp => (upStreamOp, workflow.getOperator(upStreamOp)))
            .toArray // Last layer of upstream operators in the same region.
        if (!builtOperators.contains(op)) {
          buildOperator(
            workflow,
            op,
            executionState.getOperatorExecution(op),
            akkaActorRefMappingService,
            akkaActorService
          )
          builtOperators.add(op)
        }
        builtOpsInRegion.add(op)
      }

      frontier = (region
        .getOperators() ++ region.blockingDownstreamOperatorsInOtherRegions.map(_._1))
        .filter(opId => {
          !builtOpsInRegion.contains(opId) && workflow.physicalPlan
            .getUpstream(opId)
            .filter(region.getOperators().contains)
            .forall(builtOperators.contains)
        })
    }
  }

  private def buildOperator(
      workflow: Workflow,
      operatorIdentity: LayerIdentity,
      opExecution: OperatorExecution,
      actorRefService: AkkaActorRefMappingService,
      controllerActorService: AkkaActorService
  ): Unit = {
    val workerLayer = workflow.getOperator(operatorIdentity)
    workerLayer.build(
      controllerActorService,
      actorRefService,
      opExecution,
      controllerConfig
    )
  }
  private def initializePythonOperators(region: PipelinedRegion): Future[Seq[Unit]] = {
    val allOperatorsInRegion =
      region.getOperators() ++ region.blockingDownstreamOperatorsInOtherRegions.map(_._1)
    val uninitializedPythonOperators = executionState.getPythonOperators(
      allOperatorsInRegion.filter(opId => !initializedPythonOperators.contains(opId))
    )
    Future
      .collect(
        // initialize python operator code
        executionState
          .getPythonWorkerToOperatorExec(uninitializedPythonOperators)
          .map(p => {
            val workerID = p._1
            val pythonUDFOpExecConfig = p._2
            val pythonUDFOpExec = pythonUDFOpExecConfig
              .initIOperatorExecutor((0, pythonUDFOpExecConfig))
              .asInstanceOf[PythonUDFOpExecV2]

            val inputMappingList = pythonUDFOpExecConfig.inputToOrdinalMapping
              .map(kv => LinkOrdinal(kv._1, kv._2))
              .toList
            val outputMappingList = pythonUDFOpExecConfig.outputToOrdinalMapping
              .map(kv => LinkOrdinal(kv._1, kv._2))
              .toList
            asyncRPCClient
              .send(
                InitializeOperatorLogic(
                  pythonUDFOpExec.getCode,
                  pythonUDFOpExec.isInstanceOf[ISourceOperatorExecutor],
                  inputMappingList,
                  outputMappingList,
                  pythonUDFOpExec.getOutputSchema
                ),
                workerID
              )
          })
          .toSeq
      )
      .onSuccess(_ =>
        uninitializedPythonOperators.foreach(opId => initializedPythonOperators.add(opId))
      )
  }

  private def activateAllLinks(workflow: Workflow, region: PipelinedRegion): Future[Seq[Unit]] = {
    val allOperatorsInRegion =
      region.getOperators() ++ region.blockingDownstreamOperatorsInOtherRegions.map(_._1)
    Future.collect(
      // activate all links
      workflow.physicalPlan.linkStrategies.values
        .filter(link => {
          !activatedLink.contains(link.id) &&
            allOperatorsInRegion.contains(link.from.id) &&
            allOperatorsInRegion.contains(link.to.id)
        })
        .map { link: LinkStrategy =>
          asyncRPCClient
            .send(LinkWorkers(link.id), CONTROLLER)
            .onSuccess(_ => activatedLink.add(link.id))
        }
        .toSeq
    )
  }

  private def openAllOperators(region: PipelinedRegion): Future[Seq[Unit]] = {
    val allOperatorsInRegion =
      region.getOperators() ++ region.blockingDownstreamOperatorsInOtherRegions.map(_._1)
    val allNotOpenedOperators =
      allOperatorsInRegion.filter(opId => !openedOperators.contains(opId))
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

  private def startRegion(workflow: Workflow, region: PipelinedRegion): Future[Seq[Unit]] = {
    val allOperatorsInRegion =
      region.getOperators() ++ region.blockingDownstreamOperatorsInOtherRegions.map(_._1)

    allOperatorsInRegion
      .filter(opId =>
        executionState.getOperatorExecution(opId).getState == WorkflowAggregatedState.UNINITIALIZED
      )
      .foreach(opId => executionState.getOperatorExecution(opId).setAllWorkerState(READY))
    asyncRPCClient.sendToClient(WorkflowStatusUpdate(executionState.getWorkflowStatus))

    val ops = workflow.getSourcesOfRegion(region)
    if (!schedulingPolicy.getRunningRegions().contains(region)) {
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
        s"Start region called on an already running region: ${region.getOperators().mkString(",")}"
      )
    }
  }

  private def prepareAndStartRegion(
      workflow: Workflow,
      region: PipelinedRegion,
      actorService: AkkaActorService
  ): Future[Unit] = {
    asyncRPCClient.sendToClient(WorkflowStatusUpdate(executionState.getWorkflowStatus))
    asyncRPCClient.sendToClient(
      WorkerAssignmentUpdate(
        executionState.getOperatorToWorkers
          .map({
            case (opId: LayerIdentity, workerIds: Seq[ActorVirtualIdentity]) =>
              opId.operator -> workerIds.map(_.name)
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
        constructingRegions.remove(region.getId())
        schedulingPolicy.addToRunningRegions(Set(region), actorService)
        startedRegions.add(region.getId())
      })
  }

  private def resumeRegion(
      region: PipelinedRegion,
      actorService: AkkaActorService
  ): Future[Unit] = {
    if (!schedulingPolicy.getRunningRegions().contains(region)) {
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
        s"Resume region called on an already running region: ${region.getOperators().mkString(",")}"
      )
    }

  }

  private def scheduleRegion(
      workflow: Workflow,
      region: PipelinedRegion,
      akkaActorRefMappingService: AkkaActorRefMappingService,
      actorService: AkkaActorService
  ): Future[Unit] = {
    if (constructingRegions.contains(region.getId())) {
      return Future(())
    }
    if (!startedRegions.contains(region.getId())) {
      constructingRegions.add(region.getId())
      constructRegion(workflow, region, akkaActorRefMappingService, actorService)
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
