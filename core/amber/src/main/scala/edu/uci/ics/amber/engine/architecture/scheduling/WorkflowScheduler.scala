package edu.uci.ics.amber.engine.architecture.scheduling

import akka.actor.{ActorContext, Address}
import com.twitter.util.Future
import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  WorkerAssignmentUpdate,
  WorkflowStatusUpdate
}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkersHandler.LinkWorkers
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.deploysemantics.locationpreference.AddressInfo
import edu.uci.ics.amber.engine.architecture.linksemantics.LinkStrategy
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkSenderActorRef
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
    availableNodes: Array[Address],
    networkCommunicationActor: NetworkSenderActorRef,
    ctx: ActorContext,
    asyncRPCClient: AsyncRPCClient,
    logger: Logger,
    workflow: Workflow,
    controllerConf: ControllerConfig
) {
  val schedulingPolicy: SchedulingPolicy =
    SchedulingPolicy.createPolicy(Constants.schedulingPolicyName, workflow, ctx)

  // Since one operator/link(i.e. links within an operator) can belong to multiple regions, we need to keep
  // track of those already built
  private val builtOperators = new mutable.HashSet[LayerIdentity]()
  private val openedOperators = new mutable.HashSet[LayerIdentity]()
  private val initializedPythonOperators = new mutable.HashSet[LayerIdentity]()
  private val activatedLink = new mutable.HashSet[LinkIdentity]()

  private val constructingRegions = new mutable.HashSet[PipelinedRegionIdentity]()
  private val startedRegions = new mutable.HashSet[PipelinedRegionIdentity]()

  def startWorkflow(): Future[Seq[Unit]] = {
    doSchedulingWork(schedulingPolicy.startWorkflow())
  }

  def onWorkerCompletion(workerId: ActorVirtualIdentity): Future[Seq[Unit]] = {
    doSchedulingWork(schedulingPolicy.onWorkerCompletion(workerId))
  }

  def onLinkCompletion(linkId: LinkIdentity): Future[Seq[Unit]] = {
    doSchedulingWork(schedulingPolicy.onLinkCompletion(linkId))
  }

  def onTimeSlotExpired(timeExpiredRegions: Set[PipelinedRegion]): Future[Seq[Unit]] = {
    val nextRegions = schedulingPolicy.onTimeSlotExpired()
    var regionsToPause: Set[PipelinedRegion] = Set()
    if (nextRegions.nonEmpty) {
      regionsToPause = timeExpiredRegions
    }

    doSchedulingWork(nextRegions)
      .flatMap(_ => {
        val pauseFutures = new ArrayBuffer[Future[Unit]]()
        regionsToPause.foreach(stoppingRegion => {
          schedulingPolicy.removeFromRunningRegion(Set(stoppingRegion))
          workflow
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

  private def doSchedulingWork(regions: Set[PipelinedRegion]): Future[Seq[Unit]] = {
    if (regions.nonEmpty) {
      Future.collect(regions.toArray.map(r => scheduleRegion(r)))
    } else {
      Future(Seq())
    }
  }

  private def constructRegion(region: PipelinedRegion): Unit = {
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
          buildOperator(op, controllerConf)
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
      operatorIdentity: LayerIdentity,
      controllerConf: ControllerConfig
  ): Unit = {
    val workerLayer = workflow.getOperator(operatorIdentity)
    workerLayer.build(
      AddressInfo(availableNodes, ctx.self.path.address),
      networkCommunicationActor,
      ctx,
      workflow.workerToOpExecConfig,
      controllerConf
    )
  }
  private def initializePythonOperators(region: PipelinedRegion): Future[Seq[Unit]] = {
    val allOperatorsInRegion =
      region.getOperators() ++ region.blockingDownstreamOperatorsInOtherRegions.map(_._1)
    val uninitializedPythonOperators = workflow.getPythonOperators(
      allOperatorsInRegion.filter(opId => !initializedPythonOperators.contains(opId))
    )
    Future
      .collect(
        // initialize python operator code
        workflow
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
            asyncRPCClient.send(
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

  private def activateAllLinks(region: PipelinedRegion): Future[Seq[Unit]] = {
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
        workflow
          .getAllWorkersForOperators(allNotOpenedOperators)
          .map { workerID =>
            asyncRPCClient.send(OpenOperator(), workerID)
          }
          .toSeq
      )
      .onSuccess(_ => allNotOpenedOperators.foreach(opId => openedOperators.add(opId)))
  }

  private def startRegion(region: PipelinedRegion): Future[Seq[Unit]] = {
    val allOperatorsInRegion =
      region.getOperators() ++ region.blockingDownstreamOperatorsInOtherRegions.map(_._1)

    allOperatorsInRegion
      .filter(opId => workflow.getOperator(opId).getState == WorkflowAggregatedState.UNINITIALIZED)
      .foreach(opId => workflow.getOperator(opId).setAllWorkerState(READY))
    asyncRPCClient.sendToClient(WorkflowStatusUpdate(workflow.getWorkflowStatus))

    if (!schedulingPolicy.getRunningRegions().contains(region)) {
      Future
        .collect(
          workflow
            .getAllWorkersForOperators(workflow.getSourcesOfRegion(region))
            .map(worker =>
              asyncRPCClient
                .send(StartWorker(), worker)
                .map(ret =>
                  // update worker state
                  workflow.getWorkerInfo(worker).state = ret
                )
            )
        )
    } else {
      throw new WorkflowRuntimeException(
        s"Start region called on an already running region: ${region.getOperators().mkString(",")}"
      )
    }
  }

  private def prepareAndStartRegion(region: PipelinedRegion): Future[Unit] = {
    asyncRPCClient.sendToClient(WorkflowStatusUpdate(workflow.getWorkflowStatus))
    asyncRPCClient.sendToClient(
      WorkerAssignmentUpdate(
        workflow.getOperatorToWorkers
          .map({
            case (opId: LayerIdentity, workerIds: Seq[ActorVirtualIdentity]) =>
              opId.operator -> workerIds.map(_.name)
          })
          .toMap
      )
    )
    Future(())
      .flatMap(_ => initializePythonOperators(region))
      .flatMap(_ => activateAllLinks(region))
      .flatMap(_ => openAllOperators(region))
      .flatMap(_ => startRegion(region))
      .map(_ => {
        constructingRegions.remove(region.getId())
        schedulingPolicy.addToRunningRegions(Set(region))
        startedRegions.add(region.getId())
      })
  }

  private def resumeRegion(region: PipelinedRegion): Future[Unit] = {
    if (!schedulingPolicy.getRunningRegions().contains(region)) {
      Future
        .collect(
          workflow
            .getAllWorkersOfRegion(region)
            .map(worker =>
              asyncRPCClient
                .send(SchedulerTimeSlotEvent(false), worker)
            )
            .toSeq
        )
        .map { _ =>
          schedulingPolicy.addToRunningRegions(Set(region))
        }
    } else {
      throw new WorkflowRuntimeException(
        s"Resume region called on an already running region: ${region.getOperators().mkString(",")}"
      )
    }

  }

  private def scheduleRegion(region: PipelinedRegion): Future[Unit] = {
    if (constructingRegions.contains(region.getId())) {
      return Future(())
    }
    if (!startedRegions.contains(region.getId())) {
      constructingRegions.add(region.getId())
      constructRegion(region)
      prepareAndStartRegion(region)
    } else {
      // region has already been constructed. Just needs to resume
      resumeRegion(region)
    }

  }

}
