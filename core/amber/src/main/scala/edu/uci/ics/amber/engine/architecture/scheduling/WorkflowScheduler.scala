package edu.uci.ics.amber.engine.architecture.scheduling

import akka.actor.{ActorContext, Address}
import com.twitter.util.Future
import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  WorkerAssignmentUpdate,
  WorkflowStatusUpdate
}
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkersHandler.LinkWorkers
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.linksemantics.LinkStrategy
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkSenderActorRef
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.InitializeOperatorLogicHandler.InitializeOperatorLogic
import edu.uci.ics.amber.engine.architecture.scheduling.policies.SchedulingPolicy
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.OpenOperatorHandler.OpenOperator
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SchedulerTimeSlotEventHandler.SchedulerTimeSlotEvent
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.READY
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.{Constants, ISourceOperatorExecutor}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LinkIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.PythonUDFOpExecV2

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
  private val builtOperators = new mutable.HashSet[OperatorIdentity]()
  private val openedOperators = new mutable.HashSet[OperatorIdentity]()
  private val initializedPythonOperators = new mutable.HashSet[OperatorIdentity]()
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
    val builtOpsInRegion = new mutable.HashSet[OperatorIdentity]()
    var frontier: Iterable[OperatorIdentity] = workflow.getSourcesOfRegion(region)
    while (frontier.nonEmpty) {
      frontier.foreach { (op: OperatorIdentity) =>
        val prev: Array[(OperatorIdentity, WorkerLayer)] =
          workflow
            .getDirectUpstreamOperators(op)
            .filter(upStreamOp =>
              builtOperators.contains(upStreamOp) && region.getOperators().contains(upStreamOp)
            )
            .map(upStreamOp =>
              (
                upStreamOp,
                workflow.getOperator(upStreamOp).topology.layers.last
              )
            )
            .toArray // Last layer of upstream operators in the same region.
        if (!builtOperators.contains(op)) {
          buildOperator(prev, op)
          builtOperators.add(op)
        }
        builtOpsInRegion.add(op)
      }

      frontier = (region
        .getOperators() ++ region.blockingDowstreamOperatorsInOtherRegions)
        .filter(opId => {
          !builtOpsInRegion.contains(opId) && workflow
            .getDirectUpstreamOperators(opId)
            .filter(region.getOperators().contains)
            .forall(builtOperators.contains)
        })
    }
  }

  private def buildOperator(
      prev: Array[(OperatorIdentity, WorkerLayer)], // used to decide deployment of workers
      operatorIdentity: OperatorIdentity
  ): Unit = {
    val opExecConfig = workflow.getOperator(operatorIdentity)
    if (opExecConfig.topology.links.isEmpty) {
      opExecConfig.topology.layers.foreach(workerLayer => {
        workerLayer.build(
          prev.map(pair => (workflow.getOperator(pair._1), pair._2)),
          availableNodes,
          networkCommunicationActor,
          ctx,
          workflow.getInlinksIdsToWorkerLayer(workerLayer.id),
          workflow.workerToLayer,
          workflow.workerToOperatorExec,
          controllerConf.supportFaultTolerance
        )
      })
    } else {
      val operatorInLinks: Map[WorkerLayer, Set[WorkerLayer]] =
        opExecConfig.topology.links.groupBy(_.to).map(x => (x._1, x._2.map(_.from).toSet))
      var layers: Iterable[WorkerLayer] =
        opExecConfig.topology.links
          .filter(linkStrategy => opExecConfig.topology.links.forall(_.to != linkStrategy.from))
          .map(_.from) // the first layers in topological order in the operator
      layers.foreach(workerLayer => {
        workerLayer.build(
          prev.map(pair => (workflow.getOperator(pair._1), pair._2)),
          availableNodes,
          networkCommunicationActor,
          ctx,
          workflow.getInlinksIdsToWorkerLayer(workerLayer.id),
          workflow.workerToLayer,
          workflow.workerToOperatorExec,
          controllerConf.supportFaultTolerance
        )
      })
      layers = operatorInLinks.filter(x => x._2.forall(_.isBuilt)).keys
      while (layers.nonEmpty) {
        layers.foreach((layer: WorkerLayer) => {
          layer.build(
            operatorInLinks(layer).map(y => (null, y)).toArray,
            availableNodes,
            networkCommunicationActor,
            ctx,
            workflow.getInlinksIdsToWorkerLayer(layer.id),
            workflow.workerToLayer,
            workflow.workerToOperatorExec,
            controllerConf.supportFaultTolerance
          )
        })
        layers = operatorInLinks.filter(x => !x._1.isBuilt && x._2.forall(_.isBuilt)).keys
      }
    }
  }

  private def initializePythonOperators(region: PipelinedRegion): Future[Seq[Unit]] = {
    val allOperatorsInRegion =
      region.getOperators() ++ region.blockingDowstreamOperatorsInOtherRegions
    val uninitializedPythonOperators = workflow.getPythonOperators(
      allOperatorsInRegion.filter(opId => !initializedPythonOperators.contains(opId))
    )
    Future
      .collect(
        // initialize python operator code
        workflow
          .getPythonWorkerToOperatorExec(
            uninitializedPythonOperators
          )
          .map {
            case (workerID: ActorVirtualIdentity, pythonOperatorExec: PythonUDFOpExecV2) =>
              asyncRPCClient.send(
                InitializeOperatorLogic(
                  pythonOperatorExec.getCode,
                  workflow
                    .getInlinksIdsToWorkerLayer(workflow.workerToLayer(workerID).id)
                    .toArray,
                  pythonOperatorExec.isInstanceOf[ISourceOperatorExecutor],
                  pythonOperatorExec.getOutputSchema
                ),
                workerID
              )
          }
          .toSeq
      )
      .onSuccess(_ =>
        uninitializedPythonOperators.foreach(opId => initializedPythonOperators.add(opId))
      )
      .onFailure((err: Throwable) => {
        logger.error("Failure when sending Python UDF code", err)
        // report error to frontend
        asyncRPCClient.sendToClient(FatalError(err))
      })
  }

  private def activateAllLinks(region: PipelinedRegion): Future[Seq[Unit]] = {
    val allOperatorsInRegion =
      region.getOperators() ++ region.blockingDowstreamOperatorsInOtherRegions
    Future.collect(
      // activate all links
      workflow.getAllLinks
        .filter(link => {
          !activatedLink.contains(link.id) &&
            allOperatorsInRegion.contains(
              OperatorIdentity(link.from.id.workflow, link.from.id.operator)
            ) &&
            allOperatorsInRegion.contains(
              OperatorIdentity(link.to.id.workflow, link.to.id.operator)
            )
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
      region.getOperators() ++ region.blockingDowstreamOperatorsInOtherRegions
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
      region.getOperators() ++ region.blockingDowstreamOperatorsInOtherRegions

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
            case (opId: OperatorIdentity, workerIds: Seq[ActorVirtualIdentity]) =>
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
