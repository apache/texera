package edu.uci.ics.amber.engine.architecture.scheduling

import akka.actor.{ActorContext, Address}
import com.twitter.util.Future
import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowStatusUpdate
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkersHandler.LinkWorkers
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartPipelinedRegionHandler.StartPipelinedRegion
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.linksemantics.{
  AllToOne,
  FullRoundRobin,
  HashBasedShuffle,
  LinkStrategy,
  OneToOne,
  RangeBasedShuffle
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkSenderActorRef
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.InitializeOperatorLogicHandler.InitializeOperatorLogic
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.OpenOperatorHandler.OpenOperator
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.READY
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.{AmberLogging, Constants, ISourceOperatorExecutor}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LinkIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.operators.{OpExecConfig, ShuffleType, SinkOpExecConfig}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.PythonUDFOpExecV2
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class WorkflowScheduler(
    availableNodes: Array[Address],
    networkCommunicationActor: NetworkSenderActorRef,
    ctx: ActorContext,
    asyncRPCClient: AsyncRPCClient,
    logger: Logger,
    workflow: Workflow
) {
  private val regionsScheduleOrderIterator =
    new TopologicalOrderIterator[PipelinedRegion, DefaultEdge](workflow.getPipelinedRegionsDAG())

  // Since one operator/link(i.e. links within an operator) can belong to multiple regions, we need to keep
  // track of those already built
  private val builtOperators =
    new mutable.HashSet[
      OperatorIdentity
    ]()
  private val openedOperators = new mutable.HashSet[OperatorIdentity]()
  private val initializedPythonOperators = new mutable.HashSet[OperatorIdentity]()
  private val activatedLink = new mutable.HashSet[LinkIdentity]()
  val workersKnowingAllInlinks = new mutable.HashSet[ActorVirtualIdentity]()

  private var constructingRegions = new mutable.HashSet[PipelinedRegion]()
  private var constructedRegions = new mutable.HashSet[PipelinedRegion]()
  var completedRegions = new mutable.HashSet[PipelinedRegion]()
  var runningRegions = new mutable.HashSet[PipelinedRegion]()
  var completedLinksOfRegion =
    new mutable.HashMap[PipelinedRegion, mutable.HashSet[LinkIdentity]]()

  private var nextRegion: PipelinedRegion = null

  /**
    * If no region is currently scheduled (nextRegion==null) or the currently
    * scheduled region has completed, then this function returns the next region
    * to be scheduled from the iterator.
    */
  def getNextRegionToConstructAndPrepare(): PipelinedRegion = {
    if (
      (nextRegion == null || completedRegions.contains(
        nextRegion
      )) && regionsScheduleOrderIterator.hasNext
    ) {
      nextRegion = regionsScheduleOrderIterator.next()
      return nextRegion
    }
    return null
  }

  private def getBlockingOutlinksOfRegion(region: PipelinedRegion): Set[LinkIdentity] = {
    val outlinks = new mutable.HashSet[LinkIdentity]()
    region.blockingDowstreamOperatorsInOtherRegions.foreach(opId => {
      workflow
        .getDirectUpstreamOperators(opId)
        .foreach(uOpId => {
          if (region.getOperators().contains(uOpId)) {
            outlinks.add(
              LinkIdentity(
                workflow.getOperator(uOpId).topology.layers.last.id,
                workflow.getOperator(opId).topology.layers.head.id
              )
            )
          }
        })
    })
    outlinks.toSet
  }

  private def isRegionCompleted(region: PipelinedRegion): Boolean = {
    getBlockingOutlinksOfRegion(region).forall(
      completedLinksOfRegion.getOrElse(region, new mutable.HashSet[LinkIdentity]()).contains
    ) && region
      .getOperators()
      .forall(opId => workflow.getOperator(opId).getState == WorkflowAggregatedState.COMPLETED)
  }

  def recordWorkerCompletion(workerId: ActorVirtualIdentity): Boolean = {
    val opId = workflow.getOperator(workerId).id
    var region: PipelinedRegion = null
    runningRegions.foreach(r =>
      if (r.getOperators().contains(opId)) {
        region = r
      }
    )

    if (region == null) {
      throw new WorkflowRuntimeException(
        s"WorkflowScheduler: Worker ${workerId} completed from a non-running region"
      )
    } else {
      if (isRegionCompleted(region)) {
        runningRegions.remove(region)
        completedRegions.add(region)
        return true
      }
    }
    false
  }

  def recordLinkCompletion(linkId: LinkIdentity): Boolean = {
    val upstreamOpId = OperatorIdentity(linkId.from.workflow, linkId.from.operator)
    var region: PipelinedRegion = null
    runningRegions.foreach(r =>
      if (r.getOperators().contains(upstreamOpId)) {
        region = r
      }
    )
    if (region == null) {
      throw new WorkflowRuntimeException(
        s"WorkflowScheduler: Link ${linkId.toString()} completed from a non-running region"
      )
    } else {
      val completedLinks =
        completedLinksOfRegion.getOrElseUpdate(region, new mutable.HashSet[LinkIdentity]())
      completedLinks.add(linkId)
      completedLinksOfRegion(region) = completedLinks
      if (isRegionCompleted(region)) {
        runningRegions.remove(region)
        completedRegions.add(region)
        return true
      }
    }
    false
  }

  /**
    * Returns the operators in a region whose all inputs are from operators that are not in this region.
    */
  def getSourcesOfRegion(region: PipelinedRegion): Array[OperatorIdentity] = {
    val sources = new ArrayBuffer[OperatorIdentity]()
    region
      .getOperators()
      .foreach(opId => {
        if (
          workflow
            .getDirectUpstreamOperators(opId)
            .forall(upOp =>
              !region
                .getOperators()
                .contains(upOp)
            )
        ) {
          sources.append(opId)
        }
      })
    sources.toArray
  }

  private def constructRegion(region: PipelinedRegion): Unit = {
    constructingRegions.add(region)
    val builtOpsInRegion = new mutable.HashSet[OperatorIdentity]()
    var frontier: Iterable[OperatorIdentity] = getSourcesOfRegion(region)
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
          networkCommunicationActor.ref,
          ctx,
          workflow.getInlinksIdsToWorkerLayer(workerLayer.id),
          workflow.workerToLayer,
          workflow.workerToOperatorExec
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
          networkCommunicationActor.ref,
          ctx,
          workflow.getInlinksIdsToWorkerLayer(workerLayer.id),
          workflow.workerToLayer,
          workflow.workerToOperatorExec
        )
      })
      layers = operatorInLinks.filter(x => x._2.forall(_.isBuilt)).keys
      while (layers.nonEmpty) {
        layers.foreach((layer: WorkerLayer) => {
          layer.build(
            operatorInLinks(layer).map(y => (null, y)).toArray,
            availableNodes,
            networkCommunicationActor.ref,
            ctx,
            workflow.getInlinksIdsToWorkerLayer(layer.id),
            workflow.workerToLayer,
            workflow.workerToOperatorExec
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
            .send(LinkWorkers(link), CONTROLLER)
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

  private def startRegion(region: PipelinedRegion): Future[Unit] = {
    val allOperatorsInRegion =
      region.getOperators() ++ region.blockingDowstreamOperatorsInOtherRegions
    allOperatorsInRegion
      .filter(opId => workflow.getOperator(opId).getState == WorkflowAggregatedState.UNINITIALIZED)
      .foreach(opId => workflow.getOperator(opId).setAllWorkerState(READY))
    asyncRPCClient.sendToClient(WorkflowStatusUpdate(workflow.getWorkflowStatus))
    constructingRegions.remove(region)
    constructedRegions.add(region)
    if (completedRegions.isEmpty) {
      asyncRPCClient.send(StartPipelinedRegion(region, true), CONTROLLER)
    } else {
      asyncRPCClient.send(StartPipelinedRegion(region, false), CONTROLLER)
    }
  }

  private def prepareRegion(region: PipelinedRegion): Future[Unit] = {
    asyncRPCClient.sendToClient(WorkflowStatusUpdate(workflow.getWorkflowStatus))
    Future()
      .flatMap(_ => initializePythonOperators(region))
      .flatMap(_ => activateAllLinks(region))
      .flatMap(_ => openAllOperators(region))
      .flatMap(_ => startRegion(region))
  }

  def constructAndPrepare(region: PipelinedRegion): Future[Unit] = {
    if (
      !constructingRegions.contains(region) && !constructedRegions.contains(
        region
      ) && !runningRegions.contains(region) && !completedRegions.contains(region)
    ) {
      constructRegion(region)
      prepareRegion(region)
    } else {
      logger.error(s"Pipelined region ${region.getId()} has already been constructed")
      Future()
    }
  }

}
