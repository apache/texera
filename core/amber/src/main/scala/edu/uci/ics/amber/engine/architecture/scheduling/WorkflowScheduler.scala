package edu.uci.ics.amber.engine.architecture.scheduling

import akka.actor.{ActorContext, Address}
import com.twitter.util.Future
import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowStatusUpdate
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkersHandler.LinkWorkers
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
import edu.uci.ics.amber.engine.common.{AmberLogging, Constants, ISourceOperatorExecutor}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.operators.{OpExecConfig, ShuffleType, SinkOpExecConfig}
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
    workflow: Workflow
) {
  val idToRegions = new WorkflowPipelinedRegionsBuilder(workflow).buildPipelinedRegions()
  val regionsToSchedule = new ArrayBuffer[PipelinedRegion]()
  val regionsCurrentlyScheduled = new ArrayBuffer[PipelinedRegion]()
  val builtOperators =
    new mutable.HashSet[
      OperatorIdentity
    ]() // Since one operator can belong to multiple regions, we need to keep
  // track of the operators already built
  val openedOperators = new mutable.HashSet[OperatorIdentity]()
  val initializedPythonOperators = new mutable.HashSet[OperatorIdentity]()
  val activatedLink = new mutable.HashSet[LinkStrategy]()
  var preparedRegions: Boolean = false

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

  def buildRegion(region: PipelinedRegion): Unit = {
    val builtOpsInRegion = new mutable.HashSet[OperatorIdentity]()
    var frontier: Iterable[OperatorIdentity] = getSourcesOfRegion(region)
    while (frontier.nonEmpty) {
      frontier.foreach { (op: OperatorIdentity) =>
        workflow.getOperator(op).checkStartDependencies(workflow)
        val prev: Array[(OperatorIdentity, WorkerLayer)] = // used to decide deployment of workers
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
            .toArray
        if (!builtOperators.contains(op)) {
          buildOperator(prev, op)
          builtOperators.add(op)
        }
        buildLinks(prev, op)
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

  def buildOperator(
      prev: Array[(OperatorIdentity, WorkerLayer)], // used to decide deployment of workers
      operatorIdentity: OperatorIdentity
  ): Unit = {
    val opExecConfig =
      workflow.getOperator(
        operatorIdentity
      ) // This metadata gets updated at the end of this function
    opExecConfig.topology.links.foreach { (linkStrategy: LinkStrategy) =>
      workflow.idToLink(linkStrategy.id) = linkStrategy
    }
    if (opExecConfig.topology.links.isEmpty) {
      opExecConfig.topology.layers.foreach(workerLayer => {
        workerLayer.build(
          prev.map(pair => (workflow.getOperator(pair._1), pair._2)),
          availableNodes,
          networkCommunicationActor.ref,
          ctx,
          workflow.workerToLayer,
          workflow.workerToOperatorExec
        )
        workflow.layerToOperatorExecConfig(workerLayer.id) = opExecConfig
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
          workflow.workerToLayer,
          workflow.workerToOperatorExec
        )
        workflow.layerToOperatorExecConfig(workerLayer.id) = opExecConfig
      })
      layers = operatorInLinks.filter(x => x._2.forall(_.isBuilt)).keys
      while (layers.nonEmpty) {
        layers.foreach((layer: WorkerLayer) => {
          layer.build(
            operatorInLinks(layer).map(y => (null, y)).toArray,
            availableNodes,
            networkCommunicationActor.ref,
            ctx,
            workflow.workerToLayer,
            workflow.workerToOperatorExec
          )
          workflow.layerToOperatorExecConfig(layer.id) = opExecConfig
        })
        layers = operatorInLinks.filter(x => !x._1.isBuilt && x._2.forall(_.isBuilt)).keys
      }
    }
  }

  def buildLinks(prev: Array[(OperatorIdentity, WorkerLayer)], to: OperatorIdentity): Unit = {
    for (from <- prev) {
      val link = linkOperators(
        (workflow.getOperator(from._1), from._2),
        (workflow.getOperator(to), workflow.getOperator(to).topology.layers.head)
      )
      workflow.idToLink(link.id) = link
      if (workflow.operatorLinks.contains(from._1)) {
        workflow.operatorLinks(from._1).append(link)
      } else {
        workflow.operatorLinks(from._1) = mutable.ArrayBuffer[LinkStrategy](link)
      }
    }
  }

  def linkOperators(
      from: (OpExecConfig, WorkerLayer),
      to: (OpExecConfig, WorkerLayer)
  ): LinkStrategy = {
    val sender = from._2
    val receiver = to._2
    val receiverOpExecConfig = to._1
    if (receiverOpExecConfig.requiresShuffle) {
      if (receiverOpExecConfig.shuffleType == ShuffleType.HASH_BASED) {
        new HashBasedShuffle(
          sender,
          receiver,
          Constants.defaultBatchSize,
          receiverOpExecConfig.getPartitionColumnIndices(sender.id)
        )
      } else if (receiverOpExecConfig.shuffleType == ShuffleType.RANGE_BASED) {
        new RangeBasedShuffle(
          sender,
          receiver,
          Constants.defaultBatchSize,
          receiverOpExecConfig.getPartitionColumnIndices(sender.id),
          receiverOpExecConfig.getRangeShuffleMinAndMax._1,
          receiverOpExecConfig.getRangeShuffleMinAndMax._2
        )
      } else {
        // unknown shuffle type. Default to full round-robin
        new FullRoundRobin(sender, receiver, Constants.defaultBatchSize)
      }
    } else if (receiverOpExecConfig.isInstanceOf[SinkOpExecConfig]) {
      new AllToOne(sender, receiver, Constants.defaultBatchSize)
    } else if (sender.numWorkers == receiver.numWorkers) {
      new OneToOne(sender, receiver, Constants.defaultBatchSize)
    } else {
      new FullRoundRobin(sender, receiver, Constants.defaultBatchSize)
    }
  }

  def prepareRegion(region: PipelinedRegion): Future[Unit] = {
    Future(asyncRPCClient.sendToClient(WorkflowStatusUpdate(workflow.getWorkflowStatus)))
      .flatMap(_ => {
        val uninitializedPythonOperators = workflow.getPythonOperators(
          (region.getOperators() ++ region.blockingDowstreamOperatorsInOtherRegions).filter(opId =>
            !initializedPythonOperators.contains(opId)
          )
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
      })
      .flatMap(_ => {
        val allOperatorsInRegion =
          region.blockingDowstreamOperatorsInOtherRegions ++ region.getOperators()
        Future.collect(
          // activate all links
          workflow.getAllLinks
            .filter(link => {
              !activatedLink.contains(link) &&
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
                .onSuccess(_ => activatedLink.add(link))
            }
            .toSeq
        )
      })
      .flatMap(
        // open all operators
        _ => {
          val allNotOpenedOperators =
            (region.blockingDowstreamOperatorsInOtherRegions ++ region.getOperators())
              .filter(opId => !openedOperators.contains(opId))
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
      )
      .flatMap(_ =>
        Future {
          (region.getOperators() ++ region.blockingDowstreamOperatorsInOtherRegions)
            .filter(opId =>
              workflow.getOperator(opId).getState == WorkflowAggregatedState.UNINITIALIZED
            )
            .foreach(opId => workflow.getOperator(opId).setAllWorkerState(READY))
          asyncRPCClient.sendToClient(WorkflowStatusUpdate(workflow.getWorkflowStatus))
        }
      )
      .flatMap(_ => {
        if (!preparedRegions) {
          preparedRegions = true
          asyncRPCClient.send(StartWorkflow(), CONTROLLER)
        } else {
          Future.Unit
        }
      })
  }

  def buildAndPrepare(): Unit = {
    idToRegions.values.foreach(region => {
      buildRegion(region)
      prepareRegion(region)
    })
  }

}
