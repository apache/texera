package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.{ActorContext, Address}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{WorkerInfo, WorkerLayer}
import edu.uci.ics.amber.engine.architecture.linksemantics._
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkSenderActorRef
import edu.uci.ics.amber.engine.common.{AmberUtils, Constants}
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.engine.common.{AmberUtils, Constants, IOperatorExecutor}
import edu.uci.ics.amber.engine.operators.{OpExecConfig, SinkOpExecConfig}
import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.PythonUDFOpExecV2

import scala.collection.mutable

class Workflow(
    workflowId: WorkflowIdentity,
    operatorToOpExecConfig: mutable.Map[OperatorIdentity, OpExecConfig],
    outLinks: Map[OperatorIdentity, Set[OperatorIdentity]]
) {
  private val inLinks: Map[OperatorIdentity, Set[OperatorIdentity]] =
    AmberUtils.reverseMultimap(outLinks)

  private val sourceOperators: Iterable[OperatorIdentity] =
    operatorToOpExecConfig.keys.filter(!inLinks.contains(_))
  private val sinkOperators: Iterable[OperatorIdentity] =
    operatorToOpExecConfig.keys.filter(!outLinks.contains(_))

  private val workerToLayer = new mutable.HashMap[ActorVirtualIdentity, WorkerLayer]()
  private val layerToOperatorExecConfig = new mutable.HashMap[LayerIdentity, OpExecConfig]()
  private val operatorLinks = {
    new mutable.HashMap[OperatorIdentity, mutable.ArrayBuffer[LinkStrategy]]
  }
  private val idToLink = new mutable.HashMap[LinkIdentity, LinkStrategy]()

  private val workerToOperatorExec = new mutable.HashMap[ActorVirtualIdentity, IOperatorExecutor]()

  def getWorkflowId(): WorkflowIdentity = workflowId

  def getSources(operator: OperatorIdentity): Set[OperatorIdentity] = {
    var result = Set[OperatorIdentity]()
    var current = Set[OperatorIdentity](operator)
    while (current.nonEmpty) {
      var next = Set[OperatorIdentity]()
      for (i <- current) {
        if (inLinks.contains(i) && inLinks(i).nonEmpty) {
          next ++= inLinks(i)
        } else {
          result += i
        }
        current = next
      }
    }
    result
  }

  def getWorkflowStatus: Map[String, OperatorRuntimeStats] = {
    operatorToOpExecConfig.map { op =>
      (op._1.operator, op._2.getOperatorStatistics)
    }.toMap
  }

  def getStartOperators: Iterable[OpExecConfig] = sourceOperators.map(operatorToOpExecConfig(_))

  def getEndOperators: Iterable[OpExecConfig] = sinkOperators.map(operatorToOpExecConfig(_))

  def getOperator(opID: String): OpExecConfig =
    operatorToOpExecConfig(OperatorIdentity(workflowId.id, opID))

  def getOperator(opID: OperatorIdentity): OpExecConfig = operatorToOpExecConfig(opID)

  def getDirectUpstreamOperators(opID: OperatorIdentity): Iterable[OperatorIdentity] = inLinks(opID)

  def getDirectDownStreamOperators(opID: OperatorIdentity): Iterable[OperatorIdentity] =
    outLinks(opID)

  def getAllOperators: Iterable[OpExecConfig] = operatorToOpExecConfig.values

  def getWorkerInfo(id: ActorVirtualIdentity): WorkerInfo = workerToLayer(id).workers(id)

  /**
    * Returns the worker layer of the upstream operators that links to the `opId` operator's
    * worker layer.
    */
  def getUpStreamConnectedWorkerLayers(
      opID: OperatorIdentity
  ): mutable.HashMap[OperatorIdentity, WorkerLayer] = {
    val upstreamOperatorToLayers = new mutable.HashMap[OperatorIdentity, WorkerLayer]()
    getDirectUpstreamOperators(opID).map(uOpID =>
      upstreamOperatorToLayers(uOpID) = getOperator(uOpID).topology.layers.last
    )
    upstreamOperatorToLayers
  }

  def getSourceLayers: Iterable[WorkerLayer] = {
    val tos = getAllLinks.map(_.to).toSet
    getAllLayers.filter(layer => !tos.contains(layer))
  }

  def getSinkLayers: Iterable[WorkerLayer] = {
    val froms = getAllLinks.map(_.from).toSet
    getAllLayers.filter(layer => !froms.contains(layer))
  }

  def getAllLayers: Iterable[WorkerLayer] = operatorToOpExecConfig.values.flatMap(_.topology.layers)

  def getAllLinks: Iterable[LinkStrategy] = idToLink.values

  def getWorkerLayer(workerID: ActorVirtualIdentity): WorkerLayer = workerToLayer(workerID)

  def getAllWorkers: Iterable[ActorVirtualIdentity] = workerToLayer.keys

  def getOperator(workerId: ActorVirtualIdentity): OpExecConfig =
    layerToOperatorExecConfig(workerToLayer(workerId).id)

  def getLink(linkID: LinkIdentity): LinkStrategy = idToLink(linkID)

  def getPythonWorkers: Iterable[ActorVirtualIdentity] =
    workerToOperatorExec
      .filter({
        case (_: ActorVirtualIdentity, operatorExecutor: IOperatorExecutor) =>
          operatorExecutor.isInstanceOf[PythonUDFOpExecV2]
      }) map { case (workerId: ActorVirtualIdentity, _: IOperatorExecutor) => workerId }

  def getPythonWorkerToOperatorExec: Iterable[(ActorVirtualIdentity, PythonUDFOpExecV2)] =
    workerToOperatorExec
      .filter({
        case (_: ActorVirtualIdentity, operatorExecutor: IOperatorExecutor) =>
          operatorExecutor.isInstanceOf[PythonUDFOpExecV2]
      })
      .asInstanceOf[Iterable[(ActorVirtualIdentity, PythonUDFOpExecV2)]]

  def isCompleted: Boolean =
    operatorToOpExecConfig.values.forall(op => op.getState == WorkflowAggregatedState.COMPLETED)

  def build(
      allNodes: Array[Address],
      communicationActor: NetworkSenderActorRef,
      ctx: ActorContext
  ): Unit = {
    val builtOperators = mutable.HashSet[OperatorIdentity]()
    var frontier: Iterable[OperatorIdentity] = sourceOperators
    while (frontier.nonEmpty) {
      frontier.foreach { (op: OperatorIdentity) =>
        operatorToOpExecConfig(op).checkStartDependencies(this)
        val prev: Array[(OpExecConfig, WorkerLayer)] = if (inLinks.contains(op)) {
          inLinks(op)
            .map(x => (operatorToOpExecConfig(x), operatorToOpExecConfig(x).topology.layers.last))
            .toArray
        } else {
          Array.empty
        }
        buildOperator(allNodes, prev, communicationActor, op, ctx)
        buildLinks(op)
        builtOperators.add(op)
      }
      frontier = inLinks.filter {
        case (op: OperatorIdentity, linkedOps: Set[OperatorIdentity]) =>
          !builtOperators.contains(op) && linkedOps.forall(builtOperators.contains)
      }.keys
    }
  }

  def buildOperator(
      allNodes: Array[Address],
      prev: Array[(OpExecConfig, WorkerLayer)],
      communicationActor: NetworkSenderActorRef,
      operatorIdentity: OperatorIdentity,
      ctx: ActorContext
  ): Unit = {
    val opExecConfig = operatorToOpExecConfig(
      operatorIdentity
    ) // This metadata gets updated at the end of this function
    opExecConfig.topology.links.foreach { (linkStrategy: LinkStrategy) =>
      idToLink(linkStrategy.id) = linkStrategy
    }
    if (opExecConfig.topology.links.isEmpty) {
      opExecConfig.topology.layers.foreach(workerLayer => {
        workerLayer.build(
          prev,
          allNodes,
          communicationActor.ref,
          ctx,
          workerToLayer,
          workerToOperatorExec
        )
        layerToOperatorExecConfig(workerLayer.id) = opExecConfig
      })
    } else {
      val operatorInLinks: Map[WorkerLayer, Set[WorkerLayer]] =
        opExecConfig.topology.links.groupBy(_.to).map(x => (x._1, x._2.map(_.from).toSet))
      var layers: Iterable[WorkerLayer] =
        opExecConfig.topology.links
          .filter(linkStrategy => opExecConfig.topology.links.forall(_.to != linkStrategy.from))
          .map(_.from)
      layers.foreach(workerLayer => {
        workerLayer.build(
          prev,
          allNodes,
          communicationActor.ref,
          ctx,
          workerToLayer,
          workerToOperatorExec
        )
        layerToOperatorExecConfig(workerLayer.id) = opExecConfig
      })
      layers = operatorInLinks.filter(x => x._2.forall(_.isBuilt)).keys
      while (layers.nonEmpty) {
        layers.foreach((layer: WorkerLayer) => {
          layer.build(
            operatorInLinks(layer).map(y => (null, y)).toArray,
            allNodes,
            communicationActor.ref,
            ctx,
            workerToLayer,
            workerToOperatorExec
          )
          layerToOperatorExecConfig(layer.id) = opExecConfig
        })
        layers = operatorInLinks.filter(x => !x._1.isBuilt && x._2.forall(_.isBuilt)).keys
      }
    }
  }

  def buildLinks(to: OperatorIdentity): Unit = {
    if (!inLinks.contains(to)) {
      return
    }
    for (from <- inLinks(to)) {
      val link = linkOperators(
        (operatorToOpExecConfig(from), operatorToOpExecConfig(from).topology.layers.last),
        (operatorToOpExecConfig(to), operatorToOpExecConfig(to).topology.layers.head)
      )
      idToLink(link.id) = link
      if (operatorLinks.contains(from)) {
        operatorLinks(from).append(link)
      } else {
        operatorLinks(from) = mutable.ArrayBuffer[LinkStrategy](link)
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
    if (receiverOpExecConfig.requiredShuffle) {
      new HashBasedShuffle(
        sender,
        receiver,
        Constants.defaultBatchSize,
        receiverOpExecConfig.getPartitionColumnIndices(sender.id)
      )
    } else if (receiverOpExecConfig.isInstanceOf[SinkOpExecConfig]) {
      new AllToOne(sender, receiver, Constants.defaultBatchSize)
    } else if (sender.numWorkers == receiver.numWorkers) {
      new OneToOne(sender, receiver, Constants.defaultBatchSize)
    } else {
      new FullRoundRobin(sender, receiver, Constants.defaultBatchSize)
    }
  }

}
