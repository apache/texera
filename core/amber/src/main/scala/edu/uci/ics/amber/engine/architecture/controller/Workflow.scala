package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.{ActorContext, Address}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{WorkerInfo, WorkerLayer}
import edu.uci.ics.amber.engine.architecture.linksemantics._
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkSenderActorRef
import edu.uci.ics.amber.engine.architecture.principal.OperatorState.Completed
import edu.uci.ics.amber.engine.architecture.principal.OperatorStatistics
import edu.uci.ics.amber.engine.common.{AmberUtils, Constants}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.{
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.operators.{OpExecConfig, SinkOpExecConfig}

import scala.collection.mutable

class Workflow(
    operators: mutable.Map[OperatorIdentity, OpExecConfig],
    outLinks: Map[OperatorIdentity, Set[OperatorIdentity]]
) {
  private val inLinks: Map[OperatorIdentity, Set[OperatorIdentity]] =
    AmberUtils.reverseMultimap(outLinks)

  private val sourceOperators: Iterable[OperatorIdentity] =
    operators.keys.filter(!inLinks.contains(_))
  private val sinkOperators: Iterable[OperatorIdentity] =
    operators.keys.filter(!outLinks.contains(_))

  private val workerToLayer = new mutable.HashMap[ActorVirtualIdentity, WorkerLayer]()
  private val layerToOperator = new mutable.HashMap[LayerIdentity, OpExecConfig]()
  private val operatorLinks = {
    new mutable.HashMap[OperatorIdentity, mutable.ArrayBuffer[LinkStrategy]]
  }
  private val idToLink = new mutable.HashMap[LinkIdentity, LinkStrategy]()

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

  def getWorkflowStatus: Map[String, OperatorStatistics] = {
    operators.map { op =>
      (op._1.operator, op._2.getOperatorStatistics)
    }.toMap
  }

  def getStartOperators: Iterable[OpExecConfig] = sourceOperators.map(operators(_))

  def getEndOperators: Iterable[OpExecConfig] = sinkOperators.map(operators(_))

  def getOperator(opID: OperatorIdentity): OpExecConfig = operators(opID)

  def getOperator(workerID: ActorVirtualIdentity): OpExecConfig =
    layerToOperator(workerToLayer(workerID).id)

  def getDirectUpstreamOperators(opID: OperatorIdentity): Iterable[OperatorIdentity] = inLinks(opID)

  def getDirectDownStreamOperators(opID: OperatorIdentity): Iterable[OperatorIdentity] =
    outLinks(opID)

  def getAllOperators: Iterable[OpExecConfig] = operators.values

  def getWorkerInfo(id: ActorVirtualIdentity): WorkerInfo = workerToLayer(id).workers(id)

  def getSourceLayers: Iterable[WorkerLayer] = {
    val tos = getAllLinks.map(_.to).toSet
    getAllLayers.filter(layer => !tos.contains(layer))
  }

  def getSinkLayers: Iterable[WorkerLayer] = {
    val froms = getAllLinks.map(_.from).toSet
    getAllLayers.filter(layer => !froms.contains(layer))
  }

  def getWorkerLayer(workerID: ActorVirtualIdentity): WorkerLayer = workerToLayer(workerID)

  def getAllWorkers: Iterable[ActorVirtualIdentity] = workerToLayer.keys

  def getAllLayers: Iterable[WorkerLayer] = operators.values.flatMap(_.topology.layers)

  def getAllLinks: Iterable[LinkStrategy] = idToLink.values

  def getLink(linkID: LinkIdentity): LinkStrategy = idToLink(linkID)

  def isCompleted: Boolean = operators.values.forall(op => op.getState == Completed)

  def buildOperator(
      allNodes: Array[Address],
      prev: Array[(OpExecConfig, WorkerLayer)],
      communicationActor: NetworkSenderActorRef,
      opID: OperatorIdentity,
      ctx: ActorContext
  ): Unit = {
    val operator = operators(opID) // This metadata gets updated at the end of this function
    operator.topology.links.foreach { link =>
      idToLink(link.id) = link
    }
    if (operator.topology.links.isEmpty) {
      operator.topology.layers.foreach(x => {
        x.build(prev, allNodes, communicationActor.ref, ctx, workerToLayer)
        layerToOperator(x.id) = operator
      })
    } else {
      val operatorInLinks: Map[WorkerLayer, Set[WorkerLayer]] =
        operator.topology.links.groupBy(x => x.to).map(x => (x._1, x._2.map(_.from).toSet))
      var currentLayer: Iterable[WorkerLayer] =
        operator.topology.links
          .filter(x => operator.topology.links.forall(_.to != x.from))
          .map(_.from)
      currentLayer.foreach(x => {
        x.build(prev, allNodes, communicationActor.ref, ctx, workerToLayer)
        layerToOperator(x.id) = operator
      })
      currentLayer = operatorInLinks.filter(x => x._2.forall(_.isBuilt)).keys
      while (currentLayer.nonEmpty) {
        currentLayer.foreach(x => {
          x.build(
            operatorInLinks(x).map(y => (null, y)).toArray,
            allNodes,
            communicationActor.ref,
            ctx,
            workerToLayer
          )
          layerToOperator(x.id) = operator
        })
        currentLayer = operatorInLinks.filter(x => !x._1.isBuilt && x._2.forall(_.isBuilt)).keys
      }
    }
  }

  def linkOperators(
      from: (OpExecConfig, WorkerLayer),
      to: (OpExecConfig, WorkerLayer)
  ): LinkStrategy = {
    val sender = from._2
    val receiver = to._2
    if (to._1.requiredShuffle) {
      new HashBasedShuffle(
        sender,
        receiver,
        Constants.defaultBatchSize,
        to._1.getShuffleHashFunction(sender.id)
      )
    } else if (to._1.isInstanceOf[SinkOpExecConfig]) {
      new AllToOne(sender, receiver, Constants.defaultBatchSize)
    } else if (sender.numWorkers == receiver.numWorkers) {
      new OneToOne(sender, receiver, Constants.defaultBatchSize)
    } else {
      new FullRoundRobin(sender, receiver, Constants.defaultBatchSize)
    }
  }

  def buildLinks(to: OperatorIdentity): Unit = {
    if (!inLinks.contains(to)) {
      return
    }
    for (from <- inLinks(to)) {
      val link = linkOperators(
        (
          operators(from),
          operators(from).topology.layers.last
        ),
        (
          operators(to),
          operators(to).topology.layers.head
        )
      )
      idToLink(link.id) = link
      if (operatorLinks.contains(from)) {
        operatorLinks(from).append(link)
      } else {
        operatorLinks(from) = mutable.ArrayBuffer[LinkStrategy](link)
      }
    }
  }

  def build(
      allNodes: Array[Address],
      communicationActor: NetworkSenderActorRef,
      ctx: ActorContext
  ): Unit = {
    val builtOperators = mutable.HashSet[OperatorIdentity]()
    var frontier = sourceOperators
    while (frontier.nonEmpty) {
      frontier.foreach { op =>
        operators(op).checkStartDependencies(this)
        val prev: Array[(OpExecConfig, WorkerLayer)] = if (inLinks.contains(op)) {
          inLinks(op)
            .map(x =>
              (
                operators(x),
                operators(x).topology.layers.last
              )
            )
            .toArray
        } else {
          Array.empty
        }
        buildOperator(allNodes, prev, communicationActor, op, ctx)
        buildLinks(op)
        builtOperators.add(op)
      }
      frontier = inLinks.filter {
        case (op, inlinks) =>
          !builtOperators.contains(op) && inlinks.forall(builtOperators.contains)
      }.keys
    }
  }

}
