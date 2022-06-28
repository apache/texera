package edu.uci.ics.amber.engine.architecture.scheduling

import akka.actor.{ActorContext, Address}
import edu.uci.ics.amber.engine.architecture.controller.Workflow
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
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.virtualidentity.{OperatorIdentity, WorkflowIdentity}
import edu.uci.ics.amber.engine.operators.{OpExecConfig, ShuffleType, SinkOpExecConfig}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class PipelinedRegionIdentity(workflowId: WorkflowIdentity, pipelineId: String)

class PipelinedRegion(
    id: PipelinedRegionIdentity,
    operators: ArrayBuffer[OperatorIdentity],
    workflow: Workflow
) {
  var dependsOn: ArrayBuffer[PipelinedRegionIdentity] =
    new ArrayBuffer[
      PipelinedRegionIdentity
    ]() // The regions that must complete before this can start
  var completed = false
  var blockingDowstreamOperatorsInOtherRegions: ArrayBuffer[OperatorIdentity] =
    new ArrayBuffer[
      OperatorIdentity
    ] // These are the operators that receive blocking inputs from this region

  def getId(): PipelinedRegionIdentity = id

  def getOperators(): ArrayBuffer[OperatorIdentity] = operators

  def getSourcesOfRegion(): ArrayBuffer[OperatorIdentity] = {
    val sources = new ArrayBuffer[OperatorIdentity]()
    operators.foreach(opId => {
      if (workflow.getDirectUpstreamOperators(opId).forall(upOp => !operators.contains(upOp))) {
        sources.append(opId)
      }
    })
    sources
  }

  def build(
      availableNodes: Array[Address],
      networkCommunicationActor: NetworkSenderActorRef,
      ctx: ActorContext
  ): Unit = {
    var frontier: Iterable[OperatorIdentity] = getSourcesOfRegion()
    while (frontier.nonEmpty) {
      frontier.foreach { (op: OperatorIdentity) =>
        val prev: Array[(OpExecConfig, WorkerLayer)] =
          if (workflow.getDirectUpstreamOperators(op).nonEmpty) {
            workflow
              .getDirectUpstreamOperators(op)
              .map(upStreamOp =>
                (
                  workflow.getOperator(upStreamOp),
                  workflow.getOperator(upStreamOp).topology.layers.last
                )
              )
              .toArray
          } else {
            Array.empty
          }
        buildOperator(availableNodes, prev, networkCommunicationActor, op, ctx)
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
    val opExecConfig =
      workflow.getOperator(
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
}
