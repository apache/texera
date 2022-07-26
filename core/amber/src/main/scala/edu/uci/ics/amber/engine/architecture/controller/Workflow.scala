package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.{ActorContext, Address}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{WorkerInfo, WorkerLayer}
import edu.uci.ics.amber.engine.architecture.linksemantics._
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkSenderActorRef
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
import edu.uci.ics.amber.engine.common.{AmberUtils, Constants}
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.operators.{OpExecConfig, ShuffleType, SinkOpExecConfig}
import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.PythonUDFOpExecV2
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}

import scala.collection.mutable

class Workflow(
    workflowId: WorkflowIdentity,
    operatorToOpExecConfig: mutable.Map[OperatorIdentity, OpExecConfig],
    outLinks: Map[OperatorIdentity, Set[OperatorIdentity]],
    pipelinedRegionsDAG: DirectedAcyclicGraph[PipelinedRegion, DefaultEdge]
) {
  // The following data structures are created when workflow object is created.
  private val inLinks: Map[OperatorIdentity, Set[OperatorIdentity]] =
    AmberUtils.reverseMultimap(outLinks)
  private val sourceOperators: Iterable[OperatorIdentity] =
    operatorToOpExecConfig.keys.filter(!inLinks.contains(_))
  private val sinkOperators: Iterable[OperatorIdentity] =
    operatorToOpExecConfig.keys.filter(!outLinks.contains(_))

  // This is updated by `instantiateAndStoreLinkInformation`
  val idToLink = new mutable.HashMap[LinkIdentity, LinkStrategy]()

  // The following data structures are updated when the operator is built (buildOperator())
  // by scheduler and the worker identities are available.
  val workerToLayer = new mutable.HashMap[ActorVirtualIdentity, WorkerLayer]()
  val workerToOperatorExec = new mutable.HashMap[ActorVirtualIdentity, IOperatorExecutor]()

  instantiateAndStoreLinkInformation()

  private def instantiateAndStoreLinkInformation(): Unit = {
    getAllOperators.foreach(opExecConfig => {
      opExecConfig.topology.links.foreach { (linkStrategy: LinkStrategy) =>
        idToLink(linkStrategy.id) = linkStrategy
      }
    })
    buildInterOperatorLinks()
  }

  private def buildInterOperatorLinks(): Unit = {
    getAllOperatorIds.foreach(toOpId => {
      for (from <- getDirectUpstreamOperators(toOpId)) {
        val link = linkOperators(
          (getOperator(from), getOperator(from).topology.layers.last),
          (getOperator(toOpId), getOperator(toOpId).topology.layers.head)
        )
        idToLink(link.id) = link
      }
    })
  }

  private def linkOperators(
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

  def getPipelinedRegionsDAG() = pipelinedRegionsDAG

  def getStartOperatorIds: Iterable[OperatorIdentity] = sourceOperators

  def getAllOperatorIds: Iterable[OperatorIdentity] = operatorToOpExecConfig.keys

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

  def getDirectUpstreamOperators(opID: OperatorIdentity): Iterable[OperatorIdentity] =
    inLinks.getOrElse(opID, Set())

  def getDirectDownStreamOperators(opID: OperatorIdentity): Iterable[OperatorIdentity] =
    outLinks.getOrElse(opID, Set())

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

  def getInlinksIdsToWorkerLayer(layerIdentity: LayerIdentity): Set[LinkIdentity] = {
    val inlinkIds = new mutable.HashSet[LinkIdentity]()
    idToLink.keys.foreach(linkId => {
      if (linkId.to == layerIdentity) {
        inlinkIds.add(linkId)
      }
    })
    inlinkIds.toSet
  }

  def getAllWorkers: Iterable[ActorVirtualIdentity] = workerToLayer.keys

  def getOperator(workerId: ActorVirtualIdentity): OpExecConfig = {
    var opExecConfigToReturn: OpExecConfig = null
    getAllOperators.foreach(opExecConfig => {
      if (opExecConfig.topology.layers.map(l => l.id).contains(workerToLayer(workerId).id)) {
        opExecConfigToReturn = opExecConfig
      }
    })
    opExecConfigToReturn
  }

  def getLink(linkID: LinkIdentity): LinkStrategy = idToLink(linkID)

  def getPythonWorkers: Iterable[ActorVirtualIdentity] =
    workerToOperatorExec
      .filter({
        case (_: ActorVirtualIdentity, operatorExecutor: IOperatorExecutor) =>
          operatorExecutor.isInstanceOf[PythonUDFOpExecV2]
      }) map { case (workerId: ActorVirtualIdentity, _: IOperatorExecutor) => workerId }

  def getAllWorkersForOperators(
      operators: Array[OperatorIdentity]
  ): Array[ActorVirtualIdentity] = {
    operators.map(opId => operatorToOpExecConfig(opId).getAllWorkers).flatten
  }

  def getPythonOperators(fromOperatorsList: Array[OperatorIdentity]): Array[OperatorIdentity] = {
    fromOperatorsList.filter(opId =>
      operatorToOpExecConfig(opId).getAllWorkers.size > 0 && operatorToOpExecConfig(
        opId
      ).getAllWorkers.forall(wid => workerToOperatorExec(wid).isInstanceOf[PythonUDFOpExecV2])
    )
  }

  def getPythonWorkerToOperatorExec(
      pythonOperators: Array[OperatorIdentity]
  ): Iterable[(ActorVirtualIdentity, PythonUDFOpExecV2)] = {
    val allWorkers = pythonOperators
      .map(opId => operatorToOpExecConfig(opId).getAllWorkers)
      .flatten
    workerToOperatorExec
      .filter({
        case (id: ActorVirtualIdentity, operatorExecutor: IOperatorExecutor) =>
          allWorkers.contains(id)
      })
      .asInstanceOf[Iterable[(ActorVirtualIdentity, PythonUDFOpExecV2)]]
  }

  def getPythonWorkerToOperatorExec: Iterable[(ActorVirtualIdentity, PythonUDFOpExecV2)] =
    workerToOperatorExec
      .filter({
        case (_: ActorVirtualIdentity, operatorExecutor: IOperatorExecutor) =>
          operatorExecutor.isInstanceOf[PythonUDFOpExecV2]
      })
      .asInstanceOf[Iterable[(ActorVirtualIdentity, PythonUDFOpExecV2)]]

  def isCompleted: Boolean =
    operatorToOpExecConfig.values.forall(op => op.getState == WorkflowAggregatedState.COMPLETED)

}
