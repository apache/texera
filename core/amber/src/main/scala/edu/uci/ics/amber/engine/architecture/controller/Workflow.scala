package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{WorkerInfo, WorkerLayer, WorkerLayerImpl}
import edu.uci.ics.amber.engine.architecture.linksemantics._
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity, LinkIdentity, OperatorIdentity, WorkflowIdentity}
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentityUtil.toOperatorIdentity
import edu.uci.ics.amber.engine.operators.{OpExecConfig, SinkOpExecConfig}
import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.PythonUDFOpExecV2

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class Workflow(val workflowId: WorkflowIdentity, physicalPlan: PhysicalPlan) {
  // This is updated by `instantiateAndStoreLinkInformation`
  val idToLink = new mutable.HashMap[LinkIdentity, LinkStrategy]()

  // The following data structures are updated when the operator is built (buildOperator())
  // by scheduler and the worker identities are available.
  val workerToLayer = new mutable.HashMap[ActorVirtualIdentity, WorkerLayer]()
  val workerToOperatorExec = new mutable.HashMap[ActorVirtualIdentity, IOperatorExecutor]()

  instantiateAndStoreLinkInformation()

  private def instantiateAndStoreLinkInformation(): Unit = {
    // TODO: build link
//
//    getAllOperators.foreach(opExecConfig => {
//      opExecConfig.topology.links.foreach { (linkStrategy: LinkStrategy) =>
//        idToLink(linkStrategy.id) = linkStrategy
//      }
//    })
//    buildInterOperatorLinks()
  }

  private def linkOperators(
      from: (OpExecConfig, WorkerLayer),
      to: (OpExecConfig, WorkerLayer)
  ): LinkStrategy = {
//    val sender = from._2
//    val receiver = to._2
//    val receiverOpExecConfig = to._1
//    if (receiverOpExecConfig.requiresShuffle) {
//      if (receiverOpExecConfig.shuffleType == ShuffleType.HASH_BASED) {
//        new HashBasedShuffle(
//          sender,
//          receiver,
//          Constants.defaultBatchSize,
//          receiverOpExecConfig.getPartitionColumnIndices(sender.id)
//        )
//      } else if (receiverOpExecConfig.shuffleType == ShuffleType.RANGE_BASED) {
//        new RangeBasedShuffle(
//          sender,
//          receiver,
//          Constants.defaultBatchSize,
//          receiverOpExecConfig.getPartitionColumnIndices(sender.id),
//          receiverOpExecConfig.getRangeShuffleMinAndMax._1,
//          receiverOpExecConfig.getRangeShuffleMinAndMax._2
//        )
//      } else {
//        // unknown shuffle type. Default to full round-robin
//        new FullRoundRobin(sender, receiver, Constants.defaultBatchSize)
//      }
//    } else if (receiverOpExecConfig.isInstanceOf[SinkOpExecConfig]) {
//      new AllToOne(sender, receiver, Constants.defaultBatchSize)
//    } else if (sender.numWorkers == receiver.numWorkers) {
//      new OneToOne(sender, receiver, Constants.defaultBatchSize)
//    } else {
//      new FullRoundRobin(sender, receiver, Constants.defaultBatchSize)
//    }
    null
  }


  def getPipelinedRegionsDAG() = physicalPlan.pipelinedRegionsDAG

  def getBlockingOutlinksOfRegion(region: PipelinedRegion): Set[LinkIdentity] = {
    val outlinks = new mutable.HashSet[LinkIdentity]()
    region.blockingDowstreamOperatorsInOtherRegions.foreach(opId => {
      getDirectUpstreamOperators(opId)
        .foreach(uOpId => {
          if (region.getOperators().contains(uOpId)) {
            outlinks.add(
              LinkIdentity(
                getOperator(uOpId).id,
                getOperator(opId).id
              )
            )
          }
        })
    })
    outlinks.toSet
  }

  /**
    * Returns the operators in a region whose all inputs are from operators that are not in this region.
    */
  def getSourcesOfRegion(region: PipelinedRegion): Array[LayerIdentity] = {
    val sources = new ArrayBuffer[LayerIdentity]()
    region
      .getOperators()
      .foreach(opId => {
        if (
          getDirectUpstreamOperators(opId)
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

  def getAllWorkersOfRegion(region: PipelinedRegion): Array[ActorVirtualIdentity] = {
    val allOperatorsInRegion =
      region.getOperators() ++ region.blockingDowstreamOperatorsInOtherRegions

    allOperatorsInRegion.flatMap(opId => getOperator(opId).getAllWorkers.toList)
  }

  def getAllOperatorIds: Iterable[LayerIdentity] = physicalPlan.allOperatorIds

  def getWorkflowId(): WorkflowIdentity = workflowId

  def getWorkflowStatus: Map[String, OperatorRuntimeStats] = {
    physicalPlan.operators.map(op => (op._1.operator, op._2.getOperatorStatistics))
  }

  def getEndOperators: Iterable[WorkerLayer] =
    physicalPlan.getSinkOperators.map(physicalPlan.operators(_))

//  def getOperator(opID: String): WorkerLayer =
//    physicalPlan.operators(OperatorIdentity(physicalPlan.workflowId.id, opID))

  def getOperator(opID: LayerIdentity): WorkerLayer = physicalPlan.operators(opID)

  def getDirectUpstreamOperators(opID: LayerIdentity): Iterable[LayerIdentity] =
    physicalPlan.getUpstream(opID)

  def getDirectDownStreamOperators(opID: LayerIdentity): Iterable[LayerIdentity] =
    physicalPlan.getDownstream(opID)

  def getAllOperators: Iterable[WorkerLayer] = physicalPlan.operators.values

  def getWorkerInfo(id: ActorVirtualIdentity): WorkerInfo = workerToLayer(id).workers(id)

  /**
    * Returns the worker layer of the upstream operators that links to the `opId` operator's
    * worker layer.
    */
  def getUpStreamConnectedWorkerLayers(
      opID: LayerIdentity
  ): mutable.HashMap[LayerIdentity, WorkerLayer] = {
    val upstreamOperatorToLayers = new mutable.HashMap[LayerIdentity, WorkerLayer]()
    getDirectUpstreamOperators(opID).foreach(uOpID =>
      upstreamOperatorToLayers(uOpID) = getOperator(uOpID)
    )
    upstreamOperatorToLayers
  }

  def getSourceLayers: Iterable[WorkerLayerImpl[_]] = {
    val tos = getAllLinks.map(_.to).toSet
    getAllLayers.filter(layer => !tos.contains(layer))
  }

  def getSinkLayers: Iterable[WorkerLayer] = {
    val froms = getAllLinks.map(_.from).toSet
    getAllLayers.filter(layer => !froms.contains(layer))
  }

  def getAllLayers: Iterable[WorkerLayer] = getAllOperators

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

  def getOperator(workerId: ActorVirtualIdentity): WorkerLayer = {
    getAllLayers.find(layer => layer.workers.contains(workerId)).orNull
  }

  // get all worker layers corresponding to an operator, sorted in topological order
  def getOperator(opId: OperatorIdentity): List[WorkerLayer] = {
    physicalPlan.topologicalIterator().filter(layerId => toOperatorIdentity(layerId) == opId)
      .map(layerId => getOperator(layerId)).toList
  }

  def getLink(linkID: LinkIdentity): LinkStrategy = idToLink(linkID)

  def getPythonWorkers: Iterable[ActorVirtualIdentity] =
    workerToOperatorExec
      .filter({
        case (_: ActorVirtualIdentity, operatorExecutor: IOperatorExecutor) =>
          operatorExecutor.isInstanceOf[PythonUDFOpExecV2]
      }) map { case (workerId: ActorVirtualIdentity, _: IOperatorExecutor) => workerId }

  def getAllWorkersForOperators(
      operators: Array[LayerIdentity]
  ): Array[ActorVirtualIdentity] = {
    operators.flatMap(opId => physicalPlan.operators(opId).getAllWorkers)
  }

  def getPythonOperators(fromOperatorsList: Array[LayerIdentity]): Array[LayerIdentity] = {
    fromOperatorsList.filter(opId =>
      physicalPlan.operators(opId).getAllWorkers.size > 0 && physicalPlan
        .operators(
          opId
        )
        .getAllWorkers
        .forall(wid => workerToOperatorExec(wid).isInstanceOf[PythonUDFOpExecV2])
    )
  }

  def getPythonWorkerToOperatorExec(
      pythonOperators: Array[LayerIdentity]
  ): Iterable[(ActorVirtualIdentity, PythonUDFOpExecV2)] = {
    val allWorkers = pythonOperators.flatMap(opId => physicalPlan.operators(opId).getAllWorkers)
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
    physicalPlan.operators.values.forall(op => op.getState == WorkflowAggregatedState.COMPLETED)

}
