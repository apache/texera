package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.Address
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{OpExecConfig, WorkerInfo}
import edu.uci.ics.amber.engine.architecture.linksemantics._
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
import edu.uci.ics.amber.engine.common.virtualidentity._
import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan
import edu.uci.ics.texera.workflow.operators.udf.python.PythonUDFOpExecV2

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class Workflow(val workflowId: WorkflowIdentity, val physicalPlan: PhysicalPlan) {

  // The following data structures are updated when the operator is built (buildOperator())
  // by scheduler and the worker identities are available.
  val workerToOpExecConfig = new mutable.HashMap[ActorVirtualIdentity, OpExecConfig]()

  def getBlockingOutLinksOfRegion(region: PipelinedRegion): Set[LinkIdentity] = {
    val outLinks = new mutable.HashSet[LinkIdentity]()
    region.blockingDownstreamOperatorsInOtherRegions.foreach(opId => {
      physicalPlan
        .getUpstream(opId)
        .foreach(upstream => {
          if (region.operators.contains(upstream)) {
            outLinks.add(LinkIdentity(upstream, opId))
          }
        })
    })
    outLinks.toSet
  }

  /**
    * Returns the operators in a region whose all inputs are from operators that are not in this region.
    */
  def getSourcesOfRegion(region: PipelinedRegion): Array[LayerIdentity] = {
    val sources = new ArrayBuffer[LayerIdentity]()
    region
      .getOperators()
      .foreach(opId => {
        val isSource = physicalPlan.getUpstream(opId).forall(up => !region.containsOperator(up))
        if (isSource) {
          sources.append(opId)
        }
      })
    sources.toArray
  }

  def getAllWorkersOfRegion(region: PipelinedRegion): Array[ActorVirtualIdentity] = {
    val allOperatorsInRegion =
      region.getOperators() ++ region.blockingDownstreamOperatorsInOtherRegions

    allOperatorsInRegion.flatMap(opId => physicalPlan.operatorMap(opId).getAllWorkers.toList)
  }

  def getAllWorkerInfoOfAddress(address: Address): Iterable[WorkerInfo] = {
    physicalPlan.operators
      .flatMap(_.workers.values)
      .filter(info => info.ref.path.address == address)
  }

  def getWorkflowId(): WorkflowIdentity = workflowId

  def getDirectUpstreamWorkers(vid: ActorVirtualIdentity): Iterable[ActorVirtualIdentity] = {
    val opId = workerToOpExecConfig(vid).id
    val upstreamLinks = physicalPlan.getUpstreamLinks(opId)
    val upstreamWorkers = mutable.HashSet[ActorVirtualIdentity]()
    upstreamLinks
      .map(link => physicalPlan.linkStrategies(link))
      .flatMap(linkStrategy => linkStrategy.getPartitioning.toList)
      .foreach {
        case (sender, _, _, receivers) =>
          if (receivers.contains(vid)) {
            upstreamWorkers.add(sender)
          }
      }
    upstreamWorkers
  }

  def getWorkflowStatus: Map[String, OperatorRuntimeStats] = {
    physicalPlan.operatorMap.map(op => (op._1.operator, op._2.getOperatorStatistics))
  }

  def getAllOperators: Iterable[OpExecConfig] = physicalPlan.operators

  def getWorkerInfo(id: ActorVirtualIdentity): WorkerInfo = workerToOpExecConfig(id).workers(id)

  /**
    * Returns the worker layer of the upstream operators that links to the `opId` operator's
    * worker layer.
    */
  def getUpStreamConnectedWorkerLayers(
      opID: LayerIdentity
  ): mutable.HashMap[LayerIdentity, OpExecConfig] = {
    val upstreamOperatorToLayers = new mutable.HashMap[LayerIdentity, OpExecConfig]()
    physicalPlan
      .getUpstream(opID)
      .foreach(uOpID => upstreamOperatorToLayers(uOpID) = physicalPlan.operatorMap(opID))
    upstreamOperatorToLayers
  }

  def getInlinksIdsToWorkerLayer(layerIdentity: LayerIdentity): Set[LinkIdentity] = {
    physicalPlan.getLayer(layerIdentity).inputToOrdinalMapping.keySet
  }

  def getAllWorkers: Iterable[ActorVirtualIdentity] = workerToOpExecConfig.keys

  def getOperator(workerID: ActorVirtualIdentity): OpExecConfig =
    workerToOpExecConfig(workerID)

  def getOperator(opID: LayerIdentity): OpExecConfig = physicalPlan.operatorMap(opID)

  def getLink(linkID: LinkIdentity): LinkStrategy = physicalPlan.linkStrategies(linkID)

  def getPythonWorkers: Iterable[ActorVirtualIdentity] =
    workerToOpExecConfig.filter(worker => worker._2.opExecClass == classOf[PythonUDFOpExecV2]).keys

  def getOperatorToWorkers: Iterable[(LayerIdentity, Seq[ActorVirtualIdentity])] = {
    physicalPlan.allOperatorIds.map(opId => {
      (opId, getAllWorkersForOperators(Array(opId)).toSeq)
    })
  }

  def getAllWorkersForOperators(
      operators: Array[LayerIdentity]
  ): Array[ActorVirtualIdentity] = {
    operators.flatMap(opId => physicalPlan.operatorMap(opId).getAllWorkers)
  }

  def getPythonOperators(fromOperatorsList: Array[LayerIdentity]): Array[LayerIdentity] = {
    fromOperatorsList.filter(opId =>
      physicalPlan.operatorMap(opId).getAllWorkers.nonEmpty &&
        physicalPlan.operatorMap(opId).isPythonOperator
    )
  }

  def getPythonWorkerToOperatorExec(
      pythonOperators: Array[LayerIdentity]
  ): Iterable[(ActorVirtualIdentity, OpExecConfig)] = {
    pythonOperators
      .map(opId => physicalPlan.operatorMap(opId))
      .filter(op => op.isPythonOperator)
      .flatMap(op => op.getAllWorkers.map(worker => (worker, op)))
      .toList
  }

  def isCompleted: Boolean =
    workerToOpExecConfig.values.forall(op => op.getState == WorkflowAggregatedState.COMPLETED)

}
