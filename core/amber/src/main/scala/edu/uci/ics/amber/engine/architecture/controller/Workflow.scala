package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.Address
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.NewOpExecConfig.NewOpExecConfig
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{WorkerInfo, WorkerLayer}
import edu.uci.ics.amber.engine.architecture.linksemantics._
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.virtualidentity._
import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.PythonUDFOpExecV2

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class Workflow(val workflowId: WorkflowIdentity, val physicalPlan: PhysicalPlan) {

  // The following data structures are updated when the operator is built (buildOperator())
  // by scheduler and the worker identities are available.
  val workerToOpExecConfig = new mutable.HashMap[ActorVirtualIdentity, NewOpExecConfig]()
//  val workerToOperatorExec = new mutable.HashMap[ActorVirtualIdentity, IOperatorExecutor]()

  def getBlockingOutLinksOfRegion(region: PipelinedRegion): Set[LinkIdentity] = {
    val outLinks = new mutable.HashSet[LinkIdentity]()
    region.blockingDowstreamOperatorsInOtherRegions.foreach(opId => {
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
      region.getOperators() ++ region.blockingDowstreamOperatorsInOtherRegions

    allOperatorsInRegion.flatMap(opId => physicalPlan.operatorMap(opId).getAllWorkers.toList)
  }

  def getAllWorkerInfoOfAddress(address: Address): Iterable[WorkerInfo] = {
    physicalPlan.operators
      .flatMap(_.workers.values)
      .filter(info => info.ref.path.address == address)
  }

//  def getStartOperatorIds: Iterable[OperatorIdentity] = sourceOperators

//  def getAllOperatorIds: Iterable[OperatorIdentity] = operatorToOpExecConfig.keys

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

//  def getSources(operator: OperatorIdentity): Set[OperatorIdentity] = {
//    var result = Set[OperatorIdentity]()
//    var current = Set[OperatorIdentity](operator)
//    while (current.nonEmpty) {
//      var next = Set[OperatorIdentity]()
//      for (i <- current) {
//        if (inLinks.contains(i) && inLinks(i).nonEmpty) {
//          next ++= inLinks(i)
//        } else {
//          result += i
//        }
//        current = next
//      }
//    }
//    result
//  }

  def getWorkflowStatus: Map[String, OperatorRuntimeStats] = {
    physicalPlan.operatorMap.map(op => (op._1.operator, op._2.getOperatorStatistics))
  }

//  def getStartOperators: Iterable[OpExecConfig] = sourceOperators.map(operatorToOpExecConfig(_))
//
//  def getEndOperators: Iterable[OpExecConfig] = sinkOperators.map(operatorToOpExecConfig(_))

//  def getOperator(opID: String): OpExecConfig =
//    operatorToOpExecConfig(OperatorIdentity(workflowId.id, opID))
//
//  def getOperator(opID: OperatorIdentity): OpExecConfig = operatorToOpExecConfig(opID)

//  def getDirectUpstreamOperators(opID: OperatorIdentity): Iterable[OperatorIdentity] =
//    inLinks.getOrElse(opID, Set())
//
//  def getDirectDownStreamOperators(opID: OperatorIdentity): Iterable[OperatorIdentity] =
//    outLinks.getOrElse(opID, Set())

  def getAllOperators: Iterable[NewOpExecConfig] = physicalPlan.operators

  def getWorkerInfo(id: ActorVirtualIdentity): WorkerInfo = workerToOpExecConfig(id).workers(id)

  /**
    * Returns the worker layer of the upstream operators that links to the `opId` operator's
    * worker layer.
    */
  def getUpStreamConnectedWorkerLayers(
      opID: LayerIdentity
  ): mutable.HashMap[LayerIdentity, NewOpExecConfig] = {
    val upstreamOperatorToLayers = new mutable.HashMap[LayerIdentity, NewOpExecConfig]()
    physicalPlan
      .getUpstream(opID)
      .foreach(uOpID => upstreamOperatorToLayers(uOpID) = physicalPlan.operatorMap(opID))
    upstreamOperatorToLayers
  }

//  def getSourceLayers: Iterable[WorkerLayer] = {
//    val tos = getAllLinks.map(_.to).toSet
//    getAllLayers.filter(layer => !tos.contains(layer))
//  }
//
//  def getSinkLayers: Iterable[WorkerLayer] = {
//    val froms = getAllLinks.map(_.from).toSet
//    getAllLayers.filter(layer => !froms.contains(layer))
//  }

//  def getAllLayers: Iterable[WorkerLayer] = operatorToOpExecConfig.values.flatMap(_.topology.layers)

//  def getAllLinks: Iterable[LinkStrategy] = idToLink.values

  def getWorkerLayer(workerID: ActorVirtualIdentity): NewOpExecConfig =
    workerToOpExecConfig(workerID)

  def getInlinksIdsToWorkerLayer(layerIdentity: LayerIdentity): Set[LinkIdentity] = {
    physicalPlan.getLayer(layerIdentity).inputToOrdinalMapping.keySet
  }

  def getAllWorkers: Iterable[ActorVirtualIdentity] = workerToOpExecConfig.keys

  def getOperator(workerId: ActorVirtualIdentity): NewOpExecConfig = {
    physicalPlan.operators.find(op => op.workers.contains(workerId)).orNull
  }

  def getOperator(opID: LayerIdentity): NewOpExecConfig = physicalPlan.operatorMap(opID)

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
        physicalPlan.operatorMap(opId).opExecClass == classOf[PythonUDFOpExecV2]
    )
  }

  def getPythonWorkerToOperatorExec(
      pythonOperators: Array[LayerIdentity]
  ): Iterable[(ActorVirtualIdentity, PythonUDFOpExecV2)] = {
    throw new NotImplementedError("fix python")
//    val allWorkers = pythonOperators.flatMap(opId => physicalPlan.operatorMap(opId).getAllWorkers)
//    workerToOperatorExec
//      .filter({
//        case (id: ActorVirtualIdentity, operatorExecutor: IOperatorExecutor) =>
//          allWorkers.contains(id)
//      })
//      .asInstanceOf[Iterable[(ActorVirtualIdentity, PythonUDFOpExecV2)]]
  }

  def getPythonWorkerToOperatorExec: Iterable[(ActorVirtualIdentity, PythonUDFOpExecV2)] =
    workerToOpExecConfig
      .filter({
        case (_: ActorVirtualIdentity, operatorExecutor: IOperatorExecutor) =>
          operatorExecutor.isInstanceOf[PythonUDFOpExecV2]
      })
      .asInstanceOf[Iterable[(ActorVirtualIdentity, PythonUDFOpExecV2)]]

  def isCompleted: Boolean =
    workerToOpExecConfig.values.forall(op => op.getState == WorkflowAggregatedState.COMPLETED)

}
