package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import akka.actor.{ActorContext, ActorRef, Address, Deploy}
import akka.remote.RemoteScope
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.DeploymentFilter
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.DeployStrategy
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.RegisterActorRef
import edu.uci.ics.amber.engine.architecture.pythonworker.PythonWorkflowWorker
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.UNINITIALIZED
import edu.uci.ics.amber.engine.architecture.worker.statistics.{WorkerState, WorkerStatistics}
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity
}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.PythonUDFOpExecV2

import scala.collection.immutable.ListMap
import scala.collection.mutable

class WorkerLayer(
    val id: LayerIdentity,
    var initIOperatorExecutor: Int => IOperatorExecutor,
    var numWorkers: Int,
    val deploymentFilter: DeploymentFilter,
    val deployStrategy: DeployStrategy
) extends Serializable {

  private val startDependencies = mutable.HashSet[LinkIdentity]()
  var workers: ListMap[ActorVirtualIdentity, WorkerInfo] = _

  def startAfter(link: LinkIdentity): Unit = {
    startDependencies.add(link)
  }

  def resolveDependency(link: LinkIdentity): Unit = {
    startDependencies.remove(link)
  }

  def hasDependency(link: LinkIdentity): Boolean = startDependencies.contains(link)

  def canStart: Boolean = startDependencies.isEmpty

  def isBuilt: Boolean = workers != null

  def identifiers: Array[ActorVirtualIdentity] = workers.values.map(_.id).toArray

  def states: Array[WorkerState] = workers.values.map(_.state).toArray

  def statistics: Array[WorkerStatistics] = workers.values.map(_.stats).toArray

  def build(
      prev: Array[(OpExecConfig, WorkerLayer)],
      all: Array[Address],
      parentNetworkCommunicationActorRef: ActorRef,
      context: ActorContext,
      workerToLayer: mutable.HashMap[ActorVirtualIdentity, WorkerLayer],
      workerToOperatorExec: mutable.HashMap[ActorVirtualIdentity, IOperatorExecutor]
  ): Unit = {
    deployStrategy.initialize(deploymentFilter.filter(prev, all, context.self.path.address))
    workers = ListMap((0 until numWorkers).map { i =>
      val operatorExecutor: IOperatorExecutor = initIOperatorExecutor(i)
      val workerId: ActorVirtualIdentity =
        ActorVirtualIdentity(s"Worker:WF${id.workflow}-${id.operator}-${id.layerID}-$i")
      val address: Address = deployStrategy.next()
      workerToOperatorExec(workerId) = operatorExecutor
      val ref: ActorRef = context.actorOf(
        if (operatorExecutor.isInstanceOf[PythonUDFOpExecV2]) {
          PythonWorkflowWorker
            .props(workerId, operatorExecutor, parentNetworkCommunicationActorRef)
            .withDeploy(Deploy(scope = RemoteScope(address)))
        } else {
          WorkflowWorker
            .props(workerId, operatorExecutor, parentNetworkCommunicationActorRef)
            .withDeploy(Deploy(scope = RemoteScope(address)))
        }
      )
      parentNetworkCommunicationActorRef ! RegisterActorRef(workerId, ref)
      workerToLayer(workerId) = this
      workerId -> WorkerInfo(
        workerId,
        UNINITIALIZED,
        WorkerStatistics(UNINITIALIZED, 0, 0)
      )
    }: _*)
  }

}
