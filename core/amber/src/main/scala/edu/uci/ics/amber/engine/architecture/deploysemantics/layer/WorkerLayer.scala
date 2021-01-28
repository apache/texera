package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.DeployStrategy
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.DeploymentFilter
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, WorkerTag}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import akka.actor.{ActorContext, ActorRef, Address, Deploy}
import akka.remote.RemoteScope
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.RegisterActorRef
import edu.uci.ics.amber.engine.architecture.worker.{WorkerStatistics, WorkflowWorker}
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.{ActorVirtualIdentity, WorkerActorVirtualIdentity}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.{Uninitialized, WorkerState}

import scala.collection.mutable

class WorkerLayer(
    val tag: LayerTag,
    val metadata: Int => IOperatorExecutor,
    var numWorkers: Int,
    val deploymentFilter: DeploymentFilter,
    val deployStrategy: DeployStrategy
) extends Serializable {

  var identifiers: Array[ActorVirtualIdentity] = _
  var states: Array[WorkerState] = _
  var statistics: Array[WorkerStatistics] = _

  def isBuilt: Boolean = identifiers != null

  def build(
      prev: Array[(OpExecConfig, WorkerLayer)],
      all: Array[Address],
      parentNetworkCommunicationActorRef: ActorRef,
      context: ActorContext,
      workerToLayer:mutable.HashMap[ActorVirtualIdentity, WorkerLayer]
  ): Unit = {
    deployStrategy.initialize(deploymentFilter.filter(prev, all, context.self.path.address))
    identifiers = new Array[ActorVirtualIdentity](numWorkers)
    states = Array.fill(numWorkers)(Uninitialized)
    statistics = Array.fill(numWorkers)(WorkerStatistics(Uninitialized,0,0))
    for (i <- 0 until numWorkers) {
      val m = metadata(i)
      val workerTag = WorkerTag(tag, i)
      val id = WorkerActorVirtualIdentity(workerTag.getGlobalIdentity)
      val d = deployStrategy.next()
      val ref = context.actorOf(
        WorkflowWorker
          .props(id, m, parentNetworkCommunicationActorRef)
          .withDeploy(Deploy(scope = RemoteScope(d)))
      )
      parentNetworkCommunicationActorRef ! RegisterActorRef(id,ref)
      identifiers(i) = id
      workerToLayer(id) = this
    }
  }

}
