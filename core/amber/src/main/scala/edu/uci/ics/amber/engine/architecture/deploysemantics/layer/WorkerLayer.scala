package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.DeployStrategy
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.DeploymentFilter
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, WorkerTag}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import akka.actor.{ActorContext, ActorRef, Address, Deploy}
import akka.remote.RemoteScope
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.{
  ActorVirtualIdentity,
  WorkerActorVirtualIdentity
}

import scala.collection.mutable

class WorkerLayer(
    val tag: LayerTag,
    val metadata: Int => IOperatorExecutor,
    var numWorkers: Int,
    val deploymentFilter: DeploymentFilter,
    val deployStrategy: DeployStrategy
) extends Serializable {

  override def clone(): AnyRef = {
    val res = new WorkerLayer(tag, metadata, numWorkers, deploymentFilter, deployStrategy)
    res.layer = layer.clone()
    res.identifiers = identifiers.clone()
    res
  }

  var layer: Array[ActorRef] = _

  var identifiers: Array[ActorVirtualIdentity] = _

  def isBuilt: Boolean = layer != null

  def build(
      prev: Array[(OpExecConfig, WorkerLayer)],
      all: Array[Address],
      parentSenderActorRef: ActorRef
  )(implicit
      context: ActorContext
  ): Unit = {
    deployStrategy.initialize(deploymentFilter.filter(prev, all, context.self.path.address))
    layer = new Array[ActorRef](numWorkers)
    identifiers = new Array[ActorVirtualIdentity](numWorkers)
    for (i <- 0 until numWorkers) {
      val m = metadata(i)
      val workerTag = WorkerTag(tag, i)
      val id = WorkerActorVirtualIdentity(workerTag.getGlobalIdentity)
      val d = deployStrategy.next()
      layer(i) = context.actorOf(
        WorkflowWorker.props(id, m, parentSenderActorRef).withDeploy(Deploy(scope = RemoteScope(d)))
      )
      identifiers(i) = WorkerActorVirtualIdentity(workerTag.getGlobalIdentity)
    }
  }

  override def hashCode(): Int = tag.hashCode()
}
