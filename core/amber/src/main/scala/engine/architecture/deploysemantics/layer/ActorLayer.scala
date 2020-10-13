package engine.architecture.deploysemantics.layer

import engine.architecture.deploysemantics.deploystrategy.DeployStrategy
import engine.architecture.deploysemantics.deploymentfilter.DeploymentFilter
import engine.common.ambertag.{LayerTag, WorkerTag}
import engine.operators.OpExecConfig
import akka.actor.{ActorContext, ActorRef, Address}

abstract class ActorLayer(
    val tag: LayerTag,
    var numWorkers: Int,
    val deploymentFilter: DeploymentFilter,
    val deployStrategy: DeployStrategy
) extends Serializable {

  override def clone(): AnyRef = ???

  var tagForFirst: WorkerTag = _

  var layer: Array[ActorRef] = _

  def isBuilt: Boolean = layer != null

  def build(prev: Array[(OpExecConfig, ActorLayer)], all: Array[Address])(implicit
                                                                          context: ActorContext
  ): Unit

  def getFirstMetadata: Any

  override def hashCode(): Int = tag.hashCode()
}
