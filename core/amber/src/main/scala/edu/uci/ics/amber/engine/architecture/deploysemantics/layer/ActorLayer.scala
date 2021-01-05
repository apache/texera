package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.DeployStrategy
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.DeploymentFilter
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, WorkerTag}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import akka.actor.{ActorContext, ActorRef, Address}
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity

import scala.collection.mutable

abstract class ActorLayer(
    val tag: LayerTag,
    var numWorkers: Int,
    val deploymentFilter: DeploymentFilter,
    val deployStrategy: DeployStrategy
) extends Serializable {

  override def clone(): AnyRef = ???

  var tagForFirst: WorkerTag = _

  var layer: Array[ActorRef] = _

  var identifiers: Array[ActorVirtualIdentity] = _

  def isBuilt: Boolean = layer != null

  def build(prev: Array[(OpExecConfig, ActorLayer)], all: Array[Address])(implicit
      context: ActorContext
  ): Unit

  def getFirstMetadata: Any

  override def hashCode(): Int = tag.hashCode()
}
