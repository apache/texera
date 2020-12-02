package edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.ActorLayer
import edu.uci.ics.amber.engine.operators.OpExecConfig
import akka.actor.Address

object FollowPrevious {
  def apply() = new FollowPrevious()
}

class FollowPrevious extends DeploymentFilter {
  override def filter(
      prev: Array[(OpExecConfig, ActorLayer)],
      all: Array[Address],
      local: Address
  ): Array[Address] = {
    val tmp: Array[Address] = prev.flatMap(x => x._2.layer.map(y => y.path.address))
    val result = tmp.distinct.intersect(all)
    if (result.isEmpty) all else result //fall back to UseAll when there is nothing to follow
  }
}
