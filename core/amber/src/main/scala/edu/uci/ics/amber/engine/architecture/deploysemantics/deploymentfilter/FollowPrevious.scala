package edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import akka.actor.Address
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer.WorkerLayer

object FollowPrevious {
  def apply() = new FollowPrevious()
}

class FollowPrevious extends DeploymentFilter {
  override def filter(
      prev: Array[WorkerLayer],
      all: Array[Address],
      local: Address
  ): Array[Address] = {
    all //the same behavior as useAll
  }
}
