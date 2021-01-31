package edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.operators.OpExecConfig
import akka.actor.Address

object FollowPrevious {
  def apply() = new FollowPrevious()
}

class FollowPrevious extends DeploymentFilter {
  override def filter(
      prev: Array[(OpExecConfig, WorkerLayer)],
      all: Array[Address],
      local: Address
  ): Array[Address] = {
    all //the same behavior as useAll
  }
}
