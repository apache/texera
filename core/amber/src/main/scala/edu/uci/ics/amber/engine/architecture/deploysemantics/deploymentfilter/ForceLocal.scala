package edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import akka.actor.Address
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer.WorkerLayer

object ForceLocal {
  def apply() = new ForceLocal()
}

class ForceLocal extends DeploymentFilter {
  override def filter(
      prev: Array[WorkerLayer],
      all: Array[Address],
      local: Address
  ): Array[Address] = Array(local)
}
