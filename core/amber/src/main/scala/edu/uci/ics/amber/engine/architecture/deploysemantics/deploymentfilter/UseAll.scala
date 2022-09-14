package edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter
import akka.actor.Address
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer.WorkerLayer

object UseAll {
  def apply() = new UseAll()
}

class UseAll extends DeploymentFilter {
  override def filter(
      prev: Array[WorkerLayer],
      all: Array[Address],
      local: Address
  ): Array[Address] = all
}
