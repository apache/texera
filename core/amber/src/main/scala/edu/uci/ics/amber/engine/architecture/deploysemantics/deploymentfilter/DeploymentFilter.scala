package edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter

import akka.actor.Address
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer.WorkerLayer

trait DeploymentFilter extends Serializable {

  def filter(
      prev: Array[WorkerLayer],
      all: Array[Address],
      local: Address
  ): Array[Address]

}
