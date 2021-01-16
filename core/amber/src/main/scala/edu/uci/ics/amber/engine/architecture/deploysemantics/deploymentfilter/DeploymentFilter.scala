package edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.operators.OpExecConfig
import akka.actor.Address

trait DeploymentFilter extends Serializable {

  def filter(
      prev: Array[(OpExecConfig, WorkerLayer)],
      all: Array[Address],
      local: Address
  ): Array[Address]

}
