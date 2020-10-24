package edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.ActorLayer
import edu.uci.ics.amber.engine.operators.OpExecConfig
import akka.actor.Address

trait DeploymentFilter extends Serializable {

  def filter(
              prev: Array[(OpExecConfig, ActorLayer)],
              all: Array[Address],
              local: Address
  ): Array[Address]

}
