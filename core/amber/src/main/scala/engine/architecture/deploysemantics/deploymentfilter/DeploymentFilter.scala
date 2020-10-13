package engine.architecture.deploysemantics.deploymentfilter

import engine.architecture.deploysemantics.layer.ActorLayer
import engine.operators.OpExecConfig
import akka.actor.Address

trait DeploymentFilter extends Serializable {

  def filter(
              prev: Array[(OpExecConfig, ActorLayer)],
              all: Array[Address],
              local: Address
  ): Array[Address]

}
