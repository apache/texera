package Engine.Architecture.DeploySemantics.DeploymentFilter

import Engine.Architecture.DeploySemantics.Layer.ActorLayer
import Engine.Operators.OpExecConfig
import akka.actor.Address

trait DeploymentFilter extends Serializable {

  def filter(
              prev: Array[(OpExecConfig, ActorLayer)],
              all: Array[Address],
              local: Address
  ): Array[Address]

}
