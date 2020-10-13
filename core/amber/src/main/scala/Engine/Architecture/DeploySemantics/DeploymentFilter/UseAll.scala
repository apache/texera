package Engine.Architecture.DeploySemantics.DeploymentFilter
import Engine.Architecture.DeploySemantics.Layer.ActorLayer
import Engine.Operators.OpExecConfig
import akka.actor.Address

object UseAll {
  def apply() = new UseAll()
}

class UseAll extends DeploymentFilter {
  override def filter(
                       prev: Array[(OpExecConfig, ActorLayer)],
                       all: Array[Address],
                       local: Address
  ): Array[Address] = all
}
