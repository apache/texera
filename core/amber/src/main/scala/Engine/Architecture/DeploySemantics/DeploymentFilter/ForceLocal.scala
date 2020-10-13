package Engine.Architecture.DeploySemantics.DeploymentFilter
import Engine.Architecture.DeploySemantics.Layer.ActorLayer
import Engine.Operators.OpExecConfig
import akka.actor.Address

object ForceLocal {
  def apply() = new ForceLocal()
}

class ForceLocal extends DeploymentFilter {
  override def filter(
                       prev: Array[(OpExecConfig, ActorLayer)],
                       all: Array[Address],
                       local: Address
  ): Array[Address] = Array(local)
}
