package engine.architecture.deploysemantics.deploymentfilter
import engine.architecture.deploysemantics.layer.ActorLayer
import engine.operators.OpExecConfig
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
