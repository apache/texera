package engine.architecture.deploysemantics.deploymentfilter
import engine.architecture.deploysemantics.layer.ActorLayer
import engine.operators.OpExecConfig
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
