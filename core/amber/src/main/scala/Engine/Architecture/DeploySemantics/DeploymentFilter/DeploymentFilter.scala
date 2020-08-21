package Engine.Architecture.DeploySemantics.DeploymentFilter

import Engine.Architecture.DeploySemantics.Layer.ActorLayer
import Engine.Operators.OperatorMetadata
import akka.actor.Address

trait DeploymentFilter extends Serializable {

  def filter(prev: Array[(OperatorMetadata, ActorLayer)], all: Array[Address], local: Address):Array[Address]



}
