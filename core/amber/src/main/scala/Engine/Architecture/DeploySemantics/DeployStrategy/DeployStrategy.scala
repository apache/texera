package Engine.Architecture.DeploySemantics.DeployStrategy

import akka.actor.Address

trait DeployStrategy extends Serializable {

  def initialize(available:Array[Address])

  def next():Address

}
