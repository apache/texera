package edu.uci.ics.amber.engine.common.virtualidentity

import edu.uci.ics.amber.virtualidentity.{
  ActorVirtualIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}

object util {

  lazy val CONTROLLER: ActorVirtualIdentity = ActorVirtualIdentity("CONTROLLER")
  lazy val SELF: ActorVirtualIdentity = ActorVirtualIdentity("SELF")
  lazy val CLIENT: ActorVirtualIdentity = ActorVirtualIdentity("CLIENT")

  lazy val SOURCE_STARTER_ACTOR: ActorVirtualIdentity = ActorVirtualIdentity("SOURCE_STARTER")
  lazy val SOURCE_STARTER_OP: PhysicalOpIdentity =
    PhysicalOpIdentity(OperatorIdentity("SOURCE_STARTER"), "SOURCE_STARTER")

}
