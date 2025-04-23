package edu.uci.ics.amber.engine.common.virtualidentity

import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity

object util {

  lazy val CONTROLLER: ActorVirtualIdentity = ActorVirtualIdentity("CONTROLLER")
  lazy val SELF: ActorVirtualIdentity = ActorVirtualIdentity("SELF")
  lazy val CLIENT: ActorVirtualIdentity = ActorVirtualIdentity("CLIENT")
}
