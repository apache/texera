package edu.uci.ics.amber.engine.common

import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

trait AmberLogging {

  def actorId: ActorVirtualIdentity

  val logger: Logger = Logger(actorId.toString + "] [" + getClass.getName)
}
