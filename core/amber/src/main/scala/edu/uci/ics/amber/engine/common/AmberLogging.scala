package edu.uci.ics.amber.engine.common

import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import org.slf4j.LoggerFactory

trait AmberLogging {

  @transient
  protected lazy val logger: Logger = Logger(
    LoggerFactory.getLogger(
      s"${VirtualIdentityUtils.toShorterString(actorId)}] [${getClass.getSimpleName}"
    )
  )

  def actorId: ActorVirtualIdentity
}
