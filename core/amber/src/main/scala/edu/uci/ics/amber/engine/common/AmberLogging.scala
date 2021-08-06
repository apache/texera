package edu.uci.ics.amber.engine.common

import com.typesafe.scalalogging.{CanLog, LazyLogging, Logger, LoggerTakingImplicit}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import org.slf4j.{LoggerFactory, MDC}

import scala.language.implicitConversions

trait AmberLogging {

  def actorId: ActorVirtualIdentity

  val logger: Logger = Logger(actorId.toString + "] [" + getClass.getName)
}
