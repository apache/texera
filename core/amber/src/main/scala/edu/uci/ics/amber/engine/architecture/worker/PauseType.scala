package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.core.virtualidentity.ChannelMarkerIdentity

sealed trait PauseType

object UserPause extends PauseType

object BackpressurePause extends PauseType

object OperatorLogicPause extends PauseType

object SchedulerTimeSlotExpiredPause extends PauseType

case class EpochMarkerPause(id: ChannelMarkerIdentity) extends PauseType
