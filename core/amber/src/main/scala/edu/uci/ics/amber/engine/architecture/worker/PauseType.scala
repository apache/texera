package edu.uci.ics.amber.engine.architecture.worker

sealed trait PauseType

object UserPause extends PauseType

object BackpressurePause extends PauseType

object OperatorLogicPause extends PauseType

object SchedulerTimeSlotExpiredPause extends PauseType

case class EpochMarkerPause(id: String) extends PauseType
