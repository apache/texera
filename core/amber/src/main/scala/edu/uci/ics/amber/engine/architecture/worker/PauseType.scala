package edu.uci.ics.amber.engine.architecture.worker

object PauseType extends Enumeration {
  val UserPause, BackpressurePause, OperatorLogicPause, SchedulerTimeSlotExpiredPause = Value
}
