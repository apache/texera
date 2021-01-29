package edu.uci.ics.amber.engine.architecture.principal

object OperatorState {

  sealed abstract class OperatorState extends Product with Serializable
  case object Uninitialized extends OperatorState
  case object Ready extends OperatorState
  case object Running extends OperatorState
  case object Paused extends OperatorState
  case object Pausing extends OperatorState
  case object Completed extends OperatorState
  case object Recovering extends OperatorState
  case object Unknown extends OperatorState

}
