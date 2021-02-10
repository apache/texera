package edu.uci.ics.amber.engine.common.statetransition

import edu.uci.ics.amber.engine.common.statetransition.StateManager.IntermediateState
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager._

// The following pattern is a good practice of enum in scala
// We've always used this pattern in the codebase
// https://nrinaudo.github.io/scala-best-practices/definitions/adt.html
// https://nrinaudo.github.io/scala-best-practices/adts/product_with_serializable.html

object WorkerStateManager {
  sealed abstract class WorkerState extends Serializable
  case object Uninitialized extends WorkerState
  case object Ready extends WorkerState
  case object Running extends WorkerState
  case object Paused extends WorkerState
  case object Pausing extends WorkerState with IntermediateState
  case object Completed extends WorkerState
  case object Recovering extends WorkerState with IntermediateState

}

class WorkerStateManager
    extends StateManager[WorkerState](
      Map(
        Uninitialized -> Set(Ready, Recovering),
        Ready -> Set(Pausing, Running, Recovering),
        Running -> Set(Pausing, Completed, Recovering),
        Pausing -> Set(Paused, Recovering),
        Paused -> Set(Running, Recovering),
        Completed -> Set(Recovering),
        Recovering -> Set(Uninitialized, Ready, Running, Pausing, Paused, Completed)
      ),
      Uninitialized
    ) {

  private var isStarted = false

}
