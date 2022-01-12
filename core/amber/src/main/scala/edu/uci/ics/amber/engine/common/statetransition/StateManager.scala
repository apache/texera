package edu.uci.ics.amber.engine.common.statetransition

import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.statetransition.StateManager.{
  InvalidStateException,
  InvalidTransitionException
}

import scala.collection.mutable

object StateManager {

  case class InvalidStateException(message: String) extends WorkflowRuntimeException(message)

  case class InvalidTransitionException(message: String) extends WorkflowRuntimeException(message)
}

class StateManager[T](stateTransitionGraph: Map[T, Set[T]], initialState: T) {

  private val stateStack = mutable.Stack[T]()
  @volatile private var currentState: T = initialState

  stateStack.push(initialState)

  def assertState(state: T): Unit = {
    if (currentState != state) {
      throw InvalidStateException(s"except state = $state but current state = $currentState")
    }
  }

  def assertState(states: T*): Unit = {
    if (!states.contains(currentState)) {
      throw InvalidStateException(
        s"except state in [${states.mkString(",")}] but current state = $currentState"
      )
    }
  }

  def confirmState(state: T): Boolean = getCurrentState == state

  def getCurrentState: T = currentState

  def confirmState(states: T*): Boolean = states.contains(getCurrentState)

  def transitTo(state: T, discardOldStates: Boolean = true): Unit = {
    if (state == currentState) {
      return
      // throw InvalidTransitionException(s"current state is already $currentState")
    }
    if (discardOldStates) {
      stateStack.clear()
    }

    stateStack.push(state)

    if (!stateTransitionGraph.getOrElse(currentState, Set()).contains(state)) {
      throw InvalidTransitionException(s"cannot transit from $currentState to $state")
    }
    currentState = state
  }

  def backToPreviousState(): Unit = {
    if (stateStack.isEmpty) {
      throw InvalidTransitionException(s"there is no previous state for $currentState")
    }
    currentState = stateStack.pop()
  }

}
