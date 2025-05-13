/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package edu.uci.ics.amber.engine.common.statetransition

import edu.uci.ics.amber.core.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.statetransition.StateManager.{
  InvalidStateException,
  InvalidTransitionException
}
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity

object StateManager {

  case class InvalidStateException(msg: String, actorId: ActorVirtualIdentity)
      extends WorkflowRuntimeException(msg, Some(actorId))

  case class InvalidTransitionException(msg: String, actorId: ActorVirtualIdentity)
      extends WorkflowRuntimeException(msg, Some(actorId))
}

class StateManager[T](
    actorId: ActorVirtualIdentity,
    stateTransitionGraph: Map[T, Set[T]],
    initialState: T
) extends Serializable {

  private var currentState: T = initialState

  def assertState(state: T): Unit = {
    if (currentState != state) {
      throw InvalidStateException(
        s"except state = $state but current state = $currentState",
        actorId
      )
    }
  }

  def assertState(states: T*): Unit = {
    if (!states.contains(currentState)) {
      throw InvalidStateException(
        s"except state in [${states.mkString(",")}] but current state = $currentState",
        actorId
      )
    }
  }

  def conditionalTransitTo(currentState: T, targetState: T, callback: () => Unit): Unit = {
    if (getCurrentState == currentState) {
      transitTo(targetState)
      callback()
    }
  }

  def confirmState(state: T): Boolean = getCurrentState == state

  def getCurrentState: T = currentState

  def confirmState(states: T*): Boolean = states.contains(getCurrentState)

  def transitTo(state: T): Unit = {
    if (state == currentState) {
      return
      // throw InvalidTransitionException(s"current state is already $currentState")
    }

    if (!stateTransitionGraph.getOrElse(currentState, Set()).contains(state)) {
      throw InvalidTransitionException(s"cannot transit from $currentState to $state", actorId)
    }
    currentState = state
  }

}
