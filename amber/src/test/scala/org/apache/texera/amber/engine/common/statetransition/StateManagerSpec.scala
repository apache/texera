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

package org.apache.texera.amber.engine.common.statetransition

import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.engine.common.statetransition.StateManager.{
  InvalidStateException,
  InvalidTransitionException
}
import org.scalatest.flatspec.AnyFlatSpec

class StateManagerSpec extends AnyFlatSpec {

  private sealed trait DummyState
  private case object S0 extends DummyState
  private case object S1 extends DummyState
  private case object S2 extends DummyState
  private case object Orphan extends DummyState

  private val actorId: ActorVirtualIdentity = ActorVirtualIdentity("test-actor")

  /** Linear graph S0 -> S1 -> S2; S2 is terminal. Orphan is unreachable. */
  private def linear(initial: DummyState = S0): StateManager[DummyState] =
    new StateManager[DummyState](
      actorId,
      Map(
        S0 -> Set(S1),
        S1 -> Set(S2),
        S2 -> Set.empty
      ),
      initial
    )

  "StateManager" should "report the initial state via getCurrentState" in {
    assert(linear(S1).getCurrentState == S1)
  }

  "StateManager.transitTo" should "advance to a state listed as a successor in the transition graph" in {
    val sm = linear()
    sm.transitTo(S1)
    assert(sm.getCurrentState == S1)
  }

  it should "be a no-op when transitioning to the current state" in {
    val sm = linear(S1)
    sm.transitTo(S1)
    assert(sm.getCurrentState == S1)
  }

  it should "throw InvalidTransitionException when the target is not a successor of the current state" in {
    val sm = linear()
    val ex = intercept[InvalidTransitionException] {
      sm.transitTo(S2)
    }
    assert(ex.getMessage.contains(S0.toString))
    assert(ex.getMessage.contains(S2.toString))
  }

  it should "throw InvalidTransitionException when transitioning out of a terminal state with no listed successors" in {
    val sm = linear(S2) // S2 is a key, but with `Set.empty`, so no transitions are allowed.
    intercept[InvalidTransitionException] {
      sm.transitTo(S0)
    }
  }

  it should "throw InvalidTransitionException when the current state is not a key in the transition graph" in {
    // Orphan is intentionally absent from `linear()`'s key set, so
    // `stateTransitionGraph.getOrElse(currentState, Set())` falls back to
    // empty and any target should be rejected.
    val sm = linear(Orphan)
    intercept[InvalidTransitionException] {
      sm.transitTo(S0)
    }
  }

  "StateManager.assertState" should "succeed when the current state matches" in {
    val sm = linear()
    sm.assertState(S0) // does not throw
    sm.assertState(S0, S1) // varargs: any-of
  }

  it should "throw InvalidStateException when the current state does not match the expected state" in {
    val sm = linear()
    intercept[InvalidStateException] {
      sm.assertState(S1)
    }
  }

  it should "throw InvalidStateException when none of the expected states match (varargs form)" in {
    val sm = linear()
    intercept[InvalidStateException] {
      sm.assertState(S1, S2)
    }
  }

  "StateManager.confirmState" should "report whether the current state matches" in {
    val sm = linear()
    assert(sm.confirmState(S0))
    assert(!sm.confirmState(S1))
  }

  it should "report whether the current state is one of the given states (varargs form)" in {
    val sm = linear()
    assert(sm.confirmState(S0, S1))
    assert(!sm.confirmState(S1, S2))
  }

  "StateManager.conditionalTransitTo" should "transition and run the callback when the precondition matches" in {
    val sm = linear()
    var called = false

    sm.conditionalTransitTo(S0, S1, () => called = true)

    assert(sm.getCurrentState == S1)
    assert(called)
  }

  it should "do nothing and skip the callback when the precondition does not match" in {
    val sm = linear()
    var called = false

    sm.conditionalTransitTo(S1, S2, () => called = true)

    assert(sm.getCurrentState == S0)
    assert(!called)
  }

  it should "still validate the transition graph and throw on an invalid transition" in {
    val sm = linear()
    intercept[InvalidTransitionException] {
      sm.conditionalTransitTo(S0, S2, () => ())
    }
  }
}
