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

package org.apache.texera.amber.engine.architecture.worker

import org.apache.texera.amber.core.virtualidentity.EmbeddedControlMessageIdentity
import org.scalatest.flatspec.AnyFlatSpec

class PauseTypeSpec extends AnyFlatSpec {

  // --- singletons ------------------------------------------------------------
  //
  // The sealed-trait subtype relationship is enforced at compile time by the
  // type ascriptions (`val u: PauseType = UserPause`, etc.) used below. There
  // is no runtime test for "singletons extend PauseType" because that would
  // be tautological — if any singleton stopped extending the trait, this
  // file would fail to compile.

  "PauseType singletons" should "compare equal to themselves and unequal to each other" in {
    // Widen to PauseType so the compiler doesn't reduce inter-singleton
    // comparisons to constant `false` at compile time.
    val u: PauseType = UserPause
    val b: PauseType = BackpressurePause
    val o: PauseType = OperatorLogicPause
    assert(u == UserPause)
    assert(b == BackpressurePause)
    assert(o == OperatorLogicPause)
    assert(u != b)
    assert(u != o)
    assert(b != o)
  }

  it should "be the same singleton instance per access (object identity)" in {
    assert((UserPause: AnyRef) eq UserPause)
    assert((BackpressurePause: AnyRef) eq BackpressurePause)
    assert((OperatorLogicPause: AnyRef) eq OperatorLogicPause)
  }

  // --- ECMPause --------------------------------------------------------------

  "ECMPause" should "carry the EmbeddedControlMessageIdentity it was constructed with" in {
    val id = EmbeddedControlMessageIdentity("ckpt-1")
    val p = ECMPause(id)
    assert(p.id == id)
  }

  it should "support case-class value equality and hashCode (same id → equal)" in {
    val a = ECMPause(EmbeddedControlMessageIdentity("ckpt-1"))
    val b = ECMPause(EmbeddedControlMessageIdentity("ckpt-1"))
    val c = ECMPause(EmbeddedControlMessageIdentity("ckpt-2"))
    assert(a == b)
    assert(a.hashCode == b.hashCode)
    assert(a != c)
  }

  it should "not equal any of the singleton PauseTypes" in {
    // Subtype relationship is already proven by the `: PauseType` ascription;
    // what we actually want to lock down here is the cross-kind inequality:
    // an ECMPause (with any id) must not collide with any singleton kind.
    val p: PauseType = ECMPause(EmbeddedControlMessageIdentity("ckpt"))
    assert(p != UserPause)
    assert(p != BackpressurePause)
    assert(p != OperatorLogicPause)
  }

  // --- pattern matching ------------------------------------------------------

  "PauseType" should "support exhaustive pattern matching that distinguishes each subtype" in {
    def label(p: PauseType): String =
      p match {
        case UserPause          => "user"
        case BackpressurePause  => "backpressure"
        case OperatorLogicPause => "operator-logic"
        case ECMPause(_)        => "ecm"
      }
    assert(label(UserPause) == "user")
    assert(label(BackpressurePause) == "backpressure")
    assert(label(OperatorLogicPause) == "operator-logic")
    assert(label(ECMPause(EmbeddedControlMessageIdentity("x"))) == "ecm")
  }

  // --- Set-based coexistence (the contract PauseManager actually relies on) --
  // PauseManager stores active pauses in a `HashSet[PauseType]` (additive,
  // no priority — resuming one type only removes that type). The override-order
  // semantics that the data type would need to support priorities don't exist
  // in PauseType; the data type only has to behave well as Set elements.
  // These tests pin that contract here. The multi-pause coexistence behavior
  // through PauseManager.pause/resume/isPaused is covered separately in
  // WorkerManagersSpec.

  it should "coexist as distinct elements in a Set without aliasing" in {
    val active: Set[PauseType] = Set(
      UserPause,
      BackpressurePause,
      OperatorLogicPause,
      ECMPause(EmbeddedControlMessageIdentity("ckpt-1"))
    )
    assert(active.size == 4, "all four pause kinds must be distinct Set elements")
    assert(active.contains(UserPause))
    assert(active.contains(BackpressurePause))
    assert(active.contains(OperatorLogicPause))
    assert(active.contains(ECMPause(EmbeddedControlMessageIdentity("ckpt-1"))))
  }

  it should "deduplicate identical pauses inside a Set" in {
    // PauseManager.pause(t) treats duplicate pauses as a no-op. That works
    // because Set deduplication leans on PauseType.equals/hashCode — pin it.
    val active: Set[PauseType] = Set(
      UserPause,
      UserPause, // singleton — must collapse
      ECMPause(EmbeddedControlMessageIdentity("ckpt-1")),
      ECMPause(EmbeddedControlMessageIdentity("ckpt-1")) // same id — must collapse
    )
    assert(active.size == 2)
  }

  it should "treat ECMPause instances with different ids as distinct Set elements" in {
    // Two checkpoint pauses with different ids must be independently
    // tracked, so the manager can resume one without clearing the other.
    val active: Set[PauseType] = Set(
      ECMPause(EmbeddedControlMessageIdentity("ckpt-1")),
      ECMPause(EmbeddedControlMessageIdentity("ckpt-2"))
    )
    assert(active.size == 2)
    val afterResumeFirst = active - ECMPause(EmbeddedControlMessageIdentity("ckpt-1"))
    assert(afterResumeFirst.size == 1)
    assert(afterResumeFirst.contains(ECMPause(EmbeddedControlMessageIdentity("ckpt-2"))))
  }
}
