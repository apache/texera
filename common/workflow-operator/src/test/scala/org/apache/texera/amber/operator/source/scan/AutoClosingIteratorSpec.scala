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

package org.apache.texera.amber.operator.source.scan

import org.scalatest.flatspec.AnyFlatSpec

class AutoClosingIteratorSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // hasNext + onClose firing semantics
  // ---------------------------------------------------------------------------

  "AutoClosingIterator.hasNext (non-empty underlying)" should
    "return true and NOT invoke onClose" in {
    var closed = false
    val it = new AutoClosingIterator[Int](Iterator(1, 2, 3), () => closed = true)
    assert(it.hasNext)
    assert(!closed, "onClose must not fire while elements remain")
  }

  "AutoClosingIterator.hasNext (exhausted underlying)" should
    "return false and invoke onClose exactly once" in {
    var closeCount = 0
    val it = new AutoClosingIterator[Int](Iterator.empty, () => closeCount += 1)
    assert(!it.hasNext)
    assert(closeCount == 1)
  }

  it should "NOT invoke onClose again on a second hasNext after exhaustion" in {
    var closeCount = 0
    val it = new AutoClosingIterator[Int](Iterator.empty, () => closeCount += 1)
    assert(!it.hasNext)
    assert(!it.hasNext)
    assert(!it.hasNext)
    assert(closeCount == 1, s"onClose must fire exactly once, got $closeCount calls")
  }

  // ---------------------------------------------------------------------------
  // next() — delegates straight through
  // ---------------------------------------------------------------------------

  "AutoClosingIterator.next" should "delegate to the wrapped iterator (in order)" in {
    val it = new AutoClosingIterator[Int](Iterator(10, 20, 30), () => ())
    assert(it.next() == 10)
    assert(it.next() == 20)
    assert(it.next() == 30)
  }

  // ---------------------------------------------------------------------------
  // Full traversal — onClose fires exactly once at the end
  // ---------------------------------------------------------------------------

  "AutoClosingIterator full traversal" should
    "yield every element of the wrapped iterator in order" in {
    val it = new AutoClosingIterator[Int](Iterator(1, 2, 3, 4, 5), () => ())
    assert(it.toList == List(1, 2, 3, 4, 5))
  }

  it should "fire onClose exactly once when toList finishes consuming" in {
    var closeCount = 0
    val it = new AutoClosingIterator[Int](Iterator(1, 2, 3), () => closeCount += 1)
    val _ = it.toList
    assert(closeCount == 1, s"expected single onClose firing, got $closeCount")
  }

  // ---------------------------------------------------------------------------
  // Already-empty source — close fires on the very first hasNext call
  // ---------------------------------------------------------------------------

  "AutoClosingIterator over an already-empty source" should
    "fire onClose on the very first hasNext call" in {
    var fired = false
    val it = new AutoClosingIterator[Int](Iterator.empty, () => fired = true)
    val _ = it.hasNext
    assert(fired, "onClose must fire when the source is already empty")
  }

  // ---------------------------------------------------------------------------
  // Mid-iteration close behavior — onClose does NOT fire before exhaustion
  // ---------------------------------------------------------------------------

  "AutoClosingIterator (mid-iteration)" should
    "leave onClose un-fired between elements (only fires after hasNext returns false)" in {
    var fired = false
    val it = new AutoClosingIterator[Int](Iterator(1, 2, 3), () => fired = true)
    // Step-by-step assertion: after each hasNext that returns TRUE,
    // onClose MUST still be un-fired. Only the hasNext that returns
    // false may flip `fired`. A bug that prematurely closed during a
    // truthy hasNext would surface here, not just at the loop's exit.
    assert(it.hasNext); assert(!fired, "onClose must not fire while element 1 is reachable")
    assert(it.next() == 1)
    assert(it.hasNext); assert(!fired, "onClose must not fire while element 2 is reachable")
    assert(it.next() == 2)
    assert(it.hasNext); assert(!fired, "onClose must not fire while element 3 is reachable")
    assert(it.next() == 3)
    // The final hasNext returns false — THIS is the call that fires onClose.
    assert(!it.hasNext)
    assert(fired, "after hasNext first returns false, onClose must have fired")
  }

  // ---------------------------------------------------------------------------
  // onClose exception propagation
  // ---------------------------------------------------------------------------

  "AutoClosingIterator" should
    "propagate exceptions thrown from onClose (no swallowing)" in {
    val it = new AutoClosingIterator[Int](
      Iterator.empty,
      () => throw new IllegalStateException("close failed")
    )
    val ex = intercept[IllegalStateException] {
      it.hasNext
    }
    assert(ex.getMessage == "close failed")
  }

  it should
    "re-invoke onClose on a retry when the previous onClose threw (alreadyClosed is set AFTER onClose)" in {
    // Reading the production code: `alreadyClosed = true` runs AFTER
    // `onClose()`. So if onClose throws, alreadyClosed stays false and
    // a second hasNext will re-invoke onClose. This is the OPPOSITE of
    // an "alreadyClosed once close was attempted" contract — characterize
    // the current (brittle) behavior so a refactor that swaps the order
    // (running `alreadyClosed = true` BEFORE `onClose()`) surfaces here.
    var closeCount = 0
    val it = new AutoClosingIterator[Int](
      Iterator.empty,
      () => {
        closeCount += 1
        throw new RuntimeException("boom")
      }
    )
    intercept[RuntimeException] { it.hasNext }
    intercept[RuntimeException] { it.hasNext } // current impl re-runs onClose
    assert(closeCount == 2, s"current impl re-fires onClose on retry; got $closeCount")
  }
}
