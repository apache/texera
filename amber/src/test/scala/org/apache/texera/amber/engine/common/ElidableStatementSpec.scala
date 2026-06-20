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

package org.apache.texera.amber.engine.common

import org.scalatest.flatspec.AnyFlatSpec

class ElidableStatementSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Context — the texera build sets `-Xelide-below WARNING` (see
  // `amber/build.sbt`). Every `ElidableStatement` helper is annotated with
  // an elide level strictly below WARNING (FINEST / FINER / FINE / INFO),
  // so the Scala compiler replaces every CALL to these helpers with a
  // `()` Unit value at *compile* time. The by-name block argument is
  // never even constructed, let alone evaluated, in production / test
  // builds — that is the entire point of the abstraction.
  //
  // This spec pins that contract: a regression that bumped a method's
  // elide level above WARNING (e.g. `@elidable(SEVERE)`), removed the
  // `@elidable` annotation, or relaxed `-Xelide-below` in the build
  // would re-enable side effects and break the silent-in-production
  // promise — and this spec would catch it.
  // ---------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  // Each helper compiles to a no-op (block side effect does NOT fire)
  // ---------------------------------------------------------------------------

  "ElidableStatement.finest" should
    "be elided at the build's elide level — its by-name block must NOT execute" in {
    var counter = 0
    ElidableStatement.finest { counter += 1 }
    assert(counter == 0, "block should be elided away, counter must remain at 0")
  }

  "ElidableStatement.finer" should
    "be elided at the build's elide level — its by-name block must NOT execute" in {
    var counter = 0
    ElidableStatement.finer { counter += 1 }
    assert(counter == 0)
  }

  "ElidableStatement.fine" should
    "be elided at the build's elide level — its by-name block must NOT execute" in {
    var counter = 0
    ElidableStatement.fine { counter += 1 }
    assert(counter == 0)
  }

  "ElidableStatement.info" should
    "be elided at the build's elide level — its by-name block must NOT execute" in {
    var counter = 0
    ElidableStatement.info { counter += 1 }
    assert(counter == 0)
  }

  // ---------------------------------------------------------------------------
  // Even a throwing block must NOT propagate — it's never evaluated.
  // ---------------------------------------------------------------------------

  "Elided helpers" should
    "not propagate an exception that would have been thrown by their block" in {
    // If `info` accidentally stopped being elided, this would re-raise the
    // RuntimeException and fail the test. Pinning the suppression directly
    // catches that regression.
    ElidableStatement.info { throw new RuntimeException("must never fire") }
    ElidableStatement.fine { throw new RuntimeException("must never fire") }
    ElidableStatement.finer { throw new RuntimeException("must never fire") }
    ElidableStatement.finest { throw new RuntimeException("must never fire") }
    succeed
  }

  // ---------------------------------------------------------------------------
  // Multiple calls don't accumulate side effects (each one is independently
  // elided).
  // ---------------------------------------------------------------------------

  "Repeated elided calls" should "stay no-ops across 1000 invocations" in {
    var counter = 0
    var i = 0
    while (i < 1000) {
      ElidableStatement.info { counter += 1 }
      i += 1
    }
    assert(
      counter == 0,
      s"1000 elided info calls should not accumulate side effects, got: $counter"
    )
  }

  // ---------------------------------------------------------------------------
  // Return-type contract — each helper still type-checks as `=> Unit ⇒ Unit`.
  // ---------------------------------------------------------------------------

  "ElidableStatement methods" should "all return Unit (compile-time enforced)" in {
    // Assignments would fail to typecheck if a method's signature drifted
    // — e.g. someone made `info` return the block's result. The fact that
    // these compile under `-Xelide-below WARNING` also confirms each call
    // is replaced with the Unit `()` value, not with an exception.
    val r1: Unit = ElidableStatement.info { () }
    val r2: Unit = ElidableStatement.fine { () }
    val r3: Unit = ElidableStatement.finer { () }
    val r4: Unit = ElidableStatement.finest { () }
    assert(r1 == r2 && r2 == r3 && r3 == r4)
  }

  // ---------------------------------------------------------------------------
  // By-name parameter shape — each helper accepts a `=> Unit` block
  // (verified at compile time by passing a parameter-less lambda body).
  // ---------------------------------------------------------------------------

  "ElidableStatement methods" should "accept a by-name `=> Unit` argument (compile-time enforced)" in {
    // The fact that these expressions compile proves the parameter shape:
    // a value-typed expression of type Unit AND a thunk that runs side
    // effects are both accepted. Under `-Xelide-below WARNING`, neither
    // executes — but the type contract still holds.
    ElidableStatement.info { () }
    ElidableStatement.info { val x = 1; val y = x + 1; () }
    ElidableStatement.info {
      println("debug")
    }
    succeed
  }
}
