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

package org.apache.texera.amber.pybuilder

import org.apache.texera.amber.pybuilder.BoundaryValidator.{CompileTimeContext, RuntimeContext}
import org.scalatest.funsuite.AnyFunSuite

/**
  * Characterization tests for the case classes hosted on `BoundaryValidator`'s
  * companion. The case classes themselves are only constructed during macro
  * expansion in production (so Jacoco never sees them at runtime), but they
  * are part of the public surface and the rest of the codebase depends on
  * their `apply`/accessor/equality contract.
  *
  * This spec exercises construction, field access, structural equality, and
  * `copy`, both to pin the data-class contract and so the generated members
  * show up in coverage.
  */
class BoundaryValidatorSpec extends AnyFunSuite {

  test("RuntimeContext exposes its fields verbatim") {
    val ctx = RuntimeContext(
      leftPart = "left",
      rightPart = "right",
      prefixSource = "prefix",
      argIndex = 0
    )

    assert(ctx.leftPart == "left")
    assert(ctx.rightPart == "right")
    assert(ctx.prefixSource == "prefix")
    assert(ctx.argIndex == 0)
  }

  test("RuntimeContext supports structural equality and copy") {
    val a = RuntimeContext("l", "r", "p", 1)
    val b = RuntimeContext("l", "r", "p", 1)
    val c = a.copy(argIndex = 2)

    assert(a == b)
    assert(a.hashCode == b.hashCode)
    assert(c.argIndex == 2)
    assert(c != a)
  }

  // Use a plain String for the `Pos` type parameter so the spec doesn't have
  // to pull in a macro `Context`. The case class is generic precisely so
  // tests like this can construct it without a Universe.
  test("CompileTimeContext exposes its fields including the generic errorPos") {
    val ctx = CompileTimeContext[String](
      leftPart = "left",
      rightPart = "right",
      prefixSource = "prefix",
      argIndex = 3,
      errorPos = "Foo.scala:42"
    )

    assert(ctx.leftPart == "left")
    assert(ctx.rightPart == "right")
    assert(ctx.prefixSource == "prefix")
    assert(ctx.argIndex == 3)
    assert(ctx.errorPos == "Foo.scala:42")
  }

  test("CompileTimeContext supports structural equality and copy") {
    val a = CompileTimeContext[String]("l", "r", "p", 0, "pos")
    val b = CompileTimeContext[String]("l", "r", "p", 0, "pos")
    val c = a.copy(errorPos = "other")

    assert(a == b)
    assert(a.hashCode == b.hashCode)
    assert(c.errorPos == "other")
    assert(c != a)
  }
}
