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
  * Characterization tests for the data carriers on `BoundaryValidator`'s
  * companion. In production the macro is the only place that constructs
  * these, so Jacoco never sees them at runtime; this spec pins the
  * apply/accessor contract that the rest of the macro pipeline depends on.
  */
class BoundaryValidatorSpec extends AnyFunSuite {

  test("BoundaryValidator companion object is loadable") {
    // Force a direct reference to the outer companion (not just the nested
    // CompileTimeContext / RuntimeContext) so its static initializer is
    // exercised by Jacoco.
    assert(BoundaryValidator.getClass.getName.endsWith("BoundaryValidator$"))
  }

  test("RuntimeContext apply binds every constructor argument to a val") {
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

  // Use a plain String for the `Pos` type parameter so the spec doesn't have
  // to pull in a macro `Context`. The class is generic precisely so tests
  // like this can construct it without a Universe.
  test("CompileTimeContext apply binds every constructor argument including the generic errorPos") {
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
}
