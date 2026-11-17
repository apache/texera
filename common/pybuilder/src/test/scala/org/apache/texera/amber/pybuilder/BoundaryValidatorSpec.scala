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
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.{
  EncodableStringRenderer,
  PythonTemplateBuilderStringContext
}
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.{ToolBox, ToolBoxError}

/**
  * Tests for `BoundaryValidator`.
  *
  * The first block are characterization tests for the data carriers on the companion. In
  * production the macro is the only place that constructs these, so Jacoco never sees them at
  * runtime; this pins the apply/accessor contract the rest of the macro pipeline depends on.
  *
  * The rest drive the two validation methods through a runtime `ToolBox` so their branches execute
  * inside the instrumented JVM. A `pyb` snippet can only be *compiled* (not `eval`-run) through the
  * ToolBox, because the expanded code calls the `private[amber]` `fromInterpolated`, which the
  * ToolBox's synthetic `__wrapper` package cannot access. Compilation is enough: the macro fully
  * expands (running the validator) before that access error surfaces, giving two observable
  * outcomes we assert on via the captured `ToolBoxError` message:
  *
  *   - a `validateCompileTime` '''abort''' for a *direct* Encodable arg in an unsafe context
  *     (message carries the specific boundary reason), versus
  *   - a '''benign''' expansion whose only failure is the `fromInterpolated` access error, meaning
  *     the validator ran without aborting.
  *
  * `runtimeChecksForNestedBuilder` never aborts compilation (it emits a deferred runtime guard), so
  * its branches are covered by compiling nested-builder snippets in each context, and the emitted
  * guard's actual throw/pass behavior is verified separately with direct `pyb` interpolations.
  */
class BoundaryValidatorSpec extends AnyFunSuite {

  private lazy val tb: ToolBox[scala.reflect.runtime.universe.type] = currentMirror.mkToolBox()

  private val header =
    """import org.apache.texera.amber.pybuilder.PythonTemplateBuilder._
      |import org.apache.texera.amber.pybuilder.PyStringTypes._""".stripMargin

  /** Marker present in every `BoundaryValidator` compile-time abort message. */
  private val boundaryMarker = "@EncodableStringAnnotation argument #"

  /** Compile a self-contained `pyb` snippet; it always fails, so capture the ToolBox message. */
  private def macroError(body: String): String =
    intercept[ToolBoxError] {
      tb.compile(tb.parse(s"{\n$header\n$body\n}"))
    }.getMessage

  /** Assert `validateCompileTime` aborted with the given reason fragment. */
  private def assertAborts(body: String, fragment: String): Unit = {
    val msg = macroError(body)
    assert(msg.contains(boundaryMarker), s"expected a boundary abort, got: $msg")
    assert(msg.contains(fragment), s"expected reason fragment [$fragment], got: $msg")
  }

  /** Assert `validateCompileTime` did NOT abort: only the benign fromInterpolated access fails. */
  private def assertNoAbort(body: String): Unit = {
    val msg = macroError(body)
    assert(!msg.contains(boundaryMarker), s"unexpected boundary abort: $msg")
    assert(
      msg.contains("fromInterpolated"),
      s"expected the benign private-access failure after full expansion, got: $msg"
    )
  }

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

  // ========================================================================
  // validateCompileTime: direct Encodable args, unsafe boundaries -> abort.
  // Each abort asserts on the specific templated reason from BoundaryErrors.
  // ========================================================================

  test("validateCompileTime aborts when a direct Encodable arg is inside a quoted string") {
    assertAborts(
      """val ui: EncodableString = "x"
        |pyb"print('$ui')"""".stripMargin,
      "inside a quoted Python string literal"
    )
  }

  test("validateCompileTime aborts when a direct Encodable arg follows a comment marker") {
    assertAborts(
      """val ui: EncodableString = "x"
        |pyb"code # $ui"""".stripMargin,
      "after a '#' comment marker"
    )
  }

  test("validateCompileTime aborts when a direct Encodable arg is glued to the left neighbor") {
    assertAborts(
      """val ui: EncodableString = "x"
        |pyb"foo$ui"""".stripMargin,
      "on the left"
    )
  }

  test("validateCompileTime aborts when a direct Encodable arg is glued to the right neighbor") {
    assertAborts(
      """val ui: EncodableString = "x"
        |pyb"${ui}bar"""".stripMargin,
      "on the right"
    )
  }

  test("validateCompileTime aborts on a quote neighbor (isBadNeighbor quote, not identifier)") {
    // Exercises the isBadNeighbor quote branch rather than the ident branch.
    assertAborts(
      """val ui: EncodableString = "x"
        |pyb"${ui}'"""".stripMargin,
      "on the right"
    )
  }

  test("validateCompileTime allows a direct Encodable arg with whitespace neighbors") {
    // All four checks fall through: no unclosed quote, no comment, both neighbors safe.
    assertNoAbort(
      """val ui: EncodableString = "x"
        |pyb"foo $ui bar"""".stripMargin
    )
  }

  test("validateCompileTime allows a direct Encodable arg with empty left and right parts") {
    // Exercises both `leftPart.nonEmpty == false` and `rightPart.nonEmpty == false` branches.
    assertNoAbort(
      """val ui: EncodableString = "x"
        |pyb"$ui"""".stripMargin
    )
  }

  test("validateCompileTime allows a direct Encodable arg next to safe punctuation") {
    // leftPart/rightPart are non-empty but the neighbors are not bad (comma/paren).
    assertNoAbort(
      """val ui: EncodableString = "x"
        |pyb"f($ui, 1)"""".stripMargin
    )
  }

  // ========================================================================
  // runtimeChecksForNestedBuilder: never aborts, emits a deferred guard.
  // Compiling nested-builder snippets in each context runs every branch of
  // the method (insideQuoted / afterComment / left / right / empty -> Nil).
  // ========================================================================

  test("nested builder inside quotes expands with a deferred guard (no compile abort)") {
    assertNoAbort(
      """val inner = pyb"${EncodableStringRenderer("x")}"
        |pyb"print('$inner')"""".stripMargin
    )
  }

  test("nested builder after a comment marker expands with a deferred guard (no compile abort)") {
    assertNoAbort(
      """val inner = pyb"${EncodableStringRenderer("x")}"
        |pyb"code # $inner"""".stripMargin
    )
  }

  test(
    "nested builder glued to the left neighbor expands with a deferred guard (no compile abort)"
  ) {
    assertNoAbort(
      """val inner = pyb"${EncodableStringRenderer("x")}"
        |pyb"foo$inner"""".stripMargin
    )
  }

  test(
    "nested builder glued to the right neighbor expands with a deferred guard (no compile abort)"
  ) {
    assertNoAbort(
      """val inner = pyb"${EncodableStringRenderer("x")}"
        |pyb"${inner}bar"""".stripMargin
    )
  }

  test("nested builder in a safe context emits no guard at all (throwStmts empty -> Nil)") {
    assertNoAbort(
      """val inner = pyb"${EncodableStringRenderer("x")}"
        |pyb"foo $inner bar"""".stripMargin
    )
  }

  test("nested builder at the string edges emits no guard (both neighbor Options are None)") {
    assertNoAbort(
      """val inner = pyb"${EncodableStringRenderer("x")}"
        |pyb"$inner"""".stripMargin
    )
  }

  // ========================================================================
  // Behavior of the emitted guard (direct pyb; the throw/pass happens at
  // runtime). These pin that the deferred check actually fires only when the
  // nested builder carries Encodable content AND the context is unsafe.
  // ========================================================================

  test("emitted guard throws when a nested Encodable builder sits in an unsafe context") {
    val inner = pyb"${EncodableStringRenderer("x")}"
    intercept[IllegalArgumentException](pyb"print('$inner')")
    intercept[IllegalArgumentException](pyb"code # $inner")
    intercept[IllegalArgumentException](pyb"foo$inner")
    intercept[IllegalArgumentException](pyb"${inner}bar")
  }

  test("emitted guard does not throw when the nested Encodable builder is in a safe context") {
    val inner = pyb"${EncodableStringRenderer("x")}"
    assert(pyb"foo $inner bar".plain == "foo x bar")
    assert(pyb"$inner".plain == "x")
  }

  test("no guard fires when the nested builder carries no Encodable content") {
    val inner = pyb"hello"
    assert(pyb"foo$inner".plain == "foohello")
    assert(pyb"print('$inner')".plain == "print('hello')")
  }
}
