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

import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.{ToolBox, ToolBoxError}

/**
  * Runtime coverage for `EncodableInspector`.
  *
  * `EncodableInspector` only executes while the `pyb"..."` macro expands. In an ordinary build
  * that expansion happens before Jacoco is attached, so none of its classification logic is
  * observed at runtime. To exercise it inside the instrumented JVM we drive real macro expansions
  * through a runtime `ToolBox`.
  *
  * We cannot `eval` a `pyb` snippet: the macro expands to a call to the `private[amber]`
  * `PythonTemplateBuilder.fromInterpolated`, which the ToolBox's synthetic `__wrapper` package
  * cannot access, so evaluation always fails on that access check. Compilation is still useful,
  * though, because the macro fully expands (running the whole inspector) *before* that
  * post-typecheck access error surfaces. That gives two observable outcomes we assert on:
  *
  *   - '''boundary abort''': the inspector classified a *direct* argument as Encodable, so
  *     `BoundaryValidator.validateCompileTime` ran and aborted with a `@EncodableStringAnnotation`
  *     message (only possible when the arg was deemed Encodable and the context is unsafe);
  *   - '''benign expansion''': no boundary abort fired and the only failure is the private-access
  *     error on `fromInterpolated`, i.e. the inspector deemed the arg a plain literal (or a nested
  *     builder), the macro finished expanding, and lowering/`wrapArg` ran.
  *
  * Placing an Encodable-marked value next to a "bad neighbor" therefore proves it was classified
  * Encodable; placing a plain value there proves it was not.
  */
class EncodableInspectorSpec extends AnyFunSuite {

  private lazy val tb: ToolBox[scala.reflect.runtime.universe.type] = currentMirror.mkToolBox()

  private val header =
    """import org.apache.texera.amber.pybuilder.PythonTemplateBuilder._
      |import org.apache.texera.amber.pybuilder.PyStringTypes._
      |import org.apache.texera.amber.pybuilder.EncodableStringAnnotation""".stripMargin

  /** Marker present in every `BoundaryValidator` compile-time abort message. */
  private val boundaryMarker = "@EncodableStringAnnotation argument #"

  /** Compile a self-contained `pyb` snippet; it always fails, so capture the ToolBox message. */
  private def macroError(body: String): String =
    intercept[ToolBoxError] {
      tb.compile(tb.parse(s"{\n$header\n$body\n}"))
    }.getMessage

  /** Assert the arg was classified Encodable: an unsafe splice triggers a boundary abort. */
  private def assertClassifiedEncodable(body: String): Unit = {
    val msg = macroError(body)
    assert(msg.contains(boundaryMarker), s"expected an Encodable boundary abort, got: $msg")
  }

  /** Assert there was no BoundaryValidator compile-time abort: the macro expands and only fromInterpolated fails. */
  private def assertNotClassifiedEncodable(body: String): Unit = {
    val msg = macroError(body)
    assert(!msg.contains(boundaryMarker), s"unexpected Encodable boundary abort: $msg")
    assert(
      msg.contains("fromInterpolated"),
      s"expected the benign private-access failure after full expansion, got: $msg"
    )
  }

  // ========================================================================
  // Classified Encodable (unsafe splice => BoundaryValidator aborts).
  // Each case forces a distinct detection path inside the inspector.
  // ========================================================================

  test("TYPE_USE EncodableString alias is classified as Encodable") {
    // typeHasEncodableString -> AnnotatedType branch.
    assertClassifiedEncodable(
      """val ui: EncodableString = "x"
        |pyb"foo$ui"""".stripMargin
    )
  }

  test("inline String @EncodableStringAnnotation type is classified as Encodable") {
    assertClassifiedEncodable(
      """val ui: String @EncodableStringAnnotation = "x"
        |pyb"foo$ui"""".stripMargin
    )
  }

  test("@EncodableStringAnnotation local val (symbol annotation, inferred type) is Encodable") {
    // treeHasEncodableString -> symHasAnn via safeAccessed on the val's own symbol.
    assertClassifiedEncodable(
      """@EncodableStringAnnotation val ui = "x"
        |pyb"foo$ui"""".stripMargin
    )
  }

  test("@(EncodableStringAnnotation @field) case class field is Encodable via accessor hop") {
    // safeAccessed hops from the accessor to the annotated backing field.
    assertClassifiedEncodable(
      """import scala.annotation.meta.field
        |final case class Holder(@(EncodableStringAnnotation @field) ui: String)
        |val h = Holder("x")
        |pyb"foo${h.ui}"""".stripMargin
    )
  }

  test("@EncodableStringAnnotation def return type is classified as Encodable") {
    // methodReturnHasAnn -> typeHasEncodableString(finalResultType).
    assertClassifiedEncodable(
      """object Holder { @EncodableStringAnnotation def ui: String = "x" }
        |pyb"foo${Holder.ui}"""".stripMargin
    )
  }

  test("pre-wrapped EncodableStringRenderer arg is classified as Encodable") {
    // isDirectEncodableStringArg via the encodableStringRendererTpe subtype check.
    assertClassifiedEncodable(
      """val r = EncodableStringRenderer("x")
        |pyb"foo$r"""".stripMargin
    )
  }

  // ========================================================================
  // NOT classified Encodable (macro expands; only fromInterpolated access fails).
  // ========================================================================

  test("plain Int arg is not classified as Encodable") {
    assertNotClassifiedEncodable("""pyb"foo${42}"""")
  }

  test("unannotated String arg is not classified as Encodable") {
    assertNotClassifiedEncodable(
      """val raw: String = "x"
        |pyb"foo$raw"""".stripMargin
    )
  }

  test("case class param annotated WITHOUT @field is not reachable as Encodable via the accessor") {
    // The accessor's accessed symbol carries no annotation, so classification stays literal.
    assertNotClassifiedEncodable(
      """final case class Holder(@EncodableStringAnnotation ui: String)
        |val h = Holder("x")
        |pyb"foo${h.ui}"""".stripMargin
    )
  }

  test("pre-wrapped PyLiteralStringRenderer arg is not classified as Encodable") {
    assertNotClassifiedEncodable(
      """val r = PyLiteralStringRenderer("x")
        |pyb"foo$r"""".stripMargin
    )
  }

  test("nested PythonTemplateBuilder arg short-circuits the direct-Encodable check") {
    // isPythonTemplateBuilderArg true => isDirectEncodableStringArg returns false immediately,
    // so even a nested builder that carries Encodable content is never a *direct* Encodable arg.
    assertNotClassifiedEncodable(
      """val inner = pyb"${EncodableStringRenderer("x")}"
        |pyb"foo$inner"""".stripMargin
    )
  }

  // ========================================================================
  // wrapArg lowering: only runs when there is no compile-time abort, so a *safe*
  // splice of an Encodable value exercises wrapArg's EncodableStringRenderer branch.
  // (The literal and StringRenderer-cast branches are covered by the plain-value and
  //  PyLiteralStringRenderer cases above, whose lowering also runs.)
  // ========================================================================

  test("safe Encodable splice expands through wrapArg without a boundary abort") {
    assertNotClassifiedEncodable(
      """val ui: EncodableString = "x"
        |pyb"a $ui b"""".stripMargin
    )
  }
}
