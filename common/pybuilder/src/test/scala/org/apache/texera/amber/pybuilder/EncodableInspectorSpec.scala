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

import scala.language.experimental.macros
import scala.reflect.macros.blackbox
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.{ToolBox, ToolBoxError}

/**
  * Test-only probe macro: reports what [[EncodableInspector]] answers for a spliced argument.
  *
  * `EncodableInspector` is context-bound, so no plain unit test can call it: every method takes
  * `c.universe` types that only exist while a macro expands. The spec therefore drives real macro
  * expansions through a runtime `ToolBox`, and this object is the macro it drives. Because the
  * probe's expansion result is a plain `String` literal, `tb.eval` returns the inspector's actual
  * answers instead of the compile-error text the `pyb"..."`-based tests have to settle for
  * (`pyb` expands to the `private[amber]` `PythonTemplateBuilder.fromInterpolated`, which the
  * ToolBox's synthetic wrapper package may not access).
  *
  * The macro definitions and their implementations may share this compilation unit: the "macro
  * implementation not found" restriction applies at the *expansion* site, and every expansion here
  * happens later, in the ToolBox's separate compilation run.
  */
object EncodableInspectorProbe {

  /** `"ptb=<b> sr=<b> enc=<b> treeEnc=<b>"` for the spliced argument. */
  def classify(arg: Any): String = macro classifyImpl

  /** `showCode` of the tree `wrapArg` lowers the spliced argument to. */
  def wrapCode(arg: Any): String = macro wrapCodeImpl

  /** Same as [[classify]], but the inspector is handed a never-typechecked tree. */
  def classifyUntyped: String = macro classifyUntypedImpl

  /** Same as [[wrapCode]], but the inspector is handed a never-typechecked tree. */
  def wrapCodeUntyped: String = macro wrapCodeUntypedImpl

  def classifyImpl(c: blackbox.Context)(arg: c.Expr[Any]): c.Expr[String] =
    literal(c)(flags(c)(arg.tree))

  def wrapCodeImpl(c: blackbox.Context)(arg: c.Expr[Any]): c.Expr[String] =
    literal(c)(wrapped(c)(arg.tree))

  def classifyUntypedImpl(c: blackbox.Context): c.Expr[String] =
    literal(c)(flags(c)(untypedTree(c)))

  def wrapCodeUntypedImpl(c: blackbox.Context): c.Expr[String] =
    literal(c)(wrapped(c)(untypedTree(c)))

  /**
    * A freshly built tree that has never been through the typer, so its `tpe` is `null`.
    *
    * This is the fail-safe input that every `tpe != null` / `tree.tpe != null` guard in the
    * inspector exists for.
    */
  private def untypedTree(c: blackbox.Context): c.Tree = {
    import c.universe._
    Literal(Constant("x"))
  }

  private def literal(c: blackbox.Context)(value: String): c.Expr[String] = {
    import c.universe._
    c.Expr[String](Literal(Constant(value)))
  }

  private def flags(c: blackbox.Context)(tree: c.Tree): String = {
    val inspector = new EncodableInspector[c.type](c)
    val argExpr = c.Expr[Any](tree)
    val ptb = inspector.isPythonTemplateBuilderArg(argExpr)
    val sr = inspector.isStringRendererArg(argExpr)
    val enc = inspector.isDirectEncodableStringArg(argExpr)
    val treeEnc = inspector.treeHasEncodableString(tree)
    s"ptb=$ptb sr=$sr enc=$enc treeEnc=$treeEnc"
  }

  private def wrapped(c: blackbox.Context)(tree: c.Tree): String = {
    val inspector = new EncodableInspector[c.type](c)
    c.universe.showCode(inspector.wrapArg(c.Expr[Any](tree)))
  }
}

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

  /** Snippet preamble for the probe-macro tests. */
  private val probeHeader =
    s"""$header
       |import org.apache.texera.amber.pybuilder.EncodableInspectorProbe._""".stripMargin

  /** Expand a probe snippet inside the ToolBox and return the macro's result string. */
  private def evalProbe(body: String): String =
    tb.eval(tb.parse(s"{\n$probeHeader\n$body\n}")).asInstanceOf[String]

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

  // ========================================================================
  // Probe-macro tests: these read the classifier's *actual* answers rather than
  // inferring them from compile-error text, and they can hand the inspector
  // argument shapes `pyb"..."` cannot produce.
  //
  // `flags` is always "ptb=<b> sr=<b> enc=<b> treeEnc=<b>", i.e.
  // isPythonTemplateBuilderArg / isStringRendererArg / isDirectEncodableStringArg /
  // treeHasEncodableString, in that order.
  // ========================================================================

  // NOTE, so nobody over-reads the coverage these two tests buy: `isStringRendererArg` has no
  // caller anywhere in the repo outside this spec (`wrapArg` inlines the same subtype check).
  // Its body is therefore production code that only the test harness executes, which is exactly
  // why its two lines were never covered before. The predicate is still worth pinning - `wrapArg`
  // duplicates its logic - but the coverage it adds is not new *production* behaviour.
  test("isStringRendererArg accepts a StringRenderer-typed argument") {
    assert(
      evalProbe("""classify(PyLiteralStringRenderer("x"))""") ==
        "ptb=false sr=true enc=false treeEnc=false"
    )
  }

  test("isStringRendererArg rejects a plain String argument") {
    assert(evalProbe("""classify("x")""") == "ptb=false sr=false enc=false treeEnc=false")
  }

  test("isPythonTemplateBuilderArg accepts a PythonTemplateBuilder-typed argument") {
    // A PythonTemplateBuilder cannot be *constructed* inside a ToolBox snippet (its factory is
    // private[amber]), but a declaration of that type is enough: the probe macro only inspects
    // the argument tree, it never evaluates it.
    assert(
      evalProbe(
        """def nested: org.apache.texera.amber.pybuilder.PythonTemplateBuilder = ???
          |classify(nested)""".stripMargin
      ) == "ptb=true sr=false enc=false treeEnc=false"
    )
  }

  test("wrapArg keeps an existing EncodableStringRenderer as a cast rather than re-wrapping it") {
    // This fixture satisfies priority 1 only (`treeEnc=false`), so it pins the cast *branch* -
    // swapping its body with the literal fallback fails here - but NOT the branch *order*.
    // The order is pinned by "wrapArg prefers the StringRenderer cast ..." below.
    assert(
      evalProbe("""classify(EncodableStringRenderer("x"))""") ==
        "ptb=false sr=true enc=true treeEnc=false"
    )
    val wrap = evalProbe("""wrapCode(EncodableStringRenderer("x"))""")
    assert(wrap.contains(".asInstanceOf["), wrap)
    assert(wrap.endsWith("PythonTemplateBuilder.StringRenderer]"), wrap)
    assert(!wrap.contains(".toString"), wrap)
  }

  test("a never-typechecked argument tree is classified non-Encodable by every predicate") {
    // Pins the fail-safe default of the `tpe != null` / `argType != null` guards used by
    // `isPythonTemplateBuilderArg`, `isStringRendererArg`, `isDirectEncodableStringArg`, and `wrapArg`:
    // swapping either `&&` so the null check runs second turns this test into an NPE.
    //
    // Honest scope note: `pyb` cannot deliver such a tree - a blackbox macro's argument trees are
    // typechecked by construction - so these guards are defensive-only, reachable in practice only
    // through a probe like this one. And the extra `tree.tpe != null` check in `treeHasEncodableString`
    // is provably redundant rather than merely untested: `typeHasEncodableString` already answers false
    // for `null`, so deleting the guard is an equivalent mutation. No test can (or should) pretend to pin it.
    assert(evalProbe("classifyUntyped") == "ptb=false sr=false enc=false treeEnc=false")
  }

  test("a never-typechecked argument tree is lowered to a PyLiteralStringRenderer") {
    val wrap = evalProbe("wrapCodeUntyped")
    assert(wrap.contains("PyLiteralStringRenderer("), wrap)
    assert(!wrap.contains(".asInstanceOf["), wrap)
  }

  test("a type annotation other than @EncodableStringAnnotation leaves the argument a literal") {
    // Pins that `annIsEncodableString` actually discriminates (flipping its `==` to `!=` fails
    // here). It does NOT pin the `encodableStringAnnotationFqn` string itself: within one
    // compilation run there is exactly one symbol per fully-qualified name, so the fullName test
    // can only be true when line 68's `<:< typeOf[EncodableStringAnnotation]` is also true.
    // Pointing the constant at a non-existent FQN is therefore an equivalent mutation (measured:
    // it passes the whole module), and no test can pin it.
    assert(
      evalProbe(
        """class ZzAnn extends scala.annotation.StaticAnnotation
          |val d: String @ZzAnn = "x"
          |classify(d)""".stripMargin
      ) == "ptb=false sr=false enc=false treeEnc=false"
    )
  }

  test("a compound type with an Encodable-marked parent is Encodable") {
    assert(
      evalProbe(
        """type Marked = String @EncodableStringAnnotation
          |val mr: Marked with java.io.Serializable =
          |  "x".asInstanceOf[Marked with java.io.Serializable]
          |classify(mr)""".stripMargin
      ) == "ptb=false sr=false enc=true treeEnc=true"
    )
  }

  test("a compound type with no Encodable-marked parent is not Encodable") {
    assert(
      evalProbe(
        """val ur: String with java.io.Serializable =
          |  "x".asInstanceOf[String with java.io.Serializable]
          |classify(ur)""".stripMargin
      ) == "ptb=false sr=false enc=false treeEnc=false"
    )
  }

  test("an unmarked method-local val is classified from its type alone") {
    // A val inside a `def` body is a plain TermSymbol, not the object accessor a ToolBox
    // top-level val becomes, so `treeHasEncodableString` takes the non-MethodSymbol path.
    assert(
      evalProbe(
        """def go: String = { val loc: String = "x"; classify(loc) }
          |go""".stripMargin
      ) == "ptb=false sr=false enc=false treeEnc=false"
    )
  }

  test("a marked method-local val is Encodable via its annotated type") {
    assert(
      evalProbe(
        """def go: String = { val loc: EncodableString = "x"; classify(loc) }
          |go""".stripMargin
      ) == "ptb=false sr=false enc=true treeEnc=true"
    )
  }

  test("a def whose result type carries the marker is Encodable") {
    // End-to-end assertion: this shape must classify Encodable. It does NOT pin the
    // `methodReturnHasAnn` clause (lines 117-123) that it happens to execute - `tree.tpe` carries
    // the same annotation, so replacing the `MethodSymbol` arm with `false` passes every test in
    // the module on a forced clean rebuild (measured, including the compile-time `pyb` expansions
    // in PythonTemplateBuilderSpec). Isolating that arm would need a production seam, so it stays
    // unpinned rather than falsely credited.
    assert(
      evalProbe(
        """object H { def ui: EncodableString = "x" }
          |classify(H.ui)""".stripMargin
      ) == "ptb=false sr=false enc=true treeEnc=true"
    )
  }

  test("an Encodable marker nested in a type argument is found") {
    assert(
      evalProbe(
        """val xs: List[EncodableString] = List("x")
          |classify(xs)""".stripMargin
      ) == "ptb=false sr=false enc=true treeEnc=true"
    )
  }

  // ========================================================================
  // Repair pass: fixtures that make a *competing* guard, a non-first list
  // element, or a nested wrapper decide the answer. Each one exists because a
  // mutation of the production code survived the fixtures above.
  // ========================================================================

  test("wrapArg prefers the StringRenderer cast when an argument is BOTH a renderer and marked") {
    // Priority 1 (already a StringRenderer) must win over priority 2 (Encodable-marked), or a
    // pre-encoded payload would be re-encoded through `.toString`. The test above cannot show
    // that: its fixture satisfies priority 1 only (treeEnc=false), so no ordering can matter.
    // This fixture satisfies BOTH guards - `PyLiteralStringRenderer <:< StringRenderer` and the
    // `@EncodableStringAnnotation` sits on the def's own symbol - so the order decides.
    val bothMarked =
      """object H { @EncodableStringAnnotation def r: PyLiteralStringRenderer = ??? }"""
    val flags = evalProbe(s"""$bothMarked
                             |classify(H.r)""".stripMargin)
    assert(flags == "ptb=false sr=true enc=true treeEnc=true", flags)

    val wrap = evalProbe(s"""$bothMarked
                            |wrapCode(H.r)""".stripMargin)
    assert(wrap.contains(".asInstanceOf["), wrap)
    assert(wrap.endsWith("PythonTemplateBuilder.StringRenderer]"), wrap)
    assert(!wrap.contains("EncodableStringRenderer("), wrap)
    assert(!wrap.contains(".toString"), wrap)
  }

  test("a nested PythonTemplateBuilder is not a direct Encodable arg even when it is marked") {
    // isDirectEncodableStringArg's `if (isPythonTemplateBuilderArg(argExpr)) false` short-circuit
    // is what keeps a *marked* nested builder off the direct-Encodable path (and therefore out of
    // BoundaryValidator.validateCompileTime, which only guards direct args; nested builders get
    // runtime checks instead). The unmarked nested-builder test cannot show that: with no marker
    // the else-branch answers false on its own.
    assert(
      evalProbe(
        """object H {
          |  @EncodableStringAnnotation
          |  def nested: org.apache.texera.amber.pybuilder.PythonTemplateBuilder = ???
          |}
          |classify(H.nested)""".stripMargin
      ) == "ptb=true sr=false enc=false treeEnc=true"
    )
  }

  test("an Encodable marker under a second type annotation is found") {
    // Forces the `loop(underlying)` disjunct of the AnnotatedType case: the outer annotation is
    // not the marker, so the answer can only come from recursing into the annotated underlying
    // type. Every other fixture in this spec finds the marker in the outer annotation list.
    assert(
      evalProbe(
        """class ZzAnnUnder extends scala.annotation.StaticAnnotation
          |type Inner = String @EncodableStringAnnotation
          |val n: Inner @ZzAnnUnder = "x".asInstanceOf[Inner @ZzAnnUnder]
          |classify(n)""".stripMargin
      ) == "ptb=false sr=false enc=true treeEnc=true"
    )
  }

  test("an Encodable marker that is not the first annotation on a type is found") {
    // scalac's stored annotation order need not match source order, so both source orders are
    // asserted: whichever way round it stores them, one of the two puts the marker off the head
    // and pins the `exists` scan rather than just position 0.
    val markerFirst = evalProbe(
      """class ZzAnnT1 extends scala.annotation.StaticAnnotation
        |val d: String @EncodableStringAnnotation @ZzAnnT1 = "x"
        |classify(d)""".stripMargin
    )
    assert(markerFirst == "ptb=false sr=false enc=true treeEnc=true", markerFirst)

    val markerSecond = evalProbe(
      """class ZzAnnT2 extends scala.annotation.StaticAnnotation
        |val d: String @ZzAnnT2 @EncodableStringAnnotation = "x"
        |classify(d)""".stripMargin
    )
    assert(markerSecond == "ptb=false sr=false enc=true treeEnc=true", markerSecond)
  }

  test("an Encodable marker that is not the first annotation on a symbol is found") {
    // Same idea for the symbol-annotation path (safeAccessed(...).annotations), which no other
    // fixture in the module reaches with more than one annotation present.
    val markerFirst = evalProbe(
      """class ZzAnnS1 extends scala.annotation.StaticAnnotation
        |def go: String = { @EncodableStringAnnotation @ZzAnnS1 val loc = "x"; classify(loc) }
        |go""".stripMargin
    )
    assert(markerFirst == "ptb=false sr=false enc=true treeEnc=true", markerFirst)

    val markerSecond = evalProbe(
      """class ZzAnnS2 extends scala.annotation.StaticAnnotation
        |def go: String = { @ZzAnnS2 @EncodableStringAnnotation val loc = "x"; classify(loc) }
        |go""".stripMargin
    )
    assert(markerSecond == "ptb=false sr=false enc=true treeEnc=true", markerSecond)
  }

  test("an Encodable marker on a non-first parent of a compound type is found") {
    // The marked-parent test above puts the marker in parents(0), so it cannot tell
    // `parents.exists(loop)` apart from `parents.head`.
    assert(
      evalProbe(
        """trait Ta; trait Tb
          |type MarkedB = Tb @EncodableStringAnnotation
          |val v: Ta with MarkedB = (new Ta with Tb {}).asInstanceOf[Ta with MarkedB]
          |classify(v)""".stripMargin
      ) == "ptb=false sr=false enc=true treeEnc=true"
    )
  }

  test("an Encodable marker in a non-first type argument is found") {
    // `List[EncodableString]` has a single type argument, so it cannot tell `args.exists(loop)`
    // apart from `args.head`. A map of UI strings is the obvious real shape that can.
    assert(
      evalProbe(
        """val m: Map[String, EncodableString] = Map("k" -> "v")
          |classify(m)""".stripMargin
      ) == "ptb=false sr=false enc=true treeEnc=true"
    )
  }
}
