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

import org.apache.texera.amber.pybuilder.PyStringTypes.{EncodableString, PythonLiteral}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.{EncodableStringRenderer, PyLiteralStringRenderer, PythonTemplateBuilderStringContext}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.annotation.meta.field
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

class PythonTemplateBuilderSpec extends AnyFunSuite {

  // ---------- Helpers ----------
  private def base64Of(text: String): String =
    Base64.getEncoder.encodeToString(text.getBytes(StandardCharsets.UTF_8))

  private def decodeExpr(text: String): String =
    PythonTemplateBuilder.wrapWithPythonDecoderExpr(base64Of(text))
  // Toolbox helpers: used to assert runtime exceptions without checking error strings.
  private lazy val tb: ToolBox[scala.reflect.runtime.universe.type] = currentMirror.mkToolBox()

  private def inPybuilderPkg(code: String): String =
    s"""package org.apache.texera.amber.pybuilder {
       |
       |$code
       |
       |}""".stripMargin

  private def assertToolboxDoesNotCompile(code: String): Unit = {
    intercept[Throwable] {
      // compile only (don’t run); macro expansion happens during compilation
      tb.compile(tb.parse(inPybuilderPkg(code)))
    }
    ()
  }
  // Unicode escapes in *generated* Scala source: must be written as "\\uXXXX" in this test file.
  private def scalaUnicodeEscape(ch: Char): String =
    f"\\\\u${ch.toInt}%04X"

  // ========================================================================
  // Rendering basics (plain vs encoded)
  // ========================================================================

  test("plain renders empty text") {
    val builder = pyb""
    assert(builder.plain == "")
  }

  test("plain renders literal text") {
    val builder = pyb"hello"
    assert(builder.plain == "hello")
  }

  test("encoded renders literal text (no UI args) same as plain") {
    val builder = pyb"hello"
    assert(builder.encode == "hello")
  }

  test("toString defaults to encoded") {
    val builder = pyb"hello"
    assert(builder.toString == builder.encode)
  }

  test("StringPyMk renders raw in both modes") {
    val pyFragment = PyLiteralStringRenderer("print('x')")
    assert(pyFragment.render(PythonTemplateBuilder.RenderMode.Plain) == "print('x')")
    assert(pyFragment.render(PythonTemplateBuilder.RenderMode.Encode) == "print('x')")
  }

  test("EncodableString renders raw in plain mode") {
    val uiText = EncodableStringRenderer("abc")
    assert(uiText.render(PythonTemplateBuilder.RenderMode.Plain) == "abc")
  }

  test("EncodableString renders B64.decode('<base64>') in encoded mode") {
    val rawText = "abc"
    val uiText = EncodableStringRenderer(rawText)
    assert(uiText.render(PythonTemplateBuilder.RenderMode.Encode) == decodeExpr(rawText))
  }

  test("EncodableString base64 uses UTF-8 and handles unicode") {
    val rawText = "你好 👋"
    val uiText = EncodableStringRenderer(rawText)
    assert(uiText.render(PythonTemplateBuilder.RenderMode.Encode) == decodeExpr(rawText))
    assert(uiText.render(PythonTemplateBuilder.RenderMode.Plain) == rawText)
  }

  test("pyb interpolator defaults to StringPyMk for normal values (toString)") {
    val value = 42
    val builder = pyb"val=$value"
    assert(builder.plain == "val=42")
    assert(builder.encode == "val=42")
  }

  test("pyb supports multiple args") {
    val firstValue = 1
    val secondValue = "two"
    val thirdValue = 3.0
    val builder = pyb"a=$firstValue b=$secondValue c=$thirdValue"
    assert(builder.plain == "a=1 b=two c=3.0")
  }

  test("passing a PyString (EncodableString) is preserved (no re-wrapping)") {
    val rawText = "ui"
    val uiPyString: PythonTemplateBuilder.StringRenderer = EncodableStringRenderer(rawText)
    val builder = pyb"$uiPyString"
    assert(builder.plain == rawText)
    assert(builder.encode == decodeExpr(rawText))
  }

  test("passing a PyString (StringPyMk) is preserved") {
    val rawPy: PythonTemplateBuilder.StringRenderer = PyLiteralStringRenderer("x + 1")
    val builder = pyb"$rawPy"
    assert(builder.plain == "x + 1")
    assert(builder.encode == "x + 1")
  }

  // ========================================================================
  // Whitespace / multiline / normalization
  // ========================================================================

  test("stripMargin is applied on render() output") {
    val builder =
      pyb"""|line1
            |line2"""
    assert(builder.plain == "line1\nline2")
  }

  test("stripMargin works with interpolation too") {
    val value = 7
    val builder =
      pyb"""|line1 $value
            |line2"""
    assert(builder.plain == "line1 7\nline2")
  }

  // ========================================================================
  // Concatenation
  // ========================================================================

  test("operator + concatenates builders") {
    val left = pyb"hello "
    val right = pyb"world"
    assert((left + right).plain == "hello world")
  }

  test("operator + preserves encoded behavior when mixing UI and raw") {
    val uiText = EncodableStringRenderer("X")
    val prefix = pyb"pre:"
    val middle = pyb"$uiText"
    val suffix = pyb":post"
    val combined = prefix + middle + suffix
    assert(combined.plain == "pre:X:post")
    assert(combined.encode == s"pre:${decodeExpr("X")}:post")
  }

  test("repeated concatenation still renders correctly") {
    val combined = pyb"a" + pyb"b" + pyb"c"
    assert(combined.plain == "abc")
    assert(combined.encode == "abc")
  }

  test("empty builder renders empty") {
    val builder = pyb""
    assert(builder.plain.isEmpty)
    assert(builder.encode.isEmpty)
  }

  // ========================================================================
  // Annotation / TYPE_USE behavior
  // ========================================================================

  test("TYPE_USE alias EncodableString triggers UI encoding") {
    val uiText: EncodableString = "hello"
    val builder = pyb"$uiText"
    assert(builder.plain == "hello")
    assert(builder.encode == decodeExpr("hello"))
  }

  test("EncodableString helper apply triggers UI encoding") {
    val uiText: EncodableString = PyStringTypes.EncodableStringFactory("hey")
    val builder = pyb"$uiText"
    assert(builder.encode == decodeExpr("hey"))
  }

  test("TYPE_USE annotation on val type triggers UI encoding") {
    val uiText: String @EncodableStringAnnotation = "typeuse"
    val builder = pyb"$uiText"
    assert(builder.encode == decodeExpr("typeuse"))
  }

  test("@StringUI parameter triggers UI encoding") {
    def build(@EncodableStringAnnotation uiText: String): PythonTemplateBuilder = pyb"$uiText"
    val builder = build("param")
    assert(builder.encode == decodeExpr("param"))
  }

  test("@StringUI local val triggers UI encoding") {
    def build(): PythonTemplateBuilder = {
      @EncodableStringAnnotation val uiText: String = "local"
      pyb"$uiText"
    }
    val builder = build()
    assert(builder.encode == decodeExpr("local"))
  }

  test("@StringUI local val triggers UI encoding even when type is inferred") {
    def build(): PythonTemplateBuilder = {
      @EncodableStringAnnotation val uiText = "local-inferred"
      pyb"$uiText"
    }
    val builder = build()
    assert(builder.encode == decodeExpr("local-inferred"))
  }

  test("@StringUI lambda parameter triggers UI encoding") {
    val uiToBuilder: (String @EncodableStringAnnotation) => PythonTemplateBuilder = uiText => pyb"$uiText"
    val builder = uiToBuilder("lambda")
    assert(builder.encode == decodeExpr("lambda"))
  }

  test("@StringUI lambda param + map + mkString triggers UI encoding per element") {
    val rawItems = List("a", "b", "c")
    val joinedEncoded =
      rawItems.map((uiItem: String @EncodableStringAnnotation) => pyb"$uiItem").mkString("[", ", ", "]")
    assert(joinedEncoded == s"[${rawItems.map(decodeExpr).mkString(", ")}]")
  }

  test("List[String @StringUI] element access preserves UI encoding") {
    val uiItems: List[String @EncodableStringAnnotation] = List("first", "second")
    val first = uiItems.head
    val builder = pyb"$first"
    assert(builder.encode == decodeExpr("first"))
  }

  test("Erasing List[String @StringUI] to List[String] drops UI encoding") {
    val uiItems: List[String @EncodableStringAnnotation] = List("erased")
    val erased: List[String] = uiItems.map((uiItem: String @EncodableStringAnnotation) => (uiItem: String))
    val builder = pyb"${erased.head}"
    assert(builder.encode == "erased")
  }

  test("@(StringUI @field) on case class field triggers UI encoding via accessor/field") {
    final case class WithFieldAnnotation(@(EncodableStringAnnotation @field) uiText: String)
    val value = WithFieldAnnotation("field")
    val builder = pyb"${value.uiText}"
    assert(builder.encode == decodeExpr("field"))
  }

  test("@StringUI on case class param without @field does not trigger UI encoding via accessor") {
    final case class WithoutFieldAnnotation(@EncodableStringAnnotation uiText: String)
    val value = WithoutFieldAnnotation("param-only")
    val builder = pyb"${value.uiText}"
    assert(builder.encode == "param-only")
  }

  test("unannotated String does not become UI (stays raw python)") {
    val rawText: String = "raw"
    val builder = pyb"$rawText"
    assert(builder.encode == "raw")
  }

  test("StringPy alias remains raw") {
    val rawText: PythonLiteral = "raw2"
    val builder = pyb"$rawText"
    assert(builder.encode == "raw2")
  }

  // ========================================================================
  // Compile-time checks (direct UI args)
  // ========================================================================

  test("UI with whitespace boundaries compiles") {
    assertCompiles("""
      import org.apache.texera.amber.pybuilder.PythonTemplateBuilder._
      import org.apache.texera.amber.pybuilder.PyStringTypes._
      object UiWhitespaceBoundariesOk { val ui: EncodableString = "x"; val b = pyb"foo $ui bar" }
    """)
  }

  test("UI next to comma is allowed (common in function args)") {
    assertCompiles("""
      import org.apache.texera.amber.pybuilder.PythonTemplateBuilder._
      import org.apache.texera.amber.pybuilder.PyStringTypes._
      object UiCommaOk { val ui: EncodableString = "x"; val b = pyb"f($ui, 1)" }
    """)
  }

  test("UI next to parentheses is allowed") {
    assertCompiles("""
      import org.apache.texera.amber.pybuilder.PythonTemplateBuilder._
      import org.apache.texera.amber.pybuilder.PyStringTypes._
      object UiParensOk { val ui: EncodableString = "x"; val b = pyb"($ui)" }
    """)
  }

  test("hash inside quotes does not count as a comment marker (UI allowed afterwards)") {
    assertCompiles("""
      import org.apache.texera.amber.pybuilder.PythonTemplateBuilder._
      import org.apache.texera.amber.pybuilder.PyStringTypes._
      object HashInQuotesOk { val ui: EncodableString = "x"; val b = pyb"print('#') $ui" }
    """)
  }

  test("UI glued to identifier on the left does not compile") {
    assertDoesNotCompile("""
      import org.apache.texera.amber.pybuilder.PythonTemplateBuilder._
      import org.apache.texera.amber.pybuilder.PyStringTypes._
      object UiGluedLeftBad { val ui: EncodableString = "x"; val b = pyb"foo$ui" }
    """)
  }

  test("UI glued to identifier on the right does not compile") {
    assertDoesNotCompile("""
      import org.apache.texera.amber.pybuilder.PythonTemplateBuilder._
      import org.apache.texera.amber.pybuilder.PyStringTypes._
      object UiGluedRightBad { val ui: EncodableString = "x"; val b = pyb"${ui}bar" }
    """)
  }

  test("UI glued to a quote on the right does not compile") {
    assertDoesNotCompile("""
      import org.apache.texera.amber.pybuilder.PythonTemplateBuilder._
      import org.apache.texera.amber.pybuilder.PyStringTypes._
      object UiGluedQuoteBad { val ui: EncodableString = "x"; val b = pyb"${ui}'" }
    """)
  }

  test("UI placed inside a quoted python string literal does not compile (single quotes)") {
    assertDoesNotCompile("""
      import org.apache.texera.amber.pybuilder.PythonTemplateBuilder._
      import org.apache.texera.amber.pybuilder.PyStringTypes._
      object UiInsideSingleQuotesBad { val ui: EncodableString = "x"; val b = pyb"print('${ui}')" }
    """)
  }

  test("UI placed inside a quoted python string literal does not compile (double quotes)") {
    assertDoesNotCompile("""
    import org.apache.texera.amber.pybuilder.PythonTemplateBuilder._
    import org.apache.texera.amber.pybuilder.PyStringTypes._
    object UiInsideDoubleQuotesBad {
      val ui: EncodableString = "x"
      val b = pyb"print(\\"${ui}\\")"
    }
  """)
  }

  test("UI placed after a python comment marker on same line does not compile") {
    assertDoesNotCompile("""
      import org.apache.texera.amber.pybuilder.PythonTemplateBuilder._
      import org.apache.texera.amber.pybuilder.PyStringTypes._
      object UiAfterCommentBad { val ui: EncodableString = "x"; val b = pyb"foo # ${ui}" }
    """)
  }

  test("UI placed after a python comment marker on same line does not compile (no whitespace)") {
    assertDoesNotCompile("""
      import org.apache.texera.amber.pybuilder.PythonTemplateBuilder._
      import org.apache.texera.amber.pybuilder.PyStringTypes._
      object UiAfterCommentNoSpaceBad { val ui: EncodableString = "x"; val b = pyb"foo #${ui}" }
    """)
  }

  test("comment marker on previous line does not affect next line (lineTail behavior)") {
    assertCompiles(
      "import org.apache.texera.amber.pybuilder.PythonTemplateBuilder._\n" +
        "import org.apache.texera.amber.pybuilder.PyStringTypes._\n" +
        "object CommentPrevLineOk {\n" +
        "  val ui: EncodableString = \"x\"\n" +
        "  val b = pyb\"\"\"|# comment\n" +
        "               |$ui\"\"\"\n" +
        "}\n"
    )
  }


  test("PyString (EncodableString) glued to identifier on the left does not compile") {
    assertDoesNotCompile("""
      import org.apache.texera.amber.pybuilder.PythonTemplateBuilder._
      import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.EncodableString
      object PyStringGluedLeftBad { val ui = EncodableString("x"); val b = pyb"foo${ui}" }
    """)
  }

  test("PyString (EncodableString) inside a quoted python string literal does not compile") {
    assertDoesNotCompile("""
      import org.apache.texera.amber.pybuilder.PythonTemplateBuilder._
      import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.EncodableString
      object PyStringInsideQuotesBad { val ui = EncodableString("x"); val b = pyb"print('${ui}')" }
    """)
  }

  test("all isBadNeighbor characters reject direct UI adjacency at compile time (left + right)") {
    val candidates = (33 to 126).map(_.toChar) // printable ASCII, avoids whitespace
    val badChars = candidates.filter(PythonLexerUtils.isBadNeighbor)

    // This is intentionally exhaustive over the implementation-defined "bad neighbor" set.
    // We assert only compile success/failure, not the specific error message.
    badChars.zipWithIndex.foreach { case (ch, i) =>
      val esc = scalaUnicodeEscape(ch)

      val leftAdj =
        s"""
           |import org.apache.texera.amber.pybuilder.PythonTemplateBuilder._
           |import org.apache.texera.amber.pybuilder.PyStringTypes._
           |object UiBadLeft_$i {
           |  val ui: EncodableString = "x"
           |  val b = pyb\"\"\"pre$esc${'$'}{ui}post\"\"\"
           |}
           |""".stripMargin

      val rightAdj =
        s"""
           |import org.apache.texera.amber.pybuilder.PythonTemplateBuilder._
           |import org.apache.texera.amber.pybuilder.PyStringTypes._
           |object UiBadRight_$i {
           |  val ui: EncodableString = "x"
           |  val b = pyb\"\"\"pre${'$'}{ui}$esc post\"\"\"
           |}
           |""".stripMargin

      assertToolboxDoesNotCompile(leftAdj)
      assertToolboxDoesNotCompile(rightAdj)
    }
  }

  // ========================================================================
  // Interpolator semantics / evaluation
  // ========================================================================

  test("interpolated args are evaluated once and not re-evaluated on render") {
    var evalCount = 0
    def nextValue(): String = {
      evalCount += 1
      "v"
    }

    val builder = pyb"${nextValue()}"
    assert(evalCount == 1)

    builder.plain
    assert(evalCount == 1)

    builder.encode
    assert(evalCount == 1)
  }

  // ========================================================================
  // Nested PythonTemplateBuilder behavior (mode propagation + runtime UI checks)
  // ========================================================================

  test("nested PythonTemplateBuilder with UI propagates mode (plain)") {
    val uiText = EncodableStringRenderer("Z")
    val inner = pyb"X=$uiText"
    val outer = pyb"pre $inner post"
    assert(outer.plain == "pre X=Z post")
  }

  test("nested PythonTemplateBuilder with UI propagates mode (encoded)") {
    val uiText = EncodableStringRenderer("Z")
    val inner = pyb"X=$uiText"
    val outer = pyb"pre $inner post"
    assert(outer.encode == s"pre X=${decodeExpr("Z")} post")
  }

  test("nested PythonTemplateBuilder without UI can appear inside python quotes (no runtime checks)") {
    val inner = pyb"hello"
    val outer = pyb"print('$inner')"
    assert(outer.plain == "print('hello')")
    assert(outer.encode == "print('hello')")
  }

  test("containsUi detects UI chunks correctly") {
    val rawBuilder = pyb"raw"
    val uiBuilder = pyb"${EncodableStringRenderer("x")}"
    val combined = rawBuilder + uiBuilder
    assert(!rawBuilder.containsEncodableString)
    assert(uiBuilder.containsEncodableString)
    assert(combined.containsEncodableString)
  }

  test("nested PythonTemplateBuilder containing UI inside single quotes throws at runtime") {
    val inner = pyb"${EncodableStringRenderer("x")}"
    intercept[IllegalArgumentException] {
      pyb"print('$inner')"
    }
  }

  test("nested PythonTemplateBuilder containing UI inside double quotes throws at runtime") {
    val inner = pyb"${EncodableStringRenderer("x")}"
    intercept[IllegalArgumentException] {
      pyb"""print("$inner")"""
    }
  }

  test("nested PythonTemplateBuilder containing UI after comment marker throws at runtime (with and without whitespace)") {
    val inner = pyb"${EncodableStringRenderer("x")}"
    intercept[IllegalArgumentException] {
      pyb"foo # $inner"
    }
    intercept[IllegalArgumentException] {
      pyb"foo #$inner"
    }
  }

  test("nested PythonTemplateBuilder containing UI glued to identifier/digit throws at runtime") {
    val inner = pyb"${EncodableStringRenderer("x")}"
    intercept[IllegalArgumentException] { pyb"foo$inner" }
    intercept[IllegalArgumentException] { pyb"${inner}bar" }
    intercept[IllegalArgumentException] { pyb"1$inner" }
    intercept[IllegalArgumentException] { pyb"${inner}2" }
  }

  test("runtime guard does NOT throw when nested builder has no UI, even in unsafe boundary contexts") {
    val inner = pyb"hello"
    val outer1 = pyb"foo$inner"
    val outer2 = pyb"${inner}bar"
    val outer3 = pyb"print('$inner')"
    val outer4 = pyb"foo #$inner"

    assert(outer1.plain == "foohello")
    assert(outer2.plain == "hellobar")
    assert(outer3.plain == "print('hello')")
    assert(outer4.plain == "foo #hello")
  }

  test("nested PythonTemplateBuilder containing UI with safe whitespace boundaries is allowed") {
    val inner = pyb"${EncodableStringRenderer("x")}"
    val outer = pyb"foo $inner bar"
    assert(outer.plain == "foo x bar")
    assert(outer.encode == s"foo ${decodeExpr("x")} bar")
  }

  test("nested PythonTemplateBuilder containing UI next to punctuation is allowed") {
    val inner = pyb"${EncodableStringRenderer("x")}"
    val outer = pyb"f($inner, 1)"
    assert(outer.plain == "f(x, 1)")
    assert(outer.encode == s"f(${decodeExpr("x")}, 1)")
  }

  test("stripMargin works across nested builders") {
    val inner =
      pyb"""A
           |B"""
    val outer =
      pyb"""|start
            |$inner
            |end"""
    assert(outer.plain == "start\nA\nB\nend")
  }

  test("""format(): EncodableString arg after closing quote is allowed""") {
    val workflowParam = "wf"
    val portParam = PythonTemplateBuilder.EncodableStringRenderer("P")

    val builder = pyb""""$workflowParam".format($portParam)"""
    assert(builder.plain == "\"wf\".format(P)")
    assert(builder.encode.contains("self.decode_python_template("))
  }

  test("format(): nested PythonTemplateBuilder containing UI is allowed (no runtime false positive)") {
    val workflowParam = "wf"
    val portParam = pyb"int (${PythonTemplateBuilder.EncodableStringRenderer("\\.")}),"

    val builder = pyb""""$workflowParam".format($portParam)"""
    assert(builder.plain.contains("format(int (\\.),"))
    assert(builder.encode.contains("self.decode_python_template("))
  }

  test("still rejects nested UI builder inside Python quotes at runtime") {
    val portParam = pyb"${PythonTemplateBuilder.EncodableStringRenderer("P")}"

    intercept[IllegalArgumentException] {
      pyb"print('${portParam}')".plain
    }
  }
}
