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

import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.RenderMode.{Encode, Plain}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.{
  EncodableStringRenderer,
  PyLiteralStringRenderer,
  fromInterpolated,
  wrapWithPythonDecoderExpr
}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.util.Base64

/**
  * Covers the non-macro public surface of PythonTemplateBuilder that PythonTemplateBuilderSpec
  * exercises only incidentally: factories, renderer mode constants, render normalization,
  * concatenation operators, and require/throw preconditions.
  */
class PythonTemplateBuilderApiSpec extends AnyFunSuite {

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  // -------- wrapWithPythonDecoderExpr --------

  test("wrapWithPythonDecoderExpr wraps text into a decode_python_template call") {
    assert(wrapWithPythonDecoderExpr("abc") == "self.decode_python_template('abc')")
  }

  test("wrapWithPythonDecoderExpr does not escape inner content (caller's responsibility)") {
    // The current contract simply interpolates the raw text. Pinning this so a future
    // escape-aware version trips this spec deliberately.
    assert(wrapWithPythonDecoderExpr("a'b") == "self.decode_python_template('a'b')")
  }

  // -------- RenderMode --------

  test("RenderMode.Plain and RenderMode.Encode are distinct singletons") {
    assert(Plain != Encode)
    assert(Plain eq PythonTemplateBuilder.RenderMode.Plain)
    assert(Encode eq PythonTemplateBuilder.RenderMode.Encode)
  }

  // -------- EncodableStringRenderer --------

  test("EncodableStringRenderer.render(Plain) returns the raw stringValue") {
    val r = EncodableStringRenderer("abc")
    assert(r.render(Plain) == "abc")
    assert(r.stringValue == "abc")
  }

  test("EncodableStringRenderer.render(Encode) wraps base64 with the python decoder expr") {
    val r = EncodableStringRenderer("abc")
    assert(r.render(Encode) == s"self.decode_python_template('${b64("abc")}')")
  }

  test("EncodableStringRenderer handles empty string in both modes") {
    val r = EncodableStringRenderer("")
    assert(r.render(Plain) == "")
    assert(r.render(Encode) == "self.decode_python_template('')")
  }

  test("EncodableStringRenderer uses UTF-8 base64 for non-ASCII content") {
    val raw = "你好"
    val r = EncodableStringRenderer(raw)
    assert(r.render(Encode) == s"self.decode_python_template('${b64(raw)}')")
  }

  // -------- PyLiteralStringRenderer --------

  test("PyLiteralStringRenderer.render ignores mode and returns the raw stringValue") {
    val r = PyLiteralStringRenderer("print('x')")
    assert(r.render(Plain) == "print('x')")
    assert(r.render(Encode) == "print('x')")
  }

  // -------- PyStringTypes factories --------

  test("PyStringTypes.EncodableStringFactory.apply returns the input string unchanged") {
    val out: String = PyStringTypes.EncodableStringFactory("hi")
    assert(out == "hi")
  }

  test("PyStringTypes.EncodableStringFactory.empty is the empty string") {
    val out: String = PyStringTypes.EncodableStringFactory.empty
    assert(out.isEmpty)
  }

  test("PyStringTypes.PyLiteralFactory.apply returns the input string unchanged") {
    assert(PyStringTypes.PyLiteralFactory("hi") == "hi")
  }

  test("PyStringTypes.PyLiteralFactory.empty is the empty string") {
    assert(PyStringTypes.PyLiteralFactory.empty.isEmpty)
  }

  // -------- fromInterpolated precondition --------

  test("fromInterpolated requires parts.length == args.length + 1") {
    val thrown = intercept[IllegalArgumentException] {
      fromInterpolated(List("only-one-part"), List(EncodableStringRenderer("x")))
    }
    assert(thrown.getMessage.contains("pyb interpolator mismatch"))
    assert(thrown.getMessage.contains("parts=1"))
    assert(thrown.getMessage.contains("args=1"))
  }

  test("fromInterpolated with zero args and one literal part renders that part") {
    val b = fromInterpolated(List("only"), Nil)
    assert(b.plain == "only")
  }

  test("fromInterpolated alternates text/value chunks in order") {
    val b = fromInterpolated(
      List("a-", "-b-", "-c"),
      List(PyLiteralStringRenderer("X"), PyLiteralStringRenderer("Y"))
    )
    assert(b.plain == "a-X-b-Y-c")
  }

  // -------- PythonTemplateBuilder.+ and concatChunks --------

  test("operator + merges adjacent literal-only builders into a single text chunk") {
    val left = fromInterpolated(List("hello "), Nil)
    val right = fromInterpolated(List("world"), Nil)
    val merged = left + right
    assert(merged.plain == "hello world")
    // Round-trip through encode mode to ensure no chunk fan-out side effects.
    assert(merged.encode == "hello world")
  }

  test("operator + preserves value chunks across the join boundary") {
    val left = fromInterpolated(List("pre-", "-mid"), List(EncodableStringRenderer("L")))
    val right = fromInterpolated(List("-end"), Nil)
    val merged = left + right
    assert(merged.plain == "pre-L-mid-end")
    assert(merged.encode == s"pre-${"self.decode_python_template('" + b64("L") + "')"}-mid-end")
  }

  test("operator + with empty left builder returns content equivalent to right") {
    val left = fromInterpolated(List(""), Nil)
    val right = fromInterpolated(List("hi"), Nil)
    assert((left + right).plain == "hi")
  }

  test("operator + with empty right builder returns content equivalent to left") {
    val left = fromInterpolated(List("hi"), Nil)
    val right = fromInterpolated(List(""), Nil)
    assert((left + right).plain == "hi")
  }

  test("operator +(String) is unsupported and includes the offending string in the message") {
    val b = fromInterpolated(List("x"), Nil)
    val thrown = intercept[UnsupportedOperationException] {
      b + "oops"
    }
    assert(thrown.getMessage.contains("oops"))
  }

  // -------- render() line-ending normalization --------

  test("render normalizes CRLF to LF") {
    val b = fromInterpolated(List("a\r\nb"), Nil)
    assert(b.plain == "a\nb")
  }

  test("render normalizes lone CR to LF") {
    val b = fromInterpolated(List("a\rb"), Nil)
    assert(b.plain == "a\nb")
  }

  test("render preserves existing LF unchanged") {
    val b = fromInterpolated(List("a\nb"), Nil)
    assert(b.plain == "a\nb")
  }

  test("render applies stripMargin (margin char '|' strips preceding whitespace per line)") {
    val b = fromInterpolated(List("first\n  |second"), Nil)
    assert(b.plain == "first\nsecond")
  }

  // -------- containsEncodableString on edge inputs --------

  test("containsEncodableString is false for a pure-text builder") {
    val b = fromInterpolated(List("just text"), Nil)
    assert(!b.containsEncodableString)
  }

  test("containsEncodableString is false for a builder holding only PyLiteralStringRenderer") {
    val b = fromInterpolated(
      List("", ""),
      List(PyLiteralStringRenderer("raw"))
    )
    assert(!b.containsEncodableString)
  }

  test("containsEncodableString is true if any chunk is an EncodableStringRenderer") {
    val b = fromInterpolated(
      List("", "", ""),
      List(PyLiteralStringRenderer("a"), EncodableStringRenderer("b"))
    )
    assert(b.containsEncodableString)
  }

  // -------- triple-quoted Python: pinning current (not-triple-quote-aware) behavior --------
  //
  // PythonLexerUtils tracks single/double quote state one character at a time and does not
  // recognize Python triple-quoted strings as a single token. With six balanced quotes the
  // lexer happens to also report balanced, but the *intermediate* states matter: any time the
  // line tail ends with an odd count of `"`/`'`, hasUnclosedQuote returns true.
  //
  // These pin the current conservative behavior. If a future change makes the lexer aware of
  // triple-quoted strings, these specs should be revisited intentionally.

  test("hasUnclosedQuote: six matched double quotes are seen as balanced") {
    assert(!PythonLexerUtils.hasUnclosedQuote("\"\"\"abc\"\"\""))
  }

  test("hasUnclosedQuote: three opening double quotes count as unclosed") {
    // A Python triple-quoted string opener `\"\"\"` is currently reported as 'inside string'.
    assert(PythonLexerUtils.hasUnclosedQuote("\"\"\"abc"))
  }

  test("hasUnclosedQuote: three opening single quotes count as unclosed") {
    assert(PythonLexerUtils.hasUnclosedQuote("'''abc"))
  }
}
