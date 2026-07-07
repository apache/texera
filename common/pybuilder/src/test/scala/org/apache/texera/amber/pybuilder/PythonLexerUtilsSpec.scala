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

class PythonLexerUtilsSpec extends AnyFunSuite {

  // -------- isIdentChar --------

  test("isIdentChar: lowercase letter is identifier char") {
    assert(PythonLexerUtils.isIdentChar('a'))
  }

  test("isIdentChar: uppercase letter is identifier char") {
    assert(PythonLexerUtils.isIdentChar('Z'))
  }

  test("isIdentChar: digit is identifier char") {
    assert(PythonLexerUtils.isIdentChar('5'))
  }

  test("isIdentChar: underscore is identifier char") {
    assert(PythonLexerUtils.isIdentChar('_'))
  }

  test("isIdentChar: dash is not identifier char") {
    assert(!PythonLexerUtils.isIdentChar('-'))
  }

  test("isIdentChar: space is not identifier char") {
    assert(!PythonLexerUtils.isIdentChar(' '))
  }

  test("isIdentChar: hash is not identifier char") {
    assert(!PythonLexerUtils.isIdentChar('#'))
  }

  // -------- isBadNeighbor --------

  test("isBadNeighbor: single quote is bad neighbor") {
    assert(PythonLexerUtils.isBadNeighbor('\''))
  }

  test("isBadNeighbor: double quote is bad neighbor") {
    assert(PythonLexerUtils.isBadNeighbor('"'))
  }

  test("isBadNeighbor: identifier chars are bad neighbors") {
    assert(PythonLexerUtils.isBadNeighbor('a'))
    assert(PythonLexerUtils.isBadNeighbor('Z'))
    assert(PythonLexerUtils.isBadNeighbor('0'))
    assert(PythonLexerUtils.isBadNeighbor('_'))
  }

  test("isBadNeighbor: whitespace is not bad neighbor") {
    assert(!PythonLexerUtils.isBadNeighbor(' '))
  }

  test("isBadNeighbor: punctuation like comma is not bad neighbor") {
    assert(!PythonLexerUtils.isBadNeighbor(','))
  }

  // -------- lineTail --------

  test("lineTail: string without newline returns full string") {
    val text = "no-newline"
    assert(PythonLexerUtils.lineTail(text) == text)
  }

  test("lineTail: returns text after single newline") {
    val text = "first\nsecond"
    assert(PythonLexerUtils.lineTail(text) == "second")
  }

  test("lineTail: returns text after last newline") {
    val text = "a\nb\nc\nlast-line"
    assert(PythonLexerUtils.lineTail(text) == "last-line")
  }

  test("lineTail: works with trailing newline (returns empty)") {
    val text = "first\nsecond\n"
    assert(PythonLexerUtils.lineTail(text) == "")
  }

  // -------- updateTripleQuotedStringState --------

  test("updateTripleQuotedStringState: enters and exits triple single quoted strings") {
    val opened = PythonLexerUtils.updateTripleQuotedStringState("sql = '''", None)
    assert(opened.contains("'''"))

    val stillOpen = PythonLexerUtils.updateTripleQuotedStringState("SELECT * FROM t", opened)
    assert(stillOpen.contains("'''"))

    val closed = PythonLexerUtils.updateTripleQuotedStringState("'''", stillOpen)
    assert(closed.isEmpty)
  }

  test("updateTripleQuotedStringState: enters and exits triple double quoted strings") {
    val opened = PythonLexerUtils.updateTripleQuotedStringState("sql = \"\"\"", None)
    assert(opened.contains("\"\"\""))

    val closed = PythonLexerUtils.updateTripleQuotedStringState("\"\"\"", opened)
    assert(closed.isEmpty)
  }

  test("updateTripleQuotedStringState: ignores triple quotes in single-line comments") {
    val state = PythonLexerUtils.updateTripleQuotedStringState("# ''' not a string", None)
    assert(state.isEmpty)
  }

  test("updateTripleQuotedStringState: ignores triple quotes inside ordinary strings") {
    val state = PythonLexerUtils.updateTripleQuotedStringState("value = \"'''\"", None)
    assert(state.isEmpty)
  }

  test(
    "updateTripleQuotedStringState: an escaped quote inside an ordinary string is not a boundary"
  ) {
    // Python:  x = "a\"b" '''  — the escaped " must not close the double-quoted string, so it ends
    // before the trailing ''', which opens a triple. If the escape were mishandled the string would
    // stay open and the ''' would be treated as content, yielding None instead of Some("'''").
    assert(
      PythonLexerUtils.updateTripleQuotedStringState("x = \"a\\\"b\" '''", None).contains("'''")
    )
    // Python:  y = 'a\'b' """  — same for a single-quoted string, then a triple-double opens.
    assert(
      PythonLexerUtils.updateTripleQuotedStringState("y = 'a\\'b' \"\"\"", None).contains("\"\"\"")
    )
  }

  test(
    "updateTripleQuotedStringState: single-quote mode suppresses a later comment and delimiter"
  ) {
    // Python:  a = '#' '''  — the # sits inside the single-quoted string (not a comment); the string
    // closes and ''' opens a triple. If single-quote mode failed to toggle on, the # would
    // early-return as a comment and the result would be None instead of Some("'''").
    assert(PythonLexerUtils.updateTripleQuotedStringState("a = '#' '''", None).contains("'''"))
  }

  test("updateTripleQuotedStringState: a hash inside a double-quoted string is not a comment") {
    // Python:  s = "a # b" """  — the # is inside the double-quoted string, which closes before the
    // trailing triple-double. If the # were wrongly treated as a comment the scan would early-return
    // None instead of Some("\"\"\"").
    assert(
      PythonLexerUtils
        .updateTripleQuotedStringState("s = \"a # b\" \"\"\"", None)
        .contains("\"\"\"")
    )
  }

  // -------- hasUnclosedQuote --------

  test("hasUnclosedQuote: empty string has no unclosed quote") {
    assert(!PythonLexerUtils.hasUnclosedQuote(""))
  }

  test("hasUnclosedQuote: balanced single quotes returns false") {
    assert(!PythonLexerUtils.hasUnclosedQuote("'a'"))
  }

  test("hasUnclosedQuote: balanced double quotes returns false") {
    assert(!PythonLexerUtils.hasUnclosedQuote("\"a\""))
  }

  test("hasUnclosedQuote: unclosed single quote returns true") {
    assert(PythonLexerUtils.hasUnclosedQuote("'unclosed"))
  }

  test("hasUnclosedQuote: unclosed double quote returns true") {
    assert(PythonLexerUtils.hasUnclosedQuote("\"unclosed"))
  }

  test("hasUnclosedQuote: escaped single quote inside single quotes does not break balance") {
    val text = "'it\\'s ok'"
    assert(!PythonLexerUtils.hasUnclosedQuote(text))
  }

  test("hasUnclosedQuote: escaped double quote inside double quotes does not break balance") {
    val text = "\"he said \\\"hi\\\"\""
    assert(!PythonLexerUtils.hasUnclosedQuote(text))
  }

  test("hasUnclosedQuote: mixed quotes with proper closing returns false") {
    val text = "'a' + \"b\""
    assert(!PythonLexerUtils.hasUnclosedQuote(text))
  }

  // -------- hasCommentOutsideQuotes --------

  test("hasCommentOutsideQuotes: no hash means no comment") {
    assert(!PythonLexerUtils.hasCommentOutsideQuotes("print(1)"))
  }

  test("hasCommentOutsideQuotes: hash outside quotes is a comment") {
    assert(PythonLexerUtils.hasCommentOutsideQuotes("x = 1  # comment"))
  }

  test("hasCommentOutsideQuotes: hash inside single quotes is not a comment") {
    assert(!PythonLexerUtils.hasCommentOutsideQuotes("print('# not comment')"))
  }

  test("hasCommentOutsideQuotes: hash inside double quotes is not a comment") {
    assert(!PythonLexerUtils.hasCommentOutsideQuotes("print(\"# not comment\")"))
  }

  test("hasCommentOutsideQuotes: escaped quotes preserve quote state correctly") {
    val line = "print(\"\\\"# still in string\\\"\")  # comment here"
    assert(PythonLexerUtils.hasCommentOutsideQuotes(line))
  }

  test("hasCommentOutsideQuotes: multiple hashes only first outside quotes matters") {
    val line = "print('# in string')  # real comment # more"
    assert(PythonLexerUtils.hasCommentOutsideQuotes(line))
  }
}
