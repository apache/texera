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

/**
  * Pure helpers used by the macro for quick, best-effort Python lexical checks.
  *
  * These are intentionally *not* macro-dependent, so they can be unit tested normally.
  */
object PythonLexerUtils {

  def isIdentChar(c: Char): Boolean = c.isLetterOrDigit || c == '_'

  /** Characters that would make an Encodable-expression splice ambiguous/invalid if adjacent. */
  def isBadNeighbor(c: Char): Boolean = c == '\'' || c == '"' || isIdentChar(c)

  /** Returns the substring after the last newline (used to reason about the "current line" context). */
  def lineTail(s: String): String = {
    val lastNewlineIndex = s.lastIndexOf('\n')
    if (lastNewlineIndex >= 0) s.substring(lastNewlineIndex + 1) else s
  }

  /**
    * Update triple-quoted-string state after scanning one physical Python source line.
    *
    * This is intentionally lightweight. It only tracks whether scanning is inside a `'''` or `"""` string so callers
    * that reason about indentation can avoid treating string contents as real Python statements.
    *
    * Known limitations: escaped delimiters inside an active triple-quoted string are still treated as closing
    * delimiters, and delimiter-like runs next to ordinary string boundaries may be detected because this helper does
    * not fully parse Python string literal adjacency.
    */
  def updateTripleQuotedStringState(
      line: String,
      activeDelimiter: Option[String]
  ): Option[String] = {
    var delimiter = activeDelimiter
    var inSingleQuotedString = false
    var inDoubleQuotedString = false
    var escaped = false
    var index = 0

    while (index < line.length) {
      delimiter match {
        case Some(active) =>
          if (line.startsWith(active, index)) {
            delimiter = None
            index += active.length
          } else {
            index += 1
          }

        case None =>
          val char = line.charAt(index)

          if (escaped) {
            escaped = false
            index += 1
          } else if ((inSingleQuotedString || inDoubleQuotedString) && char == '\\') {
            escaped = true
            index += 1
          } else if (!inSingleQuotedString && !inDoubleQuotedString && char == '#') {
            return delimiter
          } else if (!inDoubleQuotedString && line.startsWith("'''", index)) {
            delimiter = Some("'''")
            index += 3
          } else if (!inSingleQuotedString && line.startsWith("\"\"\"", index)) {
            delimiter = Some("\"\"\"")
            index += 3
          } else if (!inDoubleQuotedString && char == '\'') {
            inSingleQuotedString = !inSingleQuotedString
            index += 1
          } else if (!inSingleQuotedString && char == '"') {
            inDoubleQuotedString = !inDoubleQuotedString
            index += 1
          } else {
            index += 1
          }
      }
    }

    delimiter
  }

  /**
    * Detect whether the provided line tail contains an unclosed single or double quote.
    *
    * This is not a full Python parser; it is a small state machine tracking quote mode and escapes.
    */
  def hasUnclosedQuote(lineText: String): Boolean = {
    var inSingleQuotes = false
    var inDoubleQuotes = false
    var escaped = false

    var i = 0
    while (i < lineText.length) {
      val ch = lineText.charAt(i)
      if (escaped) escaped = false
      else if (ch == '\\') escaped = true
      else if (!inDoubleQuotes && ch == '\'') inSingleQuotes = !inSingleQuotes
      else if (!inSingleQuotes && ch == '"') inDoubleQuotes = !inDoubleQuotes
      i += 1
    }
    inSingleQuotes || inDoubleQuotes
  }

  /**
    * Detect whether the provided line tail contains a `#` that is outside of any quote context.
    *
    * If true, any Encodable-expression splice after that point would be inside a Python comment.
    */
  def hasCommentOutsideQuotes(lineText: String): Boolean = {
    var inSingleQuotes = false
    var inDoubleQuotes = false
    var escaped = false

    var i = 0
    while (i < lineText.length) {
      val ch = lineText.charAt(i)
      if (escaped) escaped = false
      else if (ch == '\\') escaped = true
      else if (!inDoubleQuotes && ch == '\'') inSingleQuotes = !inSingleQuotes
      else if (!inSingleQuotes && ch == '"') inDoubleQuotes = !inDoubleQuotes
      else if (!inSingleQuotes && !inDoubleQuotes && ch == '#') return true
      i += 1
    }
    false
  }
}
