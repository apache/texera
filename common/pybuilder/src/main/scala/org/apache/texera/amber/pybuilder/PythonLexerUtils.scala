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
