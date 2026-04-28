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

import scala.reflect.macros.blackbox

/**
 * Macro-only helper: validates boundaries for Encodable insertions.
 *
 * Compile-time: abort with good messages for direct Encodable args.
 * Runtime: for nested builders (unknown content at compile time), generate a check that throws if the builder contains Encodable chunks.
 */
final class BoundaryValidator[C <: blackbox.Context](val c: C) {
  import PythonLexerUtils._
  import c.universe._

  /**
   * Centralized, templatized error messages (Option A).
   *
   * NOTE: This object lives inside the class so it can freely use string templates
   * without any macro-context type gymnastics.
   */
  private object BoundaryErrors {

    // Provide a hint that can differ between compile-time and runtime wording.
    sealed trait RendererHint { def text: String }

    case object CompileTimeHint extends RendererHint {
      override val text: String =
        "EncodableString renders as a Python expression (self.PythonTemplateDecoder.decode(...))"
    }

    case object RuntimeHint extends RendererHint {
      override val text: String =
        "EncodableString renders as a Python expression (self...decode(...))"
    }

    private def prefix(argNum1Based: Int): String =
      s"pyb interpolator:  @EncodableStringAnnotation argument #$argNum1Based "

    def insideQuoted(argNum1Based: Int, hint: RendererHint): String =
      prefix(argNum1Based) +
        "appears inside a quoted Python string literal. " +
        s"${hint.text}, so it must not be placed inside quotes."

    def afterComment(argNum1Based: Int): String =
      prefix(argNum1Based) +
        "appears after a '#' comment marker on the same line."

    def badLeftNeighbor(argNum1Based: Int, ch: Char): String =
      prefix(argNum1Based) +
        s"must not be immediately adjacent to '$ch' on the left. " +
        "Add whitespace or punctuation to separate tokens."

    def badRightNeighbor(argNum1Based: Int, ch: Char): String =
      prefix(argNum1Based) +
        s"must not be immediately adjacent to '$ch' on the right. " +
        "Add whitespace or punctuation to separate tokens."
  }

  final case class CompileTimeContext(
                                       leftPart: String,
                                       rightPart: String,
                                       prefixSource: String,
                                       argIndex: Int,
                                       errorPos: Position
                                     )

  final case class RuntimeContext(
                                   leftPart: String,
                                   rightPart: String,
                                   prefixSource: String,
                                   argIndex: Int
                                 )

  def validateCompileTime(ctx: CompileTimeContext): Unit = {
    val prefixLine = lineTail(ctx.prefixSource)
    val argNum = ctx.argIndex + 1

    if (hasUnclosedQuote(prefixLine)) {
      c.abort(
        ctx.errorPos,
        BoundaryErrors.insideQuoted(argNum, BoundaryErrors.CompileTimeHint)
      )
    }

    if (hasCommentOutsideQuotes(prefixLine)) {
      c.abort(
        ctx.errorPos,
        BoundaryErrors.afterComment(argNum)
      )
    }

    if (ctx.leftPart.nonEmpty) {
      val leftNeighbor = ctx.leftPart.charAt(ctx.leftPart.length - 1)
      if (isBadNeighbor(leftNeighbor)) {
        c.abort(
          ctx.errorPos,
          BoundaryErrors.badLeftNeighbor(argNum, leftNeighbor)
        )
      }
    }

    if (ctx.rightPart.nonEmpty) {
      val rightNeighbor = ctx.rightPart.charAt(0)
      if (isBadNeighbor(rightNeighbor)) {
        c.abort(
          ctx.errorPos,
          BoundaryErrors.badRightNeighbor(argNum, rightNeighbor)
        )
      }
    }
  }

  /**
   * Generate runtime checks for nested PythonTemplateBuilder args.
   *
   * This is only emitted when the boundary context is unsafe. The runtime guard is:
   *   if (arg.containsEncodableString) throw ...
   */
  def runtimeChecksForNestedBuilder(ctx: RuntimeContext, argIdent: Tree): List[Tree] = {
    val prefixLine = lineTail(ctx.prefixSource)
    val argNum = ctx.argIndex + 1

    val insideQuoted = hasUnclosedQuote(prefixLine)
    val afterComment = hasCommentOutsideQuotes(prefixLine)

    val leftNeighborOpt: Option[Char] =
      if (ctx.leftPart.nonEmpty) Some(ctx.leftPart.charAt(ctx.leftPart.length - 1)) else None

    val rightNeighborOpt: Option[Char] =
      if (ctx.rightPart.nonEmpty) Some(ctx.rightPart.charAt(0)) else None

    val throwStmts = List.newBuilder[Tree]

    if (insideQuoted) {
      val msg = BoundaryErrors.insideQuoted(argNum, BoundaryErrors.RuntimeHint)
      throwStmts += q"throw new IllegalArgumentException(${Literal(Constant(msg))})"
    }

    if (afterComment) {
      val msg = BoundaryErrors.afterComment(argNum)
      throwStmts += q"throw new IllegalArgumentException(${Literal(Constant(msg))})"
    }

    leftNeighborOpt.foreach { ch =>
      if (isBadNeighbor(ch)) {
        val msg = BoundaryErrors.badLeftNeighbor(argNum, ch)
        throwStmts += q"throw new IllegalArgumentException(${Literal(Constant(msg))})"
      }
    }

    rightNeighborOpt.foreach { ch =>
      if (isBadNeighbor(ch)) {
        val msg = BoundaryErrors.badRightNeighbor(argNum, ch)
        throwStmts += q"throw new IllegalArgumentException(${Literal(Constant(msg))})"
      }
    }

    val throws = throwStmts.result()
    if (throws.isEmpty) Nil
    else {
      List(q"""
        if ($argIdent.containsEncodableString) {
          ..$throws
        }
      """)
    }
  }
}
