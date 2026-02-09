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

import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/**
 * Convenience type aliases for strings passed into the [[PythonTemplateBuilder]] interpolator.
 *
 * Design intent:
 *   - Some strings are “UI-provided” and must be rendered as a Python expression that decodes base64 at runtime.
 *   - Other strings are regular Python source fragments and should be spliced in as-is.
 *
 * The macro distinguishes Encodable strings via a TYPE_USE annotation (`String @EncodableStringAnnotation`).
 */
object PyStringTypes {

  /**
   * Treated as an Encodable string by the macro via a TYPE_USE annotation.
   *
   * Example:
   * {{{
   * import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableStringType
   * import org.apache.texera.amber.pybuilder.PythonTemplateBuilder._
   *
   * val label: EncodableStringType = "Hello"
   * val code = pyb"print($label)"
   * }}}
   */
  type EncodableString = String @EncodableStringAnnotation

  /**
   * Normal python string (macro defaults to [[PythonLiteral]] when no [[EncodableStringAnnotation]] is present).
   *
   * This alias exists mostly for readability and symmetry with [[EncodableStringFactory]].
   */
  type PythonLiteral = String

  /**
   * Helper “constructor” and constants for [[EncodableString]].
   *
   * Note: the object and members are annotated so downstream type inference tends
   * to keep the TYPE_USE annotation attached in common scenarios.
   */
  @EncodableStringAnnotation
  object EncodableStringFactory {

    /** Wrap a raw Scala string as an Encodable-marked string. */
    @EncodableStringAnnotation
    def apply(s: String): EncodableString = s

    /** Empty Encodable string (still Encodable-marked). */
    @EncodableStringAnnotation
    val empty: EncodableString = ""
  }

  /**
   * Helper “constructor” and constants for [[PythonLiteral]].
   *
   * This does not apply any Encodable semantics. It is regular Scala `String` usage.
   */
  object PyLiteralFactory {

    /** Identity wrapper, used as a readability hint at call sites. */
    def apply(s: String): PythonLiteral = s

    /** Empty python string. */
    val empty: PythonLiteral = ""
  }
}

/**
 * =PythonTemplateBuilder: ergonomic Python codegen via `pyb"..."`=
 *
 * This module provides a tiny DSL for assembling Python source code from Scala while preserving two competing goals:
 * (1) developers want to write templates that look like normal Python, and (2) user-provided text must not be injected
 * into the emitted Python as raw literals that can break syntax or create ambiguous token boundaries.
 *
 * The core idea is that every value spliced into a `pyb"..."` template is first classified into one of two buckets:
 *
 *  - '''Python literals''' (ordinary Scala strings or already-safe fragments) are inserted as-is.
 *  - '''Encodable strings''' (typically UI-provided text) are base64-encoded at build time and rendered as a *Python
 *    expression* that decodes at runtime, rather than being embedded as a Python string literal.
 *
 * This classification is driven by a TYPE_USE annotation: `String @EncodableStringAnnotation`. The annotation is defined
 * with a runtime retention and is allowed on fields, parameters, local variables, and type uses, so it survives many
 * common Scala typing patterns (e.g., inferred vals, constructor params, or aliases). Users normally do not construct the
 * annotation directly; instead, they use helper type aliases/factories in `PyStringTypes` for readability.
 *
 * ==Render modes==
 *
 * A `PythonTemplateBuilder` can be rendered in two modes:
 *
 *  - `plain`: emit everything as raw text (useful for debugging or when you know all content is safe).
 *  - `encode`: emit encodable chunks as Python decode expressions (the default `toString` behavior).
 *
 * Internally this is represented as a small sealed trait enum (`RenderMode.Plain` / `RenderMode.Encode`) rather than an
 * integer flag, to keep call sites self-documenting and avoid “magic numbers”.
 *
 * ==Chunk model (immutable, composable)==
 *
 * A builder is an immutable list of chunks:
 *
 *  - `Text(value)` for literal template parts
 *  - `Value(renderer)` for interpolated arguments that know how to render in each mode
 *
 * Two concrete renderers are provided:
 *
 *  - `EncodableStringRenderer`: pre-encodes `stringValue` as base64 (UTF-8) once, and in `Encode` mode produces a Python
 *    expression like `self.decode_python_template('<b64>')` given by [[wrapWithPythonDecoderExpr]].
 *  - `PyLiteralStringRenderer`: always emits the raw string value unchanged.
 *
 * Builders can be concatenated with `+` (builder + builder), which merges adjacent `Text` chunks for compactness.
 * Direct concatenation with a plain `String` is intentionally unsupported to prevent bypassing the macro’s safety checks.
 *
 * ==How the `pyb"..."` macro works==
 *
 * The `pyb` interpolator is implemented as a Scala macro. At compile time it receives:
 *
 *  - the literal parts from the `StringContext` (the “gaps” around `$args`)
 *  - the argument trees corresponding to each `$arg`
 *
 * The macro’s pipeline is:
 *
 *  1. '''Extract literal parts''' from the `StringContext` AST and ensure they are *string literals*. If any part is not
 *     a literal, compilation aborts. This prevents “template text” from being computed dynamically where correctness and
 *     boundary analysis would become unreliable.
 *
 * 2. '''Classify direct encodable arguments''' using `EncodableInspector`:
 * it inspects both the argument symbol and the argument type to determine whether the encodable annotation is present.
 * This includes a small “accessor hop” so that annotations placed on fields/constructor params are still visible when
 * call sites reference getters.
 *
 * 3. '''Compile-time boundary validation for direct encodables''':
 * if an argument is directly encodable (and not a nested builder), `BoundaryValidator.validateCompileTime` is run on
 * its surrounding literal context. The validator performs quick lexical checks on the current line:
 *
 *       - the splice must not occur inside an unclosed single/double-quoted string
 *       - the splice must not occur after a `#` comment marker
 *       - the splice must not be immediately adjacent to identifier characters or quote characters on either side
 *
 * These restrictions exist because an Encodable string renders as a Python *expression*, not a Python string literal.
 * Putting an expression inside quotes, inside a comment, or glued to an identifier would either be invalid Python or
 * silently change tokenization in surprising ways.
 *
 * 4. '''Lower each argument into a builder''':
 * every `$arg` becomes a `PythonTemplateBuilder`.
 *
 *       - If the argument is already a `PythonTemplateBuilder`, it is used directly.
 *       - Otherwise, it is wrapped into a `StringRenderer` (`EncodableStringRenderer` or `PyLiteralStringRenderer`) and
 *         turned into a minimal builder containing a single `Value(...)` chunk.
 *
 * Each argument is evaluated once and stored in a fresh local `val __pyb_argN` so that expensive expressions or
 * side-effects are not duplicated by expansion.
 *
 * 5. '''Runtime safety for nested builders''':
 * for arguments that are themselves `PythonTemplateBuilder`s, the macro cannot always know at compile time whether they
 * contain Encodable chunks (they may be computed, returned, or composed elsewhere). For these nested builders, the macro
 * conditionally emits runtime guards *only when the surrounding context is unsafe* (inside quotes, after comments, or
 * adjacent to “bad neighbor” characters). The guard pattern is:
 *
 * {{{
 *     if (__pyb_argN.containsEncodableString) throw new IllegalArgumentException("...")
 * }}}
 *
 * This preserves the ergonomics of composing builders while keeping the same safety contract as direct splices.
 *
 * 6. '''Assemble the final builder''':
 * the macro concatenates `text0 + arg0 + text1 + arg1 + ... + textN` into one `PythonTemplateBuilder`.
 *
 * ==Lexical checks (best-effort, intentionally small)==
 *
 * The boundary rules rely on `PythonLexerUtils`, a tiny state machine that scans only the “current line tail” to decide
 * whether quotes are unbalanced and whether a `#` begins a comment outside quotes. This is not a full Python parser.
 * It is deliberately lightweight so the macro stays fast and so the helpers can be unit-tested independently.
 *
 * ==Extensibility notes==
 *
 * The design keeps all rendering behavior behind `StringRenderer`, and keeps boundary policy in `BoundaryValidator`.
 * If new encoding schemes, alternate runtime decode helpers, or additional safety rules are needed, they can be introduced
 * without rewriting the template-building API. In particular, swapping `wrapWithPythonDecoderExpr` or adding new renderers
 * is a contained change: the macro only needs to decide *which renderer* to use, not *how it renders*.
 */
object PythonTemplateBuilder {

  // ===== render mode enum (no Ints) =====
  def wrapWithPythonDecoderExpr(text: String): String =
    s"self.decode_python_template('$text')"

  sealed trait RenderMode extends Product with Serializable
  object RenderMode {
    case object Plain extends RenderMode
    case object Encode extends RenderMode
  }

  // ===== wrappers =====

  /**
   * Base abstraction for values that can be spliced into a [[PythonTemplateBuilder]].
   *
   * A [[StringRenderer]] knows how to render itself depending on `mode`.
   */
  sealed trait StringRenderer extends Product with Serializable {
    def stringValue: String
    def render(mode: RenderMode): String
  }

  /**
   * Encodable string: encoded-mode wraps with [[wrapWithPythonDecoderExpr]],
   * plain-mode is raw `stringValue`.
   */
  final case class EncodableStringRenderer(stringValue: String) extends StringRenderer {
    private val encodedB64: String =
      Base64.getEncoder.encodeToString(stringValue.getBytes(StandardCharsets.UTF_8))

    override def render(mode: RenderMode): String =
      if (mode == Encode) wrapWithPythonDecoderExpr(encodedB64) else stringValue
  }

  /**
   * Python literal string: always raw `stringValue` regardless of mode.
   */
  final case class PyLiteralStringRenderer(stringValue: String) extends StringRenderer {
    override def render(mode: RenderMode): String = stringValue
  }

  // ===== internal chunk model =====

  private[pybuilder] sealed trait Chunk extends Product with Serializable
  private[pybuilder] final case class Text(value: String) extends Chunk
  private[pybuilder] final case class Value(value: StringRenderer) extends Chunk

  /**
   * Build a [[PythonTemplateBuilder]] from literal parts and already-wrapped args.
   *
   * @param literalParts raw StringContext parts (length = args + 1)
   * @param pyArgs       args wrapped as [[StringRenderer]]
   */
  private[amber] def fromInterpolated(literalParts: List[String], pyArgs: List[StringRenderer]): PythonTemplateBuilder = {
    require(
      literalParts.length == pyArgs.length + 1,
      s"pyb interpolator mismatch: parts=${literalParts.length}, args=${pyArgs.length}"
    )

    val chunkBuilder = List.newBuilder[Chunk]
    chunkBuilder += Text(literalParts.head)

    var argIndex = 0
    while (argIndex < pyArgs.length) {
      chunkBuilder += Value(pyArgs(argIndex))
      chunkBuilder += Text(literalParts(argIndex + 1))
      argIndex += 1
    }

    new PythonTemplateBuilder(compact(chunkBuilder.result()))
  }

  /** Merge adjacent text chunks. */
  private def compact(chunksToCompact: List[Chunk]): List[Chunk] =
    chunksToCompact.foldRight(List.empty[Chunk]) {
      case (Text(leftText), Text(rightText) :: remaining) =>
        Text(leftText + rightText) :: remaining
      case (chunk, compactedTail) =>
        chunk :: compactedTail
    }

  /** Concatenate chunk lists, merging boundary text chunks when possible. */
  private def concatChunks(leftChunks: List[Chunk], rightChunks: List[Chunk]): List[Chunk] =
    (leftChunks, rightChunks) match {
      case (Nil, _) => rightChunks
      case (_, Nil) => leftChunks
      case _ =>
        (leftChunks.last, rightChunks.head) match {
          case (Text(leftText), Text(rightText)) =>
            compact(leftChunks.dropRight(1) ::: Text(leftText + rightText) :: rightChunks.tail)
          case _ =>
            leftChunks ::: rightChunks
        }
    }

  // ===== custom interpolator =====

  /** Adds the `pyb"..."` string interpolator. */
  implicit final class PythonTemplateBuilderStringContext(private val stringContext: StringContext) extends AnyVal {
    def pyb(argValues: Any*): PythonTemplateBuilder = macro Macros.pybImpl
  }

  object Macros {

    /** Macro entry point for `pyb"..."`. */
    def pybImpl(macroCtx: blackbox.Context)(
      argValues: macroCtx.Expr[Any]*
    ): macroCtx.Expr[PythonTemplateBuilder] = {
      import macroCtx.universe._

      // Stable, fully-qualified references as Trees/TypeTrees (NOT Strings)
      val PTBTerm: Tree =
        q"_root_.org.apache.texera.amber.pybuilder.PythonTemplateBuilder"
      val PTBType: Tree =
        tq"_root_.org.apache.texera.amber.pybuilder.PythonTemplateBuilder"
      val StringRendererTpt: Tree =
        tq"_root_.org.apache.texera.amber.pybuilder.PythonTemplateBuilder.StringRenderer"

      val inspector = new EncodableInspector[macroCtx.type](macroCtx)
      val validator = new BoundaryValidator[macroCtx.type](macroCtx)

      // --- extract literal parts from StringContext ---
      val literalPartTrees: List[Tree] = macroCtx.prefix.tree match {
        case Apply(_, List(Apply(_, rawPartTrees))) => rawPartTrees
        case prefixTree =>
          macroCtx.abort(
            macroCtx.enclosingPosition,
            s"pyb interpolator: cannot extract StringContext parts from: ${showRaw(prefixTree)}"
          )
      }

      // Ensure parts are string literals.
      literalPartTrees.foreach {
        case Literal(Constant(_: String)) => // ok
        case nonLiteral =>
          macroCtx.abort(
            macroCtx.enclosingPosition,
            s"pyb interpolator requires literal parts; got: ${showRaw(nonLiteral)}"
          )
      }

      val literalPartStrings: List[String] =
        literalPartTrees.map { case Literal(Constant(s: String)) => s }

      // --- compile-time boundary checks for *direct* Encodable args ---
      argValues.toList.zipWithIndex.foreach {
        case (argExpr, argIndex) if inspector.isDirectEncodableStringArg(argExpr) =>
          val leftPart = literalPartStrings(argIndex)
          val rightPart = literalPartStrings(argIndex + 1)
          val prefixSource = literalPartStrings.take(argIndex + 1).mkString("")
          val errorPos =
            if (argExpr.tree.pos != NoPosition) argExpr.tree.pos else macroCtx.enclosingPosition

          validator.validateCompileTime(
            validator.CompileTimeContext(leftPart, rightPart, prefixSource, argIndex, errorPos)
          )

        case _ => // no-op
      }

      // --- builders for literal parts and args ---
      val emptyRenderArgs =
        q"_root_.scala.List.empty[$StringRendererTpt]"

      def textBuilder(partTree: Tree): Tree =
        q"$PTBTerm.fromInterpolated(_root_.scala.List($partTree), $emptyRenderArgs)"

      val emptyStrLit: Tree = Literal(Constant(""))

      def valueBuilder(argExpr: macroCtx.Expr[Any]): Tree = {
        val wrapped = inspector.wrapArg(argExpr)
        q"$PTBTerm.fromInterpolated(_root_.scala.List($emptyStrLit, $emptyStrLit), _root_.scala.List($wrapped))"
      }

      val pythonTemplateBuilderTpe =
        typeOf[_root_.org.apache.texera.amber.pybuilder.PythonTemplateBuilder]

      def argAsBuilder(argExpr: macroCtx.Expr[Any]): Tree = {
        val argTree = argExpr.tree
        val argType = argTree.tpe
        if (argType != null && (argType.dealias.widen <:< pythonTemplateBuilderTpe)) {
          q"$argTree.asInstanceOf[$PTBType]"
        } else {
          valueBuilder(argExpr)
        }
      }

      // Evaluate each arg once.
      val evaluatedArgBuilders: List[Tree] =
        argValues.toList.zipWithIndex.map {
          case (argExpr, i) =>
            val argValName = TermName(s"__pyb_arg$i")
            q"val $argValName: $PTBType = ${argAsBuilder(argExpr)}"
        }

      // Runtime boundary checks for nested PythonTemplateBuilders that *may* contain Encodable chunks.
      val nestedBuilderBoundaryChecks: List[Tree] =
        argValues.toList.zipWithIndex.flatMap {
          case (argExpr, argIndex) if inspector.isPythonTemplateBuilderArg(argExpr) =>
            val leftPart = literalPartStrings(argIndex)
            val rightPart = literalPartStrings(argIndex + 1)
            val prefixSource = literalPartStrings.take(argIndex + 1).mkString("")

            val argIdent = Ident(TermName(s"__pyb_arg$argIndex"))
            validator.runtimeChecksForNestedBuilder(
              validator.RuntimeContext(leftPart, rightPart, prefixSource, argIndex),
              argIdent
            )

          case _ => Nil
        }

      // Concatenate: text0 + arg0 + text1 + arg1 + ... + textN
      val renderTree: Tree = {
        val baseTree = textBuilder(literalPartTrees.head)
        argValues.toList.zipWithIndex.foldLeft(baseTree) {
          case (acc, (_, i)) =>
            val argIdent = Ident(TermName(s"__pyb_arg$i"))
            val nextText = textBuilder(literalPartTrees(i + 1))
            q"$acc + $argIdent + $nextText"
        }
      }

      val finalExpr: Tree =
        q"""
      {
        ..$evaluatedArgBuilders
        ..$nestedBuilderBoundaryChecks
        $renderTree
      }
    """

      macroCtx.Expr[PythonTemplateBuilder](finalExpr)
    }
  }
}

/**
 * An immutable builder for Python source produced via `pyb"..."` interpolation.
 */
final class PythonTemplateBuilder private[pybuilder] (private val chunks: List[PythonTemplateBuilder.Chunk])
  extends Serializable {
  import PythonTemplateBuilder._

  def +(that: PythonTemplateBuilder): PythonTemplateBuilder =
    new PythonTemplateBuilder(concatChunks(this.chunks, that.chunks))

  def +(that: String): PythonTemplateBuilder =
    throw new UnsupportedOperationException(s"Direct String concatenation is not supported $that")

  def plain: String = render(Plain)

  def encode: String = render(Encode)

  override def toString: String = encode

  def containsEncodableString: Boolean =
    chunks.exists {
      case Value(_: EncodableStringRenderer) => true
      case _                                 => false
    }

  private def render(renderMode: RenderMode): String = {
    val out = new java.lang.StringBuilder
    chunks.foreach {
      case Text(text)      => out.append(text)
      case Value(renderer) => out.append(renderer.render(renderMode))
    }
    out.toString
      .stripMargin
      .replace("\r\n", "\n")
      .replace("\r", "\n")
  }
}
