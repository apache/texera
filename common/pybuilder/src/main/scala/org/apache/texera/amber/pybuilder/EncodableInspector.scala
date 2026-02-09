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
 * Macro-only helper: inspects argument trees / types / symbols to decide if a value is Encodable-marked.
 *
 * NOTE: This must be context-bound because Tree/Type/Annotation are from `c.universe`.
 */
final class EncodableInspector[C <: blackbox.Context](val c: C) {

  import c.universe._

  private val stringRendererTpe: Type =
    typeOf[
      PythonTemplateBuilder.StringRenderer
    ]

  private val pythonTemplateBuilderTpe: Type =
    typeOf[PythonTemplateBuilder]

  // Previous/original approach: direct encodable args include values already wrapped as EncodableStringRenderer
  private val encodableStringRendererTpe: Type =
    typeOf[
      PythonTemplateBuilder.EncodableStringRenderer
    ]

  // Keep this as a string so it also works if the annotation is referenced indirectly.
  private val encodableStringAnnotationFqn =
    "org.apache.texera.amber.EncodableStringAnn"

  /**
   * If we are pointing at a getter/accessor, hop to its accessed field symbol when possible.
   *
   * Why: Many annotations are placed on constructor params/fields, but call sites see the accessor.
   */
  private def safeAccessed(sym: Symbol): Symbol =
    sym match {
      case termAccessor: TermSymbol if termAccessor.isAccessor       => termAccessor.accessed
      case methodAccessor: MethodSymbol if methodAccessor.isAccessor => methodAccessor.accessed
      case _                                                         => sym
    }

  /** True if an annotation instance is @EncodableStringAnn. */
  private def annIsEncodableString(annotation: Annotation): Boolean = {
    val annotationType = annotation.tree.tpe
    annotationType != null && (
      annotationType.typeSymbol.fullName == encodableStringAnnotationFqn ||
        (annotationType <:< typeOf[EncodableStringAnnotation])
      )
  }

  /**
   * True if a [[Type]] carries @EncodableStringAnnotation as a TYPE_USE annotation (via [[AnnotatedType]]).
   *
   * Walks common wrappers (existentials, refinements, type refs) to find nested annotations.
   */
  private def typeHasEncodableString(typeToCheck: Type): Boolean = {
    def loop(t: Type): Boolean = {
      if (t == null) false
      else {
        val widened = t.dealias.widen
        widened match {
          case AnnotatedType(anns, underlying) =>
            anns.exists(annIsEncodableString) || loop(underlying)
          case ExistentialType(_, underlying) =>
            loop(underlying)
          case RefinedType(parents, _) =>
            parents.exists(loop)
          case TypeRef(_, _, args) =>
            args.exists(loop)
          case other =>
            val sym = other.typeSymbol
            val symHasAnn =
              sym != null && sym != NoSymbol && sym.annotations.exists(annIsEncodableString)
            symHasAnn || other.typeArgs.exists(loop)
        }
      }
    }

    loop(typeToCheck)
  }

  /**
   * Checks @EncodableStringAnnotation on either:
   *   - accessed symbol (field/param), or
   *   - type (TYPE_USE), via [[AnnotatedType]].
   */
  def treeHasEncodableString(tree: Tree): Boolean = {
    val rawSym = tree.symbol
    val symHasAnn =
      rawSym != null && rawSym != NoSymbol && {
        val accessed = safeAccessed(rawSym)
        accessed != null && accessed != NoSymbol && accessed.annotations.exists(annIsEncodableString)
      }

    symHasAnn || (tree.tpe != null && typeHasEncodableString(tree.tpe))
  }

  def isPythonTemplateBuilderArg(argExpr: c.Expr[Any]): Boolean = {
    val tpe = argExpr.tree.tpe
    tpe != null && (tpe.dealias.widen <:< pythonTemplateBuilderTpe)
  }

  def isStringRendererArg(argExpr: c.Expr[Any]): Boolean = {
    val tpe = argExpr.tree.tpe
    tpe != null && (tpe.dealias.widen <:< stringRendererTpe)
  }

  /** True if the arg is Encodable (direct argument, not a nested builder). */
  def isDirectEncodableStringArg(argExpr: c.Expr[Any]): Boolean = {
    if (isPythonTemplateBuilderArg(argExpr)) false
    else {
      val tpe = argExpr.tree.tpe
      // Previous/original behavior:
      //  - treat already-wrapped EncodableStringRenderer as encodable
      //  - OR detect @EncodableStringAnnotation on symbol/type
      (tpe != null && (tpe.dealias.widen <:< encodableStringRendererTpe)) ||
        treeHasEncodableString(argExpr.tree)
    }
  }

  /**
   * Wrap an argument expression as a [[PythonTemplateBuilder.StringRenderer]] AST node.
   *
   * Priority:
   * 1) If it's already a StringRenderer, keep it (cast).
   * 2) Else if Encodable-marked, wrap as EncodableStringRenderer.
   * 3) Else wrap as PyLiteralStringRenderer.
   */
  def wrapArg(argExpr: c.Expr[Any]): Tree = {
    val argTree = argExpr.tree
    val argType = argTree.tpe

    if (argType != null && (argType.dealias.widen <:< stringRendererTpe)) {
      q"$argTree.asInstanceOf[_root_.org.apache.texera.amber.pybuilder.PythonTemplateBuilder.StringRenderer]"
    } else if (treeHasEncodableString(argTree)) {
      q"_root_.org.apache.texera.amber.pybuilder.PythonTemplateBuilder.EncodableStringRenderer($argTree.toString)"
    } else {
      q"_root_.org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PyLiteralStringRenderer($argTree.toString)"
    }
  }
}
