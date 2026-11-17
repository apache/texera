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

package org.apache.texera.amber.operator.visualization

import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral

private[visualization] object PythonCodeFragment {
  def render(code: String, indent: Int): String =
    code.stripMargin.linesIterator
      .map { line =>
        if (line.isEmpty) line else (" " * indent) + line
      }
      .mkString("\n")

  def runtimeError(
      title: String,
      plural: Boolean = false,
      typed: Boolean = false,
      colonOnly: Boolean = false,
      singleLineClose: Boolean = false,
      indent: Int = 4
  ): String = {
    val label =
      if (colonOnly) "Reason" else if (plural) "Reasons are" else "Reason is"
    val sig = s"def render_error(self, error_msg)${if (typed) " -> str" else ""}:"
    val pfx = " " * indent
    val closeInline = if (singleLineClose) "'''.format(error_msg)" else ""
    val tail =
      if (singleLineClose) "" else s"\n${" " * (indent + 11)}'''.format(error_msg)"
    s"""$pfx$sig
       |$pfx    return '''<h1>$title</h1>
       |$pfx              <p>$label: {} </p>$closeInline$tail""".stripMargin
  }

  def emptyTableGuard(
      message: String,
      indent: Int = 8,
      bodyIndent: Int = 3,
      doubleQuoted: Boolean = false
  ): String = {
    val q = if (doubleQuoted) "\"" else "'"
    val p0 = " " * indent
    val pb = " " * (indent + bodyIndent)
    val msgLit = pyStringLiteral(message)
    s"""${p0}if table.empty:
       |${pb}yield {${q}html-content${q}: self.render_error($msgLit)}
       |${pb}return""".stripMargin
  }

  def plotlyHtmlYield(
      indent: Int = 8,
      includeComment: Boolean = false,
      module: String = "plotly.io",
      extraArgs: String = "auto_play=False",
      yieldVar: String = "html",
      doubleQuoted: Boolean = false,
      spacesAroundEq: Boolean = false,
      compactDict: Boolean = false
  ): String = {
    val p = " " * indent
    val eq = if (spacesAroundEq) " = " else "="
    val kv = if (compactDict) ":" else ": "
    val q = if (doubleQuoted) "\"" else "'"
    val comment = if (includeComment) s"$p# convert fig to html content\n" else ""
    s"""$comment$p$yieldVar = $module.to_html(fig, include_plotlyjs${eq}'cdn', $extraArgs)
       |${p}yield {${q}html-content${q}$kv$yieldVar}""".stripMargin
  }
}
