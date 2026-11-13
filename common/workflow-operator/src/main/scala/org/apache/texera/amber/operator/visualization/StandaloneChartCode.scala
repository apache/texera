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

private[visualization] object StandaloneChartCode {
  def standaloneError(title: String, plural: Boolean = false, typed: Boolean = false): String =
    s"""def render_error(error_msg)${if (typed) " -> str" else ""}:
       |    return '''<h1>$title</h1>
       |                  <p>${if (plural) "Reasons are" else "Reason is"}: {} </p>
       |               '''.format(error_msg)""".stripMargin

  def standaloneFigure(label: String, indent: Int = 8, bothOutputs: Boolean = false): String =
    PythonCodeFragment.render(
      s"""fig.write_json(outputJson)
         |fig.write_html(outputHtml)
         |print(${pyStringLiteral(label + " saved to ")} + ${if (bothOutputs)
        """outputJson + " and " + outputHtml"""
      else "outputHtml"})""",
      indent,
      _ => true
    )

  def standaloneErrorPage(message: String, indent: Int = 4, handle: String = "output"): String =
    PythonCodeFragment.render(
      s"""with open(outputHtml, "w", encoding="utf-8") as $handle:
         |    $handle.write(render_error(${pyStringLiteral(message)}))""",
      indent,
      _ => true
    )
}
