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

package org.apache.texera.amber.operator.visualization.hierarchychart

import org.apache.texera.amber.operator.visualization.PythonCodeFragment._
import org.apache.texera.amber.operator.visualization.StandaloneChartCode._
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.{
  PythonTemplateBuilderStringContext,
  pyStringLiteral
}

/** Shared Python code generation helpers for hierarchy-path Plotly charts (Hierarchy, Icicle). */
private[visualization] object HierarchyChartSupport {
  def attributesInPython(hierarchy: List[HierarchySection]): String =
    hierarchy.map(c => pyb"${c.attributeName}").mkString(",")

  def filterPositiveAndDropNa(
      value: EncodableString,
      hierarchy: List[HierarchySection]
  ): PythonTemplateBuilder = {
    assert(value.nonEmpty, "Value Column cannot be empty")
    val attributes = attributesInPython(hierarchy)
    pyb"""
       |        table[$value] = table[table[$value] > 0][$value] # remove non-positive numbers from the data
       |        table.dropna(subset = [$attributes], inplace = True) #remove missing values
       |"""
  }

  def createPlotlyFigure(
      plotlyMethod: => String,
      value: EncodableString,
      hierarchy: List[HierarchySection]
  ): PythonTemplateBuilder = {
    assert(hierarchy.nonEmpty, "Hierarchy Path cannot be empty")
    val attributes = attributesInPython(hierarchy)
    pyb"""
       |        fig = px.$plotlyMethod(table, path=[$attributes], values=$value,
       |                                                               color=$value, hover_data=[$attributes],
       |                                                               color_continuous_scale='RdBu')
       |"""
  }

  def runtimePythonCode(
      errorTitle: String,
      tableCode: PythonTemplateBuilder,
      figureCode: PythonTemplateBuilder
  ): String =
    pyb"""
       |from pytexera import *
       |
       |import plotly.express as px
       |import plotly.graph_objects as go
       |import plotly.io
       |import numpy as np
       |
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    # Generate custom error message as html string
       |${runtimeError(errorTitle, typed = true)}
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |${emptyTableGuard("input table is empty.")}
       |        $tableCode
       |${emptyTableGuard("value column contains only non-positive numbers or nulls.")}
       |        $figureCode
       |        # convert fig to html content
       |        fig.update_layout(margin=dict(l=0, r=0, b=0, t=0))
       |${plotlyHtmlYield()}
       |""".encode

  def standalonePythonCode(
      chartLabel: String,
      plotlyMethod: String,
      value: EncodableString,
      hierarchy: List[HierarchySection]
  ): String = {
    val attributes = hierarchy.map(section => pyStringLiteral(section.attributeName)).mkString(", ")
    val valueLit = pyStringLiteral(value)
    // The error page is written to the same file a plotted chart lands in, so a
    // reason for "no chart" is where the reader looks for the chart — printing it
    // to the terminal alone left that file absent. render_error's continuation
    // line keeps the runtime path's indentation, since the HTML is triple-quoted
    // and those spaces reach the browser. chart_df is a copy: the input can feed another branch.
    s"""${standaloneError(s"$chartLabel is not available.")}
       |
       |def fail(error_msg):
       |    with open(outputHtml, "w", encoding="utf-8") as output:
       |        output.write(render_error(error_msg))
       |    print(f"$chartLabel error: {error_msg}")
       |
       |if in1df.empty:
       |    fail("input table is empty.")
       |else:
       |    chart_df = in1df.copy()
       |    chart_df[$valueLit] = chart_df[chart_df[$valueLit] > 0][$valueLit]
       |    chart_df = chart_df.dropna(subset=[$attributes])
       |    if chart_df.empty:
       |        fail("value column contains only non-positive numbers or nulls.")
       |    else:
       |        fig = px.$plotlyMethod(chart_df, path=[$attributes], values=$valueLit,
       |                         color=$valueLit, hover_data=[$attributes],
       |                         color_continuous_scale='RdBu')
       |        fig.update_layout(margin=dict(l=0, r=0, b=0, t=0))
       |${standaloneFigure(chartLabel)}""".stripMargin
  }
}
