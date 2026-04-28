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

package org.apache.texera.amber.operator.visualization.ecdfPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext

import javax.validation.constraints.NotNull

@JsonSchemaInject(
  json = """{"attributeTypeRules":{"valueColumn":{"enum":["integer","long","double"]}}}"""
)
class ECDFPlotOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Value Column")
  @JsonPropertyDescription("Numeric column used to compute the empirical cumulative distribution.")
  @AutofillAttributeName
  @NotNull(message = "Value column cannot be empty")
  var valueColumn: EncodableString = ""

  @JsonProperty(required = false)
  @JsonSchemaTitle("Color Column")
  @JsonPropertyDescription("Optional column for coloring ECDF lines by group.")
  @AutofillAttributeName
  var colorColumn: EncodableString = ""

  @JsonProperty(required = false)
  @JsonSchemaTitle("Separate By Column")
  @JsonPropertyDescription("Optional column for splitting ECDF plots into subplots.")
  @AutofillAttributeName
  var separateBy: EncodableString = ""

  @JsonProperty(required = false, defaultValue = "probability")
  @JsonSchemaTitle("Y Axis Mode")
  @JsonPropertyDescription("Display cumulative probability, raw count, or cumulative sum.")
  @JsonSchemaInject(
    json = """{ "enum": ["probability", "count", "sum"], "default": "probability" }"""
  )
  var yAxisMode: String = "probability"

  @JsonProperty(required = false, defaultValue = "standard")
  @JsonSchemaTitle("CDF Mode")
  @JsonPropertyDescription(
    "'standard' shows P(X ≤ x), 'reversed' shows P(X ≥ x), " +
      "'complementary' shows 1 - P(X ≤ x)."
  )
  @JsonSchemaInject(
    json = """{ "enum": ["standard", "reversed", "complementary"], "default": "standard" }"""
  )
  var cdfMode: EncodableString = "standard"

  @JsonProperty(required = false, defaultValue = "vertical")
  @JsonSchemaTitle("Orientation")
  @JsonPropertyDescription("Plot ECDF vertically or horizontally.")
  @JsonSchemaInject(json = """{ "enum": ["vertical", "horizontal"], "default": "vertical" }""")
  var orientation: EncodableString = "vertical"

  @JsonProperty(required = false, defaultValue = "false")
  @JsonSchemaTitle("Show Markers")
  @JsonPropertyDescription("Display sample markers on the ECDF line.")
  var showMarkers: Boolean = false

  @JsonProperty(required = false, defaultValue = "none")
  @JsonSchemaTitle("Marginal Plot")
  @JsonPropertyDescription("Optional marginal plot to display alongside the ECDF.")
  @JsonSchemaInject(
    json = """{ "enum": ["none", "histogram", "rug"], "default": "none" }"""
  )
  var marginal: EncodableString = "none"

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      "Empirical Cumulative Distribution Plot",
      "Visualize the empirical cumulative distribution of a numeric column.",
      OperatorGroupConstants.VISUALIZATION_STATISTICAL_GROUP
    )

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema().add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  def manipulateTable(): PythonTemplateBuilder = {
    assert(valueColumn.nonEmpty)
    val requiredCols =
      List(
        Some(pyb"$valueColumn"),
        Option.when(colorColumn.nonEmpty)(pyb"$colorColumn"),
        Option.when(separateBy.nonEmpty)(pyb"$separateBy")
      ).flatten
    val requiredColsExpr = requiredCols.mkString(", ")

    pyb"""
       |        required_cols = [$requiredColsExpr]
       |        table.dropna(subset=required_cols, inplace=True)
       |        table[$valueColumn] = pd.to_numeric(table[$valueColumn], errors='coerce')
       |        table.dropna(subset=[$valueColumn], inplace=True)
       |"""
  }

  def createPlotlyFigure(): PythonTemplateBuilder = {
    assert(valueColumn.nonEmpty)

    val args = scala.collection.mutable.ArrayBuffer[PythonTemplateBuilder](
      pyb"table",
      pyb"x=$valueColumn"
    )
    if (colorColumn.nonEmpty) args += pyb"color=$colorColumn"
    if (separateBy.nonEmpty) args += pyb"facet_col=$separateBy"
    yAxisMode match {
      case "count" => args += pyb"ecdfnorm=None"
      case "sum"   => args += pyb"ecdfnorm=None"
      case _       =>
    }
    if (yAxisMode == "sum") args += pyb"y=$valueColumn"
    if (cdfMode != "standard") args += pyb"ecdfmode=$cdfMode"
    if (orientation == "horizontal") args += pyb"orientation='h'"
    if (showMarkers) args += pyb"markers=True"
    if (marginal != "none") args += pyb"marginal=$marginal"

    val joinedArgs = args.mkString(", ")
    pyb"""
       |        fig = px.ecdf($joinedArgs)
       |        fig.update_layout(margin=dict(l=0, r=0, t=30, b=0))
       |"""
  }

  override def generatePythonCode(): String = {
    val finalCode =
      pyb"""
         |from pytexera import *
         |
         |import pandas as pd
         |import plotly.express as px
         |import plotly.io
         |
         |class ProcessTableOperator(UDFTableOperator):
         |    def render_error(self, error_msg):
         |        return '''<h1>Empirical cumulative distribution plot is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |            yield {'html-content': self.render_error("input table is empty.")}
         |            return
         |        ${manipulateTable()}
         |        if table.empty:
         |            yield {'html-content': self.render_error("no valid rows left after removing missing or non-numeric values.")}
         |            return
         |        ${createPlotlyFigure()}
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |"""
    finalCode.encode
  }
}
