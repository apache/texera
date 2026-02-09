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

package org.apache.texera.amber.operator.visualization.scatterplot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.OutputPort.OutputMode
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder

import javax.validation.constraints.NotNull

@JsonSchemaInject(
  json =
    "{" +
      "  \"attributeTypeRules\": {" +
      "    \"xColumn\":{" +
      "      \"enum\": [\"integer\", \"double\"]" +
      "    }," +
      "    \"yColumn\":{" +
      "      \"enum\": [\"integer\", \"double\"]" +
      "    }" +
      "  }" +
      "}"
)
class ScatterplotOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("X-Column")
  @JsonPropertyDescription("X Column")
  @AutofillAttributeName
  @NotNull(message = "X-Column cannot be null")
  private val xColumn: EncodableString = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Y-Column")
  @JsonPropertyDescription("Y Column")
  @AutofillAttributeName
  @NotNull(message = "Y-Column cannot be null")
  private val yColumn: EncodableString = ""

  @JsonProperty(required = false)
  @JsonSchemaTitle("Alpha Value")
  @JsonPropertyDescription("Alpha (opacity) value from 0.0 (transparent) to 1.0 (opaque)")
  @JsonSchemaInject(json = """{ "minimum": 0.0, "maximum": 1.0, "default": 1.0 }""")
  private val alpha: Double = 1.0

  @JsonProperty(required = false)
  @JsonSchemaTitle("Color-Column")
  @JsonPropertyDescription(
    "Dots will be assigned different colors based on their values of this column"
  )
  @AutofillAttributeName
  private val colorColumn: EncodableString = ""

  @JsonProperty(required = false, defaultValue = "false")
  @JsonSchemaTitle("log scale X")
  @JsonPropertyDescription("Values in X-column is log-scaled")
  var xLogScale: Boolean = false

  @JsonProperty(required = false, defaultValue = "false")
  @JsonSchemaTitle("log scale Y")
  @JsonPropertyDescription("Values in Y-column is log-scaled")
  var yLogScale: Boolean = false

  @JsonProperty(required = false)
  @JsonSchemaTitle("Hover column")
  @JsonPropertyDescription("Column value to display when a dot is hovered over")
  @AutofillAttributeName
  var hoverName: EncodableString = ""

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Scatter Plot",
      "View the result in a scatterplot",
      OperatorGroupConstants.VISUALIZATION_BASIC_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  def manipulateTable(): PythonTemplateBuilder = {
    assert(xColumn.nonEmpty && yColumn.nonEmpty)
    val colorColExpr = if (colorColumn.nonEmpty) {
      pyb"$colorColumn"
    } else {
      pyb""
    }
    pyb"""
       |        # drops rows with missing values pertaining to relevant columns
       |        table.dropna(subset=[$xColumn, $yColumn, $colorColExpr], inplace = True)
       |
       |"""
  }

  def createPlotlyFigure(): PythonTemplateBuilder = {
    assert(xColumn.nonEmpty && yColumn.nonEmpty)

    val args = scala.collection.mutable.ArrayBuffer(
      pyb"x=$xColumn",
      pyb"y=$yColumn",
      pyb"opacity=$alpha"
    )
    if (colorColumn.nonEmpty) args += pyb"color=$colorColumn"
    if (xLogScale) args += pyb"log_x=True"
    if (yLogScale) args += pyb"log_y=True"
    if (hoverName.nonEmpty) args += pyb"hover_name=$hoverName"

    val joined = args.mkString(", ")
    pyb"""
       |        fig = go.Figure(px.scatter(table, $joined))
       |        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
       |"""
  }

  override def generatePythonCode(): String = {
    val finalCode =
      pyb"""
         |from pytexera import *
         |
         |import plotly.express as px
         |import plotly.graph_objects as go
         |import plotly.io
         |import numpy as np
         |
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |    def render_error(self, error_msg):
         |        return '''<h1>Scatter Plot is not available.</h1>
         |                  <p>Reasons are: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |            yield {'html-content': self.render_error("Input table is empty.")}
         |            return
         |        ${manipulateTable()}
         |        ${createPlotlyFigure()}
         |        if table.empty:
         |            yield {'html-content': self.render_error("No valid rows left (every row has at least 1 missing value).")}
         |            return
         |        html = plotly.io.to_html(fig, include_plotlyjs = 'cdn', auto_play = False)
         |        yield {'html-content':html}
         |"""
    finalCode.encode
  }
}
