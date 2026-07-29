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

package org.apache.texera.amber.operator.visualization.dumbbellPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.{
  PythonTemplateBuilderStringContext,
  pyStringLiteral
}
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.{PythonOperatorDescriptor, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder

import java.util
import javax.validation.constraints.{NotBlank, NotNull}
import scala.jdk.CollectionConverters.CollectionHasAsScala
//type constraint: measurementColumnName can only be a numeric column
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "measurementColumnName": {
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class DumbbellPlotOpDesc extends PythonOperatorDescriptor with StandaloneCodeGenerator {

  @JsonProperty(value = "categoryColumnName", required = true)
  @JsonSchemaTitle("Category Column Name")
  @JsonPropertyDescription("the name of the category column")
  @AutofillAttributeName
  @NotNull(message = "Category Column Name cannot be empty")
  var categoryColumnName: EncodableString = ""

  @JsonProperty(value = "dumbbellStartValue", required = true)
  @JsonSchemaTitle("Dumbbell Start Value")
  @JsonPropertyDescription("the start point value of each dumbbell")
  @NotBlank(message = "Dumbbell Start Value cannot be empty")
  var dumbbellStartValue: EncodableString = ""

  @JsonProperty(value = "dumbbellEndValue", required = true)
  @JsonSchemaTitle("Dumbbell End Value")
  @JsonPropertyDescription("the end value of each dumbbell")
  @NotBlank(message = "Dumbbell End Value cannot be empty")
  var dumbbellEndValue: EncodableString = ""

  @JsonProperty(value = "measurementColumnName", required = true)
  @JsonSchemaTitle("Measurement Column Name")
  @JsonPropertyDescription("the name of the measurement column")
  @AutofillAttributeName
  @NotNull(message = "Measurement Column Name cannot be empty")
  var measurementColumnName: EncodableString = ""

  @JsonProperty(value = "comparedColumnName", required = true)
  @JsonSchemaTitle("Compared Column Name")
  @JsonPropertyDescription("the column name that is being compared")
  @AutofillAttributeName
  @NotNull(message = "Compared Column Name cannot be empty")
  var comparedColumnName: EncodableString = ""

  @JsonProperty(value = "dots", required = false)
  var dots: util.List[DumbbellDotConfig] = _

  @JsonProperty(value = "showLegends", required = false, defaultValue = "false")
  @JsonSchemaTitle("Show Legends?")
  @JsonPropertyDescription("whether to show legends in the graph")
  var showLegends: Boolean = false

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      "Dumbbell Plot",
      "Visualize data in a Dumbbell Plot. A dumbbell plot (also known as a lollipop chart) is typically used to compare two distinct values or time points for the same entity.",
      OperatorGroupConstants.VISUALIZATION_BASIC_GROUP
    )

  def createPlotlyDumbbellLineFigure(): PythonTemplateBuilder = {
    val dumbbellValues = pyb"$dumbbellStartValue, $dumbbellEndValue"
    var showLegendsOption = "showlegend=False"
    if (showLegends) {
      showLegendsOption = "showlegend=True"
    }
    pyb"""
       |
       |        entityNames = list(table[$comparedColumnName].unique())
       |        entityNames = sorted(entityNames, reverse=True)
       |        categoryValues = [$dumbbellValues]
       |        filtered_table = table[(table[$comparedColumnName].isin(entityNames)) &
       |                    (table[$categoryColumnName].isin(categoryValues))]
       |
       |        # Create the dumbbell line using Plotly
       |        fig = go.Figure()
       |        color = 'black'
       |        for entity in entityNames:
       |          entity_data = filtered_table[filtered_table[$comparedColumnName] == entity]
       |          fig.add_trace(go.Scatter(x=entity_data[$measurementColumnName],
       |                             y=[entity]*len(entity_data),
       |                             mode='lines',
       |                             name=entity,
       |                             line=dict(color=color)))
       |
       |          fig.update_layout(xaxis_title=$measurementColumnName,
       |                  yaxis_title=$comparedColumnName,
       |                  yaxis=dict(categoryorder='array', categoryarray=entityNames),
       |                  $showLegendsOption
       |                  )
       |"""
  }

  def addPlotlyDots(): PythonTemplateBuilder = {

    var dotColumnNames = ""
    if (dots != null && dots.size() != 0) {
      dotColumnNames = dots.asScala
        .map { dot =>
          pyb"${dot.dotValue}"
        }
        .mkString(",")
    }

    pyb"""
       |        dotColumnNames = [$dotColumnNames]
       |        if len(dotColumnNames) > 0:
       |          for dotColumn in dotColumnNames:
       |              # Extract dot data for each entity
       |              for entity in entityNames:
       |                  entity_dot_data = filtered_table[filtered_table[$comparedColumnName] == entity]
       |                  # Extract X and Y values for the dot
       |                  x_values = entity_dot_data[dotColumn].values
       |                  y_values = [entity] * len(x_values)
       |                  # Add scatter plot for dots
       |                  fig.add_trace(go.Scatter(x=x_values, y=y_values,
       |                                         mode='markers',
       |                                         name=entity + ' ' + dotColumn,
       |                                         marker=dict(color='black', size=5)))  # Customize color and size as needed
       |"""
  }

  override def generatePythonCode(): String = {
    pyb"""
       |from pytexera import *
       |
       |import plotly.express as px
       |import plotly.graph_objects as go
       |import plotly.io
       |import numpy as np
       |
       |class ProcessTableOperator(UDFTableOperator):
       |    def render_error(self, error_msg):
       |        return '''<h1>DumbbellPlot is not available.</h1>
       |                  <p>Reason is: {} </p>
       |               '''.format(error_msg)
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        if table.empty:
       |           yield {'html-content': self.render_error("input table is empty.")}
       |           return
       |        ${createPlotlyDumbbellLineFigure()}
       |        ${addPlotlyDots()}
       |        # convert fig to html content
       |        fig.update_layout(margin=dict(l=0, r=0, b=60, t=0))
       |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
       |        yield {'html-content': html}
       |
       |""".encode
  }

  override def producesDataFrame(): Boolean = false

  override def generateStandaloneCode(): String = {
    // Typed-in values and column names become escaped Python literals; the runtime
    // path splices them as decode expressions, which a standalone script cannot use.
    val comparedLit = pyStringLiteral(comparedColumnName)
    val measurementLit = pyStringLiteral(measurementColumnName)
    val showLegendsOption = if (showLegends) "showlegend=True" else "showlegend=False"
    // Python list literal of dot column names, matching addPlotlyDots().
    val dotColumnNames =
      if (dots != null && dots.size() != 0)
        dots.asScala.map(dot => pyStringLiteral(dot.dotValue)).mkString(", ")
      else ""
    s"""import plotly.graph_objects as go
       |
       |def render_error(error_msg):
       |    return '''<h1>DumbbellPlot is not available.</h1>
       |              <p>Reason is: {} </p>
       |           '''.format(error_msg)
       |
       |if in1df.empty:
       |    with open("output.html", "w", encoding="utf-8") as output:
       |        output.write(render_error("input table is empty."))
       |else:
       |    table = in1df
       |    entityNames = list(table[$comparedLit].unique())
       |    entityNames = sorted(entityNames, reverse=True)
       |    categoryValues = [${pyStringLiteral(dumbbellStartValue)}, ${pyStringLiteral(
      dumbbellEndValue
    )}]
       |    filtered_table = table[(table[$comparedLit].isin(entityNames)) &
       |                (table[${pyStringLiteral(categoryColumnName)}].isin(categoryValues))]
       |    fig = go.Figure()
       |    color = 'black'
       |    for entity in entityNames:
       |        entity_data = filtered_table[filtered_table[$comparedLit] == entity]
       |        fig.add_trace(go.Scatter(x=entity_data[$measurementLit],
       |                                 y=[entity] * len(entity_data),
       |                                 mode='lines',
       |                                 name=entity,
       |                                 line=dict(color=color)))
       |    fig.update_layout(xaxis_title=$measurementLit,
       |                      yaxis_title=$comparedLit,
       |                      yaxis=dict(categoryorder='array', categoryarray=entityNames),
       |                      $showLegendsOption,
       |                      margin=dict(l=0, r=0, b=60, t=0))
       |    dotColumnNames = [$dotColumnNames]
       |    for dotColumn in dotColumnNames:
       |        for entity in entityNames:
       |            entity_dot_data = filtered_table[filtered_table[$comparedLit] == entity]
       |            x_values = entity_dot_data[dotColumn].values
       |            y_values = [entity] * len(x_values)
       |            fig.add_trace(go.Scatter(x=x_values, y=y_values,
       |                                     mode='markers',
       |                                     name=entity + ' ' + dotColumn,
       |                                     marker=dict(color='black', size=5)))
       |    fig.write_json("output.json")
       |    fig.write_html("output.html")
       |    print("Dumbbell plot saved to output.html")""".stripMargin
  }
}
