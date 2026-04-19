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

package org.apache.texera.amber.operator.visualization.radarChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameList
}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext

import javax.validation.constraints.NotNull

// type constraint: value can only be numeric
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "valueColumns": {
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class RadarChartOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(value = "nameColumn", required = true)
  @JsonSchemaTitle("Name Column")
  @JsonPropertyDescription("Column containing entity names for each radar")
  @AutofillAttributeName
  @NotNull(message = "Name column cannot be empty")
  var nameColumn: EncodableString = ""

  @JsonProperty(value = "valueColumns", required = true)
  @JsonSchemaTitle("Value Columns")
  @JsonPropertyDescription("Columns containing numeric values for radar chart axes")
  @AutofillAttributeNameList
  var valueColumns: List[EncodableString] = _

  @JsonProperty(value = "fillOpacity", required = true)
  @JsonSchemaTitle("Fill Opacity")
  @JsonPropertyDescription(
    "Opacity value for radar chart fill from 0.0 (transparent) to 1.0 (opaque)"
  )
  @JsonSchemaInject(json = """{ "minimum": 0.0, "maximum": 1.0, "default": 0.5 }""")
  var fillOpacity: Double = 0.5

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      "Radar Chart",
      "Visualize data in a Radar Chart",
      OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP
    )

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  def manipulateTable(): PythonTemplateBuilder = {
    assert(nameColumn.nonEmpty)
    assert(valueColumns != null && valueColumns.nonEmpty)
    val valueColsList = valueColumns.map(col => pyb"""$col""").mkString(", ")
    pyb"""
       |        required_cols = [$nameColumn, $valueColsList]
       |        table.dropna(subset=required_cols, inplace=True)
       |        value_cols = [$valueColsList]
       |        for col in value_cols:
       |            table[col] = pd.to_numeric(table[col], errors='coerce')
       |        table.dropna(subset=value_cols, inplace=True)
       |"""
  }

  def createPlotlyFigure(): PythonTemplateBuilder = {
    val valueColsList = valueColumns.map(col => pyb"""$col""").mkString(", ")
    pyb"""
       |        fig = go.Figure()
       |        categories = [$valueColsList]
       |
       |        for idx, row in table.iterrows():
       |            values = [row[col] for col in categories]
       |            values.append(values[0])
       |            categories_closed = categories + [categories[0]]
       |
       |            fig.add_trace(go.Scatterpolar(
       |                r=values,
       |                theta=categories_closed,
       |                fill='toself',
       |                name=str(row[$nameColumn]),
       |                opacity=$fillOpacity
       |            ))
       |
       |        fig.update_layout(
       |            polar=dict(
       |                radialaxis=dict(
       |                    visible=True,
       |                    range=[0, None]
       |                )
       |            ),
       |            showlegend=True,
       |            margin=dict(t=40, b=40, l=40, r=40)
       |        )
       |"""
  }

  override def generatePythonCode(): String = {
    val finalcode =
      pyb"""
         |from pytexera import *
         |
         |import plotly.graph_objects as go
         |import plotly.io
         |import pandas as pd
         |
         |class ProcessTableOperator(UDFTableOperator):
         |    def render_error(self, error_msg):
         |        return '''<h1>RadarChart is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |           yield {'html-content': self.render_error("input table is empty.")}
         |           return
         |        ${manipulateTable()}
         |        if table.empty:
         |           yield {'html-content': self.render_error("input table is empty after removing missing values.")}
         |           return
         |        ${createPlotlyFigure()}
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |
         |"""
    finalcode.encode
  }
}
