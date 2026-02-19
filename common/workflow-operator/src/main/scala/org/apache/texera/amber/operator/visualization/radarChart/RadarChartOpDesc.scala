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

package org.apache.texera.amber.operator.visualization.radarChart // namespace for operator

// Jackson annotations - for JSON serialization (form fields → JSON → backend)
import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}

// Core Texera types
import org.apache.texera.amber.core.tuple.{AttributeType, Schema} // for defining output schema
import org.apache.texera.amber.core.workflow.OutputPort.OutputMode // for SINGLE_SNAPSHOT mode
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity} // ports

// Base class and metadata
import org.apache.texera.amber.operator.PythonOperatorDescriptor // base class extend
import org.apache.texera.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameList
} // auto-fills column names in UI
import org.apache.texera.amber.operator.metadata.{
  OperatorGroupConstants,
  OperatorInfo
} // for operator metadata

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
  var nameColumn: String = ""

  @JsonProperty(value = "valueColumns", required = true)
  @JsonSchemaTitle("Value Columns")
  @JsonPropertyDescription("Columns containing numeric values for radar chart axes")
  @AutofillAttributeNameList
  var valueColumns: List[String] = List()

  @JsonProperty(value = "fillOpacity", required = true)
  @JsonSchemaTitle("Fill Opacity")
  @JsonPropertyDescription(
    "Opacity value for radar chart fill from 0.0 (transparent) to 1.0 (opaque)"
  )
  @JsonSchemaInject(json = """{ "minimum": 0.0, "maximum": 1.0, "default": 0.5 }""")
  var fillOpacity: Double = 0.5

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Radar Chart",
      "Visualize data in a Radar Chart",
      OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  def manipulateTable(): String = {
   assert(nameColumn.nonEmpty && valueColumns != null && !valueColumns.isEmpty)
   // Escape apostrophes in column names
   val safeNameColumn = nameColumn.replace("'", "\\'")
   val safeValueColumns = valueColumns.map(_.replace("'", "\\'"))
   val valueColsList = safeValueColumns.mkString("', '")
   s"""
     |        required_cols = ['$safeNameColumn', '$valueColsList']
     |        table.dropna(subset=required_cols, inplace=True)
     |        # Ensure all value columns are numeric
     |        value_cols = ['$valueColsList']
     |        for col in value_cols:
     |            table[col] = pd.to_numeric(table[col], errors='coerce')
     |        table.dropna(subset=value_cols, inplace=True)
     |""".stripMargin
  }

  def createPlotlyFigure(): String = {
    // Escape apostrophes in value columns for plotly figure as well (consistently)
    val safeValueColumns = valueColumns.map(_.replace("'", "\\'"))
    val valueColsList = safeValueColumns.mkString("', '")
    s"""
       |        # Create radar chart
       |        fig = go.Figure()
       |        categories = ['$valueColsList']
       |        
       |        for idx, row in table.iterrows():
       |            values = [row[col] for col in categories]
       |            values.append(values[0])  # Close the radar chart
       |            categories_closed = categories + [categories[0]]
       |            
       |            fig.add_trace(go.Scatterpolar(
       |                r=values,
       |                theta=categories_closed,
       |                fill='toself',
       |                name=str(row['$nameColumn']),
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
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
    val finalcode =
      s"""
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
         |        # convert fig to html content
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |
         |""".stripMargin
    finalcode
  }
}
