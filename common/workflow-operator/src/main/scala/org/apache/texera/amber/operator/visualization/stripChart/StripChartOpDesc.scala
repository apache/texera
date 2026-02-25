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

package org.apache.texera.amber.operator.visualization.stripChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.OutputPort.OutputMode
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

class StripChartOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(value = "x", required = true)
  @JsonSchemaTitle("X-Axis Column")
  @JsonPropertyDescription("Column containing numeric values for the x-axis")
  @AutofillAttributeName
  var x: EncodableString = ""

  @JsonProperty(value = "y", required = true)
  @JsonSchemaTitle("Y-Axis Column")
  @JsonPropertyDescription("Column containing categorical values for the y-axis")
  @AutofillAttributeName
  var y: EncodableString = ""

  @JsonProperty(value = "colorBy", required = false)
  @JsonSchemaTitle("Color By")
  @JsonPropertyDescription("Optional - Color points by category")
  @AutofillAttributeName
  var colorBy: EncodableString = ""

  @JsonProperty(value = "facetColumn", required = false)
  @JsonSchemaTitle("Facet Column")
  @JsonPropertyDescription("Optional - Create separate subplots for each category")
  @AutofillAttributeName
  var facetColumn: EncodableString = ""

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Strip Chart",
      "Visualize distribution of data points as a strip plot",
      OperatorGroupConstants.VISUALIZATION_STATISTICAL_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  override def generatePythonCode(): String = {
    val colorByParam = if (colorBy != null && colorBy.nonEmpty) pyb", color=$colorBy" else ""
    val facetColParam =
      if (facetColumn != null && facetColumn.nonEmpty) pyb", facet_col=$facetColumn" else ""

    pyb"""from pytexera import *
       |import plotly.express as px
       |import plotly.io as pio
       |
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        x_values = table[$x]
       |        y_values = table[$y]
       |
       |        # Create data dictionary
       |        data = {$x: x_values, $y: y_values}
       |
       |        # Add optional color column if specified
       |        if $colorBy:
       |            data[$colorBy] = table[$colorBy]
       |
       |        # Add optional facet column if specified
       |        if $facetColumn:
       |            data[$facetColumn] = table[$facetColumn]
       |
       |        # Create strip chart
       |        fig = px.strip(
       |            data,
       |            x=$x,
       |            y=$y$colorByParam$facetColParam
       |        )
       |
       |        # Update layout for better visualization
       |        fig.update_traces(marker=dict(size=8, line=dict(width=0.5, color='DarkSlateGrey')))
       |        fig.update_layout(
       |            xaxis_title=$x,
       |            yaxis_title=$y,
       |            hovermode='closest'
       |        )
       |
       |        # Convert to HTML
       |        html = pio.to_html(fig, include_plotlyjs='cdn', full_html=False)
       |        yield {'html-content': html}
       |""".encode
  }
}
