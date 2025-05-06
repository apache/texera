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

package edu.uci.ics.amber.operator.visualization.rangeSlider

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName

// type constraint: value can only be numeric
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "value": {
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class RangeSliderOpDesc extends PythonOperatorDescriptor {
  @JsonProperty(value = "Y-axis", required = true)
  @JsonSchemaTitle("Y-axis")
  @JsonPropertyDescription("The name of the column to represent y-axis")
  @AutofillAttributeName var yAxis: String = ""

  @JsonProperty(value = "X-axis", required = true)
  @JsonSchemaTitle("X-axis")
  @JsonPropertyDescription("The name of the column to represent the x-axis")
  @AutofillAttributeName var xAxis: String = ""

  @JsonProperty(value = "Duplicates", required = false)
  @JsonSchemaTitle("Handle Duplicates")
  @JsonPropertyDescription("How to handle duplicate values in y-axis")
  var duplicateType: RangeSliderHandleDuplicateFunction = RangeSliderHandleDuplicateFunction.NOTHING

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Range Slider",
      "Visualize data in a Range Slider",
      OperatorGroupConstants.VISUALIZATION_BASIC_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  def manipulateTable(): String = {
    s"""
       |        table = table.dropna(subset=['$xAxis', '$yAxis'])
       |        functionType = '${duplicateType.getFunctionType}'
       |        if functionType.lower() == "mean":
       |          table = table.groupby('$xAxis')['$yAxis'].mean().reset_index() #get mean of values
       |        elif functionType.lower() == "sum":
       |          table = table.groupby('$xAxis')['$yAxis'].sum().reset_index() #get sum of values
       |""".stripMargin
  }

  def createPlotlyFigure(): String = {
    s"""
       |        # Create figure
       |        fig = go.Figure()
       |
       |        fig.add_trace(go.Scatter(x=table['$xAxis'], y=table['$yAxis'], mode = "markers+lines"))
       |
       |        # Add range slider
       |        fig.update_layout(
       |            xaxis_title='$xAxis',
       |            yaxis_title='$yAxis',
       |            xaxis=dict(
       |                rangeslider=dict(
       |                    visible=True
       |                )
       |            )
       |        )
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
    val finalcode =
      s"""
         |from pytexera import *
         |
         |import plotly.express as px
         |import plotly.graph_objects as go
         |import plotly.io
         |import numpy as np
         |
         |class ProcessTableOperator(UDFTableOperator):
         |    def render_error(self, error_msg):
         |        return '''<h1>RangeChart is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        original_table = table
         |        if table.empty:
         |           yield {'html-content': self.render_error("input table is empty.")}
         |           return
         |        if '$yAxis'.strip() == "" or '$xAxis'.strip() == "":
         |           yield {'html-content': self.render_error("Y-axis or X-axis is empty")}
         |           return
         |        ${manipulateTable()}
         |        ${createPlotlyFigure()}
         |        # convert fig to html content
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |""".stripMargin
    finalcode
  }

}
