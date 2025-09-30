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

package edu.uci.ics.amber.operator.visualization.choroplethMap

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "locations": {
      "enum": ["string"]
    },
    "color": {
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class ChoroplethMapOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(value = "locations", required = true)
  @JsonSchemaTitle("Locations Column")
  @JsonPropertyDescription(
    "Column used to describe location. Currently only supports countries and needs to be three-letter ISO country code"
  )
  @AutofillAttributeName
  var locations: String = ""

  @JsonProperty(value = "color", required = true)
  @JsonSchemaTitle("Color Column")
  @JsonPropertyDescription(
    "Column used to determine intensity of color of the region"
  )
  @AutofillAttributeName
  var color: String = ""

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Choropleth Map",
      "Visualize data using a Choropleth Map that uses shades of colors to show differences in properties or quantities between regions",
      OperatorGroupConstants.VISUALIZATION_ADVANCED_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  def manipulateTable(): String = {
    assert(locations.nonEmpty)
    assert(color.nonEmpty)
    s"""
       |        table.dropna(subset=['$locations', '$color'], inplace = True)
       |""".stripMargin
  }

  def createPlotlyFigure(): String = {
    assert(locations.nonEmpty && color.nonEmpty)
    s"""
       |        fig = px.choropleth(table, locations="$locations", color="$color", color_continuous_scale=px.colors.sequential.Plasma)
       |        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
    val finalCode =
      s"""
         |from pytexera import *
         |
         |import plotly.express as px
         |import plotly.io
         |import plotly
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |    # Generate custom error message as html string
         |    def render_error(self, error_msg) -> str:
         |        return '''<h1>Choropleth map is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |           yield {'html-content': self.render_error("Input table is empty.")}
         |           return
         |        ${manipulateTable()}
         |        if table.empty:
         |           yield {'html-content': self.render_error("No valid rows left (every row has at least 1 missing value).")}
         |           return
         |        ${createPlotlyFigure()}
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |""".stripMargin
    finalCode
  }
}
