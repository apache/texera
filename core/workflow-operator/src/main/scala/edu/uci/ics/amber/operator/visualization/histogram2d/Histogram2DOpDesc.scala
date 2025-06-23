/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.amber.operator.visualization.histogram2d

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.operator.visualization.histogram2d.NormalizationType

class Histogram2DOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("X Column")
  @JsonPropertyDescription("Numeric column for the X axis bins.")
  @AutofillAttributeName
  var xColumn = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Y Column")
  @JsonPropertyDescription("Numeric column for the Y axis bins.")
  @AutofillAttributeName
  var yColumn = ""

  @JsonProperty(required = true, defaultValue = "10")
  @JsonSchemaTitle("X Bins")
  @JsonPropertyDescription("Number of bins along the X axis (Default: 10)")
  var xBins: Int = _

  @JsonProperty(required = true, defaultValue = "10")
  @JsonSchemaTitle("Y Bins")
  @JsonPropertyDescription("Number of bins along the Y axis (Default: 10)")
  var yBins: Int = _

  @JsonProperty(required = false, defaultValue = "density")
  @JsonSchemaTitle("Normalization")
  @JsonPropertyDescription(
    "Type of histogram normalization"
  )
  var normalize: NormalizationType = NormalizationType.DENSITY

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Histogram2D",
      "Displays a bivariate histogram as a density heatmap",
      OperatorGroupConstants.VISUALIZATION_STATISTICAL_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema().add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def generatePythonCode(): String = {
    assert(xBins > 0, s"X Bins must be > 0, but got $xBins")
    assert(yBins > 0, s"Y Bins must be > 0, but got $yBins")

    val normArg =
      s"histnorm='${normalize.getValue}',"

    s"""
       |from pytexera import *
       |import plotly.express as px
       |import plotly.io
       |
       |class ProcessTableOperator(UDFTableOperator):
       |    def render_error(self, msg):
       |        return f"<h1>2D Histogram failed</h1><p>{msg}</p>"
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        # Empty-table guard
       |        if table.empty:
       |            yield {"html-content": self.render_error("Input table is empty.")}
       |            return
       |
       |        # Drop rows with missing x/y
       |        table.dropna(subset=['${xColumn}', '${yColumn}'], inplace=True)
       |        if table.empty:
       |            yield {"html-content": self.render_error("No rows after dropping nulls.")}
       |            return
       |
       |        fig = px.density_heatmap(
       |            table,
       |            x='${xColumn}',
       |            y='${yColumn}',
       |            nbinsx=${xBins},
       |            nbinsy=${yBins},
       |            ${normArg}
       |            text_auto=True
       |        )
       |
       |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
       |        yield {"html-content": html}
       |""".stripMargin
  }
}
