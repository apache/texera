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

package org.apache.texera.amber.operator.visualization.windRoseChart

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
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder
import javax.validation.constraints.NotNull

class WindRoseChartOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(value = "rColumn", required = true)
  @JsonSchemaTitle("Radial Values (r)")
  @JsonPropertyDescription("Numeric values representing magnitude (e.g., frequency)")
  @AutofillAttributeName
  @NotNull(message = "Radial Values (r) column must be selected.")
  var rColumn: EncodableString = _

  @JsonProperty(value = "thetaColumn", required = true)
  @JsonSchemaTitle("Angular Values (θ)")
  @JsonPropertyDescription("Direction or angle categories (e.g., N, NE, E)")
  @AutofillAttributeName
  @NotNull(message = "Angular Values (θ) column must be selected.")
  var thetaColumn: EncodableString = _

  @JsonProperty(value = "colorColumn", required = false)
  @JsonSchemaTitle("Color Group")
  @JsonPropertyDescription("Optional grouping column (e.g., wind strength)")
  @AutofillAttributeName
  var colorColumn: EncodableString = _

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Wind Rose Chart",
      operatorDescription = "Displays wind distribution using a polar bar chart",
      operatorGroupName = OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP,
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

  def createPlotlyFigure(): PythonTemplateBuilder = {
    val colorArg =
      if (colorColumn != null && colorColumn.nonEmpty)
        pyb"""
             |        color=$colorColumn,
             |"""
      else
        pyb""

    pyb"""
         |        fig = px.bar_polar(
         |            table,
         |            r=$rColumn,
         |            theta=$thetaColumn,
         |$colorArg
         |            color_discrete_sequence=px.colors.sequential.Plasma_r
         |        )
         |"""
  }

  override def generatePythonCode(): String = {
    val finalCode =
      pyb"""
         |from pytexera import *
         |
         |import plotly.graph_objects as go
         |import plotly.io
         |import plotly.express as px
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |    # Generate custom error message as html string
         |    def render_error(self, error_msg) -> str:
         |        return '''<h1>Wind Rose chart is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |            yield {'html-content': self.render_error("input table is empty.")}
         |            return
         |        if table[$rColumn].dtype.kind not in ["i", "u", "f"]:
         |            yield {'html-content': self.render_error(
         |                "Radial column must be numeric (int, float, or double)."
         |            )}
         |            return
         |        ${createPlotlyFigure()}
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |"""
    finalCode.encode
  }

}
