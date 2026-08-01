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

package org.apache.texera.amber.operator.visualization.contourPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
import org.apache.texera.amber.pybuilder.PyStringTypes.{EncodableString, PythonLiteral}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

import javax.validation.constraints.NotNull

class ContourPlotOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(value = "x", required = true)
  @JsonSchemaTitle("x")
  @JsonPropertyDescription("The column name of X-axis")
  @AutofillAttributeName
  @NotNull(message = "x cannot be empty")
  var x: EncodableString = ""

  @JsonProperty(value = "y", required = true)
  @JsonSchemaTitle("y")
  @JsonPropertyDescription("The column name of Y-axis")
  @AutofillAttributeName
  @NotNull(message = "y cannot be empty")
  var y: EncodableString = ""

  @JsonProperty(value = "z", required = true)
  @JsonSchemaTitle("z")
  @JsonPropertyDescription("The column name of color bar")
  @AutofillAttributeName
  @NotNull(message = "z cannot be empty")
  var z: EncodableString = ""

  // Numeric: only used as int(). contentAs names the boxed class — Option erases
  // its element type, and a blank must not read as 0.
  @JsonProperty(required = false, defaultValue = "10")
  @JsonSchemaTitle("Grid Size")
  @JsonPropertyDescription("Grid resolution of the final image")
  @JsonDeserialize(contentAs = classOf[Integer])
  var gridSize: Option[Int] = None

  @JsonProperty(required = false, defaultValue = "true")
  @JsonSchemaTitle("Connect Gaps")
  @JsonPropertyDescription("Automatically fill in the missing parts")
  var connectGaps: Boolean = Boolean.box(false)

  @JsonProperty(
    value = "Coloring Method",
    required = false,
    defaultValue = "heatmap"
  )
  var coloringMethod: ContourPlotColoringFunction = _

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      "Contour Plot",
      "Displays terrain or gradient variations in a Contour Plot",
      OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP
    )

  override def generatePythonCode(): String = {
    // A number in the generated code, so it needs no int() around it.
    val gridSizeLiteral: PythonLiteral =
      gridSize.getOrElse(ContourPlotOpDesc.DefaultGridSize).toString
    pyb"""from pytexera import *
       |import numpy as np
       |import plotly.graph_objects as go
       |from scipy.interpolate import griddata
       |import plotly.io as pio
       |
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        x = table[$x].values
       |        y = table[$y].values
       |        z = table[$z].values
       |        grid_size = $gridSizeLiteral
       |        connGaps = True if '$connectGaps' == 'true' else False
       |
       |        grid_x, grid_y = np.meshgrid(np.linspace(min(x), max(x), grid_size), np.linspace(min(y), max(y), grid_size))
       |        grid_z = griddata((x, y), z, (grid_x, grid_y), method='cubic')
       |
       |        fig = go.Figure(data=go.Contour(
       |            x=np.linspace(min(x), max(x), grid_size),
       |            y=np.linspace(min(y), max(y), grid_size),
       |            z=grid_z,
       |            connectgaps=connGaps,
       |            contours_coloring ='${coloringMethod.getColoringMethod}',
       |            colorbar_title=$z
       |        ))
       |        fig.update_layout(title='Contour Plot')
       |        html = pio.to_html(fig, include_plotlyjs='cdn', full_html=False)
       |        yield {'html-content': html}
       |""".encode
  }
}

object ContourPlotOpDesc {

  /** Matches the form's `defaultValue`, so an unset Grid Size plots at 10. */
  private val DefaultGridSize: Int = 10
}
