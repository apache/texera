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
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.{
  PythonTemplateBuilderStringContext,
  pyStringLiteral
}
import org.apache.texera.amber.pybuilder.PyStringTypes.{EncodableString, PythonLiteral}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.visualization.PlotlyStandaloneCode
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

import javax.validation.constraints.NotNull

// type constraint: x / y / z are plotted on numeric axes and interpolated via
// scipy.griddata, so they can only be numeric columns.
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "x": { "enum": ["integer", "long", "double"] },
    "y": { "enum": ["integer", "long", "double"] },
    "z": { "enum": ["integer", "long", "double"] }
  }
}
""")
class ContourPlotOpDesc extends PythonOperatorDescriptor with PlotlyStandaloneCode {

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
       |    def render_error(self, error_msg) -> str:
       |        return '''<h1>Contour plot is not available.</h1>
       |                  <p>Reason is: {} </p>
       |               '''.format(error_msg)
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        # A row missing any of the three has no point to contribute, and
       |        # griddata refuses a NaN coordinate outright.
       |        table = table.dropna(subset=[$x, $y, $z])
       |        if table.empty:
       |            yield {'html-content': self.render_error("Table should not have any empty/null values or fields.")}
       |            return
       |
       |        x = table[$x].values
       |        y = table[$y].values
       |        z = table[$z].values
       |
       |        # Cubic interpolation triangulates the points before it interpolates,
       |        # which needs them to span a plane. Points that all fall on one line
       |        # leave Qhull without a simplex to start from and it raises instead.
       |        points = np.unique(np.column_stack((x, y)), axis=0)
       |        if np.linalg.matrix_rank(points - points.mean(axis=0)) < 2:
       |            yield {'html-content': self.render_error("The x and y values all fall on one line, so there is no area to contour.")}
       |            return
       |
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

  override def producesDataFrame(): Boolean = false

  override def generateStandaloneCode(): String =
    // render_error's continuation line keeps the runtime path's indentation — the
    // HTML is triple-quoted, so those spaces reach the browser.
    s"""import numpy as np
       |from scipy.interpolate import griddata
       |
       |def render_error(error_msg):
       |    return '''<h1>Contour plot is not available.</h1>
       |                  <p>Reason is: {} </p>
       |               '''.format(error_msg)
       |
       |def _write_error(message):
       |    with open("output.html", "w", encoding="utf-8") as output:
       |        output.write(render_error(message))
       |
       |# A row missing any of the three has no point to contribute, and griddata
       |# refuses a NaN coordinate outright. Bound to a name of its own: the same
       |# frame can feed another branch of the plan, which must still see every row.
       |chart_df = in1df.dropna(subset=[${pyStringLiteral(x)}, ${pyStringLiteral(
      y
    )}, ${pyStringLiteral(
      z
    )}])
       |if chart_df.empty:
       |    _write_error("Table should not have any empty/null values or fields.")
       |else:
       |    x = chart_df[${pyStringLiteral(x)}].values
       |    y = chart_df[${pyStringLiteral(y)}].values
       |    z = chart_df[${pyStringLiteral(z)}].values
       |
       |    # Cubic interpolation triangulates the points before it interpolates, which
       |    # needs them to span a plane. Points that all fall on one line leave Qhull
       |    # without a simplex to start from and it raises instead.
       |    points = np.unique(np.column_stack((x, y)), axis=0)
       |    if np.linalg.matrix_rank(points - points.mean(axis=0)) < 2:
       |        _write_error("The x and y values all fall on one line, so there is no area to contour.")
       |        raise SystemExit(0)
       |
       |    grid_size = ${gridSize.getOrElse(ContourPlotOpDesc.DefaultGridSize)}
       |    connGaps = True if ${pyStringLiteral(connectGaps.toString)} == "true" else False
       |
       |    grid_x, grid_y = np.meshgrid(np.linspace(min(x), max(x), grid_size), np.linspace(min(y), max(y), grid_size))
       |    grid_z = griddata((x, y), z, (grid_x, grid_y), method='cubic')
       |
       |    fig = go.Figure(data=go.Contour(
       |        x=np.linspace(min(x), max(x), grid_size),
       |        y=np.linspace(min(y), max(y), grid_size),
       |        z=grid_z,
       |        connectgaps=connGaps,
       |        contours_coloring='${coloringMethod.getColoringMethod}',
       |        colorbar_title=${pyStringLiteral(z)}
       |    ))
       |    fig.update_layout(title='Contour Plot')
       |    fig.write_json("output.json")
       |    fig.write_html("output.html")
       |    print("Contour plot saved to output.json and output.html")""".stripMargin
}

object ContourPlotOpDesc {

  /** Matches the form's `defaultValue`, so an unset Grid Size plots at 10. */
  private val DefaultGridSize: Int = 10
}
