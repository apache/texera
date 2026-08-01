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

package org.apache.texera.amber.operator.visualization.hierarchychart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.{PythonOperatorDescriptor, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder

import javax.validation.constraints.{NotEmpty, NotNull}

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
class HierarchyChartOpDesc extends PythonOperatorDescriptor with StandaloneCodeGenerator {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Chart Type")
  @JsonPropertyDescription("Treemap or Sunburst")
  @NotNull(message = "Chart Type cannot be empty")
  var hierarchyChartType: HierarchyChartType = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Hierarchy Path")
  @JsonPropertyDescription(
    "Hierarchy of attributes from a higher-level category to lower-level category"
  )
  @NotEmpty(message = "Hierarchy Path cannot be empty")
  var hierarchy: List[HierarchySection] = List()

  @JsonProperty(value = "value", required = true)
  @JsonSchemaTitle("Value Column")
  @JsonPropertyDescription("The value associated with the size of each sector in the chart")
  @AutofillAttributeName
  @NotNull(message = "Value Column cannot be empty")
  var value: EncodableString = ""

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      "Hierarchy Chart",
      "Visualize data in hierarchy",
      OperatorGroupConstants.VISUALIZATION_BASIC_GROUP
    )

  private def getHierarchyAttributesInPython: String =
    hierarchy.map(c => pyb"${c.attributeName}").mkString(",")

  def manipulateTable(): PythonTemplateBuilder = {
    assert(value.nonEmpty, "Value Column cannot be empty")
    val attributes = getHierarchyAttributesInPython
    pyb"""
       |        table[$value] = table[table[$value] > 0][$value] # remove non-positive numbers from the data
       |        table.dropna(subset = [$attributes], inplace = True) #remove missing values
       |"""
  }

  def createPlotlyFigure(): PythonTemplateBuilder = {
    assert(hierarchy.nonEmpty, "Hierarchy Path cannot be empty")
    val attributes = getHierarchyAttributesInPython
    pyb"""
       |        fig = px.${hierarchyChartType.getPlotlyExpressApiName}(table, path=[$attributes], values=$value,
       |                                                               color=$value, hover_data=[$attributes],
       |                                                               color_continuous_scale='RdBu')
       |"""
  }

  override def generatePythonCode(): String = {
    val finalCode =
      pyb"""
         |from pytexera import *
         |
         |import plotly.express as px
         |import plotly.graph_objects as go
         |import plotly.io
         |import numpy as np
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |    # Generate custom error message as html string
         |    def render_error(self, error_msg) -> str:
         |        return '''<h1>Hierarchy chart is not available.</h1>
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
         |           yield {'html-content': self.render_error("value column contains only non-positive numbers or nulls.")}
         |           return
         |        ${createPlotlyFigure()}
         |        # convert fig to html content
         |        fig.update_layout(margin=dict(l=0, r=0, b=0, t=0))
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |"""
    finalCode.encode
  }

  // Output is an HTML chart, not a tabular DataFrame.
  // The translator skips it in the leaf-DataFrame print block.
  override def producesDataFrame(): Boolean = false

  override def generateStandaloneCode(): String = {
    val attributes = hierarchy.map(section => s""""${section.attributeName}"""").mkString(", ")
    // The error page is written to output.html, the same file a plotted chart lands
    // in, so a reason for "no chart" is where the reader looks for the chart —
    // printing it to the terminal alone left output.html absent. render_error's
    // continuation line keeps the runtime path's indentation, since the HTML is
    // triple-quoted and those spaces reach the browser.
    s"""def render_error(error_msg):
       |    return '''<h1>Hierarchy chart is not available.</h1>
       |                  <p>Reason is: {} </p>
       |               '''.format(error_msg)
       |
       |def fail(error_msg):
       |    with open("output.html", "w", encoding="utf-8") as output:
       |        output.write(render_error(error_msg))
       |    print(f"Hierarchy chart error: {error_msg}")
       |
       |if in1df.empty:
       |    fail("input table is empty.")
       |else:
       |    in1df["$value"] = in1df[in1df["$value"] > 0]["$value"]
       |    in1df.dropna(subset=[$attributes], inplace=True)
       |    if in1df.empty:
       |        fail("value column contains only non-positive numbers or nulls.")
       |    else:
       |        fig = px.${hierarchyChartType.getPlotlyExpressApiName}(in1df, path=[$attributes], values="$value",
       |                         color="$value", hover_data=[$attributes],
       |                         color_continuous_scale='RdBu')
       |        fig.update_layout(margin=dict(l=0, r=0, b=0, t=0))
       |        fig.write_json("output.json")
       |        fig.write_html("output.html")
       |        print("Hierarchy chart saved to output.html")""".stripMargin
  }

}
