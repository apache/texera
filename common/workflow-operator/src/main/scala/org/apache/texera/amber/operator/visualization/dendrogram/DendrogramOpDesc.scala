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

package org.apache.texera.amber.operator.visualization.dendrogram

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
import org.apache.texera.amber.operator.{PythonOperatorDescriptor, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder

import javax.validation.constraints.NotNull

// type constraint: xVal / yVal are stacked into a numeric point matrix for
// hierarchical clustering, so they can only be numeric columns.
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "xVal": { "enum": ["integer", "long", "double"] },
    "yVal": { "enum": ["integer", "long", "double"] }
  }
}
""")
class DendrogramOpDesc extends PythonOperatorDescriptor with StandaloneCodeGenerator {
  @JsonProperty(value = "xVal", required = true)
  @JsonSchemaTitle("Value X Column")
  @JsonPropertyDescription("The x values of points in dendrogram")
  @AutofillAttributeName
  @NotNull(message = "Value X Column cannot be empty")
  var xVal: EncodableString = ""

  @JsonProperty(value = "yVal", required = true)
  @JsonSchemaTitle("Value Y Column")
  @JsonPropertyDescription("The y value of points in dendrogram")
  @AutofillAttributeName
  @NotNull(message = "Value Y Column cannot be empty")
  var yVal: EncodableString = ""

  @JsonProperty(value = "Labels", required = true)
  @JsonSchemaTitle("Labels")
  @JsonPropertyDescription("The label of points in dendrogram")
  @AutofillAttributeName
  @NotNull(message = "Labels cannot be empty")
  var labels: EncodableString = ""

  // Numeric: scipy compares it against the linkage distances. contentAs names the
  // boxed class — Option erases its element type, and a blank must not read as 0.
  @JsonProperty(defaultValue = "", required = false)
  @JsonSchemaTitle("Color Threshold")
  @JsonPropertyDescription("Value at which separation of clusters will be made")
  @JsonDeserialize(contentAs = classOf[java.lang.Double])
  var threshold: Option[Double] = None

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      "Dendrogram",
      "Visualize data in a Dendrogram",
      OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP
    )

  /** An unset threshold reaches the generated code as Python's `None`, which is
    * scipy's own 0.7 * max distance — what a blank field already meant.
    */
  private def thresholdExpr: PythonLiteral = threshold.map(_.toString).getOrElse("None")

  private def createDendrogram(): PythonTemplateBuilder = {
    assert(xVal.nonEmpty, "Value X Column cannot be empty")
    assert(yVal.nonEmpty, "Value Y Column cannot be empty")
    assert(labels.nonEmpty, "Labels cannot be empty")
    pyb"""
       |            x = np.array(table[$xVal])
       |            y = np.array(table[$yVal])
       |            data = np.column_stack((x, y))
       |            labels = table[$labels].tolist()
       |
       |            fig = ff.create_dendrogram(data, labels=labels, color_threshold=$thresholdExpr)
       |            fig.update_layout(yaxis_title="Linkage Distance", margin=dict(l=0, r=0, b=0, t=0))
       |"""
  }

  override def generatePythonCode(): String = {
    val finalcode =
      pyb"""
         |from pytexera import *
         |
         |import plotly.express as px
         |import plotly.figure_factory as ff
         |import plotly.io
         |import pandas as pd
         |import numpy as np
         |
         |class ProcessTableOperator(UDFTableOperator):
         |    def render_error(self, error_msg):
         |        return '''<h1>Dendrogram is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |           yield {'html-content': self.render_error("input table is empty.")}
         |           return
         |        try:
         |            ${createDendrogram()}
         |            # convert fig to html content
         |            html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |            yield {'html-content': html}
         |        except Exception as e:
         |            yield {'html-content': self.render_error(f"General error: {str(e)}")}
         |
         |"""
    finalcode.encode
  }

  override def producesDataFrame(): Boolean = false

  override def generateStandaloneCode(): String = {

    // render_error's continuation line keeps the runtime path's indentation — the
    // HTML is triple-quoted, so those spaces reach the browser.
    s"""import numpy as np
       |import plotly.figure_factory as ff
       |
       |def render_error(error_msg):
       |    return '''<h1>Dendrogram is not available.</h1>
       |                  <p>Reason is: {} </p>
       |               '''.format(error_msg)
       |
       |if in1df.empty:
       |    with open("output.html", "w", encoding="utf-8") as output:
       |        output.write(render_error("input table is empty."))
       |else:
       |    try:
       |        x = np.array(in1df[${pyStringLiteral(xVal)}])
       |        y = np.array(in1df[${pyStringLiteral(yVal)}])
       |        data = np.column_stack((x, y))
       |        labels = in1df[${pyStringLiteral(labels)}].tolist()
       |        fig = ff.create_dendrogram(data, labels=labels, color_threshold=$thresholdExpr)
       |        fig.update_layout(yaxis_title="Linkage Distance", margin=dict(l=0, r=0, b=0, t=0))
       |        fig.write_json("output.json")
       |        fig.write_html("output.html")
       |        print("Dendrogram saved to output.html")
       |    except Exception as e:
       |        with open("output.html", "w", encoding="utf-8") as output:
       |            output.write(render_error(f"General error: {str(e)}"))""".stripMargin
  }

}
