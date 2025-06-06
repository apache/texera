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

package edu.uci.ics.amber.operator.visualization.dendrogram

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}

class DendrogramOpDesc extends PythonOperatorDescriptor {
  @JsonProperty(value = "xVal", required = true)
  @JsonSchemaTitle("Value X Column")
  @JsonPropertyDescription("The x values of points in dendrogram")
  @AutofillAttributeName
  var xVal: String = ""

  @JsonProperty(value = "yVal", required = true)
  @JsonSchemaTitle("Value Y Column")
  @JsonPropertyDescription("The y value of points in dendrogram")
  @AutofillAttributeName
  var yVal: String = ""

  @JsonProperty(value = "Labels", required = true)
  @JsonSchemaTitle("Labels")
  @JsonPropertyDescription("The label of points in dendrogram")
  @AutofillAttributeName
  var labels: String = ""

  @JsonProperty(defaultValue = "", required = false)
  @JsonSchemaTitle("Color Threshold")
  @JsonPropertyDescription("Value at which separation of clusters will be made")
  var threshold: String = ""

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Dendrogram",
      "Visualize data in a Dendrogram",
      OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  private def createDendrogram(): String = {
    assert(xVal.nonEmpty)
    assert(yVal.nonEmpty)
    assert(labels.nonEmpty)
    val strippedThreshold = threshold.trim
    val isThreshold =
      if (strippedThreshold.nonEmpty) s"color_threshold=$strippedThreshold"
      else "color_threshold=None"
    s"""
       |        x = np.array(table["$xVal"])
       |        y = np.array(table["$yVal"])
       |        data = np.column_stack((x, y))
       |        labels = table["$labels"].tolist()
       |
       |        fig = ff.create_dendrogram(data, labels=labels, $isThreshold)
       |        fig.update_layout(yaxis_title="Linkage Distance", margin=dict(l=0, r=0, b=0, t=0))
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
    val finalcode =
      s"""
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
         |        ${createDendrogram()}
         |        # convert fig to html content
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |
         |""".stripMargin
    finalcode
  }
}
