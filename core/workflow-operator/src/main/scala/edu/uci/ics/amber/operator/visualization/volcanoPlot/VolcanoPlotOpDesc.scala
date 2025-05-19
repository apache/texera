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

package edu.uci.ics.amber.operator.visualization.volcanoPlot

import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName

class VolcanoPlotOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Effect Size (log2 Fold Change)")
  @JsonPropertyDescription(
    "Select the column representing the effect size or magnitude " +
      "of change between two experimental groups. This value is typically a log2 fold change " +
      "and is used for the x-axis of the volcano plot."
  )
  @AutofillAttributeName var effectColumn: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("P-Value Column")
  @JsonPropertyDescription(
    "Select the column representing the p-value associated with the " +
      "statistical test for each feature. This value is transformed using -log10(p-value) and " +
      "plotted on the y-axis to indicate statistical significance."
  )
  @AutofillAttributeName var pvalueColumn: String = ""

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Volcano Plot",
      operatorDescription = "Displays statistical significance versus effect size",
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
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def generatePythonCode(): String = {
    s"""
       |from pytexera import *
       |import plotly.express as px
       |import plotly.io
       |import numpy as np
       |
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    def render_error(self, msg):
       |        return f"<h1>Volcano Plot failed</h1><p>{msg}</p>"
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        if table.empty:
       |            yield {"html-content": self.render_error("Input table is empty.")}
       |            return
       |
       |        if "$pvalueColumn" not in table.columns or "$effectColumn" not in table.columns:
       |            yield {"html-content": self.render_error("Missing required columns in table.")}
       |            return
       |
       |        # Filter out non-positive p-values to avoid math errors
       |        table = table[table["$pvalueColumn"] > 0]
       |        if table.empty:
       |            yield {"html-content": self.render_error("No rows with valid p-values.")}
       |            return
       |
       |        table["-log10(pvalue)"] = -np.log10(table["$pvalueColumn"])
       |
       |        fig = px.scatter(
       |            table,
       |            x="$effectColumn",
       |            y="-log10(pvalue)",
       |            hover_name=table.columns[0],
       |            color="$effectColumn",
       |            color_continuous_scale="RdBu",
       |            title="Volcano Plot"
       |        )
       |
       |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
       |        yield {"html-content": html}
       |""".stripMargin
  }

}
