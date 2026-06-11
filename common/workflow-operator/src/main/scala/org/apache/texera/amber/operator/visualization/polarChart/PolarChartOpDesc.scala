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

package org.apache.texera.amber.operator.visualization.polarChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.OutputPort.OutputMode
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext

class PolarChartOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(value = "r", required = true)
  @JsonSchemaTitle("r")
  @JsonPropertyDescription("The column name for radial values (must be numeric)")
  @AutofillAttributeName
  var r: EncodableString = ""

  @JsonProperty(value = "theta", required = true)
  @JsonSchemaTitle("theta")
  @JsonPropertyDescription("The column name for angular values (must be numeric)")
  @AutofillAttributeName
  var theta: EncodableString = ""

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)

    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Polar Chart",
      "Displays data points in a polar scatter plot",
      OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  override def generatePythonCode(): String = {
    val finalCode =
      pyb"""from pytexera import *
       |import plotly.graph_objects as go
       |import plotly.io as pio
       |import numpy as np
       |
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |
       |        if table is None or table.empty:
       |            yield {'html-content': '<h3>No data available for Polar Chart</h3>'}
       |            return
       |
       |        if $r not in table.columns or $theta not in table.columns:
       |            yield {'html-content': '<h3>Selected columns not found in input table</h3>'}
       |            return
       |
       |        if not np.issubdtype(table[$r].dtype, np.number) or not np.issubdtype(table[$theta].dtype, np.number):
       |            yield {'html-content': '<h3>Selected columns must be numeric</h3>'}
       |            return
       |
       |        r_vals = table[$r].values
       |        theta_vals = table[$theta].values
       |
       |        fig = go.Figure(data=go.Scatterpolargl(
       |            r=r_vals,
       |            theta=theta_vals,
       |            mode='markers',
       |            marker=dict(
       |                size=10,
       |                opacity=0.7,
       |                line=dict(color='white')
       |            )
       |        ))
       |
       |        fig.update_layout(
       |            title='Polar Chart',
       |            showlegend=False
       |        )
       |
       |        html = pio.to_html(fig, include_plotlyjs='cdn', full_html=False)
       |        yield {'html-content': html}
       |"""
    finalCode.encode
  }
}
