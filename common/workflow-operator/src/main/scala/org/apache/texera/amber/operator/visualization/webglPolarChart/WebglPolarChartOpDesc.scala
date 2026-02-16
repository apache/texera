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
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY KIND, either express or implied.
 */

package org.apache.texera.amber.operator.visualization.webglPolarChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.OutputPort.OutputMode
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

class WebglPolarChartOpDesc extends PythonOperatorDescriptor {



  @JsonProperty(value = "r", required = true)
  @JsonSchemaTitle("r")
  @JsonPropertyDescription("The column name for radial values")
  @AutofillAttributeName
  var r: String = ""

  @JsonProperty(value = "theta", required = true)
  @JsonSchemaTitle("theta")
  @JsonPropertyDescription("The column name for angular values (degrees)")
  @AutofillAttributeName
  var theta: String = ""


  override def getOutputSchemas(
                                 inputSchemas: Map[PortIdentity, Schema]
                               ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)

    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }



  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "WebGL Polar Chart",
      "Displays data points in a WebGL-accelerated polar scatter plot",
      OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )



  override def generatePythonCode(): String = {
    s"""from pytexera import *
       |import plotly.graph_objects as go
       |import plotly.io as pio
       |
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |
       |        if table is None or table.empty:
       |            yield {'html-content': '<h3>No data available for WebGL Polar Chart</h3>'}
       |            return
       |
       |        r_vals = table['$r'].values
       |        theta_vals = table['$theta'].values
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
       |            title='WebGL Polar Chart',
       |            showlegend=False,
       |            polar=dict(
       |                bgcolor='rgb(223, 223, 223)',
       |                angularaxis=dict(
       |                    linewidth=3,
       |                    showline=True,
       |                    linecolor='black'
       |                ),
       |                radialaxis=dict(
       |                    showline=True,
       |                    linewidth=2,
       |                    gridcolor='white',
       |                    gridwidth=2
       |                )
       |            ),
       |            paper_bgcolor='rgb(223, 223, 223)'
       |        )
       |
       |        html = pio.to_html(fig, include_plotlyjs='cdn', full_html=False)
       |        yield {'html-content': html}
       |""".stripMargin
  }
}
