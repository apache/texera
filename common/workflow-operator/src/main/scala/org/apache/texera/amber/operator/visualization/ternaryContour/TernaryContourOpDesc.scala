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

package org.apache.texera.amber.operator.visualization.ternaryContour

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.OutputPort.OutputMode
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

/**
  * Visualization Operator for Ternary Plots.
  *
  * This operator uses three data fields to construct a ternary plot.
  * The points can optionally be color coded using a data field.
  */

class TernaryContourOpDesc extends PythonOperatorDescriptor {

  // Add annotations for the first variable
  @JsonProperty(value = "firstVariable", required = true)
  @JsonSchemaTitle("Variable 1")
  @JsonPropertyDescription("First variable data field")
  @AutofillAttributeName var firstVariable: String = ""

  // Add annotations for the second variable
  @JsonProperty(value = "secondVariable", required = true)
  @JsonSchemaTitle("Variable 2")
  @JsonPropertyDescription("Second variable data field")
  @AutofillAttributeName var secondVariable: String = ""

  // Add annotations for the third variable
  @JsonProperty(value = "thirdVariable", required = true)
  @JsonSchemaTitle("Variable 3")
  @JsonPropertyDescription("Third variable data field")
  @AutofillAttributeName var thirdVariable: String = ""

  // Add annotations for the fourth variable
  @JsonProperty(value = "fourthVariable", required = true)
  @JsonSchemaTitle("Variable 4")
  @JsonPropertyDescription("Fourth variable data field")
  @AutofillAttributeName var fourthVariable: String = ""

  // OperatorInfo instance describing ternary plot
  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Ternary Contour",
      operatorDescription = "A ternary contour plot shows how a measured value changes across all mixtures of three components that always sum to a constant (usually 100%).",
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

  /** Returns a Python string that drops any tuples with missing values */
  def manipulateTable(): String = {
    // Check for any empty data field names
    assert(firstVariable.nonEmpty && secondVariable.nonEmpty && thirdVariable.nonEmpty)
    s"""
       |        # Remove any tuples that contain missing values
       |        table.dropna(subset=['$firstVariable', '$secondVariable', '$thirdVariable', '$fourthVariable'], inplace = True)
       |
       |        #Remove rows where any of the first three variables are negative
       |        table = table[(table[['$firstVariable', '$secondVariable', '$thirdVariable']] >= 0).all(axis=1)]
       |
       |        #Remove zero-sum rows
       |        s = table['$firstVariable'] + table['$secondVariable'] + table['$thirdVariable']
       |        table = table[s > 0]
       |""".stripMargin
  }

  /** Returns a Python string that creates the ternary contour plot figure */
  def createPlotlyFigure(): String = {
    s"""
       |        A = table['$firstVariable'].to_numpy()
       |        B = table['$secondVariable'].to_numpy()
       |        C = table['$thirdVariable'].to_numpy()
       |        Z = table['$fourthVariable'].to_numpy()
       |        fig = ff.create_ternary_contour(np.array([A,B,C]), Z, pole_labels=['$firstVariable', '$secondVariable', '$thirdVariable'], interp_mode='cartesian')
       |""".stripMargin
  }

  /** Returns a Python string that yields the html content of the ternary contour plot */
  override def generatePythonCode(): String = {
    val finalCode =
      s"""
         |from pytexera import *
         |
         |import plotly.express as px
         |import plotly.io
         |import plotly.figure_factory as ff
         |import numpy as np
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |    # Generate custom error message as html string
         |    def render_error(self, error_msg):
         |        return '''<h1>TernaryContour is not available.</h1>
         |                  <p>Reasons are: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |            yield {'html-content': self.render_error("Input table is empty.")}
         |            return
         |        ${manipulateTable()}
         |        if table.empty:
         |            yield {'html-content': self.render_error("No valid rows left (every row has at least 1 missing value).")}
         |            return
         |        ${createPlotlyFigure()}
         |        # Convert fig to html content
         |        html = plotly.io.to_html(fig, include_plotlyjs = 'cdn', auto_play = False)
         |        yield {'html-content':html}
         |""".stripMargin
    finalCode
  }

}
