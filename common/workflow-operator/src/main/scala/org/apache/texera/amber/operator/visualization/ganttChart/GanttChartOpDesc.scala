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

package org.apache.texera.amber.operator.visualization.ganttChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
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

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "start": {
      "enum": ["timestamp"]
    },
    "finish": {
      "enum": ["timestamp"]
    }
  }
}
""")
class GanttChartOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(value = "start", required = true)
  @JsonSchemaTitle("Start Datetime Column")
  @JsonPropertyDescription("the start timestamp of the task")
  @AutofillAttributeName
  @NotNull(message = "Start Datetime Column cannot be empty")
  var start: EncodableString = ""

  @JsonProperty(value = "finish", required = true)
  @JsonSchemaTitle("Finish Datetime Column")
  @JsonPropertyDescription("the end timestamp of the task")
  @AutofillAttributeName
  @NotNull(message = "Finish Datetime Column cannot be empty")
  var finish: EncodableString = ""

  @JsonProperty(value = "task", required = true)
  @JsonSchemaTitle("Task Column")
  @JsonPropertyDescription("the name of the task")
  @AutofillAttributeName
  @NotNull(message = "Task Column cannot be empty")
  var task: EncodableString = ""

  @JsonProperty(value = "color", required = false)
  @JsonSchemaTitle("Color Column")
  @JsonPropertyDescription("column to color tasks")
  @AutofillAttributeName
  var color: EncodableString = ""

  @JsonProperty(required = false)
  @JsonSchemaTitle("Pattern")
  @JsonPropertyDescription("Add texture to the chart based on an attribute")
  @AutofillAttributeName
  var pattern: EncodableString = ""

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Gantt Chart",
      "A Gantt chart is a type of bar chart that illustrates a project schedule. The chart lists the tasks to be performed on the vertical axis, and time intervals on the horizontal axis. The width of the horizontal bars in the graph shows the duration of each activity.",
      OperatorGroupConstants.VISUALIZATION_BASIC_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  def manipulateTable(): PythonTemplateBuilder = {
    val optionalFilterTable = if (color.nonEmpty) pyb"&(table[$color].notnull())" else ""
    pyb"""
       |        table = table[(table[$start].notnull())&(table[$finish].notnull())&(table[$finish].notnull())$optionalFilterTable].copy()
       |"""
  }

  def createPlotlyFigure(): PythonTemplateBuilder = {
    val colorSetting = if (color.nonEmpty) pyb", color=$color" else pyb""
    val patternParam = if (pattern.nonEmpty) pyb", pattern_shape=$pattern" else pyb""

    pyb"""
       |        fig = px.timeline(table, x_start=$start, x_end=$finish, y=$task $colorSetting $patternParam)
       |        fig.update_yaxes(autorange='reversed')
       |        fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
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
         |    def render_error(self, error_msg):
         |        return '''<h1>Gantt Chart is not available.</h1>
         |                  <p>Reason: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |           yield {'html-content': self.render_error("Input table is empty.")}
         |           return
         |        ${manipulateTable()}
         |        if table.empty:
         |           yield {'html-content': self.render_error("One or more of your input columns have all missing values")}
         |           return
         |        ${createPlotlyFigure()}
         |        # convert fig to html content
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |"""
    finalCode.encode
  }
}
