/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.amber.operator.visualization.gaugeChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}

class GaugeChartOpDesc extends PythonOperatorDescriptor {
  @JsonProperty(value = "value", required = true)
  @JsonSchemaTitle("Gauge Value")
  @JsonPropertyDescription("The primary value displayed on the gauge chart")
  @AutofillAttributeName
  var value: String = ""

  @JsonProperty(value = "delta", required = false)
  @JsonSchemaTitle("Delta")
  @JsonPropertyDescription("The baseline value used to calculate the delta from the gauge value")
  var delta: String = ""

  @JsonProperty(value = "threshold", required = false)
  @JsonSchemaTitle("Threshold Value")
  @JsonPropertyDescription("Defines a boundary or target value shown on the gauge chart")
  var threshold: String = ""

  @JsonProperty(value = "steps", required = false)
  @JsonSchemaTitle("Steps")
  @JsonPropertyDescription("List of step ranges for the gauge")
  var steps: List[GaugeChartSteps] = List()

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema().add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Gauge Chart",
      "Visualize a single value with a radial gauge chart, showing progress towards a goal with optional steps, threshold, and delta.",
      OperatorGroupConstants.VISUALIZATION_FINANCIAL_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  private def serializeSteps(steps: List[GaugeChartSteps]): String = {
    mapper.writeValueAsString(steps)
  }

  override def generatePythonCode(): String = {
    val stepsStr: String = serializeSteps(steps)

    s"""
         |from pytexera import *
         |import plotly.graph_objects as go
         |import plotly.io as pio
         |import json
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |    def render_error(self, error_msg) -> str:
         |        return '''<h1>Gauge chart is not available.</h1>
         |                  <p>Reason: {} </p>'''.format(error_msg)
         |
         |    def generate_gray_gradient(self, step_count):
         |        colors = []
         |        for i in range(step_count):
         |            lightness = 90 - (i * (60 / max(1, step_count - 1)))
         |            colors.append(f"hsl(0, 0%, {lightness}%)")
         |        return colors
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |            yield {'html-content': self.render_error("Input table is empty.")}
         |            return
         |
         |        try:
         |            gauge_value = "$value"
         |            try:
         |                delta_ref = float("$delta") if "$delta".strip() else None
         |            except ValueError:
         |                delta_ref = None
         |            try:
         |                threshold_val = float("$threshold") if "$threshold".strip() else None
         |            except ValueError:
         |                threshold_val = None
         |
         |            table = table.dropna(subset=[gauge_value])
         |            if table.empty:
         |                yield {'html-content': self.render_error("No non-null rows found for the value column.")}
         |                return
         |
         |            try:
         |                valid_steps = json.loads('''$stepsStr''')
         |                step_colors = self.generate_gray_gradient(len(valid_steps))
         |                steps_list = []
         |                for index, step_data in enumerate(valid_steps):
         |                    color = step_colors[index]
         |                    steps_list.append({
         |                        "range": [float(step_data["start"]), float(step_data["end"])],
         |                        "color": color
         |                    })
         |            except Exception:
         |                steps_list = []
         |
         |            html_chunks = []
         |            for _, row in table.iterrows():
         |                try:
         |                    actual = float(row[gauge_value])
         |                    max_val = actual
         |                    if delta_ref is not None:
         |                        max_val = max(max_val, delta_ref)
         |                    if threshold_val is not None:
         |                        max_val = max(max_val, threshold_val)
         |                    if steps_list:
         |                        for r in steps_list:
         |                            max_val = max(max_val, r["range"][1])
         |
         |                    gauge_config = {'axis': {'range': [None, max_val * 1.2]}}
         |                    if steps_list:
         |                        gauge_config['steps'] = steps_list
         |                    if threshold_val is not None:
         |                        gauge_config['threshold'] = {
         |                            "value": threshold_val,
         |                            "line": {"color": "red", "width": 3},
         |                            "thickness": 0.75
         |                        }
         |
         |                    mode_parts = ["number", "gauge"]
         |                    if delta_ref is not None:
         |                        mode_parts.append("delta")
         |                    mode = "+".join(mode_parts)
         |                    delta_config = {"reference": delta_ref} if delta_ref is not None else None
         |
         |                    fig = go.Figure(go.Indicator(
         |                        mode=mode,
         |                        value=actual,
         |                        delta=delta_config,
         |                        gauge=gauge_config,
         |                        domain={"x": [0, 1], "y": [0, 1]},
         |                        title={"text": gauge_value}
         |                    ))
         |
         |                    fig.update_layout(margin=dict(l=20, r=20, b=40, t=60), height=250)
         |                    html_chunk = pio.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |
         |                    if hasattr(self, 'step_errors') and self.step_errors:
         |                        error_notes = "<br><b>Step Errors:</b><ul>" + "".join([f"<li>{msg}</li>" for msg in self.step_errors]) + "</ul>"
         |                        html_chunk += error_notes
         |
         |                    html_chunks.append(html_chunk)
         |                except Exception as e:
         |                    html_chunks.append(self.render_error(f"Error generating chart: {str(e)}"))
         |
         |            yield {"html-content": "<div>" + "".join(html_chunks) + "</div>"}
         |
         |        except Exception as e:
         |            yield {'html-content': self.render_error(f"General error: {str(e)}")}
         |""".stripMargin
  }
}
