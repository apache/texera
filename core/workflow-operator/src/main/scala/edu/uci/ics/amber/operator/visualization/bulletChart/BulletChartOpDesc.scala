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

package edu.uci.ics.amber.operator.visualization.bulletChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}

import java.util.{ArrayList, List => JList}
import scala.jdk.CollectionConverters._

/**
  * Visualization Operator to visualize results as a Bullet Chart
  */

class BulletChartOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(value = "value", required = true)
  @JsonSchemaTitle("Value")
  @JsonPropertyDescription("The actual value to display on the bullet chart")
  @AutofillAttributeName var value: String = ""

  @JsonProperty(value = "deltaReference", required = true)
  @JsonSchemaTitle("Delta Reference")
  @JsonPropertyDescription("The reference value for the delta indicator. e.g., 100")
  var deltaReference: String = ""

  @JsonProperty(value = "thresholdValue", required = false)
  @JsonSchemaTitle("Threshold Value")
  @JsonPropertyDescription("The performance threshold value. e.g., 100")
  var thresholdValue: String = ""

  @JsonProperty(value = "steps", required = false)
  @JsonSchemaTitle("Steps")
  @JsonPropertyDescription("Optional: Each step includes a start and end value e.g., 0, 100.")
  var steps: JList[BulletChartStepDefinition] = new ArrayList[BulletChartStepDefinition]()

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema().add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Bullet Chart",
      """Visualize data using a Bullet Chart that shows a primary quantitative bar and delta indicator.
        |Optional elements such as qualitative ranges (steps) and a performance threshold are displayed only when provided.""".stripMargin,
      OperatorGroupConstants.VISUALIZATION_FINANCIAL_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  override def generatePythonCode(): String = {
    // Convert the Scala list of steps into a list of dictionaries
    val stepsStr = if (steps != null && !steps.isEmpty) {
      val stepsSeq =
        steps.asScala.map(step => s"""{"start": "${step.start}", "end": "${step.end}"}""")
      "[" + stepsSeq.mkString(", ") + "]"
    } else {
      "[]"
    }

    val finalCode =
      s"""
         |from pytexera import *
         |import plotly.graph_objects as go
         |import plotly.io as pio
         |import json
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |    # Render an error message in HTML format
         |    def render_error(self, error_msg) -> str:
         |        return '''<h1>Bullet chart is not available.</h1>
         |                  <p>Reason: {} </p>'''.format(error_msg)
         |
         |    # Generate a list of grayscale HSL colors with decreasing brightness
         |    def generate_gray_gradient(self, step_count):
         |        colors = []
         |        for i in range(step_count):
         |            lightness = 90 - (i * (60 / max(1, step_count - 1)))
         |            colors.append(f"hsl(0, 0%, {lightness}%)")
         |        return colors
         |
         |    # Validate and convert user-provided step definitions
         |    def generate_valid_steps(self, steps_data):
         |        valid_steps = []
         |        self.step_errors = []
         |
         |        for index, step in enumerate(steps_data):
         |            start = step.get('start', '')
         |            end = step.get('end', '')
         |            if start and end:
         |                try:
         |                    s_val = float(start)
         |                    e_val = float(end)
         |                    if s_val < e_val:
         |                        valid_steps.append({"start": s_val, "end": e_val})
         |                    else:
         |                        self.step_errors.append(f"Step {index + 1}: start ≥ end ({s_val} ≥ {e_val})")
         |                except Exception as e:
         |                    self.step_errors.append(f"Step {index + 1}: Invalid step values: start='{start}', end='{end}'")
         |        return valid_steps
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |            yield {'html-content': self.render_error("Input table is empty.")}
         |            return
         |
         |        try:
         |            value_col = "$value"
         |            delta_ref = float("$deltaReference") if "$deltaReference".strip() else 0
         |
         |            if value_col not in table.columns:
         |                yield {'html-content': self.render_error(f"Column '{value_col}' not found in input table.")}
         |                return
         |
         |            table = table.dropna(subset=[value_col])
         |            if table.empty:
         |                yield {'html-content': self.render_error("No valid data rows found after dropping nulls.")}
         |                return
         |
         |            try:
         |                threshold_val = float("$thresholdValue") if "$thresholdValue".strip() else None
         |            except ValueError:
         |                threshold_val = None
         |
         |            # Parse and validate steps input
         |            try:
         |                steps_data = $stepsStr
         |                valid_steps = self.generate_valid_steps(steps_data)
         |                step_colors = self.generate_gray_gradient(len(valid_steps))
         |                steps_list = []
         |                for index, step_data in enumerate(valid_steps):
         |                    color = step_colors[index]
         |                    steps_list.append({
         |                        "range": [step_data["start"], step_data["end"]],
         |                        "color": color
         |                    })
         |            except Exception:
         |                steps_list = []
         |
         |            # Iterate through up to 10 rows of the input table
         |            count = 0
         |            html_chunks = []
         |            for _, row in table.iterrows():
         |                if count >= 10:  # Limit to 10 charts
         |                    break
         |                try:
         |                    actual = float(row[value_col])
         |                    ref = delta_ref
         |
         |                    # Construct gauge configuration
         |                    gauge_config = {'shape': 'bullet'}
         |                    if steps_list:
         |                        gauge_config['steps'] = steps_list
         |
         |                    max_range_values = [actual, ref]
         |                    if threshold_val is not None:
         |                        max_range_values.append(threshold_val)
         |
         |                    if steps_list:
         |                        for r in steps_list:
         |                            max_range_values.append(r["range"][1])
         |
         |                    gauge_config['axis'] = {"range": [0, max(max_range_values) * 1.2]}
         |
         |                    if threshold_val is not None:
         |                        gauge_config["threshold"] = {
         |                            "value": threshold_val,
         |                            "line": {"color": "red", "width": 2},
         |                            "thickness": 1
         |                        }
         |
         |                    fig = go.Figure(go.Indicator(
         |                        mode="number+gauge+delta",
         |                        value=actual,
         |                        delta={"reference": ref},
         |                        gauge=gauge_config,
         |                        domain={"x": [0.1, 1], "y": [0.1, 0.9]},
         |                        title={"text": value_col}
         |                    ))
         |
         |                    fig.update_layout(margin=dict(l=80, r=20, b=40, t=40), height=150)
         |                    html_chunk = pio.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |
         |                    # Add step error
         |                    if hasattr(self, 'step_errors') and self.step_errors:
         |                        error_notes = "<br><b>Step Errors:</b><ul>" + "".join([f"<li>{msg}</li>" for msg in self.step_errors]) + "</ul>"
         |                        html_chunk += error_notes
         |
         |                    html_chunks.append(html_chunk)
         |                    count += 1
         |
         |                except Exception as e:
         |                    html_chunks.append(self.render_error(f"Error generating bullet chart: {str(e)}"))
         |
         |            # Combine HTML chunks into final output
         |            final_html = "<div>" + "".join(html_chunks) + "</div>"
         |            yield {"html-content": final_html}
         |        except Exception as e:
         |            yield {'html-content': self.render_error(f"General error: {str(e)}")}
         |""".stripMargin
    finalCode
  }
}
