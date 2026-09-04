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

package org.apache.texera.amber.operator.visualization.bulletChart

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
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.visualization.PlotlyStandaloneCode
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

import java.util
import java.util.{List => JList}
import javax.validation.constraints.NotNull
import scala.jdk.CollectionConverters._

/**
  * Visualization Operator to visualize results as a Bullet Chart
  */

// type constraint: the measured value is read as float(row[value]), so it can only
// be a numeric column.
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "value": { "enum": ["integer", "long", "double"] }
  }
}
""")
class BulletChartOpDesc extends PythonOperatorDescriptor with PlotlyStandaloneCode {

  @JsonProperty(value = "value", required = true)
  @JsonSchemaTitle("Value")
  @JsonPropertyDescription("The actual value to display on the bullet chart")
  @AutofillAttributeName
  @NotNull(message = "Value cannot be empty")
  var value: EncodableString = ""

  // Numeric: both are only used as float(). contentAs names the boxed class —
  // Option erases its element type, and a blank must not read as 0.
  @JsonProperty(value = "deltaReference", required = true)
  @JsonSchemaTitle("Delta Reference")
  @JsonPropertyDescription("The reference value for the delta indicator. e.g., 100")
  @NotNull(message = "Delta Reference cannot be empty")
  @JsonDeserialize(contentAs = classOf[java.lang.Double])
  var deltaReference: Option[Double] = None

  @JsonProperty(value = "thresholdValue", required = false)
  @JsonSchemaTitle("Threshold Value")
  @JsonPropertyDescription("The performance threshold value. e.g., 100")
  @JsonDeserialize(contentAs = classOf[java.lang.Double])
  var thresholdValue: Option[Double] = None

  @JsonProperty(value = "steps", required = false)
  @JsonSchemaTitle("Steps")
  @JsonPropertyDescription("Optional: Each step includes a start and end value e.g., 0, 100.")
  var steps: JList[BulletChartStepDefinition] = new util.ArrayList[BulletChartStepDefinition]()

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema().add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      "Bullet Chart",
      """Visualize data using a Bullet Chart that shows a primary quantitative bar and delta indicator.
        |Optional elements such as qualitative ranges (steps) and a performance threshold are displayed only when provided.""".stripMargin,
      OperatorGroupConstants.VISUALIZATION_FINANCIAL_GROUP
    )

  private def assertRequiredFields(): Unit = {
    assert(value.nonEmpty)
    assert(deltaReference.nonEmpty)
  }

  override def generatePythonCode(): String = {
    // The reference keeps the 0 the generated code used to fall back to; an unset
    // threshold stays absent as None.
    val deltaReferenceExpr: PythonLiteral = deltaReference.getOrElse(0.0).toString
    val thresholdExpr: PythonLiteral = thresholdValue.map(_.toString).getOrElse("None")

    // The steps whose bounds are both filled in, as a list literal of numbers.
    val stepsExpr: PythonLiteral =
      Option(steps)
        .map(_.asScala.toSeq)
        .getOrElse(Seq.empty)
        .flatMap(step => step.start.zip(step.end))
        .map { case (start, end) => s"""{"start": $start, "end": $end}""" }
        .mkString("[", ", ", "]")

    val finalCode =
      pyb"""
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
         |    # Validate user-provided step definitions
         |    def generate_valid_steps(self, steps_data):
         |        valid_steps = []
         |        self.step_errors = []
         |
         |        for index, step in enumerate(steps_data):
         |            s_val = step["start"]
         |            e_val = step["end"]
         |            if s_val < e_val:
         |                valid_steps.append({"start": s_val, "end": e_val})
         |            else:
         |                self.step_errors.append(f"Step {index + 1}: start ≥ end ({s_val} ≥ {e_val})")
         |        return valid_steps
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |            yield {'html-content': self.render_error("Input table is empty.")}
         |            return
         |
         |        try:
         |            value_col = $value
         |            delta_ref = $deltaReferenceExpr
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
         |            threshold_val = $thresholdExpr
         |
         |            valid_steps = self.generate_valid_steps($stepsExpr)
         |            step_colors = self.generate_gray_gradient(len(valid_steps))
         |            steps_list = []
         |            for index, step_data in enumerate(valid_steps):
         |                color = step_colors[index]
         |                steps_list.append({
         |                    "range": [step_data["start"], step_data["end"]],
         |                    "color": color
         |                })
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
         |"""
    finalCode.encode
  }

  override def producesDataFrame(): Boolean = false

  override def generateStandaloneCode(): String = {
    assertRequiredFields()

    // The column name is typed-in text, so it is emitted as a properly escaped
    // Python literal. The numeric settings carry no text and are emitted as numbers,
    // the same as the runtime path, so the two scripts agree without either of them
    // parsing anything.
    val valueLit = pyStringLiteral(value)
    val deltaReferenceExpr = deltaReference.getOrElse(0.0).toString
    val thresholdExpr = thresholdValue.map(_.toString).getOrElse("None")
    val stepsExpr =
      Option(steps)
        .map(_.asScala.toSeq)
        .getOrElse(Seq.empty)
        .flatMap(step => step.start.zip(step.end))
        .map { case (start, end) => s"""{"start": $start, "end": $end}""" }
        .mkString("[", ", ", "]")

    // render_error's continuation line keeps the runtime path's indentation — the
    // HTML is triple-quoted, so those spaces reach the browser.
    s"""def render_error(error_msg):
       |    return '''<h1>Bullet chart is not available.</h1>
       |                  <p>Reason: {} </p>'''.format(error_msg)
       |
       |def generate_gray_gradient(step_count):
       |    colors = []
       |    for i in range(step_count):
       |        lightness = 90 - (i * (60 / max(1, step_count - 1)))
       |        colors.append(f"hsl(0, 0%, {lightness}%)")
       |    return colors
       |
       |def generate_valid_steps(steps_data):
       |    valid_steps = []
       |    step_errors = []
       |    for index, step in enumerate(steps_data):
       |        s_val = step["start"]
       |        e_val = step["end"]
       |        if s_val < e_val:
       |            valid_steps.append({"start": s_val, "end": e_val})
       |        else:
       |            step_errors.append(f"Step {index + 1}: start >= end ({s_val} >= {e_val})")
       |    return valid_steps, step_errors
       |
       |if in1df.empty:
       |    with open("output.html", "w", encoding="utf-8") as output:
       |        output.write(render_error("Input table is empty."))
       |else:
       |    try:
       |        value_col = $valueLit
       |        delta_ref = $deltaReferenceExpr
       |        if value_col not in in1df.columns:
       |            with open("output.html", "w", encoding="utf-8") as output:
       |                output.write(render_error(f"Column '{value_col}' not found in input table."))
       |        else:
       |            table = in1df.dropna(subset=[value_col])
       |            if table.empty:
       |                with open("output.html", "w", encoding="utf-8") as output:
       |                    output.write(render_error("No valid data rows found after dropping nulls."))
       |            else:
       |                threshold_val = $thresholdExpr
       |                valid_steps, step_errors = generate_valid_steps($stepsExpr)
       |                step_colors = generate_gray_gradient(len(valid_steps))
       |                steps_list = []
       |                for index, step_data in enumerate(valid_steps):
       |                    steps_list.append({
       |                        "range": [step_data["start"], step_data["end"]],
       |                        "color": step_colors[index]
       |                    })
       |                html_chunks = []
       |                first_fig = None
       |                for _, row in table.head(10).iterrows():
       |                    try:
       |                        actual = float(row[value_col])
       |                        gauge_config = {'shape': 'bullet'}
       |                        if steps_list:
       |                            gauge_config['steps'] = steps_list
       |                        max_range_values = [actual, delta_ref]
       |                        if threshold_val is not None:
       |                            max_range_values.append(threshold_val)
       |                        for r in steps_list:
       |                            max_range_values.append(r["range"][1])
       |                        gauge_config['axis'] = {"range": [0, max(max_range_values) * 1.2]}
       |                        if threshold_val is not None:
       |                            gauge_config["threshold"] = {
       |                                "value": threshold_val,
       |                                "line": {"color": "red", "width": 2},
       |                                "thickness": 1
       |                            }
       |                        fig = go.Figure(go.Indicator(
       |                            mode="number+gauge+delta",
       |                            value=actual,
       |                            delta={"reference": delta_ref},
       |                            gauge=gauge_config,
       |                            domain={"x": [0.1, 1], "y": [0.1, 0.9]},
       |                            title={"text": value_col}
       |                        ))
       |                        fig.update_layout(margin=dict(l=80, r=20, b=40, t=40), height=150)
       |                        if first_fig is None:
       |                            first_fig = fig
       |                        html_chunk = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
       |                        if step_errors:
       |                            html_chunk += "<br><b>Step Errors:</b><ul>" + "".join([f"<li>{msg}</li>" for msg in step_errors]) + "</ul>"
       |                        html_chunks.append(html_chunk)
       |                    except Exception as e:
       |                        html_chunks.append(render_error(f"Error generating bullet chart: {str(e)}"))
       |                if first_fig is not None:
       |                    first_fig.write_json("output.json")
       |                with open("output.html", "w", encoding="utf-8") as output:
       |                    output.write("<div>" + "".join(html_chunks) + "</div>")
       |    except Exception as e:
       |        with open("output.html", "w", encoding="utf-8") as output:
       |            output.write(render_error(f"General error: {str(e)}"))""".stripMargin
  }
}
