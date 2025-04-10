package edu.uci.ics.amber.operator.visualization.bulletChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}

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
  @JsonPropertyDescription("The reference value for the delta indicator. e.g., 200")
  var deltaReference: String = ""

  // Threshold
  @JsonProperty(value = "thresholdValue", required = false)
  @JsonSchemaTitle("Threshold Value")
  @JsonPropertyDescription("The performance threshold value. e.g., 80")
  var thresholdValue: String = ""

  // Step 1 (Optional)
  @JsonProperty(value = "step1Start", required = false)
  @JsonSchemaTitle("Step 1 Start")
  @JsonPropertyDescription("Start value for the first qualitative step range. e.g., 0")
  var step1Start: String = ""

  @JsonProperty(value = "step1End", required = false)
  @JsonSchemaTitle("Step 1 End")
  @JsonPropertyDescription("End value for the first qualitative step range. e.g., 100")
  var step1End: String = ""

  // Step 2 (Optional)
  @JsonProperty(value = "step2Start", required = false)
  @JsonSchemaTitle("Step 2 Start")
  @JsonPropertyDescription("Start value for the second qualitative step range. e.g., 100")
  var step2Start: String = ""

  @JsonProperty(value = "step2End", required = false)
  @JsonSchemaTitle("Step 2 End")
  @JsonPropertyDescription("End value for the second qualitative step range. e.g., 200")
  var step2End: String = ""

  // Step 3 (Optional)
  @JsonProperty(value = "step3Start", required = false)
  @JsonSchemaTitle("Step 3 Start")
  @JsonPropertyDescription("Start value for the third qualitative step range. e.g., 200")
  var step3Start: String = ""

  @JsonProperty(value = "step3End", required = false)
  @JsonSchemaTitle("Step 3 End")
  @JsonPropertyDescription("End value for the third qualitative step range. e.g., 300")
  var step3End: String = ""

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema().add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Bullet Chart",
      "Visualize data using a Bullet Chart that combines qualitative ranges (steps), a quantitative bar, and a performance threshold into a compact view.",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  override def generatePythonCode(): String = {
    val finalCode =
      s"""
         |from pytexera import *
         |import plotly.graph_objects as go
         |import plotly.io as pio
         |
         |class ProcessTableOperator(UDFTableOperator):
         |    def render_error(self, error_msg) -> str:
         |        return '''<h1>Bullet chart is not available.</h1>
         |                  <p>Reason: {} </p>'''.format(error_msg)
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
         |            steps_list = []
         |            # Step 1
         |            if "$step1Start".strip() and "$step1End".strip():
         |                try:
         |                    step1_start = float("$step1Start")
         |                    step1_end = float("$step1End")
         |                    if step1_start < step1_end:
         |                        steps_list.append({"range": [step1_start, step1_end], "color": "lightgray"})
         |                except ValueError:
         |                  pass
         |
         |            # Step 2
         |            if "$step2Start".strip() and "$step2End".strip():
         |                try:
         |                    step2_start = float("$step2Start")
         |                    step2_end = float("$step2End")
         |                    if step2_start < step2_end:
         |                        steps_list.append({"range": [step2_start, step2_end], "color": "gray"})
         |                except ValueError:
         |                    pass
         |
         |            # Step 3
         |            if "$step3Start".strip() and "$step3End".strip():
         |                try:
         |                    step3_start = float("$step3Start")
         |                    step3_end = float("$step3End")
         |                    if step3_start < step3_end:
         |                        steps_list.append({"range": [step3_start, step3_end], "color": "darkgray"})
         |                except ValueError:
         |                    pass
         |
         |            count = 0
         |            html_chunks = []
         |            for _, row in table.iterrows():
         |                if count >= 10: # Limit to 10 charts
         |                    break
         |                try:
         |                    actual = float(row[value_col])
         |                    ref = delta_ref
         |
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
         |                    max_range = max(max_range_values) * 1.2
         |                    gauge_config['axis'] = {"range": [0, max_range]}
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
         |                    html_chunks.append(html_chunk)
         |                    count += 1
         |
         |                except Exception as e:
         |                    html_chunks.append(self.render_error(f"Error generating bullet chart: {str(e)}"))
         |
         |            final_html = "<div>" + "".join(html_chunks) + "</div>"
         |            yield {"html-content": final_html}
         |        except Exception as e:
         |            yield {'html-content': self.render_error(f"General error: {str(e)}")}
         |""".stripMargin
    finalCode
  }
}
