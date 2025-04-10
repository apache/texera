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
  @JsonProperty(value = "title", required = false)
  @JsonSchemaTitle("Chart Title")
  @JsonPropertyDescription("The title to display on the bullet chart")
  var title: String = ""

  @JsonProperty(value = "value", required = true)
  @JsonSchemaTitle("Value")
  @JsonPropertyDescription("The actual value to display on the bullet chart (column name)")
  @AutofillAttributeName var value: String = ""

  @JsonProperty(value = "deltaReference", required = true)
  @JsonSchemaTitle("Delta Reference")
  @JsonPropertyDescription("The reference value for the delta indicator (column name)")
  @AutofillAttributeName var deltaReference: String = ""

  // x-axis
  @JsonProperty(value = "xAxisMin", required = false)
  @JsonSchemaTitle("X-axis Minimum")
  @JsonPropertyDescription("The minimum value for the x-axis scale. Default: 0")
  var xAxisMin: String = ""

  @JsonProperty(value = "xAxisMax", required = false)
  @JsonSchemaTitle("X-axis Maximum")
  @JsonPropertyDescription("The maximum value for the x-axis scale. Default: auto-calculated")
  var xAxisMax: String = ""

  @JsonProperty(value = "rowIndex", required = false)
  @JsonSchemaTitle("Row Index")
  @JsonPropertyDescription(
    "Pick the N-th row (0-based) from the input table to visualize. Default: 0 (first row)"
  )
  var rowIndex: String = "0"

  // Step 1 (Optional)
  @JsonProperty(value = "step1Start", required = false)
  @JsonSchemaTitle("Step 1 Start")
  @JsonPropertyDescription("Start value for the first qualitative step range")
  var step1Start: String = ""

  @JsonProperty(value = "step1End", required = false)
  @JsonSchemaTitle("Step 1 End")
  @JsonPropertyDescription("End value for the first qualitative step range")
  var step1End: String = ""

  @JsonProperty(value = "step1Color", required = false)
  @JsonSchemaTitle("Step 1 Color")
  @JsonPropertyDescription(
    "Color for the first qualitative step range, e.g., 'lightgray', '#e0e0e0'"
  )
  var step1Color: String = "lightgray"

  // Step 2 (Optional)
  @JsonProperty(value = "step2Start", required = false)
  @JsonSchemaTitle("Step 2 Start")
  @JsonPropertyDescription("Start value for the second qualitative step range")
  var step2Start: String = ""

  @JsonProperty(value = "step2End", required = false)
  @JsonSchemaTitle("Step 2 End")
  @JsonPropertyDescription("End value for the second qualitative step range")
  var step2End: String = ""

  @JsonProperty(value = "step2Color", required = false)
  @JsonSchemaTitle("Step 2 Color")
  @JsonPropertyDescription("Color for the second qualitative step range, e.g., 'gray', '#a0a0a0'")
  var step2Color: String = "gray"

  // Step 3 (Optional)
  @JsonProperty(value = "step3Start", required = false)
  @JsonSchemaTitle("Step 3 Start")
  @JsonPropertyDescription("Start value for the third qualitative step range")
  var step3Start: String = ""

  @JsonProperty(value = "step3End", required = false)
  @JsonSchemaTitle("Step 3 End")
  @JsonPropertyDescription("End value for the third qualitative step range")
  var step3End: String = ""

  @JsonProperty(value = "step3Color", required = false)
  @JsonSchemaTitle("Step 3 Color")
  @JsonPropertyDescription(
    "Color for the third qualitative step range, e.g., 'darkgray', '#808080'"
  )
  var step3Color: String = "darkgray"

  // Threshold
  @JsonProperty(value = "thresholdValue", required = false)
  @JsonSchemaTitle("Threshold Value")
  @JsonPropertyDescription("The performance threshold value. e.g., 80")
  var thresholdValue: String = ""

  @JsonProperty(value = "thresholdLineColor", required = false)
  @JsonSchemaTitle("Threshold Line Color")
  @JsonPropertyDescription("The color of the threshold line, e.g., 'red', '#ff0000'.")
  var thresholdLineColor: String = "red"

  @JsonProperty(value = "thresholdLineWidth", required = false)
  @JsonSchemaTitle("Threshold Line Width")
  @JsonPropertyDescription("The width of the threshold line. e.g., 3")
  var thresholdLineWidth: String = "3"

  @JsonProperty(value = "thresholdThickness", required = false)
  @JsonSchemaTitle("Threshold Thickness")
  @JsonPropertyDescription("The thickness of the threshold line. e.g., 1")
  var thresholdThickness: String = "0.75"

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
         |            delta_ref_col = "$deltaReference"
         |
         |            if value_col not in table.columns:
         |                yield {'html-content': self.render_error(f"Column '{value_col}' not found in input table.")}
         |                return
         |
         |            if delta_ref_col not in table.columns:
         |                yield {'html-content': self.render_error(f"Column '{delta_ref_col}' not found in input table.")}
         |                return
         |
         |            table = table.dropna(subset=[value_col, delta_ref_col])
         |            if table.empty:
         |                yield {'html-content': self.render_error("No valid data rows found after dropping nulls.")}
         |                return
         |
         |            try:
         |                row_idx = int("$rowIndex") if "$rowIndex".strip() else 0
         |            except ValueError:
         |                row_idx = 0
         |
         |            if row_idx < 0 or row_idx >= len(table):
         |                row_idx = 0
         |
         |            actual_value = float(table[value_col].iloc[row_idx])
         |            delta_reference_value = float(table[delta_ref_col].iloc[row_idx])
         |
         |            gauge_config = {'shape': 'bullet'}
         |
         |            steps_list = []
         |            # Process Step 1 if both start and end are provided
         |            if "$step1Start".strip() and "$step1End".strip():
         |                try:
         |                    step1_start = float("$step1Start")
         |                    step1_end = float("$step1End")
         |                    if step1_start < step1_end:
         |                        step1_color = "$step1Color" if "$step1Color".strip() else 'lightgray'
         |                        steps_list.append({"range": [step1_start, step1_end], "color": step1_color})
         |                except ValueError:
         |                    pass
         |
         |            # Process Step 2 if both start and end are provided
         |            if "$step2Start".strip() and "$step2End".strip():
         |                try:
         |                    step2_start = float("$step2Start")
         |                    step2_end = float("$step2End")
         |                    if step2_start < step2_end:
         |                        step2_color = "$step2Color" if "$step2Color".strip() else 'gray'
         |                        steps_list.append({"range": [step2_start, step2_end], "color": step2_color})
         |                except ValueError:
         |                    pass
         |
         |            # Process Step 3 if both start and end are provided
         |            if "$step3Start".strip() and "$step3End".strip():
         |                try:
         |                    step3_start = float("$step3Start")
         |                    step3_end = float("$step3End")
         |                    if step3_start < step3_end:
         |                        step3_color = "$step3Color" if "$step3Color".strip() else 'darkgray'
         |                        steps_list.append({"range": [step3_start, step3_end], "color": step3_color})
         |                except ValueError:
         |                    pass
         |
         |            gauge_config['steps'] = steps_list
         |
         |            # Configure threshold
         |            if "$thresholdValue".strip():
         |                try:
         |                    threshold_value = float("$thresholdValue")
         |                except ValueError:
         |                    threshold_value = 300
         |
         |                threshold_line_color = "$thresholdLineColor" if "$thresholdLineColor".strip() else 'red'
         |
         |                try:
         |                    threshold_line_width = int("$thresholdLineWidth") if "$thresholdLineWidth".strip() else 3
         |                except ValueError:
         |                    threshold_line_width = 3
         |
         |                try:
         |                    threshold_thickness = float("$thresholdThickness") if "$thresholdThickness".strip() else 0.75
         |                except ValueError:
         |                    threshold_thickness = 0.75
         |
         |                gauge_config['threshold'] = {
         |                    "value": threshold_value,
         |                    "line": {"color": threshold_line_color, "width": threshold_line_width},
         |                    "thickness": threshold_thickness
         |                }
         |
         |            if "$xAxisMin".strip() and "$xAxisMax".strip():
         |                try:
         |                    x_axis_min = float("$xAxisMin")
         |                    x_axis_max = float("$xAxisMax")
         |                    if x_axis_min < x_axis_max:
         |                        gauge_config['axis'] = {"range": [x_axis_min, x_axis_max]}
         |                    else:
         |                        gauge_config['axis'] = {"range": [0, actual_value + 200]}
         |                except ValueError:
         |                    gauge_config['axis'] = {"range": [0, actual_value + 200]}
         |            else:
         |                gauge_config['axis'] = {"range": [0, actual_value + 200]}
         |
         |            title_text = "$title" if "$title".strip() else 'Bullet Chart'
         |
         |            fig = go.Figure(go.Indicator(
         |                mode="number+gauge+delta",
         |                value=actual_value,
         |                delta={'reference': delta_reference_value},
         |                gauge=gauge_config,
         |                domain={'x': [0.1, 1], 'y': [0.1, 0.9]},
         |                title={'text': title_text}
         |            ))
         |
         |            fig.update_layout(margin=dict(l=80, r=20, b=40, t=40), height=250)
         |            html = fig.to_html(include_plotlyjs='cdn', full_html=False)
         |            yield {'html-content': html}
         |
         |        except Exception as e:
         |            yield {'html-content': self.render_error(f"Error generating bullet chart: {str(e)}")}
         |""".stripMargin
    finalCode
  }
}
