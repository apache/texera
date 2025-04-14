package edu.uci.ics.amber.operator.visualization.bulletChart

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty, JsonPropertyDescription}
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

class StepDefinition @JsonCreator() (
    @JsonProperty("start")
    @JsonSchemaTitle("Start")
    var start: String,
    @JsonProperty("end")
    @JsonSchemaTitle("End")
    var end: String
)

class BulletChartOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(value = "value", required = true)
  @JsonSchemaTitle("Value")
  @JsonPropertyDescription("The actual value to display on the bullet chart")
  @AutofillAttributeName var value: String = ""

  @JsonProperty(value = "deltaReference", required = true)
  @JsonSchemaTitle("Delta Reference")
  @JsonPropertyDescription("The reference value for the delta indicator. e.g., 100")
  var deltaReference: String = ""

  // Threshold
  @JsonProperty(value = "thresholdValue", required = false)
  @JsonSchemaTitle("Threshold Value")
  @JsonPropertyDescription("The performance threshold value. e.g., 100")
  var thresholdValue: String = ""

  // Steps
  @JsonProperty(value = "steps", required = false)
  @JsonSchemaTitle("Steps")
  @JsonPropertyDescription("Optional: Each step includes a start and end value e.g., 0, 100.")
  var steps: JList[StepDefinition] = new ArrayList[StepDefinition]()

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema().add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Bullet Chart",
      "Visualize data using a Bullet Chart that shows a primary quantitative bar and delta indicator. " +
        "Optional elements such as qualitative ranges (steps) and a performance threshold are displayed only when provided.",
      OperatorGroupConstants.VISUALIZATION_FINANCIAL_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  override def generatePythonCode(): String = {
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
         |    def render_error(self, error_msg) -> str:
         |        return '''<h1>Bullet chart is not available.</h1>
         |                  <p>Reason: {} </p>'''.format(error_msg)
         |
         |    def generate_gray_gradient(self, step_count):
         |        colors = []
         |        for i in range(step_count):
         |            lightness = 90 - (i * (60 / max(1, step_count - 1)))
         |            colors.append(f"hsl(0, 0%, {lightness}%)")
         |        return colors
         |
         |    def generate_valid_steps(self, steps_data):
         |        valid_steps = []
         |        for index, step in enumerate(steps_data):
         |            start = step.get('start', '')
         |            end = step.get('end', '')
         |            if start and end:
         |                try:
         |                    s_val = float(start)
         |                    e_val = float(end)
         |                    if s_val < e_val:
         |                        valid_steps.append({"start": s_val, "end": e_val})
         |                except ValueError:
         |                    print(f"Could not convert step values: {start}, {end}")
         |                except Exception as e:
         |                    print(f"Error processing steps: {str(e)}")
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
         |            # Process steps
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
         |            except Exception as e:
         |                print(f"Error processing steps: {str(e)}")
         |                steps_list = []
         |
         |            count = 0
         |            html_chunks = []
         |            for _, row in table.iterrows():
         |                if count >= 10:  # Limit to 10 charts
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
