package edu.uci.ics.texera.workflow.operators.visualization.logPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.visualization.{
  VisualizationConstants,
  VisualizationOperator
}

class LogPlotOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("X-axis Column")
  @JsonPropertyDescription("X-axis column for log plot.")
  @AutofillAttributeName
  var xAxis: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Y-axis Column")
  @JsonPropertyDescription("Y-axis column for log plot.")
  @AutofillAttributeName
  var yAxis: String = ""

  @JsonProperty(required = false, defaultValue = "false")
  @JsonSchemaTitle("log scale X")
  @JsonPropertyDescription("values in X-axis is log scaled")
  var xLogScale: Boolean = false

  @JsonProperty(required = false, defaultValue = "false")
  @JsonSchemaTitle("log scale Y")
  @JsonPropertyDescription("values in Y-axis is log scaled")
  var yLogScale: Boolean = false

  @JsonProperty(required = false)
  @JsonSchemaTitle("Hover column")
  @JsonPropertyDescription("column value to display when a dot is hovered over")
  @AutofillAttributeName
  var hoverName: String = ""

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.builder().add(new Attribute("html-content", AttributeType.STRING)).build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "LogPlot",
      "Visualize data using a log plot",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  private def createPlotlyFigure(): String = {
    var argDetails = if (xLogScale) ", log_x=True" else ""
    argDetails = argDetails + (if (yLogScale) ", log_y=True" else "")
    argDetails = argDetails + (if (hoverName.nonEmpty) s""", hover_name="$hoverName"""" else "")
    s"""
       |        fig = px.scatter(table, x="$xAxis", y="$yAxis"$argDetails)
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
    val finalCode =
      s"""
         |from pytexera import *
         |
         |import plotly.express as px
         |import plotly.graph_objects as go
         |import plotly.io
         |import pandas as pd
         |import numpy as np
         |
         |class ProcessTableOperator(UDFTableOperator):
         |    def render_error(self, error_msg):
         |        return '''<h1>Chart is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |           yield {'html-content': self.render_error("input table is empty.")}
         |           return
         |        ${createPlotlyFigure()}
         |        # convert fig to html content
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |
         |""".stripMargin

    finalCode
  }

  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
