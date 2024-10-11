package edu.uci.ics.texera.workflow.operators.visualization.quiverPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.engine.common.model.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.operators.visualization.{
  VisualizationConstants,
  VisualizationOperator
}

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "value": {
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class QuiverPlotOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  //property panel variable: 4 requires: {x,y,u,v}, all columns should only contain numerical data

  @JsonProperty(value = "x", required = true)
  @JsonSchemaTitle("x")
  @JsonPropertyDescription("column for the x-coordinate of the starting point")
  @AutofillAttributeName var x: String = ""

  @JsonProperty(value = "y", required = true)
  @JsonSchemaTitle("y")
  @JsonPropertyDescription("column for the y-coordinate of the starting point")
  @AutofillAttributeName var y: String = ""

  @JsonProperty(value = "u", required = true)
  @JsonSchemaTitle("u")
  @JsonPropertyDescription("column for the vector component in the x-direction")
  @AutofillAttributeName var u: String = ""

  @JsonProperty(value = "v", required = true)
  @JsonSchemaTitle("v")
  @JsonPropertyDescription("column for the vector component in the y-direction")
  @AutofillAttributeName var v: String = ""

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.builder().add(new Attribute("html-content", AttributeType.STRING)).build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Quiver Plot",
      "Visualize vector data in a Quiver Plot",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  //data cleaning for missing value
  def manipulateTable(): String = {
    s"""
       |        table = table.dropna() #remove missing values
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
    val finalCode = s"""
                       |from pytexera import *
                       |import pandas as pd
                       |import plotly.figure_factory as ff
                       |import numpy as np
                       |import plotly.io
                       |import plotly.graph_objects as go
                       |
                       |class ProcessTableOperator(UDFTableOperator):
                       |
                       |    def render_error(self, error_msg):
                       |        return '''<h1>Quiver Plot is not available.</h1>
                       |                  <p>Reasons are: {} </p>
                       |               '''.format(error_msg)
                       |
                       |    @overrides
                       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
                       |        if table.empty:
                       |            yield {'html-content': self.render_error("Input table is empty.")}
                       |            return
                       |
                       |        required_columns = {'${x}', '${y}', '${u}', '${v}'}
                       |        if not required_columns.issubset(table.columns):
                       |            yield {'html-content': self.render_error(f"Input table must contain columns: {', '.join(required_columns)}")}
                       |            return
                       |
                       |        ${manipulateTable()}
                       |
                       |        def type_check(value):
                       |            return isinstance(value,(int,float))
                       |        for col in required_columns:
                       |            if not table[col].apply(type_check).all():
                       |                yield {"html-content": "Type error: All columns should only contain numerical data"}
                       |                return
                       |
                       |        try:
                       |            #graph the quiver plot
                       |            fig = ff.create_quiver(
                       |                table['${x}'], table['${y}'],
                       |                table['${u}'], table['${v}'],
                       |                scale=0.1
                       |            )
                       |            html = fig.to_html(include_plotlyjs='cdn', full_html=False)
                       |        except Exception as e:
                       |            yield {'html-content': self.render_error(f"Plotly error: {str(e)}")}
                       |            return
                       |
                       |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
                       |        yield {'html-content': html}
                       |""".stripMargin
    finalCode
  }

  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
