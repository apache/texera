package edu.uci.ics.amber.operator.visualization.tablesChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
class TablesPlotOpDesc extends PythonOperatorDescriptor {

  @JsonPropertyDescription("List of columns to include in the table chart")
  @JsonProperty(value = "add attribute", required = true)
  var includedColumns: List[TablesConfig] = List()

  private def getAttributes: String =
    includedColumns.map(_.attributeName).mkString("'", "','", "'")

  def manipulateTable(): String = {
    assert(includedColumns.nonEmpty)
    val attributes = getAttributes
    s"""
       |        # drops rows with missing values pertaining to relevant columns
       |        table = table.dropna(subset=[$attributes])
       |
       |""".stripMargin
  }

  def createPlotlyFigure(): String = {
    assert(includedColumns.nonEmpty)
    val attributes = getAttributes
    s"""
       |
       |        filtered_table = table[[$attributes]]
       |        headers = filtered_table.columns.tolist()
       |        cell_values = [filtered_table[col].tolist() for col in headers]
       |
       |        fig = go.Figure(data=[go.Table(
       |            header=dict(values=headers),
       |            cells=dict(values=cell_values)
       |        )])
       |
       |
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
    s"""
       |from pytexera import *
       |import plotly.graph_objects as go
       |import plotly.io
       |class TableChartOperator(UDFTableOperator):
       |
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |
       |        if table.empty:
       |           yield {'html-content': self.render_error("input table is empty.")}
       |           return
       |
       |        ${manipulateTable()}
       |
       |        if table.empty:
       |           yield {'html-content': self.render_error("value column contains only non-positive numbers or nulls.")}
       |           return

       |
       |        ${createPlotlyFigure()}
       |        fig.update_layout(margin=dict(l=0, r=0, b=0, t=0))
       |        html_content = plotly.io.to_html(fig, include_plotlyjs='cdn')
       |        yield {'html-content': html_content}
    """.stripMargin
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "Tables Plot",
      "Visualize data in a table chart.",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )
  }

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }
}
