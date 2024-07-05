package edu.uci.ics.texera.workflow.operators.visualization.figureFactoryTable

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.visualization.{
  VisualizationConstants,
  VisualizationOperator
}

class FigureFactoryTableOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(required = false)
  @JsonSchemaTitle("Font Size")
  @JsonPropertyDescription("Font size of the Figure Factory Table")
  var fontSize: String = "12"

  @JsonProperty(required = false)
  @JsonSchemaTitle("Font Color (Hex Code)")
  @JsonPropertyDescription("Font color of the Figure Factory Table")
  var fontColor: String = "#000000"

  @JsonProperty(required = false)
  @JsonSchemaTitle("Row Height")
  @JsonPropertyDescription("Row height of the Figure Factory Table")
  var rowHeight: String = "30"

  @JsonPropertyDescription("List of columns to include in the figure factory table")
  @JsonProperty(value = "add attribute", required = true)
  var columns: List[FigureFactoryTableConfig] = List()

  private def getAttributes: String =
    columns.map(_.attributeName).mkString("'", "','", "'")

  def manipulateTable(): String = {
    assert(columns.nonEmpty)

    val attributes = getAttributes
    s"""
       |        # drops rows with missing values pertaining to relevant columns
       |        table = table.dropna(subset=[$attributes])
       |
       |""".stripMargin
  }

  def createFigureFactoryTablePlotlyFigure(): String = {
    assert(columns.nonEmpty)

    val intFontSize: Option[Double] = fontSize.toDoubleOption
    val intRowHeight: Option[Double] = rowHeight.toDoubleOption

    assert(intFontSize.isDefined && intFontSize.get >= 0)
    assert(intRowHeight.isDefined && intRowHeight.get >= 30)

    val attributes = getAttributes
    s"""
       |        filtered_table = table[[$attributes]]
       |        headers = filtered_table.columns.tolist()
       |        cell_values = [filtered_table[col].tolist() for col in headers]
       |
       |        data = [headers] + list(map(list, zip(*cell_values)))
       |        fig = ff.create_table(data, height_constant = ${intRowHeight.get}, font_colors=['$fontColor'])
       |
       |        # Make text size larger
       |        for i in range(len(fig.layout.annotations)):
       |          fig.layout.annotations[i].font.size = ${intFontSize.get}
       |
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
    s"""
       |from pytexera import *
       |import plotly.figure_factory as ff
       |import plotly.io
       |
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
       |        ${createFigureFactoryTablePlotlyFigure()}
       |        fig.update_layout(margin=dict(l=0, r=0, b=0, t=0))
       |        html_content = plotly.io.to_html(fig, include_plotlyjs='cdn', include_mathjax='cdn')
       |        yield {'html-content': html_content}
  """.stripMargin
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "FigureFactoryTable",
      "Visualize data in a figure factory table",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )
  }

  override def chartType(): String = VisualizationConstants.HTML_VIZ

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.builder().add(new Attribute("html-content", AttributeType.STRING)).build()
  }
}
