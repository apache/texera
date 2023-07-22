package edu.uci.ics.texera.workflow.operators.visualization.horiBarChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import edu.uci.ics.texera.workflow.operators.visualization.{
  VisualizationConstants,
  VisualizationOperator
}

//type constraint: value can only be numeric
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "value": {
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class TreeMapVisualizerOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(value = "value", required = true)
  @JsonSchemaTitle("Value Column")
  @JsonPropertyDescription("the value associated with each tree node")
  @AutofillAttributeName
  var value: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Hierarchy Path")
  @JsonPropertyDescription("hierarchy of sectors, from root to leaves")
  var hierarchy: List[HierarchySection] = List()

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "TreeMap Visualizer",
      "Visualize data in a tree hierarchy",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  def manipulateTable(): String = {
    assert(value.nonEmpty)
    s"""
       |        table['$value'] = table[table['$value'] > 0]['$value'] # remove non-positive numbers from the data
       |        table = table.dropna() #remove missing values
       |""".stripMargin
  }

  override def numWorkers() = 1

  def createPlotlyFigure(): String = {
    assert(hierarchy.nonEmpty)
    val attributes = hierarchy.map(_.attributeName).mkString("'", "','", "'")
    s"""
       |           fig = px.treemap(table, path=[$attributes], values='$value',
       |                        color='$value', hover_data=[$attributes],
       |                        color_continuous_scale='RdBu')
       |""".stripMargin
  }

  override def generatePythonCode(operatorSchemaInfo: OperatorSchemaInfo): String = {
    val final_code = s"""
                        |from pytexera import *
                        |
                        |import plotly.express as px
                        |import plotly.graph_objects as go
                        |import plotly.io
                        |import numpy as np
                        |
                        |class ProcessTableOperator(UDFTableOperator):
                        |
                        |    @overrides
                        |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
                        |        ${manipulateTable()}
                        |        if not table.empty:
                        |           ${createPlotlyFigure()}
                        |           # convert fig to html content
                        |           html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
                        |           yield {'html-content': html}
                        |        else:
                        |           html = '''<h1>TreeMap is not available.</h1>
                        |                     <p>Possible reasons are:</p>
                        |                     <ul>
                        |                     <li>input table is empty</li>
                        |                     <li>value column contains only non-positive numbers</li>
                        |                     <li>value column is not available</li>
                        |                     </ul>'''
                        |           yield {'html-content': html}
                        |""".stripMargin
    final_code
  }

  // make the chart type to html visualization so it can be recognized by both backend and frontend.
  override def chartType(): String = VisualizationConstants.HTML_VIZ
}