package edu.uci.ics.texera.workflow.operators.visualization.hierarchychart

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

// type constraint: value can only be numeric
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "value": {
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class HierarchyChartOpDesc extends VisualizationOperator with PythonOperatorDescriptor {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Chart Type")
  @JsonPropertyDescription("treemap or sunburst")
  var hierarchyChartType: HierarchyChartType = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Hierarchy Path")
  @JsonPropertyDescription(
    "hierarchy of attributes from a higher-level category to lower-level category"
  )
  var hierarchy: List[HierarchySection] = List()

  @JsonProperty(value = "value", required = true)
  @JsonSchemaTitle("Value Column")
  @JsonPropertyDescription("the value associated with the size of each sector in the chart")
  @AutofillAttributeName
  var value: String = ""

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.builder().add(new Attribute("html-content", AttributeType.STRING)).build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Hierarchy Chart",
      "Visualize data in hierarchy",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  private def getHierarchyAttributesInPython: String =
    hierarchy.map(_.attributeName).mkString("'", "','", "'")

  def manipulateTable(): String = {
    assert(value.nonEmpty)
    val attributes = getHierarchyAttributesInPython
    s"""
       |        table['$value'] = table[table['$value'] > 0]['$value'] # remove non-positive numbers from the data
       |        table.dropna(subset = [$attributes], inplace = True) #remove missing values
       |""".stripMargin
  }

  def createPlotlyFigure(): String = {
    assert(hierarchy.nonEmpty)
    val attributes = getHierarchyAttributesInPython
    s"""
       |        fig = px.${hierarchyChartType.getPlotlyExpressApiName}(table, path=[$attributes], values='$value',
       |                                                               color='$value', hover_data=[$attributes],
       |                                                               color_continuous_scale='RdBu')
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
         |import numpy as np
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |    # Generate custom error message as html string
         |    def render_error(self, error_msg) -> str:
         |        return '''<h1>Hierarchy chart is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |           yield {'html-content': self.render_error("input table is empty.")}
         |           return
         |        ${manipulateTable()}
         |        if table.empty:
         |           yield {'html-content': self.render_error("value column contains only non-positive numbers or nulls.")}
         |           return
         |        ${createPlotlyFigure()}
         |        # convert fig to html content
         |        fig.update_layout(margin=dict(l=0, r=0, b=0, t=0))
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |""".stripMargin
    finalCode
  }

  // make the chart type to html visualization so it can be recognized by both backend and frontend.
  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
