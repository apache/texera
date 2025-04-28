package edu.uci.ics.amber.operator.visualization.treemap

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.operator.visualization.hierarchychart.HierarchySection

class TreeMapOpDesc extends PythonOperatorDescriptor{

  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute")
  @AutofillAttributeName
  var textColumn: String = ""


  override def getOutputSchemas(
     inputSchemas: Map[PortIdentity, Schema]
   ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Tree Map",
      "Generate Tree Map for result texts",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  var hierarchy: List[HierarchySection] = List()

  private def getHierarchyAttributesInPython: String =
    hierarchy.map(h => s"'${h.attributeName}'").mkString("[", ", ", "]")

  def createPlotlyFigure(): String = {
    //assert(hierarchy.nonEmpty)
    val attributes = getHierarchyAttributesInPython
    s"""
       |    fig = px.treemap(table, path=[$attributes], values='$textColumn',
       |                     color='$textColumn', hover_data=[$attributes],
       |                     color_continuous_scale='RdBu')
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
    val finalCode =
      s"""
         |from pytexera import *
         |import plotly.express as px
         |import plotly.graph_objects as go
         |import plotly.io
         |import numpy as np
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |    # Generate custom error message as html string
         |    def render_error(self, error_msg) -> str:
         |        return '''<h1>Tree Map is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |            yield {'html-content': self.render_error("input table is empty.")}
         |            return
         |        ${createPlotlyFigure()}
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |""".stripMargin
    finalCode
  }
}