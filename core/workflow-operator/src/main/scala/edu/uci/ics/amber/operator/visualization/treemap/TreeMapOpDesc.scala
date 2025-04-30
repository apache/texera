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

class TreeMapOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute")
  @AutofillAttributeName
  var textColumn: String = ""

  var hierarchy: List[HierarchySection] = List()

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
      "Visualize Data into a Tree Map",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  private def getHierarchyAttributesInPython: String =
    hierarchy.map(h => s"'${h.attributeName}'").mkString("[", ", ", "]")

  override def generatePythonCode(): String = {
    val hierarchyAttributes = getHierarchyAttributesInPython
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
         |
         |        hierarchy_columns = $hierarchyAttributes
         |        value_column = "$textColumn"
         |        required_columns = hierarchy_columns + [value_column]
         |
         |        # Drop rows with nulls
         |        table = table.dropna(subset=required_columns)
         |
         |        if table.empty:
         |            yield {'html-content': self.render_error("input table became empty after dropping nulls.")}
         |            return
         |
         |        # Ensure value column is numeric
         |        if not np.issubdtype(table[value_column].dtype, np.number):
         |            yield {'html-content': self.render_error(f"Value column '{value_column}' must be numeric.")}
         |            return
         |
         |        # Ensure values are positive
         |        if (table[value_column] <= 0).all():
         |            yield {'html-content': self.render_error("Value column contains only non-positive numbers.")}
         |            return
         |
         |        # Tree Map Visualization
         |        fig = px.treemap(
         |            table,
         |            path=hierarchy_columns,
         |            values=value_column,
         |            color=value_column,
         |            hover_data=hierarchy_columns,
         |            color_continuous_scale='RdBu'
         |        )
         |
         |        # To HTML Content
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |""".stripMargin
    finalCode
  }
}
