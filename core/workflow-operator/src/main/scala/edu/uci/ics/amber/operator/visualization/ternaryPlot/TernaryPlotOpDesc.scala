package edu.uci.ics.amber.operator.visualization.ternaryPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.workflow.{InputPort, OutputPort}

/**
  * Visualization Operator for Ternary Plots.
  *
  * This operator uses three data fields to construct a ternary plot.
  * The points can optionally be color coded using a data field.
  */

class TernaryPlotOpDesc extends PythonOperatorDescriptor {

  // Add annotations for the first variable
  @JsonProperty(value = "firstVariable", required = true)
  @JsonSchemaTitle("Variable 1")
  @JsonPropertyDescription("First variable data field")
  @AutofillAttributeName var firstVariable: String = ""

  // Add annotations for the second variable
  @JsonProperty(value = "secondVariable", required = true)
  @JsonSchemaTitle("Variable 2")
  @JsonPropertyDescription("Second variable data field")
  @AutofillAttributeName var secondVariable: String = ""

  // Add annotations for the third variable
  @JsonProperty(value = "thirdVariable", required = true)
  @JsonSchemaTitle("Variable 3")
  @JsonPropertyDescription("Third variable data field")
  @AutofillAttributeName var thirdVariable: String = ""

  // Add annotations for enabling color and selecting its associated data field
  @JsonProperty(value = "colorEnabled", defaultValue = "false")
  @JsonSchemaTitle("Categorize by Color")
  @JsonPropertyDescription("Optionally color points using a data field")
  var colorEnabled: Boolean = false

  @JsonProperty(value = "colorDataField", required = false)
  @JsonSchemaTitle("Color Data Field")
  @JsonPropertyDescription("Specify the data field to color")
  @AutofillAttributeName var colorDataField: String = ""

  // OperatorInfo instance describing ternary plot
  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Ternary Plot",
      operatorDescription = "Points are graphed on a Ternary Plot using 3 specified data fields",
      operatorGroupName = OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  /** Returns the output schema set as html-content */
  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.builder().add(new Attribute("html-content", AttributeType.STRING)).build()
  }

  /** Returns a Python string that drops any tuples with missing values */
  def manipulateTable(): String = {
    // Check for any empty data field names
    assert(firstVariable.nonEmpty && secondVariable.nonEmpty && thirdVariable.nonEmpty)
    s"""
       |        # Remove any tuples that contain missing values
       |        table.dropna(subset=['$firstVariable', '$secondVariable', '$thirdVariable'], inplace = True)
       |""".stripMargin
  }

  /** Returns a Python string that creates the ternary plot figure */
  def createPlotlyFigure(): String = {
    s"""
       |        if '$colorEnabled' == 'true' and '$colorDataField' != "":
       |            fig = px.scatter_ternary(table, a='$firstVariable', b='$secondVariable', c='$thirdVariable', color='$colorDataField')
       |        else:
       |            fig = px.scatter_ternary(table, a='$firstVariable', b='$secondVariable', c='$thirdVariable')
       |""".stripMargin
  }

  /** Returns a Python string that yields the html content of the ternary plot */
  override def generatePythonCode(): String = {
    val finalCode =
      s"""
         |from pytexera import *
         |
         |import plotly.express as px
         |import plotly.io
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |    # Generate custom error message as html string
         |    def render_error(self, error_msg):
         |        return '''<h1>TernaryPlot is not available.</h1>
         |                  <p>Reasons are: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |            yield {'html-content': self.render_error("Input table is empty.")}
         |            return
         |        ${manipulateTable()}
         |        if table.empty:
         |            yield {'html-content': self.render_error("No valid rows left (every row has at least 1 missing value).")}
         |            return
         |        ${createPlotlyFigure()}
         |        # Convert fig to html content
         |        html = plotly.io.to_html(fig, include_plotlyjs = 'cdn', auto_play = False)
         |        yield {'html-content':html}
         |""".stripMargin
    finalCode
  }

}
