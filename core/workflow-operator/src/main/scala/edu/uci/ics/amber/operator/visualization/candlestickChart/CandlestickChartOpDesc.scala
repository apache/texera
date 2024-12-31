package edu.uci.ics.amber.operator.visualization.candlestickChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}

class CandlestickChartOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(value = "date", required = true)
  @JsonSchemaTitle("Date Column")
  @JsonPropertyDescription("the date of the candlestick")
  @AutofillAttributeName
  var date: String = ""

  @JsonProperty(value = "open", required = true)
  @JsonSchemaTitle("Opening Price Column")
  @JsonPropertyDescription("the opening price of the candlestick")
  @AutofillAttributeName
  var open: String = ""

  @JsonProperty(value = "high", required = true)
  @JsonSchemaTitle("Highest Price Column")
  @JsonPropertyDescription("the highest price of the candlestick")
  @AutofillAttributeName
  var high: String = ""

  @JsonProperty(value = "low", required = true)
  @JsonSchemaTitle("Lowest Price Column")
  @JsonPropertyDescription("the lowest price of the candlestick")
  @AutofillAttributeName
  var low: String = ""

  @JsonProperty(value = "close", required = true)
  @JsonSchemaTitle("Closing Price Column")
  @JsonPropertyDescription("the closing price of the candlestick")
  @AutofillAttributeName
  var close: String = ""

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema
      .builder()
      .add(new Attribute("html-content", AttributeType.STRING))
      .build()
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Candlestick Chart",
      "Visualize data in a Candlestick Chart",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  override def generatePythonCode(): String = {
    s"""
       |from pytexera import *
       |
       |import plotly.graph_objects as go
       |import pandas as pd
       |
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        # Convert table to dictionary
       |        table_dict = table.to_dict()
       |
       |        # Create a DataFrame from the dictionary
       |        df = pd.DataFrame(table_dict)
       |
       |        fig = go.Figure(data=[go.Candlestick(
       |            x=df['$date'],
       |            open=df['$open'],
       |            high=df['$high'],
       |            low=df['$low'],
       |            close=df['$close']
       |        )])
       |        fig.update_layout(title='Candlestick Chart')
       |        html = fig.to_html(include_plotlyjs='cdn', full_html=False)
       |        yield {'html-content': html}
       |""".stripMargin
  }

}
