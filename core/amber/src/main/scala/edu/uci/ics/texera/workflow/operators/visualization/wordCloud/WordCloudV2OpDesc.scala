package edu.uci.ics.texera.workflow.operators.visualization.wordCloud

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaInt,
  JsonSchemaTitle
}
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
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

class WordCloudV2OpDesc extends VisualizationOperator with PythonOperatorDescriptor {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Text column")
  @AutofillAttributeName
  var textColumn: String = ""

  @JsonProperty(defaultValue = "100")
  @JsonSchemaTitle("Number of most frequent words")
  @JsonSchemaInject(ints = Array(new JsonSchemaInt(path = "exclusiveMinimum", value = 0)))
  var topN: Integer = 100

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.newBuilder.add(new Attribute("image-binary", AttributeType.BINARY)).build
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Word Cloud V2",
      "Generate word cloud for result texts",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  def manipulateTable(): String = {
    s"""
       |        table.dropna(subset = ['$textColumn'], inplace = True) #remove missing values
       |""".stripMargin
  }

  def createByte64EncodedImage(): String = {
    s"""
       |        stopwords = set(STOPWORDS)
       |        text = ' '.join(table['$textColumn'])
       |        wordcloud = WordCloud(width=1000, height=600, stopwords=stopwords, max_words=$topN, background_color='white').generate(text)
       |        image_stream = BytesIO()
       |        wordcloud.to_image().save(image_stream, format='PNG')
       |""".stripMargin
  }

  override def numWorkers() = 1

  override def generatePythonCode(operatorSchemaInfo: OperatorSchemaInfo): String = {
    val finalCode =
      s"""
         |from pytexera import *
         |
         |import plotly.io
         |from wordcloud import WordCloud, STOPWORDS
         |from io import BytesIO
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
         |           yield {'image-binary': self.render_error("input table is empty.")}
         |           return
         |        ${manipulateTable()}
         |        if table.empty:
         |           yield {'image-binary': self.render_error("the text column contains missing values only.")}
         |           return
         |        ${createByte64EncodedImage()}
         |        yield {'image-binary': image_stream.getvalue()}
         |""".stripMargin
    finalCode
  }

  override def chartType(): String = VisualizationConstants.WORD_CLOUD
}
