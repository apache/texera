package edu.uci.ics.texera.workflow.operators.visualization.wordCloud

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaInt,
  JsonSchemaTitle
}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import edu.uci.ics.texera.workflow.operators.visualization.{
  ImageUtility,
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
    Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build
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
       |        table = table[table['$textColumn'].str.contains(r'\\w', regex=True)]
       |""".stripMargin
  }

  def createWordCloudFigure(): String = {
    s"""
       |        text = ' '.join(table['$textColumn'])
       |
       |        # Generate an image in a FHD resolution
       |        from wordcloud import WordCloud, STOPWORDS
       |        wordcloud = WordCloud(width=1920, height=1080, stopwords=set(STOPWORDS), max_words=$topN, background_color='white', include_numbers=True).generate(text)
       |
       |        from io import BytesIO
       |        image_stream = BytesIO()
       |        wordcloud.to_image().save(image_stream, format='PNG')
       |        binary_image_data = image_stream.getvalue()
       |""".stripMargin
  }

  override def generatePythonCode(operatorSchemaInfo: OperatorSchemaInfo): String = {
    val finalCode =
      s"""
         |from pytexera import *
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |    # Generate custom error message as html string
         |    def render_error(self, error_msg) -> str:
         |        return '''<h1>Wordcloud is not available.</h1>
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
         |           yield {'html-content': self.render_error("text column does not contain words or contains only nulls.")}
         |           return
         |        ${createWordCloudFigure()}
         |        ${ImageUtility.encodeImageToHTML()}
         |        yield {'html-content': html}
         |""".stripMargin

    print(finalCode)
    finalCode
  }

  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
