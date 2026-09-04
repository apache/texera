/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.operator.visualization.wordCloud

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaInt,
  JsonSchemaTitle
}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.{PythonOperatorDescriptor, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.visualization.ImageUtility
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral

import javax.validation.constraints.NotNull
// type constraint: the words are counted out of the column's text, and the
// filter that finds them uses pandas' .str accessor, which refuses a column of
// anything else. A word cloud of numbers would say nothing anyway.
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "textColumn": { "enum": ["string"] }
  }
}
""")
class WordCloudOpDesc extends PythonOperatorDescriptor with StandaloneCodeGenerator {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Text column")
  @AutofillAttributeName
  @NotNull(message = "Text column cannot be empty")
  var textColumn: EncodableString = ""

  @JsonProperty(defaultValue = "100")
  @JsonSchemaTitle("Number of most frequent words")
  @JsonSchemaInject(ints = Array(new JsonSchemaInt(path = "exclusiveMinimum", value = 0)))
  var topN: Integer = 100

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      "Word Cloud",
      "Generate word cloud for texts",
      OperatorGroupConstants.VISUALIZATION_MEDIA_GROUP
    )

  def manipulateTable(): PythonTemplateBuilder = {
    pyb"""
       |        table.dropna(subset = [$textColumn], inplace = True) #remove missing values
       |        table = table[table[$textColumn].str.contains(r'\w', regex=True)]
       |"""
  }

  def createWordCloudFigure(): PythonTemplateBuilder = {
    pyb"""
       |        text = ' '.join(table[$textColumn])
       |
       |        # Generate an image in a FHD resolution
       |        from wordcloud import WordCloud, STOPWORDS
       |        # random_state seeds the layout so the same text draws the same
       |        # picture every run; positions, angles and colours are random otherwise.
       |        wordcloud = WordCloud(width=1920, height=1080, stopwords=set(STOPWORDS), max_words=$topN, background_color='white', include_numbers=True, random_state=0).generate(text)
       |
       |        from io import BytesIO
       |        image_stream = BytesIO()
       |        wordcloud.to_image().save(image_stream, format='PNG')
       |        binary_image_data = image_stream.getvalue()
       |"""
  }

  override def generatePythonCode(): String = {
    pyb"""
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
         |""".encode
  }

  override def producesDataFrame(): Boolean = false

  // The two guards mirror generatePythonCode's: WordCloud.generate raises on a
  // wordless string, so without them an input that is empty — or whose text
  // column survives neither the dropna nor the word filter — crashes here where
  // the runtime path explains itself.
  override def generateStandaloneCode(): String = {
    val textLit = pyStringLiteral(textColumn)
    s"""def render_error(error_msg):
       |    return '''<h1>Wordcloud is not available.</h1>
       |                  <p>Reason is: {} </p>
       |               '''.format(error_msg)
       |
       |table = in1df
       |if table.empty:
       |    with open("output.html", "w", encoding="utf-8") as f:
       |        f.write(render_error("input table is empty."))
       |else:
       |    table = table.dropna(subset=[$textLit])
       |    table = table[table[$textLit].str.contains(r'\\w', regex=True)]
       |    if table.empty:
       |        with open("output.html", "w", encoding="utf-8") as f:
       |            f.write(render_error("text column does not contain words or contains only nulls."))
       |    else:
       |        text = ' '.join(table[$textLit])
       |        from wordcloud import WordCloud, STOPWORDS
       |        wordcloud = WordCloud(width=1920, height=1080, stopwords=set(STOPWORDS), max_words=$topN, background_color='white', include_numbers=True, random_state=0).generate(text)
       |        from io import BytesIO
       |        image_stream = BytesIO()
       |        wordcloud.to_image().save(image_stream, format='PNG')
       |        binary_image_data = image_stream.getvalue()
       |        import base64
       |        encoded_image_str = base64.b64encode(binary_image_data).decode("utf-8")
       |        html = f'<img src="data:image;base64,{encoded_image_str}" alt="Image" style="max-width: 100vw; max-height: 90vh; width: auto; height: auto;">'
       |        with open("output.html", "w", encoding="utf-8") as f:
       |            f.write(html)""".stripMargin
  }
}
