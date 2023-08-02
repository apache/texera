package edu.uci.ics.texera.workflow.operators.visualization.ImageViz

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
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

class ImageVisualizerOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Binary Content")
  @JsonPropertyDescription("The Binary data of the Image")
  @AutofillAttributeName
  var BinaryContent: String = _

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Image Visualizer",
      "Visualize binary of an image",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def numWorkers() = 1

  def createBinaryData(): String = {
    assert(BinaryContent.nonEmpty)
    s"""
       |        binary_image_data = tuple_['$BinaryContent']
       |""".stripMargin
  }

  override def generatePythonCode(operatorSchemaInfo: OperatorSchemaInfo): String = {
    val finalCode = s"""
                       |from pytexera import *
                       |from PIL import Image
                       |import base64
                       |import numpy as np
                       |
                       |class ProcessTupleOperator(UDFOperatorV2):
                       |
                       |    def render_error(self, error_msg):
                       |        return '''<h1>Image is not available.</h1>
                       |                  <p>Reason is: {} </p>
                       |               '''.format(error_msg)
                       |
                       |    @overrides
                       |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
                       |
                       |        ${createBinaryData()}
                       |
                       |        try:
                       |            encoded_image_data = base64.b64encode(binary_image_data)
                       |            encoded_image_str = encoded_image_data.decode("utf-8")
                       |        except:
                       |            yield {'html-content': self.render_error("Binary input is not valid")}
                       |            return
                       |        html = f'<img src="data:image;base64,{encoded_image_str}" alt="Image">'
                       |        yield {"html-content": html}
                       |""".stripMargin
    finalCode
  }

  // make the chart type to html visualization so it can be recognized by both backend and frontend.
  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
