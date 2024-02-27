package edu.uci.ics.texera.workflow.operators.visualization.ImageViz

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.operators.visualization.{
  ImageUtility,
  VisualizationConstants,
  VisualizationOperator
}

class ImageVisualizerOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("image content column")
  @JsonPropertyDescription("The Binary data of the Image")
  @AutofillAttributeName
  var binaryContent: String = _

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.builder().add(new Attribute("html-content", AttributeType.STRING)).build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Image Visualizer",
      "visualize image content",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  def createBinaryData(): String = {
    assert(binaryContent.nonEmpty)
    s"""
       |        binary_image_data = tuple_['$binaryContent']
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
    val finalCode = s"""
                       |from pytexera import *
                       |from PIL import Image
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
                       |        ${createBinaryData()}
                       |        ${ImageUtility.encodeImageToHTML()}
                       |        yield {"html-content": html}
                       |""".stripMargin
    finalCode
  }

  // make the chart type to html visualization so it can be recognized by both backend and frontend.
  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
