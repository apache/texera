package edu.uci.ics.texera.workflow.operators.huggingFace

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}

class HuggingFaceSpamSMSDetectionOpDesc extends PythonOperatorDescriptor {
  @JsonProperty(value = "attribute", required = true)
  @JsonPropertyDescription("column to perform spam detection on")
  @AutofillAttributeName
  var attribute: String = _

  @JsonProperty(
    value = "Spam result attribute",
    required = true,
    defaultValue = "is_spam"
  )
  @JsonPropertyDescription("column name of whether spam or not")
  var resultAttributeSpam: String = _

  @JsonProperty(
    value = "Score result attribute",
    required = true,
    defaultValue = "score"
  )
  @JsonPropertyDescription("column name of Probability for classification")
  var resultAttributeProbability: String = _

  override def generatePythonCode(): String = {
    s"""from transformers import pipeline
       |from pytexera import *
       |
       |class ProcessTupleOperator(UDFOperatorV2):
       |
       |    def open(self):
       |        self.pipeline = pipeline("text-classification", model="mrm8488/bert-tiny-finetuned-sms-spam-detection")
       |
       |    @overrides
       |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
       |        result = self.pipeline(tuple_["$attribute"])[0]
       |        tuple_["$resultAttributeSpam"] = (result["label"] == "LABEL_1")
       |        tuple_["$resultAttributeProbability"] = result["score"]
       |        yield tuple_""".stripMargin
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Hugging Face Spam Detection",
      "Spam Detection by SMS Spam Detection Model from Hugging Face",
      OperatorGroupConstants.HUGGINGFACE_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema
      .builder()
      .add(schemas(0))
      .add(resultAttributeSpam, AttributeType.BOOLEAN)
      .add(resultAttributeProbability, AttributeType.DOUBLE)
      .build()
  }
}
