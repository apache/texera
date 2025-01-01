package edu.uci.ics.amber.operator.huggingFace

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
class HuggingFaceTextSummarizationOpDesc extends PythonOperatorDescriptor {
  @JsonProperty(value = "attribute", required = true)
  @JsonPropertyDescription("attribute to perform text summarization on")
  @AutofillAttributeName
  var attribute: String = _

  @JsonProperty(
    value = "Result attribute name",
    required = false,
    defaultValue = "summary"
  )
  @JsonPropertyDescription("attribute name of the text summary result")
  var resultAttribute: String = _

  override def generatePythonCode(): String = {
    s"""
       |from transformers import BertTokenizerFast, EncoderDecoderModel
       |import torch
       |from pytexera import *
       |
       |class ProcessTupleOperator(UDFOperatorV2):
       |
       |    def open(self):
       |        model_name = "mrm8488/bert-mini2bert-mini-finetuned-cnn_daily_mail-summarization"
       |        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
       |        self.tokenizer = BertTokenizerFast.from_pretrained(model_name)
       |        self.model = EncoderDecoderModel.from_pretrained(model_name).to(self.device)
       |
       |    @overrides
       |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
       |        text = tuple_["$attribute"]
       |
       |        inputs = self.tokenizer([text], padding="max_length", truncation=True, max_length=512, return_tensors="pt")
       |        input_ids = inputs.input_ids.to(self.device)
       |        attention_mask = inputs.attention_mask.to(self.device)
       |
       |        output = self.model.generate(input_ids, attention_mask=attention_mask)
       |        summary = self.tokenizer.decode(output[0], skip_special_tokens=True)
       |        tuple_["$resultAttribute"] = summary
       |        yield tuple_""".stripMargin
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Hugging Face Text Summarization",
      "Summarize the given text content with a mini2bert pre-trained model from Hugging Face",
      OperatorGroupConstants.HUGGINGFACE_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    if (resultAttribute == null || resultAttribute.trim.isEmpty)
      throw new RuntimeException("Result attribute name should be given")
    Map(
      operatorInfo.outputPorts.head.id -> inputSchemas.values.head
        .add(resultAttribute, AttributeType.STRING)
    )
  }
}
