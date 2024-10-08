package edu.uci.ics.texera.workflow.operators.huggingFace

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.engine.common.model.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor

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

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    if (resultAttribute == null || resultAttribute.trim.isEmpty)
      throw new RuntimeException("Result attribute name should be given")
    Schema
      .builder()
      .add(schemas(0))
      .add(resultAttribute, AttributeType.STRING)
      .build()
  }
}
