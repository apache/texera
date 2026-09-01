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

package org.apache.texera.amber.operator.huggingFace

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
// type constraint: the tokenizer summarizes text and refuses anything that is not
// a string, so the column can only be a string.
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "attribute": { "enum": ["string"] }
  }
}
""")
class HuggingFaceTextSummarizationOpDesc extends PythonOperatorDescriptor {
  @JsonProperty(value = "attribute", required = true)
  @JsonPropertyDescription("attribute to perform text summarization on")
  @AutofillAttributeName
  var attribute: EncodableString = _

  @JsonProperty(
    value = "Result attribute name",
    required = false,
    defaultValue = "summary"
  )
  @JsonPropertyDescription("attribute name of the text summary result")
  var resultAttribute: EncodableString = _

  override def generatePythonCode(): String = {
    pyb"""
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
       |        text = tuple_[$attribute]
       |        # An empty cell arrives as None, which the tokenizer rejects. Keep the row
       |        # and leave the summary empty rather than ending the run over a value the
       |        # model has nothing to say about.
       |        if text is None or (isinstance(text, str) and not text.strip()):
       |            tuple_[$resultAttribute] = None
       |            yield tuple_
       |            return
       |
       |        inputs = self.tokenizer([text], padding="max_length", truncation=True, max_length=512, return_tensors="pt")
       |        input_ids = inputs.input_ids.to(self.device)
       |        attention_mask = inputs.attention_mask.to(self.device)
       |
       |        output = self.model.generate(input_ids, attention_mask=attention_mask)
       |        summary = self.tokenizer.decode(output[0], skip_special_tokens=True)
       |        tuple_[$resultAttribute] = summary
       |        yield tuple_""".encode
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
