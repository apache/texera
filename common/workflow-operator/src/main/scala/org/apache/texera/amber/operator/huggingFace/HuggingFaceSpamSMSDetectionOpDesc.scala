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
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.{PythonOperatorDescriptor, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.annotations.{AutofillAttributeName, SampleColumn}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.{
  PythonTemplateBuilderStringContext,
  pyStringLiteral
}
class HuggingFaceSpamSMSDetectionOpDesc
    extends PythonOperatorDescriptor
    with StandaloneCodeGenerator {
  @JsonProperty(value = "attribute", required = true)
  @JsonPropertyDescription("column to perform spam detection on")
  @AutofillAttributeName
  @SampleColumn("short_text")
  var attribute: EncodableString = _

  @JsonProperty(
    value = "Spam result attribute",
    required = true,
    defaultValue = "is_spam"
  )
  @JsonPropertyDescription("column name of whether spam or not")
  var resultAttributeSpam: EncodableString = _

  @JsonProperty(
    value = "Score result attribute",
    required = true,
    defaultValue = "score"
  )
  @JsonPropertyDescription("column name of Probability for classification")
  var resultAttributeProbability: EncodableString = _

  override def generatePythonCode(): String = {
    pyb"""from transformers import pipeline
       |from pytexera import *
       |
       |class ProcessTupleOperator(UDFOperatorV2):
       |
       |    def open(self):
       |        self.pipeline = pipeline("text-classification", model="mrm8488/bert-tiny-finetuned-sms-spam-detection")
       |
       |    @overrides
       |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
       |        text = tuple_[$attribute]
       |        # An empty cell arrives as None, which the pipeline rejects. Keep the row
       |        # and leave the results empty rather than ending the run over a value the
       |        # model has nothing to say about.
       |        if text is None or (isinstance(text, str) and not text.strip()):
       |            tuple_[$resultAttributeSpam] = None
       |            tuple_[$resultAttributeProbability] = None
       |            yield tuple_
       |            return
       |        result = self.pipeline(text)[0]
       |        tuple_[$resultAttributeSpam] = (result["label"] == "LABEL_1")
       |        tuple_[$resultAttributeProbability] = result["score"]
       |        yield tuple_""".encode
  }

  override def producesDataFrame(): Boolean = true

  // Standalone mirror of generatePythonCode: build the text-classification
  // pipeline once, run it per row, and add the BOOLEAN spam flag (LABEL_1) and
  // the DOUBLE score columns (in getOutputSchemas order) to produce out1df.
  override def generateStandaloneCode(): String = {
    val attributeLit = pyStringLiteral(attribute)
    val spamLit = pyStringLiteral(resultAttributeSpam)
    val probabilityLit = pyStringLiteral(resultAttributeProbability)
    s"""from transformers import pipeline
       |
       |_pipeline = pipeline("text-classification", model="mrm8488/bert-tiny-finetuned-sms-spam-detection")
       |out1df = in1df.copy()
       |_results = [_pipeline(_t)[0] for _t in out1df[$attributeLit]]
       |out1df[$spamLit] = [_r["label"] == "LABEL_1" for _r in _results]
       |out1df[$probabilityLit] = [_r["score"] for _r in _results]""".stripMargin
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Hugging Face Spam Detection",
      "Spam Detection by SMS Spam Detection Model from Hugging Face",
      OperatorGroupConstants.HUGGINGFACE_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    Map(
      operatorInfo.outputPorts.head.id -> inputSchemas.values.head
        .add(resultAttributeSpam, AttributeType.BOOLEAN)
        .add(resultAttributeProbability, AttributeType.DOUBLE)
    )
  }
}
