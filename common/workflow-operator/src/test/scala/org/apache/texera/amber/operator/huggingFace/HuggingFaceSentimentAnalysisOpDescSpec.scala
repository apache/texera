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

import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class HuggingFaceSentimentAnalysisOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  // EncodableString fields are always base64-wrapped in .encode mode
  // (self.decode_python_template('<base64>')), so assert on the base64 form only: the raw
  // column name can appear in the generated Python for unrelated reasons (e.g. the
  // "positive"/"neutral"/"negative" label keys), which would mask a missing splice.
  private def carries(output: String, name: String): Boolean =
    output.contains(b64(name))

  private def configured(): HuggingFaceSentimentAnalysisOpDesc = {
    val d = new HuggingFaceSentimentAnalysisOpDesc
    d.attribute = "text"
    d.resultAttributePositive = "pos"
    d.resultAttributeNeutral = "neu"
    d.resultAttributeNegative = "neg"
    d
  }

  "HuggingFaceSentimentAnalysisOpDesc.operatorInfo" should
    "advertise the name, Hugging Face group, and a 1-in/1-out reconfigurable shape" in {
    val info = (new HuggingFaceSentimentAnalysisOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Hugging Face Sentiment Analysis"
    info.operatorDescription shouldBe
      "Analyzing Sentiments with a Twitter-Based Model from Hugging Face"
    info.operatorGroupName shouldBe OperatorGroupConstants.HUGGINGFACE_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    info.supportReconfiguration shouldBe true
  }

  "HuggingFaceSentimentAnalysisOpDesc" should "default all column fields to null" in {
    val d = new HuggingFaceSentimentAnalysisOpDesc
    d.attribute shouldBe null
    d.resultAttributePositive shouldBe null
    d.resultAttributeNeutral shouldBe null
    d.resultAttributeNegative shouldBe null
  }

  "HuggingFaceSentimentAnalysisOpDesc.getOutputSchemas" should
    "return null when any result column is unset" in {
    val d = new HuggingFaceSentimentAnalysisOpDesc
    val in = Schema().add("text", AttributeType.STRING)
    d.getOutputSchemas(Map(d.operatorInfo.inputPorts.head.id -> in)) shouldBe null
  }

  it should "append the three sentiment columns as DOUBLE, keyed by the declared output port" in {
    val d = configured()
    val in = Schema().add("text", AttributeType.STRING)
    val out = d.getOutputSchemas(Map(d.operatorInfo.inputPorts.head.id -> in))
    val schema = out(d.operatorInfo.outputPorts.head.id)
    schema.getAttribute("text").getType shouldBe AttributeType.STRING
    schema.getAttribute("pos").getType shouldBe AttributeType.DOUBLE
    schema.getAttribute("neu").getType shouldBe AttributeType.DOUBLE
    schema.getAttribute("neg").getType shouldBe AttributeType.DOUBLE
  }

  "HuggingFaceSentimentAnalysisOpDesc.generatePythonCode" should
    "emit the cardiffnlp sentiment operator carrying the configured columns (encoded)" in {
    val d = configured()
    val code = d.generatePythonCode()
    code should include("class ProcessTupleOperator(UDFOperatorV2)")
    code should include("cardiffnlp/twitter-roberta-base-sentiment-latest")
    code should include("from scipy.special import softmax")
    code should include("self.decode_python_template(")
    carries(code, "text") shouldBe true
    carries(code, "pos") shouldBe true
    // EncodableString columns are base64-encoded, not embedded raw.
    code should not include "\"text\"]"
  }

  "HuggingFaceSentimentAnalysisOpDesc.getPhysicalOp" should
    "wire an OpExecWithCode python executor carrying the operator's ports" in {
    val d = configured()
    val physical = d.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithCode(_, language) => language shouldBe "python"
      case other                       => fail(s"expected OpExecWithCode, got $other")
    }
    physical.inputPorts.keySet shouldBe d.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }

  "HuggingFaceSentimentAnalysisOpDesc" should
    "round-trip its config fields through the polymorphic base" in {
    val d = configured()
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[HuggingFaceSentimentAnalysisOpDesc]
    val h = restored.asInstanceOf[HuggingFaceSentimentAnalysisOpDesc]
    h.attribute shouldBe "text"
    h.resultAttributePositive shouldBe "pos"
    h.resultAttributeNeutral shouldBe "neu"
    h.resultAttributeNegative shouldBe "neg"
  }
}
