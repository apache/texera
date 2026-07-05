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

class HuggingFaceSpamSMSDetectionOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  // EncodableString fields are always base64-wrapped in .encode mode
  // (self.decode_python_template('<base64>')), so assert on the base64 form only: the raw
  // column name can appear in the generated Python for unrelated reasons (e.g. "text" in the
  // "text-classification" task string, "score" in result["score"]), masking a missing splice.
  private def carries(output: String, name: String): Boolean =
    output.contains(b64(name))

  private def configured(): HuggingFaceSpamSMSDetectionOpDesc = {
    val d = new HuggingFaceSpamSMSDetectionOpDesc
    d.attribute = "text"
    d.resultAttributeSpam = "is_spam"
    d.resultAttributeProbability = "score"
    d
  }

  "HuggingFaceSpamSMSDetectionOpDesc.operatorInfo" should
    "advertise the name, Hugging Face group, and a 1-in/1-out shape" in {
    val info = (new HuggingFaceSpamSMSDetectionOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Hugging Face Spam Detection"
    info.operatorDescription shouldBe "Spam Detection by SMS Spam Detection Model from Hugging Face"
    info.operatorGroupName shouldBe OperatorGroupConstants.HUGGINGFACE_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "HuggingFaceSpamSMSDetectionOpDesc" should "default all column fields to null" in {
    val d = new HuggingFaceSpamSMSDetectionOpDesc
    d.attribute shouldBe null
    d.resultAttributeSpam shouldBe null
    d.resultAttributeProbability shouldBe null
  }

  "HuggingFaceSpamSMSDetectionOpDesc.getOutputSchemas" should
    "append a BOOLEAN spam column and a DOUBLE score column, keyed by the declared output port" in {
    val d = configured()
    val in = Schema().add("msg", AttributeType.STRING)
    val out = d.getOutputSchemas(Map(d.operatorInfo.inputPorts.head.id -> in))
    val schema = out(d.operatorInfo.outputPorts.head.id)
    schema.getAttribute("msg").getType shouldBe AttributeType.STRING
    schema.getAttribute("is_spam").getType shouldBe AttributeType.BOOLEAN
    schema.getAttribute("score").getType shouldBe AttributeType.DOUBLE
  }

  "HuggingFaceSpamSMSDetectionOpDesc.generatePythonCode" should
    "emit the spam-detection pipeline carrying the configured columns (encoded)" in {
    val d = configured()
    val code = d.generatePythonCode()
    code should include("from transformers import pipeline")
    code should include("class ProcessTupleOperator(UDFOperatorV2)")
    code should include("mrm8488/bert-tiny-finetuned-sms-spam-detection")
    code should include("result[\"label\"] == \"LABEL_1\"")
    code should include("self.decode_python_template(")
    carries(code, "text") shouldBe true
    carries(code, "is_spam") shouldBe true
    carries(code, "score") shouldBe true
  }

  "HuggingFaceSpamSMSDetectionOpDesc.getPhysicalOp" should
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

  "HuggingFaceSpamSMSDetectionOpDesc" should
    "round-trip its config fields through the polymorphic base" in {
    val d = configured()
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[HuggingFaceSpamSMSDetectionOpDesc]
    val h = restored.asInstanceOf[HuggingFaceSpamSMSDetectionOpDesc]
    h.attribute shouldBe "text"
    h.resultAttributeSpam shouldBe "is_spam"
    h.resultAttributeProbability shouldBe "score"
  }
}
