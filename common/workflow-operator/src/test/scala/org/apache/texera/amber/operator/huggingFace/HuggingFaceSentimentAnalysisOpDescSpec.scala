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

import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
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

  /** The exact runtime decode expression an EncodableString column renders into. */
  private def decodeSite(name: String): String =
    s"self.decode_python_template('${b64(name)}')"

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
    "append the three sentiment columns as DOUBLE, keyed by the declared output port" in {
    val d = configured()
    val in = Schema().add("text", AttributeType.STRING)
    val out = d.getOutputSchemas(Map(d.operatorInfo.inputPorts.head.id -> in))
    val schema = out(d.operatorInfo.outputPorts.head.id)
    schema.getAttribute("text").getType shouldBe AttributeType.STRING
    schema.getAttribute("pos").getType shouldBe AttributeType.DOUBLE
    schema.getAttribute("neu").getType shouldBe AttributeType.DOUBLE
    schema.getAttribute("neg").getType shouldBe AttributeType.DOUBLE
  }

  private val textInput = Schema().add("text", AttributeType.STRING)

  private def schemaError(d: HuggingFaceSentimentAnalysisOpDesc, in: Schema = textInput): String =
    intercept[RuntimeException] {
      d.getOutputSchemas(Map(d.operatorInfo.inputPorts.head.id -> in))
    }.getMessage

  it should "reject each unset or blank result column, naming the field" in {
    // Split per column: a copy-pasted guard that re-tests one field in another's
    // clause would pass an all-unset case, and naming the field catches it.
    val fields: Seq[(String, (HuggingFaceSentimentAnalysisOpDesc, String) => Unit)] = Seq(
      "Positive result attribute" -> ((d, v) => d.resultAttributePositive = v),
      "Neutral result attribute" -> ((d, v) => d.resultAttributeNeutral = v),
      "Negative result attribute" -> ((d, v) => d.resultAttributeNegative = v)
    )
    for ((title, set) <- fields; empty <- Seq(null, "", "   ", "\t")) {
      withClue(s"$title = [$empty]: ") {
        val d = configured()
        set(d, empty)
        schemaError(d) shouldBe s"$title cannot be empty"
      }
    }
  }

  it should "reject an unset input attribute" in {
    val d = configured()
    d.attribute = "  "
    schemaError(d) shouldBe "Attribute cannot be empty"
  }

  it should "reject an input attribute that is not in the input table, listing the columns" in {
    val d = configured()
    d.attribute = "review"
    schemaError(d) shouldBe "Attribute 'review' is not in the input table. Available columns: text"
  }

  it should "match the input attribute's case exactly, as the generated Python does" in {
    val d = configured()
    d.attribute = "TEXT"
    schemaError(d) should startWith("Attribute 'TEXT' is not in the input table")
  }

  it should "leave a readable error on its output port when the compiler propagates the schema" in {
    // The compiler runs getOutputSchemas inside a Try and stores a thrown error on the
    // output port. A returned null escaped that Try as a NullPointerException instead.
    val d = configured()
    d.resultAttributeNeutral = null
    val physical = d
      .getPhysicalOp(workflowId, executionId)
      .propagateSchema(Some(d.operatorInfo.inputPorts.head.id -> textInput))
    val port = physical.outputPorts(d.operatorInfo.outputPorts.head.id)._3
    port.left.map(_.getMessage) shouldBe Left("Neutral result attribute cannot be empty")
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

    // `carries` is a bare substring probe, so it stays true no matter which model label
    // a column is attached to. The label -> column map is the whole point of this
    // operator: `labels[self.config.id2label[...]]` is what decides whether a negative
    // score lands in the negative column, so pin the PAIRING, not just the presence.
    val labelsLine = code.linesIterator
      .find(_.contains("labels = {"))
      .getOrElse(fail("generated code no longer builds the label->column map"))
    labelsLine should include("\"positive\": " + decodeSite("pos"))
    labelsLine should include("\"neutral\": " + decodeSite("neu"))
    labelsLine should include("\"negative\": " + decodeSite("neg"))
  }

  it should "guard an empty text cell before it reaches the tokenizer" in {
    val d = configured()
    val code = d.generatePythonCode()

    // An empty cell arrives as None, and the tokenizer answers it with
    // `ValueError: You need to specify either text or text_target`, ending the run.
    // pandas is asked rather than None compared, because the type rule naming the
    // column string is a warning the editor prints, not a filter, and a numeric
    // column reaches the executor with its own NaN.
    val guard = code.linesIterator
      .find(_.contains("pd.isna(text)"))
      .getOrElse(fail("generated code no longer guards an empty text cell"))
    guard should include("strip()")
    code should include("import pandas as pd")
    code.indexOf("pd.isna(text)") should be < code.indexOf("self.tokenizer(")

    // getOutputSchemas advertises all three score columns on the output port, so the
    // row this path yields has to carry all three keys -- a fill loop that blanks only
    // two of them emits a tuple that does not match the schema the operator declared.
    val fill = code.linesIterator
      .find(_.contains("for label in ("))
      .getOrElse(fail("generated code no longer blanks the score columns for an empty cell"))
    Seq("pos", "neu", "neg").foreach { column =>
      withClue(s"$column missing from the empty-cell fill: ") {
        fill should include(decodeSite(column))
      }
    }
  }

  // The script reads a frame, where a column holding nothing else comes back as
  // float64 and its cells as NaN, which `is None` does not catch.
  it should "guard a missing text cell in the exported script, NaN included" in {
    val d = configured()
    val code = d.generateStandaloneCode()

    val guard = code.linesIterator
      .find(_.contains("pd.isna(_text)"))
      .getOrElse(fail("exported script no longer guards a missing text cell"))
    guard should include("strip()")
    code should include("import pandas as pd")
    code.indexOf("pd.isna(_text)") should be < code.indexOf("tokenizer(_text")
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

  "HuggingFaceSentimentAnalysisOpDesc (class-level)" should
    "carry @JsonSchemaInject restricting `attribute` to STRING columns" in {
    val ann = classOf[HuggingFaceSentimentAnalysisOpDesc].getAnnotation(classOf[JsonSchemaInject])
    ann should not be null
    val payload = ann.json
    payload should include("attributeTypeRules")
    payload should include("attribute")
    payload should include("string")
  }
}
