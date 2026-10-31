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

class HuggingFaceIrisLogisticRegressionOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  // EncodableString fields are always base64-wrapped in .encode mode
  // (self.decode_python_template('<base64>')), so assert on the base64 form only rather than
  // the raw column name, which could appear in the generated Python for unrelated reasons.
  private def carries(output: String, name: String): Boolean =
    output.contains(b64(name))

  private def configured(): HuggingFaceIrisLogisticRegressionOpDesc = {
    val d = new HuggingFaceIrisLogisticRegressionOpDesc
    d.petalLengthCmAttribute = "petalLength"
    d.petalWidthCmAttribute = "petalWidth"
    d.predictionClassName = "species"
    d.predictionProbabilityName = "probability"
    d
  }

  "HuggingFaceIrisLogisticRegressionOpDesc.operatorInfo" should
    "advertise the name, Hugging Face group, and a 1-in/1-out shape" in {
    val info = (new HuggingFaceIrisLogisticRegressionOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Hugging Face Iris Logistic Regression"
    info.operatorDescription shouldBe
      "Predict whether an iris is an Iris-setosa using a pre-trained logistic regression model"
    info.operatorGroupName shouldBe OperatorGroupConstants.HUGGINGFACE_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "HuggingFaceIrisLogisticRegressionOpDesc" should "default all column fields to null" in {
    val d = new HuggingFaceIrisLogisticRegressionOpDesc
    d.petalLengthCmAttribute shouldBe null
    d.petalWidthCmAttribute shouldBe null
    d.predictionClassName shouldBe null
    d.predictionProbabilityName shouldBe null
  }

  "HuggingFaceIrisLogisticRegressionOpDesc.getOutputSchemas" should
    "append a STRING class column and a DOUBLE probability column, keyed by the output port" in {
    val d = configured()
    val in = Schema()
      .add("sepal", AttributeType.STRING)
      .add("petalLength", AttributeType.DOUBLE)
      .add("petalWidth", AttributeType.DOUBLE)
    val out = d.getOutputSchemas(Map(d.operatorInfo.inputPorts.head.id -> in))
    val schema = out(d.operatorInfo.outputPorts.head.id)
    schema.getAttribute("sepal").getType shouldBe AttributeType.STRING
    schema.getAttribute("species").getType shouldBe AttributeType.STRING
    schema.getAttribute("probability").getType shouldBe AttributeType.DOUBLE
  }

  private def petals(
      lengthType: AttributeType = AttributeType.DOUBLE,
      widthType: AttributeType = AttributeType.DOUBLE
  ): Schema = Schema().add("petalLength", lengthType).add("petalWidth", widthType)

  private def schemaError(
      d: HuggingFaceIrisLogisticRegressionOpDesc,
      in: Schema = petals()
  ): String =
    intercept[RuntimeException] {
      d.getOutputSchemas(Map(d.operatorInfo.inputPorts.head.id -> in))
    }.getMessage

  it should "reject each unset or blank prediction result name, naming the field" in {
    for (empty <- Seq(null, "", "   ")) {
      withClue(s"[$empty]: ") {
        val noClass = configured()
        noClass.predictionClassName = empty
        schemaError(noClass) shouldBe "Prediction class name cannot be empty"

        val noProbability = configured()
        noProbability.predictionProbabilityName = empty
        schemaError(noProbability) shouldBe "Prediction probability name cannot be empty"
      }
    }
  }

  it should "reject an unset petal column" in {
    val noLength = configured()
    noLength.petalLengthCmAttribute = null
    schemaError(noLength) shouldBe "Petal Length Cm Attribute cannot be empty"

    val noWidth = configured()
    noWidth.petalWidthCmAttribute = ""
    schemaError(noWidth) shouldBe "Petal Width Cm Attribute cannot be empty"
  }

  it should "reject a petal column that is not in the input table, listing the columns" in {
    val d = configured()
    d.petalWidthCmAttribute = "petal_width"
    schemaError(d) shouldBe
      "Petal Width Cm Attribute 'petal_width' is not in the input table. " +
        "Available columns: petalLength, petalWidth"
  }

  it should "match a petal column's case exactly, as the generated Python does" in {
    val d = configured()
    d.petalLengthCmAttribute = "PetalLength"
    schemaError(d) should startWith(
      "Petal Length Cm Attribute 'PetalLength' is not in the input table"
    )
  }

  it should "reject a petal column that is not numeric" in {
    schemaError(configured(), petals(lengthType = AttributeType.STRING)) shouldBe
      "Petal Length Cm Attribute 'petalLength' must be numeric (integer, long or double), not string"
    schemaError(configured(), petals(widthType = AttributeType.BOOLEAN)) shouldBe
      "Petal Width Cm Attribute 'petalWidth' must be numeric (integer, long or double), not boolean"
  }

  it should "accept integer, long and double petal columns" in {
    for (numeric <- Seq(AttributeType.INTEGER, AttributeType.LONG, AttributeType.DOUBLE)) {
      val d = configured()
      val in = petals(numeric, numeric)
      noException should be thrownBy d.getOutputSchemas(
        Map(d.operatorInfo.inputPorts.head.id -> in)
      )
    }
  }

  it should "leave a readable error on its output port when the compiler propagates the schema" in {
    val d = configured()
    d.predictionClassName = null
    val physical = d
      .getPhysicalOp(workflowId, executionId)
      .propagateSchema(Some(d.operatorInfo.inputPorts.head.id -> petals()))
    val port = physical.outputPorts(d.operatorInfo.outputPorts.head.id)._3
    port.left.map(_.getMessage) shouldBe Left("Prediction class name cannot be empty")
  }

  "HuggingFaceIrisLogisticRegressionOpDesc.generatePythonCode" should
    "emit the logistic-regression operator carrying the configured columns (encoded)" in {
    val d = configured()
    val code = d.generatePythonCode()
    code should include("class ProcessTupleOperator(UDFOperatorV2)")
    code should include("LinearModel.from_pretrained")
    code should include("sadhaklal/logistic-regression-iris")
    code should include("self.decode_python_template(")
    carries(code, "petalLength") shouldBe true
    carries(code, "species") shouldBe true
  }

  // An empty cell reaches the standardization as a None, which numpy cannot
  // subtract from, and a NaN is no measurement either, so the row is answered
  // rather than ending the run or predicting from it, as the export does.
  it should "leave the prediction empty when a measurement is missing, NaN included" in {
    val d = configured()
    val code = d.generatePythonCode()
    code should include("if pd.isna(length) or pd.isna(width):")
    code should include("yield tuple_")
    code should include("return")
  }

  "HuggingFaceIrisLogisticRegressionOpDesc.getPhysicalOp" should
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

  "HuggingFaceIrisLogisticRegressionOpDesc" should
    "round-trip its config fields through the polymorphic base" in {
    val d = configured()
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[HuggingFaceIrisLogisticRegressionOpDesc]
    val h = restored.asInstanceOf[HuggingFaceIrisLogisticRegressionOpDesc]
    h.petalLengthCmAttribute shouldBe "petalLength"
    h.petalWidthCmAttribute shouldBe "petalWidth"
    h.predictionClassName shouldBe "species"
    h.predictionProbabilityName shouldBe "probability"
  }

  "HuggingFaceIrisLogisticRegressionOpDesc (class-level)" should
    "carry @JsonSchemaInject restricting both petal columns to numeric attributes" in {
    val ann =
      classOf[HuggingFaceIrisLogisticRegressionOpDesc].getAnnotation(classOf[JsonSchemaInject])
    ann should not be null
    val payload = ann.json
    payload should include("attributeTypeRules")
    payload should include("petalLengthCmAttribute")
    payload should include("petalWidthCmAttribute")
    payload should include("integer")
    payload should include("long")
    payload should include("double")
  }
}
