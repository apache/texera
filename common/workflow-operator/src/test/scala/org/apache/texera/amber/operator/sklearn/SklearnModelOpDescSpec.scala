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

package org.apache.texera.amber.operator.sklearn

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SklearnModelOpDescSpec extends AnyFlatSpec with Matchers {

  // Minimal concrete subclass: SklearnModelOpDesc is abstract and leaves the
  // model-specific pieces and operatorInfo unimplemented. Only the shared
  // getOutputSchemas contract and the default flag values are under test here.
  private class TestSklearnModelOpDesc extends SklearnModelOpDesc {
    override def getImportStatements: String =
      "from sklearn.linear_model import LogisticRegression"
    override def getUserFriendlyModelName: String = "Test Model"
    override def generatePythonCode(): String = ""
    // dropMissingRows is protected for the codegen bases; reach it from inside.
    def generateDropForTest: String = dropMissingRows
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        getUserFriendlyModelName,
        "Sklearn " + getUserFriendlyModelName + " Operator",
        OperatorGroupConstants.SKLEARN_GROUP,
        inputPorts = List(InputPort(PortIdentity(), "training")),
        outputPorts = List(OutputPort())
      )
  }

  "SklearnModelOpDesc" should "default the vectorizer and transformer flags to false" in {
    val d = new TestSklearnModelOpDesc
    d.countVectorizer shouldBe false
    d.tfidfTransformer shouldBe false
  }

  // The safe default is the estimator that cannot take a NaN, so an operator only
  // keeps incomplete rows when its own class says the estimator places them.
  it should "assume an estimator cannot fit a missing value" in {
    (new TestSklearnModelOpDesc).handlesMissingValues shouldBe false
  }

  "SklearnModelOpDesc.dropMissingRows" should
    "drop on every column when the estimator cannot fit a missing value" in {
    val d = new TestSklearnModelOpDesc
    d.target = "y"
    d.generateDropForTest shouldBe "table.dropna()"
  }

  // The target is refused by every estimator, so it is dropped even here, but a blank
  // feature is left in place for the estimator to make its own use of.
  it should "drop on the target alone when the estimator places a missing value" in {
    val d = new TestSklearnModelOpDesc {
      override def handlesMissingValues = true
    }
    d.target = "y"
    d.generateDropForTest should include("dropna(subset=[")
    d.generateDropForTest should not be "table.dropna()"
  }

  // CountVectorizer calls .lower() on each document, which a None does not answer, so
  // every column it reads goes even for an estimator that would otherwise keep the row.
  it should "drop on the text columns too when the vectorizer is on" in {
    val d = new TestSklearnModelOpDesc {
      override def handlesMissingValues = true
    }
    d.target = "y"
    d.text = List("note", "body")
    d.countVectorizer = true
    // the two text columns and the target, each named through the decoder
    d.generateDropForTest.split("decode_python_template").length - 1 shouldBe 3
  }

  "SklearnModelOpDesc.getOutputSchemas" should
    "key the single output schema by the operator's output port id" in {
    val d = new TestSklearnModelOpDesc
    val out = d.getOutputSchemas(Map.empty)
    out.keySet shouldBe Set(d.operatorInfo.outputPorts.head.id)
  }

  it should "produce a two-column model schema of model_name:STRING and model:BINARY" in {
    val d = new TestSklearnModelOpDesc
    val schema = d.getOutputSchemas(Map.empty)(d.operatorInfo.outputPorts.head.id)
    schema.getAttributeNames shouldBe List("model_name", "model")
    schema.getAttribute("model_name").getType shouldBe AttributeType.STRING
    schema.getAttribute("model").getType shouldBe AttributeType.BINARY
  }

  it should "ignore the input schemas entirely (fixed output regardless of input)" in {
    val d = new TestSklearnModelOpDesc
    val fromEmpty = d.getOutputSchemas(Map.empty)
    val arbitraryInput = Map(
      PortIdentity() -> Schema().add("anything", AttributeType.DOUBLE),
      PortIdentity(1) -> Schema().add("other", AttributeType.INTEGER)
    )
    d.getOutputSchemas(arbitraryInput) shouldBe fromEmpty
  }

  it should "let Count Vectorizer through for an estimator that named no alternative" in {
    // The default, and what every estimator but GaussianNB relies on: the sparse
    // matrix is what the others accept.
    val d = new TestSklearnModelOpDesc
    d.countVectorizer = true
    d.getOutputSchemas(Map.empty).keySet shouldBe Set(d.operatorInfo.outputPorts.head.id)
  }

  it should "reject Count Vectorizer for an estimator that named one" in {
    val d = new TestSklearnModelOpDesc {
      override protected def countVectorizerAlternatives: Option[String] = Some("Some Other Model")
    }
    d.countVectorizer = true
    val thrown = intercept[RuntimeException](d.getOutputSchemas(Map.empty))
    thrown.getMessage should include("Test Model")
    thrown.getMessage should include("Count Vectorizer")
    thrown.getMessage should include("Some Other Model")
  }

  it should "stay silent while Count Vectorizer is off, whatever the estimator" in {
    // The switch defaults to off, so a freshly dropped operator must not be
    // reported invalid before anyone has configured it.
    val d = new TestSklearnModelOpDesc {
      override protected def countVectorizerAlternatives: Option[String] = Some("Some Other Model")
    }
    d.getOutputSchemas(Map.empty).keySet shouldBe Set(d.operatorInfo.outputPorts.head.id)
  }

  it should "reject the target named as a text column" in {
    val d = new TestSklearnModelOpDesc
    d.countVectorizer = true
    d.target = "species"
    d.text = List("note", "species")
    val thrown = intercept[RuntimeException](d.getOutputSchemas(Map.empty))
    thrown.getMessage should include("species")
    thrown.getMessage should include("Target Attribute")
    thrown.getMessage should include("Text Attribute")
  }

  it should "let the text columns through while none of them is the target" in {
    val d = new TestSklearnModelOpDesc
    d.countVectorizer = true
    d.target = "species"
    d.text = List("note")
    d.getOutputSchemas(Map.empty).keySet shouldBe Set(d.operatorInfo.outputPorts.head.id)
  }

  it should "leave a stale text column alone while Count Vectorizer is off" in {
    // Nothing reads `text` with the switch off, and the panel hides it, so a value
    // left behind by an earlier configuration must not report the operator invalid.
    val d = new TestSklearnModelOpDesc
    d.target = "species"
    d.text = List("species")
    d.getOutputSchemas(Map.empty).keySet shouldBe Set(d.operatorInfo.outputPorts.head.id)
  }
}
