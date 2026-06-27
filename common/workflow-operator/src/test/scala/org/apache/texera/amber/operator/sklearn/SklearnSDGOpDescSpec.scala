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

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SklearnSDGOpDescSpec extends AnyFlatSpec with Matchers {

  "SklearnSDGOpDesc.operatorInfo" should
    "advertise the model name, Sklearn group, and the training/testing port shape" in {
    val info = (new SklearnSDGOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Stochastic Gradient Descent"
    info.operatorDescription shouldBe "Sklearn Stochastic Gradient Descent Operator"
    info.operatorGroupName shouldBe OperatorGroupConstants.SKLEARN_GROUP
    info.inputPorts.map(_.displayName) shouldBe List("training", "testing")
    info.outputPorts should have length 1
    info.outputPorts.head.blocking shouldBe true
  }

  "SklearnSDGOpDesc" should "default its config fields" in {
    val d = new SklearnSDGOpDesc
    d.countVectorizer shouldBe false
    d.tfidfTransformer shouldBe false
    d.target shouldBe null
    d.text shouldBe null
  }

  "SklearnSDGOpDesc.getOutputSchemas" should
    "emit the model_name/model schema keyed by the declared output port" in {
    val d = new SklearnSDGOpDesc
    val schema = d.getOutputSchemas(Map.empty)(d.operatorInfo.outputPorts.head.id)
    schema.getAttribute("model_name").getType shouldBe AttributeType.STRING
    schema.getAttribute("model").getType shouldBe AttributeType.BINARY
  }

  "SklearnSDGOpDesc.generatePythonCode" should "import the configured sklearn estimator" in {
    val d = new SklearnSDGOpDesc
    d.target = "y"
    val code = d.generatePythonCode()
    code should include("from sklearn.linear_model import SGDClassifier")
    code should include("make_pipeline")
    code should include("Stochastic Gradient Descent")
  }

  "SklearnSDGOpDesc" should "round-trip its config fields through the polymorphic base" in {
    val d = new SklearnSDGOpDesc
    d.target = "label"
    d.countVectorizer = true
    val json = objectMapper.writeValueAsString(d)
    json should include("\"operatorType\":\"SklearnSDG\"")
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[SklearnSDGOpDesc]
    val r = restored.asInstanceOf[SklearnSDGOpDesc]
    r.target shouldBe "label"
    r.countVectorizer shouldBe true
  }
}
