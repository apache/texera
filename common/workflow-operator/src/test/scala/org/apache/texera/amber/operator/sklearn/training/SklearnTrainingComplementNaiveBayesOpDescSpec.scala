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

package org.apache.texera.amber.operator.sklearn.training

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SklearnTrainingComplementNaiveBayesOpDescSpec extends AnyFlatSpec with Matchers {

  "SklearnTrainingComplementNaiveBayesOpDesc.operatorInfo" should
    "advertise the model name, Sklearn Training group, and the single training port" in {
    val info = (new SklearnTrainingComplementNaiveBayesOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Training: Complement Naive Bayes"
    info.operatorDescription shouldBe "Sklearn Training: Complement Naive Bayes Operator"
    info.operatorGroupName shouldBe OperatorGroupConstants.SKLEARN_TRAINING_GROUP
    info.inputPorts.map(_.displayName) shouldBe List("training")
    info.outputPorts should have length 1
    info.outputPorts.head.blocking shouldBe true
  }

  "SklearnTrainingComplementNaiveBayesOpDesc" should "default its config fields" in {
    val d = new SklearnTrainingComplementNaiveBayesOpDesc
    d.countVectorizer shouldBe false
    d.tfidfTransformer shouldBe false
    d.target shouldBe null
    d.text shouldBe null
  }

  "SklearnTrainingComplementNaiveBayesOpDesc.getOutputSchemas" should
    "emit the model_name/model schema keyed by the declared output port" in {
    val d = new SklearnTrainingComplementNaiveBayesOpDesc
    val schema = d.getOutputSchemas(Map.empty)(d.operatorInfo.outputPorts.head.id)
    schema.getAttribute("model_name").getType shouldBe AttributeType.STRING
    schema.getAttribute("model").getType shouldBe AttributeType.BINARY
  }

  "SklearnTrainingComplementNaiveBayesOpDesc.generatePythonCode" should "import the configured sklearn estimator" in {
    val d = new SklearnTrainingComplementNaiveBayesOpDesc
    d.target = "y"
    val code = d.generatePythonCode()
    code should include("from sklearn.naive_bayes import ComplementNB")
    code should include("make_pipeline")
    code should include("Training: Complement Naive Bayes")
  }

  "SklearnTrainingComplementNaiveBayesOpDesc" should "round-trip its config fields through the polymorphic base" in {
    val d = new SklearnTrainingComplementNaiveBayesOpDesc
    d.target = "label"
    d.countVectorizer = true
    val json = objectMapper.writeValueAsString(d)
    json should include("\"operatorType\":\"SklearnTrainingComplementNaiveBayes\"")
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[SklearnTrainingComplementNaiveBayesOpDesc]
    val r = restored.asInstanceOf[SklearnTrainingComplementNaiveBayesOpDesc]
    r.target shouldBe "label"
    r.countVectorizer shouldBe true
  }
}
