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
import org.apache.texera.amber.operator.sklearn.training.SklearnTrainingOpDesc
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.reflect.ClassTag

/**
  * The five tests every per-model sklearn descriptor spec shares. Each estimator
  * ships a classifier descriptor (training and testing ports, Sklearn group) and a
  * training-only descriptor (one training port, Sklearn Training group), and both
  * pin the same contracts: operatorInfo, the config fields' defaults, the
  * model_name/model output schema, the estimator import in the generated code, and
  * a JSON round-trip through the polymorphic `LogicalOp` base.
  *
  * Each spec passes its JSON `operatorType`, model name and sklearn import as
  * literals rather than reading them back from the descriptor, so a renamed
  * model or a changed import still fails that model's own spec. The
  * `operatorType` is what saved workflows persist, so it is pinned on its own
  * rather than derived from the class name: a class can be renamed while it
  * keeps its persisted name. Test names keep the
  * `<Descriptor>[.<member>] should ...` form, so a model's spec reports the same
  * names as before.
  *
  * `newDesc` is by-name: every test builds its own descriptor.
  */
trait SklearnModelOpDescBehaviors extends AnyFlatSpecLike with Matchers {

  /** Registers the shared tests for a classifier descriptor. */
  protected def sklearnClassifier[T <: SklearnClassifierOpDesc: ClassTag](
      newDesc: => T,
      operatorType: String,
      modelName: String,
      estimatorImport: String,
      estimatorTestName: String = "import the configured sklearn estimator"
  ): Unit =
    sklearnModel(
      newDesc,
      operatorType,
      modelName,
      estimatorImport,
      estimatorTestName,
      operatorInfoTestName =
        "advertise the model name, Sklearn group, and the training/testing port shape",
      group = OperatorGroupConstants.SKLEARN_GROUP,
      inputPorts = List("training", "testing")
    )

  /** Registers the shared tests for a training-only descriptor. */
  protected def sklearnTrainer[T <: SklearnTrainingOpDesc: ClassTag](
      newDesc: => T,
      operatorType: String,
      modelName: String,
      estimatorImport: String
  ): Unit =
    sklearnModel(
      newDesc,
      operatorType,
      modelName,
      estimatorImport,
      estimatorTestName = "import the configured sklearn estimator",
      operatorInfoTestName =
        "advertise the model name, Sklearn Training group, and the single training port",
      group = OperatorGroupConstants.SKLEARN_TRAINING_GROUP,
      inputPorts = List("training")
    )

  private def sklearnModel[T <: SklearnModelOpDesc: ClassTag](
      newDesc: => T,
      operatorType: String,
      modelName: String,
      estimatorImport: String,
      estimatorTestName: String,
      operatorInfoTestName: String,
      group: String,
      inputPorts: List[String]
  ): Unit = {
    val descriptor = implicitly[ClassTag[T]].runtimeClass.getSimpleName

    s"$descriptor.operatorInfo" should operatorInfoTestName in {
      val info = newDesc.operatorInfo
      info.userFriendlyName shouldBe modelName
      info.operatorDescription shouldBe s"Sklearn $modelName Operator"
      info.operatorGroupName shouldBe group
      info.inputPorts.map(_.displayName) shouldBe inputPorts
      info.outputPorts should have length 1
      info.outputPorts.head.blocking shouldBe true
    }

    descriptor should "default its config fields" in {
      val d = newDesc
      d.countVectorizer shouldBe false
      d.tfidfTransformer shouldBe false
      d.target shouldBe null
      d.text shouldBe empty
    }

    s"$descriptor.getOutputSchemas" should
      "emit the model_name/model schema keyed by the declared output port" in {
      val d = newDesc
      val schema = d.getOutputSchemas(Map.empty)(d.operatorInfo.outputPorts.head.id)
      schema.getAttribute("model_name").getType shouldBe AttributeType.STRING
      schema.getAttribute("model").getType shouldBe AttributeType.BINARY
    }

    s"$descriptor.generatePythonCode" should estimatorTestName in {
      val d = newDesc
      d.target = "y"
      val code = d.generatePythonCode()
      code should include(estimatorImport)
      code should include("make_pipeline")
      code should include(modelName)
    }

    descriptor should "round-trip its config fields through the polymorphic base" in {
      val d = newDesc
      d.target = "label"
      d.countVectorizer = true
      val json = objectMapper.writeValueAsString(d)
      json should include("\"operatorType\":\"" + operatorType + "\"")
      val restored = objectMapper.readValue(json, classOf[LogicalOp])
      restored shouldBe a[T]
      val r = restored.asInstanceOf[SklearnModelOpDesc]
      r.target shouldBe "label"
      r.countVectorizer shouldBe true
    }
  }
}
