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

package org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.KNNTrainer.SklearnAdvancedKNNClassifierTrainerOpDesc
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SklearnMLOperatorDescriptorSpec extends AnyFlatSpec with Matchers {

  // Exercise the abstract base's contract through a real concrete subclass — no test
  // stub. A stub placed under org.apache.texera.amber.operator.* would be picked up by
  // PythonCodeRawInvalidTextSpec's classpath scanner; using a real subclass avoids that.
  private def newOp(): SklearnMLOperatorDescriptor[_] =
    new SklearnAdvancedKNNClassifierTrainerOpDesc

  "SklearnMLOperatorDescriptor.operatorInfo" should
    "derive name/description and advertise the training + parameter inputs and one output" in {
    val info = newOp().operatorInfo
    info.userFriendlyName shouldBe "KNN Classifier"
    info.operatorDescription shouldBe "Sklearn KNN Classifier Operator"
    info.operatorGroupName shouldBe OperatorGroupConstants.ADVANCED_SKLEARN_GROUP
    info.inputPorts should have length 2
    info.inputPorts.head.id shouldBe PortIdentity(0)
    info.inputPorts.head.displayName shouldBe "training"
    info.inputPorts.last.id shouldBe PortIdentity(1)
    info.inputPorts.last.displayName shouldBe "parameter"
    info.inputPorts.last.dependencies shouldBe List(PortIdentity(0))
    info.outputPorts should have length 1
  }

  "SklearnMLOperatorDescriptor.getOutputSchemas" should
    "produce the fixed Model/Parameters schema keyed by the declared output port" in {
    val op = newOp()
    op.getOutputSchemas(Map.empty) shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> Schema(
        List(
          new Attribute("Model", AttributeType.BINARY),
          new Attribute("Parameters", AttributeType.STRING)
        )
      )
    )
  }

  "SklearnMLOperatorDescriptor" should
    "default paraList to empty, groundTruthAttribute to empty, and selectedFeatures to null" in {
    val op = newOp()
    op.paraList shouldBe empty
    op.groundTruthAttribute shouldBe ""
    op.selectedFeatures shouldBe null
  }
}
