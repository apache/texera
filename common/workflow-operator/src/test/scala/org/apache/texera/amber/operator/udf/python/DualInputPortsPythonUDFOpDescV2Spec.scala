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

package org.apache.texera.amber.operator.udf.python

import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PortIdentity, UnknownPartition}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DualInputPortsPythonUDFOpDescV2Spec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def twoPortSchemas(dataSchema: Schema): Map[PortIdentity, Schema] =
    Map(
      PortIdentity() -> Schema().add(new Attribute("model", AttributeType.STRING)),
      PortIdentity(1) -> dataSchema
    )

  "DualInputPortsPythonUDFOpDescV2.operatorInfo" should
    "advertise the 2-in Python UDF with a model port and a dependent tuples port" in {
    val info = (new DualInputPortsPythonUDFOpDescV2).operatorInfo
    info.userFriendlyName shouldBe "2-in Python UDF"
    info.operatorGroupName shouldBe OperatorGroupConstants.PYTHON_GROUP
    info.inputPorts should have length 2
    info.inputPorts.head.displayName shouldBe "model"
    info.inputPorts(1).displayName shouldBe "tuples"
    info.inputPorts(1).id shouldBe PortIdentity(1)
    info.inputPorts(1).dependencies shouldBe List(PortIdentity(0))
    info.outputPorts should have length 1
  }

  "DualInputPortsPythonUDFOpDescV2.getPhysicalOp" should "reject a non-positive worker count" in {
    val d = new DualInputPortsPythonUDFOpDescV2
    d.workers = 0
    intercept[IllegalArgumentException] {
      d.getPhysicalOp(workflowId, executionId)
    }
  }

  it should "wire a single-worker op with the default environment and unknown partition" in {
    val d = new DualInputPortsPythonUDFOpDescV2
    d.code = "yield t"
    val physical = d.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithCode(code, language) =>
        language shouldBe "python"
        code shouldBe "yield t"
      case other => fail(s"expected OpExecWithCode, got $other")
    }
    physical.parallelizable shouldBe false
    physical.suggestedWorkerNum shouldBe None
    physical.pveName shouldBe ""
    physical.inputPorts.keySet shouldBe d.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
    physical.derivePartition(List.empty) shouldBe UnknownPartition()
  }

  it should "wire a parallelizable op with the suggested worker count when workers > 1" in {
    val d = new DualInputPortsPythonUDFOpDescV2
    d.workers = 2
    val physical = d.getPhysicalOp(workflowId, executionId)
    physical.parallelizable shouldBe true
    physical.suggestedWorkerNum shouldBe Some(2)
  }

  it should "reject a blank virtual-environment name when the default env is disabled" in {
    val d = new DualInputPortsPythonUDFOpDescV2
    d.defaultEnv = false
    d.envName = "   "
    val ex = intercept[RuntimeException] {
      d.getPhysicalOp(workflowId, executionId)
    }
    ex.getMessage shouldBe
      "Virtual Environment name is required when not using the default Python environment."
  }

  it should "trim and carry a custom virtual-environment name" in {
    val d = new DualInputPortsPythonUDFOpDescV2
    d.defaultEnv = false
    d.envName = "  myenv  "
    d.getPhysicalOp(workflowId, executionId).pveName shouldBe "myenv"
  }

  "DualInputPortsPythonUDFOpDescV2 schema propagation" should
    "emit only the output columns when input columns are not retained" in {
    val d = new DualInputPortsPythonUDFOpDescV2
    d.outputColumns = List(new Attribute("res", AttributeType.INTEGER))
    val out = d.getExternalOutputSchemas(
      twoPortSchemas(Schema().add(new Attribute("in", AttributeType.STRING)))
    )
    out shouldBe Map(
      d.operatorInfo.outputPorts.head.id -> Schema().add(
        new Attribute("res", AttributeType.INTEGER)
      )
    )
  }

  it should "retain the tuples-port columns (not the model port) when retaining input" in {
    val d = new DualInputPortsPythonUDFOpDescV2
    d.retainInputColumns = true
    d.outputColumns = List(new Attribute("res", AttributeType.INTEGER))
    val out = d.getExternalOutputSchemas(
      twoPortSchemas(Schema().add(new Attribute("in", AttributeType.STRING)))
    )
    out shouldBe Map(
      d.operatorInfo.outputPorts.head.id -> Schema()
        .add(new Attribute("in", AttributeType.STRING))
        .add(new Attribute("res", AttributeType.INTEGER))
    )
  }

  it should "reject an output column that collides with a retained input column" in {
    val d = new DualInputPortsPythonUDFOpDescV2
    d.retainInputColumns = true
    d.outputColumns = List(new Attribute("dup", AttributeType.INTEGER))
    val ex = intercept[RuntimeException] {
      d.getExternalOutputSchemas(
        twoPortSchemas(Schema().add(new Attribute("dup", AttributeType.STRING)))
      )
    }
    ex.getMessage shouldBe "Column name dup already exists!"
  }

  "DualInputPortsPythonUDFOpDescV2" should
    "round-trip its config fields through the polymorphic base" in {
    val d = new DualInputPortsPythonUDFOpDescV2
    d.code = "print(1)"
    d.workers = 3
    d.retainInputColumns = true
    d.defaultEnv = false
    d.envName = "myenv"
    d.outputColumns = List(new Attribute("res", AttributeType.INTEGER))
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[DualInputPortsPythonUDFOpDescV2]
    val r = restored.asInstanceOf[DualInputPortsPythonUDFOpDescV2]
    r.code shouldBe "print(1)"
    r.workers shouldBe 3
    r.retainInputColumns shouldBe true
    r.defaultEnv shouldBe false
    r.envName shouldBe "myenv"
    r.outputColumns shouldBe List(new Attribute("res", AttributeType.INTEGER))
  }
}
