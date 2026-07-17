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

package org.apache.texera.amber.operator.udf.python.source

import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PythonUDFSourceOpDescV2Spec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  "PythonUDFSourceOpDescV2.operatorInfo" should
    "advertise the 1-out Python UDF source (no inputs, one output, reconfigurable)" in {
    val info = (new PythonUDFSourceOpDescV2).operatorInfo
    info.userFriendlyName shouldBe "1-out Python UDF"
    info.operatorGroupName shouldBe OperatorGroupConstants.PYTHON_GROUP
    info.inputPorts shouldBe empty
    info.outputPorts should have length 1
    info.supportReconfiguration shouldBe true
  }

  "PythonUDFSourceOpDescV2.sourceSchema" should "be empty by default and reflect the configured columns" in {
    (new PythonUDFSourceOpDescV2).sourceSchema().getAttributes shouldBe empty
    val d = new PythonUDFSourceOpDescV2
    d.columns = List(new Attribute("a", AttributeType.STRING))
    d.sourceSchema() shouldBe Schema().add(new Attribute("a", AttributeType.STRING))
  }

  "PythonUDFSourceOpDescV2.getPhysicalOp" should
    "wire OpExecWithCode(code, \"python\") as a source op with one output port" in {
    val d = new PythonUDFSourceOpDescV2
    d.code = "yield {'a': 1}"
    val physical = d.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithCode(code, language) =>
        language shouldBe "python"
        code shouldBe "yield {'a': 1}"
      case other => fail(s"expected OpExecWithCode, got $other")
    }
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }

  it should "reject a non-positive worker count" in {
    val d = new PythonUDFSourceOpDescV2
    d.workers = 0
    intercept[IllegalArgumentException] { d.getPhysicalOp(workflowId, executionId) }
  }

  it should "reject a blank virtual-environment name when the default env is disabled" in {
    val d = new PythonUDFSourceOpDescV2
    d.code = "yield"
    d.defaultEnv = false
    d.envName = "   "
    val ex = intercept[RuntimeException] { d.getPhysicalOp(workflowId, executionId) }
    ex.getMessage shouldBe
      "Virtual Environment name is required when not using the default Python environment."
  }

  it should "carry the trimmed virtual-environment name when the default env is disabled" in {
    val d = new PythonUDFSourceOpDescV2
    d.code = "yield"
    d.defaultEnv = false
    d.envName = "  my-venv  "
    d.getPhysicalOp(workflowId, executionId).pveName shouldBe "my-venv"
  }

  it should "mark the op parallelizable with the requested worker count when workers > 1" in {
    val d = new PythonUDFSourceOpDescV2
    d.code = "yield"
    d.workers = 3
    val physical = d.getPhysicalOp(workflowId, executionId)
    physical.parallelizable shouldBe true
    physical.suggestedWorkerNum shouldBe Some(3)
  }

  "PythonUDFSourceOpDescV2" should "round-trip its config fields through the polymorphic base" in {
    val d = new PythonUDFSourceOpDescV2
    d.code = "yield"
    d.workers = 2
    d.defaultEnv = false
    d.envName = "venv"
    d.columns = List(new Attribute("a", AttributeType.STRING))
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[PythonUDFSourceOpDescV2]
    val p = restored.asInstanceOf[PythonUDFSourceOpDescV2]
    p.code shouldBe "yield"
    p.workers shouldBe 2
    p.defaultEnv shouldBe false
    p.envName shouldBe "venv"
    p.columns shouldBe List(new Attribute("a", AttributeType.STRING))
  }
}
