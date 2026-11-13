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
import org.apache.texera.amber.core.workflow.{PortIdentity, UnknownPartition}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.operator.udf.UdfDescriptorSpecSupport
import org.scalatest.flatspec.AnyFlatSpec

class PythonUDFOpDescV2Spec extends AnyFlatSpec with UdfDescriptorSpecSupport {

  "PythonUDFOpDescV2.operatorInfo" should
    "advertise the name, Python group, dynamic ports, and a default 1-in/1-out shape" in {
    val info = expectOperatorInfo(
      new PythonUDFOpDescV2,
      "Python UDF",
      OperatorGroupConstants.PYTHON_GROUP,
      1
    )
    info.dynamicInputPorts shouldBe true
    info.dynamicOutputPorts shouldBe true
  }

  "PythonUDFOpDescV2" should "default code/workers/flags" in {
    val d = new PythonUDFOpDescV2
    d.code shouldBe ""
    d.workers shouldBe 1
    d.retainInputColumns shouldBe false
    d.defaultEnv shouldBe true
  }

  "PythonUDFOpDescV2.getPhysicalOp" should
    "wire OpExecWithCode(code, \"python\") and carry port identities" in {
    val d = new PythonUDFOpDescV2
    d.code = "yield t"
    val physical = buildPhysicalOp(d)
    physical.opExecInitInfo shouldBe OpExecWithCode("yield t", "python")
    physical.inputPorts.keySet shouldBe d.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }

  it should "reject a non-positive worker count" in {
    expectInvalidWorkerCount(new PythonUDFOpDescV2)(_.workers = 0)
  }

  it should "inject configured UI parameters into the generated Python code" in {
    val d = new PythonUDFOpDescV2
    d.code = tupleUdfCode
    d.uiParameters = List(uiParameter("count", "7"))

    expectInjectedPython(d)
  }

  it should "defer nothing when no parameter names a resource" in {
    val d = new PythonUDFOpDescV2
    d.code = tupleUdfCode
    d.uiParameters = List(uiParameter("count", "7"))
    buildPhysicalOp(d).executionTimeBinding shouldBe None
  }

  it should "leave a resource parameter to the execution, compiling without resolving it" in {
    val d = new PythonUDFOpDescV2
    d.code = tupleUdfCode
    d.uiParameters = List(datasetParameter("DS", "/dataset/owner@x.com/ds/v1"))
    // Resolving needs a database, which this suite has none of, so compiling must not try.
    buildPhysicalOp(d).executionTimeBinding should not be empty
  }

  it should "refuse a resource parameter whose version was never chosen" in {
    val d = new PythonUDFOpDescV2
    d.code = tupleUdfCode
    d.uiParameters = List(datasetParameter("DS", "  "))
    val failure = the[RuntimeException] thrownBy buildPhysicalOp(d)
    failure.getMessage should include("No dataset selected for the parameter 'DS'")
  }

  it should "reject a blank virtual-environment name when the default env is disabled" in {
    val d = new PythonUDFOpDescV2
    d.defaultEnv = false
    d.envName = "   "
    intercept[RuntimeException] { buildPhysicalOp(d) }
  }

  "PythonUDFOpDescV2 schema propagation" should
    "emit only the output columns when input columns are not retained (default)" in {
    val d = new PythonUDFOpDescV2
    d.outputColumns = List(resultColumn)
    expectOutputColumns(d, retainInput = false)
  }

  it should "retain input columns plus the output columns when retainInputColumns is true" in {
    val d = new PythonUDFOpDescV2
    d.retainInputColumns = true
    d.outputColumns = List(resultColumn)
    expectOutputColumns(d, retainInput = true)
  }

  it should "reject an output column that collides with a retained input column" in {
    val d = new PythonUDFOpDescV2
    d.retainInputColumns = true
    d.outputColumns = List(new Attribute("dup", AttributeType.INTEGER))
    val input = Schema().add(new Attribute("dup", AttributeType.STRING))
    intercept[RuntimeException] {
      d.getExternalOutputSchemas(Map(d.operatorInfo.inputPorts.head.id -> input))
    }
  }

  "PythonUDFOpDescV2" should "round-trip its config fields through the polymorphic base" in {
    val d = new PythonUDFOpDescV2
    d.code = "print(1)"
    d.workers = 3
    d.retainInputColumns = true
    d.defaultEnv = false
    d.envName = "myenv"
    d.outputColumns = List(new Attribute("res", AttributeType.INTEGER))
    d.uiParameters = List(uiParameter("count", "7"))
    val p = roundTrip(d)
    p.code shouldBe "print(1)"
    p.workers shouldBe 3
    p.retainInputColumns shouldBe true
    p.defaultEnv shouldBe false
    p.envName shouldBe "myenv"
    p.outputColumns shouldBe List(new Attribute("res", AttributeType.INTEGER))
    p.uiParameters.map(_.attribute) shouldBe List(new Attribute("count", AttributeType.INTEGER))
    p.uiParameters.map(_.value) shouldBe List("7")
  }

  "PythonUDFOpDescV2.getPhysicalOp" should
    "use a parallelizable one-to-one op with the suggested worker count when workers > 1" in {
    val d = new PythonUDFOpDescV2
    d.code = "yield t"
    d.workers = 4
    val physical = buildPhysicalOp(d)
    physical.parallelizable shouldBe true
    physical.suggestedWorkerNum shouldBe Some(4)
    physical.opExecInitInfo shouldBe OpExecWithCode("yield t", "python")
  }

  it should "carry the trimmed virtual-environment name when the default env is disabled" in {
    val d = new PythonUDFOpDescV2
    d.defaultEnv = false
    d.envName = "  myenv  "
    buildPhysicalOp(d).pveName shouldBe "myenv"
  }

  it should "map each custom input port's partitionRequirement into the physical op" in {
    val d = new PythonUDFOpDescV2
    d.code = "yield t"
    d.inputPorts = List(
      portDescription("p0", "in")
    )
    buildPhysicalOp(d).partitionRequirement shouldBe List(
      Some(UnknownPartition())
    )
  }

  "PythonUDFOpDescV2.operatorInfo" should
    "derive input ports from a custom inputPorts descriptor list" in {
    val d = new PythonUDFOpDescV2
    d.inputPorts = List(
      portDescription("p0", "left", disallowMultiInputs = true),
      portDescription("p1", "right", dependencies = List(0))
    )
    val info = d.operatorInfo
    info.inputPorts should have length 2
    info.inputPorts.head.displayName shouldBe "left"
    info.inputPorts.head.disallowMultiLinks shouldBe true
    info.inputPorts(1).displayName shouldBe "right"
    info.inputPorts(1).dependencies shouldBe List(PortIdentity(0))
  }

  it should "derive output ports from a custom outputPorts descriptor list" in {
    val d = new PythonUDFOpDescV2
    d.outputPorts = List(
      portDescription("o0", "result")
    )
    val info = d.operatorInfo
    info.outputPorts should have length 1
    info.outputPorts.head.displayName shouldBe "result"
  }

  "PythonUDFOpDescV2.runtimeReconfiguration" should
    "return the new op's physical op with no state transfer" in {
    val oldOp = new PythonUDFOpDescV2
    val newOp = new PythonUDFOpDescV2
    newOp.code = "yield reconfigured"
    val result = oldOp.runtimeReconfiguration(workflowId, executionId, oldOp, newOp)
    result.isSuccess shouldBe true
    val (physical, stateTransfer) = result.get
    stateTransfer shouldBe None
    physical.opExecInitInfo match {
      case OpExecWithCode(code, _) => code shouldBe "yield reconfigured"
      case other                   => fail(s"expected OpExecWithCode, got $other")
    }
  }
}
