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

package org.apache.texera.amber.operator.udf.java

import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.workflow.UnknownPartition
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.operator.udf.UdfDescriptorSpecSupport
import org.scalatest.flatspec.AnyFlatSpec

class JavaUDFOpDescSpec extends AnyFlatSpec with UdfDescriptorSpecSupport {

  "JavaUDFOpDesc.operatorInfo" should
    "advertise the name, Java group, and a default 1-in/1-out shape" in {
    expectOperatorInfo(new JavaUDFOpDesc, "Java UDF", OperatorGroupConstants.JAVA_GROUP, 1)
  }

  "JavaUDFOpDesc" should "default code to empty, workers to 1, retainInputColumns to false" in {
    val d = new JavaUDFOpDesc
    d.code shouldBe ""
    d.workers shouldBe 1
    d.retainInputColumns shouldBe false
  }

  "JavaUDFOpDesc.getPhysicalOp" should
    "wire OpExecWithCode(code, \"java\") and carry port identities" in {
    val d = new JavaUDFOpDesc
    d.code = "return t;"
    val physical = buildPhysicalOp(d)
    physical.opExecInitInfo shouldBe OpExecWithCode("return t;", "java")
    physical.inputPorts.keySet shouldBe d.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }

  it should "reject a non-positive worker count" in {
    expectInvalidWorkerCount(new JavaUDFOpDesc)(_.workers = 0)
  }

  "JavaUDFOpDesc schema propagation" should
    "emit only the output columns when input columns are not retained (default)" in {
    val d = new JavaUDFOpDesc
    d.outputColumns = List(resultColumn)
    expectOutputColumns(d, retainInput = false)
  }

  it should "retain input columns plus the output columns when retainInputColumns is true" in {
    val d = new JavaUDFOpDesc
    d.retainInputColumns = true
    d.outputColumns = List(resultColumn)
    expectOutputColumns(d, retainInput = true)
  }

  "JavaUDFOpDesc" should "round-trip its config fields through the polymorphic base" in {
    val d = new JavaUDFOpDesc
    d.code = "x"
    val p = uiParameter("count", "42")
    d.uiParameters = List(p)
    d.workers = 4
    d.retainInputColumns = true
    val j = roundTrip(d)
    j.code shouldBe "x"
    j.uiParameters.head.attribute shouldBe p.attribute
    j.uiParameters.head.value shouldBe "42"
    j.workers shouldBe 4
    j.retainInputColumns shouldBe true
  }

  "JavaUDFOpDesc.getPhysicalOp" should
    "build a parallelizable one-to-many op when workers > 1" in {
    val d = new JavaUDFOpDesc
    d.code = "return t;"
    d.workers = 2
    val physical = buildPhysicalOp(d)
    physical.parallelizable shouldBe true
    physical.isOneToManyOp shouldBe true
    physical.suggestedWorkerNum shouldBe Some(2)
    physical.opExecInitInfo shouldBe OpExecWithCode("return t;", "java")
  }

  "JavaUDFOpDesc.operatorInfo" should
    "derive input ports from a configured inputPorts list" in {
    val d = new JavaUDFOpDesc
    d.inputPorts = List(
      portDescription("0", "left", disallowMultiInputs = true)
    )
    val info = d.operatorInfo
    info.inputPorts should have length 1
    info.inputPorts.head.displayName shouldBe "left"
    // also drives the getPhysicalOp inputPorts != null partitionRequirement branch
    val physical = buildPhysicalOp(d)
    physical.inputPorts.keySet shouldBe info.inputPorts.map(_.id).toSet
    physical.partitionRequirement shouldBe List(Some(UnknownPartition()))
  }

  it should "derive output ports from a configured outputPorts list" in {
    val d = new JavaUDFOpDesc
    d.outputPorts = List(
      portDescription("0", "result")
    )
    val info = d.operatorInfo
    info.outputPorts should have length 1
    info.outputPorts.head.displayName shouldBe "result"
  }

  "JavaUDFOpDesc.runtimeReconfiguration" should
    "return the new op's physical op with no state transfer" in {
    val oldOp = new JavaUDFOpDesc
    val newOp = new JavaUDFOpDesc
    newOp.code = "return t2;"
    val result = oldOp.runtimeReconfiguration(workflowId, executionId, oldOp, newOp)
    result.isSuccess shouldBe true
    val (physical, stateTransfer) = result.get
    stateTransfer shouldBe None
    physical.opExecInitInfo match {
      case OpExecWithCode(code, _) => code shouldBe "return t2;"
      case other                   => fail(s"expected OpExecWithCode, got $other")
    }
  }
}
