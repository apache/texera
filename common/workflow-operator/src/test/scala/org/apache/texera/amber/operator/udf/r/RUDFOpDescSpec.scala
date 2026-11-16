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

package org.apache.texera.amber.operator.udf.r

import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.workflow.{PortIdentity, UnknownPartition}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.operator.udf.UdfDescriptorSpecSupport
import org.scalatest.flatspec.AnyFlatSpec

class RUDFOpDescSpec extends AnyFlatSpec with UdfDescriptorSpecSupport {

  "RUDFOpDesc.operatorInfo" should
    "advertise the name, R group, and a default 1-in/1-out shape" in {
    expectOperatorInfo(new RUDFOpDesc, "R UDF", OperatorGroupConstants.R_GROUP, 1)
  }

  "RUDFOpDesc" should "default code/workers/useTupleAPI/retainInputColumns" in {
    val d = new RUDFOpDesc
    d.code shouldBe ""
    d.workers shouldBe 1
    d.useTupleAPI shouldBe false
    d.retainInputColumns shouldBe false
  }

  "RUDFOpDesc.getPhysicalOp" should
    "wire OpExecWithCode(code, \"r-table\") and carry port identities" in {
    val d = new RUDFOpDesc
    d.code = "function(t) t"
    val physical = buildPhysicalOp(d)
    physical.opExecInitInfo shouldBe OpExecWithCode("function(t) t", "r-table")
    physical.inputPorts.keySet shouldBe d.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }

  it should "reject a non-positive worker count" in {
    expectInvalidWorkerCount(new RUDFOpDesc)(_.workers = 0)
  }

  "RUDFOpDesc schema propagation" should
    "emit only the output columns when input columns are not retained (default)" in {
    val d = new RUDFOpDesc
    d.outputColumns = List(resultColumn)
    expectOutputColumns(d, retainInput = false)
  }

  it should "retain input columns plus the output columns when retainInputColumns is true" in {
    val d = new RUDFOpDesc
    d.retainInputColumns = true
    d.outputColumns = List(resultColumn)
    expectOutputColumns(d, retainInput = true)
  }

  "RUDFOpDesc" should "round-trip its config fields through the polymorphic base" in {
    val d = new RUDFOpDesc
    d.code = "f"
    d.workers = 2
    d.useTupleAPI = true
    d.retainInputColumns = true
    val r = roundTrip(d)
    r.code shouldBe "f"
    r.workers shouldBe 2
    r.useTupleAPI shouldBe true
    r.retainInputColumns shouldBe true
  }

  "RUDFOpDesc.getPhysicalOp" should
    "use r-tuple, parallelizable with the suggested worker count when workers > 1 and useTupleAPI" in {
    val d = new RUDFOpDesc
    d.code = "gen"
    d.useTupleAPI = true
    d.workers = 3
    val physical = buildPhysicalOp(d)
    physical.opExecInitInfo shouldBe OpExecWithCode("gen", "r-tuple")
    physical.parallelizable shouldBe true
    physical.suggestedWorkerNum shouldBe Some(3)
  }

  it should "derive partitionRequirement from custom inputPorts when set" in {
    val d = new RUDFOpDesc
    d.code = "f"
    d.inputPorts = List(
      portDescription("0", "in0")
    )
    buildPhysicalOp(d).partitionRequirement shouldBe List(
      Some(UnknownPartition())
    )
  }

  "RUDFOpDesc.operatorInfo" should
    "build ports from custom inputPorts/outputPorts descriptions when provided" in {
    val d = new RUDFOpDesc
    d.inputPorts = List(
      portDescription("0", "in0", disallowMultiInputs = true)
    )
    d.outputPorts = List(
      portDescription("0", "out0")
    )
    val info = d.operatorInfo
    info.inputPorts should have length 1
    info.inputPorts.head.id shouldBe PortIdentity(0)
    info.inputPorts.head.displayName shouldBe "in0"
    info.inputPorts.head.disallowMultiLinks shouldBe true
    info.outputPorts should have length 1
    info.outputPorts.head.displayName shouldBe "out0"
  }

  "RUDFOpDesc.runtimeReconfiguration" should
    "return the new op's physical op with no state transfer" in {
    val oldOp = new RUDFOpDesc
    val newOp = new RUDFOpDesc
    newOp.code = "f2"
    val result = newOp.runtimeReconfiguration(workflowId, executionId, oldOp, newOp)
    result.isSuccess shouldBe true
    val (physical, stateTransfer) = result.get
    stateTransfer shouldBe None
    physical.opExecInitInfo shouldBe OpExecWithCode("f2", "r-table")
  }
}
