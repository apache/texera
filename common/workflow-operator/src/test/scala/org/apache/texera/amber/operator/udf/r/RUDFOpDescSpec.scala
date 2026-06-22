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
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RUDFOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  "RUDFOpDesc.operatorInfo" should
    "advertise the name, R group, and a default 1-in/1-out shape" in {
    val info = (new RUDFOpDesc).operatorInfo
    info.userFriendlyName shouldBe "R UDF"
    info.operatorGroupName shouldBe OperatorGroupConstants.R_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
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
    val physical = d.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithCode(code, language) =>
        language shouldBe "r-table"
        code shouldBe "function(t) t"
      case other => fail(s"expected OpExecWithCode, got $other")
    }
    physical.inputPorts.keySet shouldBe d.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }

  it should "reject a non-positive worker count" in {
    val d = new RUDFOpDesc
    d.workers = 0
    intercept[IllegalArgumentException] { d.getPhysicalOp(workflowId, executionId) }
  }

  "RUDFOpDesc schema propagation" should
    "emit only the output columns when input columns are not retained (default)" in {
    val d = new RUDFOpDesc
    d.outputColumns = List(new Attribute("res", AttributeType.INTEGER))
    val input = Schema().add(new Attribute("in", AttributeType.STRING))
    d.getExternalOutputSchemas(Map(d.operatorInfo.inputPorts.head.id -> input)) shouldBe Map(
      d.operatorInfo.outputPorts.head.id -> Schema().add(
        new Attribute("res", AttributeType.INTEGER)
      )
    )
  }

  it should "retain input columns plus the output columns when retainInputColumns is true" in {
    val d = new RUDFOpDesc
    d.retainInputColumns = true
    d.outputColumns = List(new Attribute("res", AttributeType.INTEGER))
    val input = Schema().add(new Attribute("in", AttributeType.STRING))
    d.getExternalOutputSchemas(Map(d.operatorInfo.inputPorts.head.id -> input)) shouldBe Map(
      d.operatorInfo.outputPorts.head.id -> Schema()
        .add(new Attribute("in", AttributeType.STRING))
        .add(new Attribute("res", AttributeType.INTEGER))
    )
  }

  "RUDFOpDesc" should "round-trip its config fields through the polymorphic base" in {
    val d = new RUDFOpDesc
    d.code = "f"
    d.workers = 2
    d.useTupleAPI = true
    d.retainInputColumns = true
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[RUDFOpDesc]
    val r = restored.asInstanceOf[RUDFOpDesc]
    r.code shouldBe "f"
    r.workers shouldBe 2
    r.useTupleAPI shouldBe true
    r.retainInputColumns shouldBe true
  }
}
