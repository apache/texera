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

class RUDFSourceOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  "RUDFSourceOpDesc.operatorInfo" should
    "advertise the 1-out R UDF source (no inputs, one output)" in {
    val info = (new RUDFSourceOpDesc).operatorInfo
    info.userFriendlyName shouldBe "1-out R UDF"
    info.operatorGroupName shouldBe OperatorGroupConstants.R_GROUP
    info.inputPorts shouldBe empty
    info.outputPorts should have length 1
  }

  "RUDFSourceOpDesc.sourceSchema" should "be empty by default and reflect the configured columns" in {
    (new RUDFSourceOpDesc).sourceSchema().getAttributes shouldBe empty
    val d = new RUDFSourceOpDesc
    d.columns = List(new Attribute("a", AttributeType.STRING))
    d.sourceSchema() shouldBe Schema().add(new Attribute("a", AttributeType.STRING))
  }

  "RUDFSourceOpDesc.getPhysicalOp" should
    "wire OpExecWithCode(code, \"r-table\") as a source op with one output port" in {
    val d = new RUDFSourceOpDesc
    d.code = "data.frame(a=1)"
    val physical = d.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithCode(code, language) =>
        language shouldBe "r-table"
        code shouldBe "data.frame(a=1)"
      case other => fail(s"expected OpExecWithCode, got $other")
    }
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }

  it should "reject a non-positive worker count" in {
    val d = new RUDFSourceOpDesc
    d.code = "x"
    d.workers = 0
    intercept[IllegalArgumentException] { d.getPhysicalOp(workflowId, executionId) }
  }

  "RUDFSourceOpDesc" should "round-trip its config fields through the polymorphic base" in {
    val d = new RUDFSourceOpDesc
    d.code = "f"
    d.workers = 2
    d.useTupleAPI = true
    d.columns = List(new Attribute("a", AttributeType.STRING))
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[RUDFSourceOpDesc]
    val r = restored.asInstanceOf[RUDFSourceOpDesc]
    r.code shouldBe "f"
    r.workers shouldBe 2
    r.useTupleAPI shouldBe true
    r.columns shouldBe List(new Attribute("a", AttributeType.STRING))
  }
}
