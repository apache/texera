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

package org.apache.texera.amber.operator.substringSearch

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SubstringSearchOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def newDesc(attr: String, sub: String, caseSensitive: Boolean): SubstringSearchOpDesc = {
    val d = new SubstringSearchOpDesc
    d.attribute = attr
    d.substring = sub
    d.isCaseSensitive = caseSensitive
    d
  }

  "SubstringSearchOpDesc.operatorInfo" should
    "advertise the name, description, Search group, and reconfiguration support" in {
    val info = (new SubstringSearchOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Substring Search"
    info.operatorDescription shouldBe "Search for Substring(s) in a string column"
    info.operatorGroupName shouldBe OperatorGroupConstants.SEARCH_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    info.supportReconfiguration shouldBe true
  }

  "SubstringSearchOpDesc.isCaseSensitive" should "default to false" in {
    (new SubstringSearchOpDesc).isCaseSensitive shouldBe false
  }

  "SubstringSearchOpDesc.getPhysicalOp" should
    "wire the SubstringSearchOpExec class name and carry forward the operatorInfo port identities" in {
    val op = newDesc("col", "ub", caseSensitive = false)
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.substringSearch.SubstringSearchOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    // Pin the actual port identities (not just counts).
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
  }

  "SubstringSearchOpDesc JSON round-trip" should
    "preserve attribute, substring, and isCaseSensitive via the polymorphic base" in {
    val json = objectMapper.writeValueAsString(newDesc("body", "lo", caseSensitive = true))
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[SubstringSearchOpDesc]
    val ss = restored.asInstanceOf[SubstringSearchOpDesc]
    ss.attribute shouldBe "body"
    ss.substring shouldBe "lo"
    ss.isCaseSensitive shouldBe true
  }
}
