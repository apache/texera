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

package org.apache.texera.amber.operator.regex

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RegexOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def newDesc(attr: String, re: String, caseInsensitive: Boolean): RegexOpDesc = {
    val d = new RegexOpDesc
    d.attribute = attr
    d.regex = re
    d.caseInsensitive = caseInsensitive
    d
  }

  "RegexOpDesc.operatorInfo" should
    "advertise the name, Search group, and reconfiguration support" in {
    val info = (new RegexOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Regular Expression"
    info.operatorDescription.toLowerCase should include("regular expression")
    info.operatorGroupName shouldBe OperatorGroupConstants.SEARCH_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    info.supportReconfiguration shouldBe true
  }

  "RegexOpDesc.getPhysicalOp" should "wire the RegexOpExec class name and carry ports" in {
    val op = newDesc("col", ".*foo.*", caseInsensitive = false)
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.regex.RegexOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    // Pin the actual port identities (not just counts).
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
  }

  "RegexOpDesc JSON round-trip" should
    "preserve attribute, regex, and caseInsensitive via the polymorphic base" in {
    val json = objectMapper.writeValueAsString(newDesc("url", "^https?://", caseInsensitive = true))
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[RegexOpDesc]
    val re = restored.asInstanceOf[RegexOpDesc]
    re.attribute shouldBe "url"
    re.regex shouldBe "^https?://"
    re.caseInsensitive shouldBe true
  }
}
