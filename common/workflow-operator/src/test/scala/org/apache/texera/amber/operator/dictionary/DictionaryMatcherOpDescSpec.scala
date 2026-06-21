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

package org.apache.texera.amber.operator.dictionary

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DictionaryMatcherOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def newDesc(
      dict: String,
      attr: String,
      result: String,
      mt: MatchingType
  ): DictionaryMatcherOpDesc = {
    val d = new DictionaryMatcherOpDesc
    d.dictionary = dict
    d.attribute = attr
    d.resultAttribute = result
    d.matchingType = mt
    d
  }

  "DictionaryMatcherOpDesc.operatorInfo" should
    "advertise the name, Search group, and reconfiguration support" in {
    val info = (new DictionaryMatcherOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Dictionary matcher"
    info.operatorGroupName shouldBe OperatorGroupConstants.SEARCH_GROUP
    info.operatorDescription.toLowerCase should include("dictionary")
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    info.supportReconfiguration shouldBe true
  }

  "DictionaryMatcherOpDesc.getPhysicalOp" should
    "wire the DictionaryMatcherOpExec class name and carry forward the operatorInfo port identities" in {
    val op = newDesc("a,b,c", "word", "matched", MatchingType.SCANBASED)
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.dictionary.DictionaryMatcherOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    // Pin the actual port identities (not just counts).
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
  }

  "DictionaryMatcherOpDesc schema propagation" should
    "append a BOOLEAN column named by resultAttribute to the input schema" in {
    val desc = newDesc("a,b,c", "word", "matched", MatchingType.SCANBASED)
    val inputSchema = Schema().add(new Attribute("word", AttributeType.STRING))
    val out = desc.getExternalOutputSchemas(Map(PortIdentity() -> inputSchema)).values.head
    out shouldBe inputSchema.add(new Attribute("matched", AttributeType.BOOLEAN))
  }

  "DictionaryMatcherOpDesc JSON round-trip" should
    "preserve all fields including MatchingType via the polymorphic base" in {
    val json = objectMapper.writeValueAsString(
      newDesc("x,y", "col", "isMatch", MatchingType.CONJUNCTION_INDEXBASED)
    )
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[DictionaryMatcherOpDesc]
    val dm = restored.asInstanceOf[DictionaryMatcherOpDesc]
    dm.dictionary shouldBe "x,y"
    dm.attribute shouldBe "col"
    dm.resultAttribute shouldBe "isMatch"
    dm.matchingType shouldBe MatchingType.CONJUNCTION_INDEXBASED
  }

  it should "serialize MatchingType via its @JsonValue name (e.g. SCANBASED -> \"Scan\")" in {
    val json = objectMapper.writeValueAsString(
      newDesc("a", "col", "matched", MatchingType.SCANBASED)
    )
    val tree = objectMapper.readTree(json)
    tree.get("Matching type").asText shouldBe "Scan"
  }
}
