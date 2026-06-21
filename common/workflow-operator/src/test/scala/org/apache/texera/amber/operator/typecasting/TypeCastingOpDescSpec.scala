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

package org.apache.texera.amber.operator.typecasting

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TypeCastingOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def castUnit(attr: String, to: AttributeType): TypeCastingUnit = {
    val u = new TypeCastingUnit()
    u.attribute = attr
    u.resultType = to
    u
  }

  "TypeCastingOpDesc.operatorInfo" should "advertise the name and Cleaning group" in {
    val info = (new TypeCastingOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Type Casting"
    info.operatorGroupName shouldBe OperatorGroupConstants.CLEANING_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "TypeCastingOpDesc.getPhysicalOp" should "wire TypeCastingOpExec and carry port identities" in {
    val op = new TypeCastingOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.typecasting.TypeCastingOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
  }

  "TypeCastingOpDesc schema propagation" should
    "leave the schema unchanged when there are no casting units" in {
    val op = new TypeCastingOpDesc
    val input = Schema().add(new Attribute("n", AttributeType.INTEGER))
    val out = op.getExternalOutputSchemas(Map(op.operatorInfo.inputPorts.head.id -> input))
    out shouldBe Map(op.operatorInfo.outputPorts.head.id -> input)
  }

  it should "change the target column's type for a casting unit" in {
    val op = new TypeCastingOpDesc
    op.typeCastingUnits = List(castUnit("n", AttributeType.STRING))
    val input = Schema().add(new Attribute("n", AttributeType.INTEGER))
    val out = op.getExternalOutputSchemas(Map(op.operatorInfo.inputPorts.head.id -> input))
    out shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> Schema().add(new Attribute("n", AttributeType.STRING))
    )
  }

  "TypeCastingOpDesc" should "round-trip its casting units through the polymorphic base" in {
    val op = new TypeCastingOpDesc
    op.typeCastingUnits = List(castUnit("n", AttributeType.STRING))
    val restored =
      objectMapper.readValue(objectMapper.writeValueAsString(op), classOf[LogicalOp])
    restored shouldBe a[TypeCastingOpDesc]
    val tc = restored.asInstanceOf[TypeCastingOpDesc]
    tc.typeCastingUnits should have size 1
    tc.typeCastingUnits.head.attribute shouldBe "n"
    tc.typeCastingUnits.head.resultType shouldBe AttributeType.STRING
  }
}
