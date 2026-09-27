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

package org.apache.texera.amber.operator.sort

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StableMergeSortOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  "StableMergeSortOpDesc.operatorInfo" should
    "advertise the name, Sort group, and a single blocking output" in {
    val info = (new StableMergeSortOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Stable Merge Sort"
    info.operatorGroupName shouldBe OperatorGroupConstants.SORT_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    // A stable sort must observe all rows before emitting, so the output blocks.
    info.outputPorts.head.blocking shouldBe true
  }

  "StableMergeSortOpDesc.getPhysicalOp" should
    "be a non-parallelizable many-to-one op wiring StableMergeSortOpExec" in {
    val op = new StableMergeSortOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.parallelizable shouldBe false
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.sort.StableMergeSortOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
  }

  "StableMergeSortOpDesc" should
    "deserialize its sort keys (List of SortCriteriaUnit) through the polymorphic base" in {
    val json =
      """{"operatorType":"StableMergeSort","keys":[{"attribute":"age","sortPreference":"DESC"}]}"""
    val desc = objectMapper.readValue(json, classOf[LogicalOp]).asInstanceOf[StableMergeSortOpDesc]
    desc.keys should have size 1
    desc.keys.head.attributeName shouldBe "age"
    desc.keys.head.sortPreference shouldBe SortPreference.DESC
  }
}
