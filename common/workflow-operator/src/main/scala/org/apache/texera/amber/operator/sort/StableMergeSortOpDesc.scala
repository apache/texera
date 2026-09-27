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

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalOp}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import scala.collection.mutable.ListBuffer

/**
  * This operator performs a stable, per-partition sort using an incremental
  * stack of sorted buckets and pairwise stable merges. The sort keys define
  * the lexicographic order and per-key direction (ASC/DESC).
  */
//TODO(#3922): disallowing sorting on binary type
class StableMergeSortOpDesc extends LogicalOp {

  @JsonProperty(value = "keys", required = true)
  @JsonSchemaTitle("Sort Keys")
  @JsonPropertyDescription("List of attributes to sort by with ordering preferences")
  var keys: ListBuffer[SortCriteriaUnit] = _

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .manyToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.sort.StableMergeSortOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Stable Merge Sort",
      "Stable per-partition sort with multi-key ordering (incremental stack of sorted buckets)",
      OperatorGroupConstants.SORT_GROUP,
      List(InputPort()),
      List(OutputPort(blocking = true))
    )
}
