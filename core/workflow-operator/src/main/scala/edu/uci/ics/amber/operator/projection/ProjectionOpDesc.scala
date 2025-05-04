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

package edu.uci.ics.amber.operator.projection

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.PhysicalOp.oneToOnePhysicalOp
import edu.uci.ics.amber.core.workflow._
import edu.uci.ics.amber.operator.map.MapOpDesc
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

class ProjectionOpDesc extends MapOpDesc {

  @JsonProperty(required = true, defaultValue = "false")
  @JsonSchemaTitle("Drop Option")
  @JsonPropertyDescription("check to drop the selected attributes")
  var isDrop: Boolean = false

  var attributes: List[AttributeUnit] = List()

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    oneToOnePhysicalOp(
      workflowId,
      executionId,
      operatorIdentifier,
      OpExecWithClassName(
        "edu.uci.ics.amber.operator.projection.ProjectionOpExec",
        objectMapper.writeValueAsString(this)
      )
    )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withDerivePartition(derivePartition())
      .withPropagateSchema(SchemaPropagationFunc(inputSchemas => {
        require(attributes.nonEmpty, "Attributes must not be empty")

        val inputSchema = inputSchemas.values.head
        val outputSchema = if (!isDrop) {
          attributes.foldLeft(Schema()) { (schema, attribute) =>
            val originalType = inputSchema.getAttribute(attribute.getOriginalAttribute).getType
            schema.add(attribute.getAlias, originalType)
          }
        } else {
          attributes.foldLeft(inputSchema) { (schema, attribute) =>
            schema.remove(attribute.getOriginalAttribute)
          }
        }

        Map(operatorInfo.outputPorts.head.id -> outputSchema)
      }))
  }

  def derivePartition()(partition: List[PartitionInfo]): PartitionInfo = {
    val inputPartitionInfo = partition.head

    val outputPartitionInfo = inputPartitionInfo match {
      case HashPartition(hashAttributeNames) =>
        if (hashAttributeNames.nonEmpty) HashPartition(hashAttributeNames) else UnknownPartition()
      case RangePartition(rangeAttributeNames, min, max) =>
        if (rangeAttributeNames.nonEmpty) RangePartition(rangeAttributeNames, min, max)
        else UnknownPartition()
      case _ => inputPartitionInfo
    }

    outputPartitionInfo
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "Projection",
      "Keeps or drops the column",
      OperatorGroupConstants.CLEANING_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )
  }
}
