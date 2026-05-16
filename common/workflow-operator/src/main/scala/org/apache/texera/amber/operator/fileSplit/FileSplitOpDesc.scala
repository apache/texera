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

package org.apache.texera.amber.operator.fileSplit

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.{LogicalOp, PortDescription}
import org.apache.texera.amber.util.JSONUtils.objectMapper

class FileSplitOpDesc extends LogicalOp {

  @JsonProperty
  @JsonSchemaTitle("File Column")
  @JsonPropertyDescription("leave empty to auto-detect source_file or filename")
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  var fileAttribute: Option[String] = None

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp =
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.fileSplit.FileSplitOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withParallelizable(false)
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas => {
          require(inputSchemas.size == 1, "File Split requires exactly one input")
          val inputSchema = inputSchemas.values.head
          resolveFileAttribute(inputSchema)
          operatorInfo.outputPorts.map(port => port.id -> inputSchema).toMap
        })
      )

  override def operatorInfo: OperatorInfo = {
    val outputPortInfo =
      if (outputPorts != null && outputPorts.nonEmpty) {
        outputPorts.zipWithIndex.map {
          case (portDesc: PortDescription, idx) =>
            OutputPort(PortIdentity(idx), displayName = portDesc.displayName)
        }
      } else {
        List(OutputPort(PortIdentity()), OutputPort(PortIdentity(1)))
      }

    OperatorInfo(
      userFriendlyName = "File Split",
      operatorDescription = "Route rows from the same file to the same output port",
      operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = outputPortInfo,
      dynamicOutputPorts = true,
      allowPortCustomization = true
    )
  }

  def resolveFileAttribute(schema: Schema): String = {
    val attributeName = fileAttribute.getOrElse {
      List("source_file", "filename")
        .find(schema.containsAttribute)
        .getOrElse(
          throw new IllegalArgumentException(
            "File Split requires a source_file or filename column, or an explicit File Column"
          )
        )
    }
    if (!schema.containsAttribute(attributeName)) {
      throw new IllegalArgumentException(s"File Split column '$attributeName' does not exist")
    }
    if (schema.getAttribute(attributeName).getType != AttributeType.STRING) {
      throw new IllegalArgumentException(s"File Split column '$attributeName' must be a STRING")
    }
    attributeName
  }
}
