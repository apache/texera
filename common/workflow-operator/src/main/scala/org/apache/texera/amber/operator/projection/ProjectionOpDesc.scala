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

package org.apache.texera.amber.operator.projection

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PhysicalOp.oneToOnePhysicalOp
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.operator.map.MapOpDesc
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.objectMapper

class ProjectionOpDesc extends MapOpDesc with StandaloneCodeGenerator {

  @JsonProperty(required = true, defaultValue = "false")
  @JsonSchemaTitle("Drop Option")
  @JsonPropertyDescription("check to drop the selected attributes")
  var isDrop: Boolean = false

  // Declared required because it already is: `getPhysicalOp` refuses an empty list.
  // Saying so puts the refusal in the form rather than at run time.
  @JsonProperty(required = true)
  @JsonSchemaTitle("Attributes")
  @JsonPropertyDescription("the attributes to keep, or to drop when Drop Option is set")
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
        "org.apache.texera.amber.operator.projection.ProjectionOpExec",
        objectMapper.writeValueAsString(this)
      )
    )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withDerivePartition(derivePartition())
      .withPropagateSchema(SchemaPropagationFunc(inputSchemas => {
        require(attributes.nonEmpty, "Please select at least one attribute to project.")

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

  override def generateStandaloneCode(): String = {
    val units = Option(attributes).getOrElse(List.empty)
    // JVM validates non-empty at runtime via Preconditions; emit passthrough
    // as best-effort so the standalone script still runs.
    if (units.isEmpty) return "out1df = in1df.copy()"

    if (isDrop) {
      // Drop mode ignores aliases (matches ProjectionOpExec).
      val cols = units.map(u => pyStringLiteral(u.getOriginalAttribute)).mkString("[", ", ", "]")
      s"out1df = in1df.drop(columns=$cols)"
    } else {
      val originals =
        units.map(u => pyStringLiteral(u.getOriginalAttribute)).mkString("[", ", ", "]")
      // AttributeUnit.getAlias returns originalAttribute when alias is blank,
      // so an explicit rename is only needed when they differ.
      val renames = units
        .filter(u => u.getAlias != u.getOriginalAttribute)
        .map(u => s"""${pyStringLiteral(u.getOriginalAttribute)}: ${pyStringLiteral(u.getAlias)}""")
      if (renames.isEmpty) {
        s"out1df = in1df[$originals].copy()"
      } else {
        val renameMap = renames.mkString("{", ", ", "}")
        s"out1df = in1df[$originals].rename(columns=$renameMap)"
      }
    }
  }
}
