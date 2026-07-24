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

package org.apache.texera.amber.operator.sortPartitions

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalOp, RangePartition}
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "sortAttributeName":{
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class SortPartitionsOpDesc extends LogicalOp with StandaloneCodeGenerator {

  // Sorting is this operator's contract: output row order is meaningful.
  override def orderSensitive: Boolean = true

  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute")
  @JsonPropertyDescription("Attribute to sort (must be numerical).")
  @AutofillAttributeName
  var sortAttributeName: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute Domain Min")
  @JsonPropertyDescription("Minimum value of the domain of the attribute.")
  var domainMin: Long = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute Domain Max")
  @JsonPropertyDescription("Maximum value of the domain of the attribute.")
  var domainMax: Long = _

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
          "org.apache.texera.amber.operator.sortPartitions.SortPartitionsOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPartitionRequirement(
        List(Option(RangePartition(List(sortAttributeName), domainMin, domainMax)))
      )

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Sort Partitions",
      "Sort Partitions",
      OperatorGroupConstants.SORT_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(blocking = true))
    )

  // JVM op buffers all tuples and calls ArrayBuffer.sortWith with the comparator
  // AttributeTypeUtils.compare(...) < 0. Scala's sortWith is guaranteed stable.
  // AttributeTypeUtils.compare puts null < non-null (NULLS FIRST in ASC).
  // Direction is ASC only (no DESC option); JSON schema restricts the attribute
  // to INTEGER/LONG/DOUBLE. Domain min/max are partitioning hints unused at sort
  // time, so the standalone code ignores them.
  //
  // pandas equivalent: stable mergesort, ascending, na_position="first" to
  // mirror NULLS FIRST. Known divergence: Double NaN. JVM Double.compare orders
  // NaN > +Inf (NaN last), while pandas treats NaN as missing and places it at
  // na_position — so NaN sorts FIRST in this Python translation.
  override def generateStandaloneCode(): String = {
    val col = toPyDoubleQuotedLiteral(Option(sortAttributeName).getOrElse(""))
    s"""out1df = in1df.sort_values(by=$col, ascending=True, kind="mergesort", na_position="first").reset_index(drop=True)"""
  }

  private def toPyDoubleQuotedLiteral(s: String): String =
    "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
}
