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

package edu.uci.ics.amber.operator.hashJoin

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.tuple.{Attribute, Schema}
import edu.uci.ics.amber.core.virtualidentity.{
  ExecutionIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.core.workflow._
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.amber.operator.hashJoin.HashJoinOpDesc.HASH_JOIN_INTERNAL_KEY_NAME
import edu.uci.ics.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameOnPort1
}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

object HashJoinOpDesc {
  val HASH_JOIN_INTERNAL_KEY_NAME = "__internal__hashtable__key__"
}

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "buildAttributeName": {
      "const": {
        "$data": "probeAttributeName"
      }
    }
  }
}
""")
class HashJoinOpDesc[K] extends LogicalOp {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Left Input Attribute")
  @JsonPropertyDescription("attribute to be joined on the Left Input")
  @AutofillAttributeName
  var buildAttributeName: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Right Input Attribute")
  @JsonPropertyDescription("attribute to be joined on the Right Input")
  @AutofillAttributeNameOnPort1
  var probeAttributeName: String = _

  @JsonProperty(required = true, defaultValue = "inner")
  @JsonSchemaTitle("Join Type")
  @JsonPropertyDescription("select the join type to execute")
  var joinType: JoinType = JoinType.INNER

  override def getPhysicalPlan(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalPlan = {

    val buildInputPort = operatorInfo.inputPorts.head
    val buildOutputPort = OutputPort(PortIdentity(0, internal = true), blocking = true)

    val buildPhysicalOp =
      PhysicalOp
        .oneToOnePhysicalOp(
          PhysicalOpIdentity(operatorIdentifier, "build"),
          workflowId,
          executionId,
          OpExecWithClassName(
            "edu.uci.ics.amber.operator.hashJoin.HashJoinBuildOpExec",
            objectMapper.writeValueAsString(this)
          )
        )
        .withInputPorts(List(buildInputPort))
        .withOutputPorts(List(buildOutputPort))
        .withPartitionRequirement(List(Option(HashPartition(List(buildAttributeName)))))
        .withPropagateSchema(
          SchemaPropagationFunc(inputSchemas =>
            Map(
              PortIdentity(internal = true) -> Schema(
                List(
                  new Attribute(
                    HASH_JOIN_INTERNAL_KEY_NAME,
                    // Because we need to materialize the outputs of build, we cannot use ANY type.
                    inputSchemas(operatorInfo.inputPorts.head.id)
                      .getAttribute(buildAttributeName)
                      .getType
                  )
                )
              ).add(inputSchemas(operatorInfo.inputPorts.head.id))
            )
          )
        )
        .withParallelizable(true)

    val probeBuildInputPort = InputPort(PortIdentity(0, internal = true))
    val probeDataInputPort =
      InputPort(operatorInfo.inputPorts(1).id, dependencies = List(probeBuildInputPort.id))
    val probeOutputPort = OutputPort(PortIdentity(0))

    val probePhysicalOp =
      PhysicalOp
        .oneToOnePhysicalOp(
          PhysicalOpIdentity(operatorIdentifier, "probe"),
          workflowId,
          executionId,
          OpExecWithClassName(
            "edu.uci.ics.amber.operator.hashJoin.HashJoinProbeOpExec",
            objectMapper.writeValueAsString(this)
          )
        )
        .withInputPorts(
          List(
            probeBuildInputPort,
            probeDataInputPort
          )
        )
        .withOutputPorts(List(probeOutputPort))
        .withPartitionRequirement(
          List(
            // Cannot use OneToOnePartition because it does not work with InputPortMaterializationReaderThreads.
            Option(HashPartition(List(buildAttributeName))),
            Option(HashPartition(List(probeAttributeName)))
          )
        )
        .withDerivePartition(_ => HashPartition(List(probeAttributeName)))
        .withParallelizable(true)
        .withPropagateSchema(
          SchemaPropagationFunc(inputSchemas => {
            val buildSchema = inputSchemas(PortIdentity(internal = true))
            val probeSchema = inputSchemas(PortIdentity(1))

            // Start with the attributes from the build schema, excluding the hash join internal key
            val leftAttributes =
              buildSchema.getAttributes.filterNot(_.getName == HASH_JOIN_INTERNAL_KEY_NAME)
            val leftAttributeNames = leftAttributes.map(_.getName).toSet

            // Filter and rename attributes from the probe schema to avoid conflicts
            val rightAttributes = probeSchema.getAttributes
              .filterNot(_.getName == probeAttributeName)
              .map { attr =>
                var newName = attr.getName
                while (leftAttributeNames.contains(newName)) {
                  val suffixIndex = """#@(\d+)$""".r
                    .findFirstMatchIn(newName)
                    .map(_.group(1).toInt + 1)
                    .getOrElse(1)
                  newName = s"${attr.getName}#@$suffixIndex"
                }
                new Attribute(newName, attr.getType)
              }

            // Combine left and right attributes into a new schema
            val outputSchema = Schema(leftAttributes ++ rightAttributes)
            Map(PortIdentity() -> outputSchema)
          })
        )

    PhysicalPlan(
      operators = Set(buildPhysicalOp, probePhysicalOp),
      links = Set(
        PhysicalLink(
          buildPhysicalOp.id,
          buildOutputPort.id,
          probePhysicalOp.id,
          probeBuildInputPort.id
        )
      )
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Hash Join",
      "join two inputs",
      OperatorGroupConstants.JOIN_GROUP,
      inputPorts = List(
        InputPort(PortIdentity(0), displayName = "left"),
        InputPort(PortIdentity(1), displayName = "right", dependencies = List(PortIdentity(0)))
      ),
      outputPorts = List(OutputPort())
    )
}
