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

package edu.uci.ics.amber.operator.typecasting

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.tuple.{AttributeTypeUtils, Schema}
import edu.uci.ics.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.map.MapOpDesc
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}

class TypeCastingOpDesc extends MapOpDesc {

  @JsonProperty(required = true)
  @JsonSchemaTitle("TypeCasting Units")
  @JsonPropertyDescription("Multiple type castings")
  var typeCastingUnits: List[TypeCastingUnit] = List.empty

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    if (typeCastingUnits == null) typeCastingUnits = List.empty
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "edu.uci.ics.amber.operator.typecasting.TypeCastingOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc { inputSchemas: Map[PortIdentity, Schema] =>
          val outputSchema = typeCastingUnits.foldLeft(inputSchemas.values.head) { (schema, unit) =>
            AttributeTypeUtils.SchemaCasting(schema, unit.attribute, unit.resultType)
          }
          Map(operatorInfo.outputPorts.head.id -> outputSchema)
        }
      )
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "Type Casting",
      "Cast between types",
      OperatorGroupConstants.CLEANING_GROUP,
      List(InputPort()),
      List(OutputPort())
    )
  }
}
