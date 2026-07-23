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

package org.apache.texera.amber.operator.intervalJoin

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameOnPort1
}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

/** This Operator have two assumptions:
  * 1. The tuples in both inputs come in ascending order
  * 2. The left input join key takes as points, join condition is: left key in the range of (right key, right key + constant)
  */
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "leftAttributeName": {
      "enum": ["integer", "long", "double", "timestamp"]
    },
    "rightAttributeName": {
      "const": {
        "$data": "leftAttributeName"
      }
    }
  }
}
""")
class IntervalJoinOpDesc extends LogicalOp with StandaloneCodeGenerator {

  // set/bag semantics: output row order is implementation-defined
  override def orderSensitive: Boolean = false

  @JsonProperty(required = true)
  @JsonSchemaTitle("Left Input attr")
  @JsonPropertyDescription("Choose one attribute in the left table")
  @AutofillAttributeName
  var leftAttributeName: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Right Input attr")
  @JsonPropertyDescription("Choose one attribute in the right table")
  @AutofillAttributeNameOnPort1
  var rightAttributeName: String = _

  @JsonProperty(required = true, defaultValue = "10")
  @JsonSchemaTitle("Interval Constant")
  @JsonPropertyDescription("left attri in (right, right + constant)")
  var constant: Long = 10

  @JsonProperty(required = true, defaultValue = "true")
  @JsonSchemaTitle("Include Left Bound")
  @JsonPropertyDescription("Include condition left attri = right attri")
  var includeLeftBound: Boolean = true

  @JsonProperty(required = true, defaultValue = "true")
  @JsonSchemaTitle("Include Right Bound")
  @JsonPropertyDescription("Include condition left attri = right attri")
  var includeRightBound: Boolean = true

  @JsonDeserialize(contentAs = classOf[TimeIntervalType])
  @JsonProperty(defaultValue = "day", required = false)
  @JsonSchemaTitle("Time interval type")
  @JsonPropertyDescription("Year, Month, Day, Hour, Minute or Second")
  var timeIntervalType: Option[TimeIntervalType] = _

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    val partitionRequirement = List(
      Option(HashPartition(List(leftAttributeName))),
      Option(HashPartition(List(rightAttributeName)))
    )

    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.intervalJoin.IntervalJoinOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas => {
          val leftTableSchema: Schema = inputSchemas(operatorInfo.inputPorts.head.id)
          val rightTableSchema: Schema = inputSchemas(operatorInfo.inputPorts.last.id)

          // Start with the left table schema
          val outputSchema = rightTableSchema.getAttributes.foldLeft(leftTableSchema) {
            (currentSchema, attr) =>
              if (currentSchema.containsAttribute(attr.getName)) {
                // Add the attribute with a suffix to avoid conflicts
                currentSchema.add(new Attribute(s"${attr.getName}#@1", attr.getType))
              } else {
                // Add the attribute as is
                currentSchema.add(attr)
              }
          }

          Map(operatorInfo.outputPorts.head.id -> outputSchema)
        })
      )
      .withPartitionRequirement(partitionRequirement)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Interval Join",
      "Join two inputs with left table join key in the range of [right table join key, right table join key + constant value]",
      OperatorGroupConstants.JOIN_GROUP,
      inputPorts = List(
        InputPort(PortIdentity(), displayName = "left table"),
        InputPort(
          PortIdentity(1),
          displayName = "right table",
          dependencies = List(PortIdentity(0))
        )
      ),
      outputPorts = List(OutputPort())
    )

  // Inner interval join: left point in [rightKey, rightKey + constant], bounds
  // toggled by include{Left,Right}Bound. Cross-join + mask computes the full
  // result (no sorted-input assumption, unlike the exec). Runtime dtype check
  // picks numeric vs pd.DateOffset (unit from timeIntervalType).
  override def generateStandaloneCode(): String = {
    val leftLit = objectMapper.writeValueAsString(leftAttributeName)
    val rightLit = objectMapper.writeValueAsString(rightAttributeName)
    val loOp = if (includeLeftBound) ">=" else ">"
    val hiOp = if (includeRightBound) "<=" else "<"
    val offsetUnit = Option(timeIntervalType).flatten match {
      case Some(TimeIntervalType.YEAR)   => "years"
      case Some(TimeIntervalType.MONTH)  => "months"
      case Some(TimeIntervalType.HOUR)   => "hours"
      case Some(TimeIntervalType.MINUTE) => "minutes"
      case Some(TimeIntervalType.SECOND) => "seconds"
      case _                             => "days" // DAY or unset
    }
    s"""_l = in1df.assign(_iv_l=in1df[$leftLit])
       |_r = in2df.assign(_iv_r=in2df[$rightLit])
       |_pairs = _l.merge(_r, how="cross", suffixes=("", "#@1"))
       |if pd.api.types.is_datetime64_any_dtype(_pairs["_iv_r"]):
       |    _iv_hi = _pairs["_iv_r"] + pd.DateOffset($offsetUnit=$constant)
       |else:
       |    _iv_hi = _pairs["_iv_r"] + $constant
       |_iv_match = (_pairs["_iv_l"] $loOp _pairs["_iv_r"]) & (_pairs["_iv_l"] $hiOp _iv_hi)
       |out1df = _pairs[_iv_match].drop(columns=["_iv_l", "_iv_r"]).reset_index(drop=True)""".stripMargin
  }

  def this(
      leftTableAttributeName: String,
      rightTableAttributeName: String,
      constant: Long,
      includeLeftBound: Boolean,
      includeRightBound: Boolean,
      timeIntervalType: TimeIntervalType
  ) = {
    this() // Calling primary constructor, and it is first line
    this.leftAttributeName = leftTableAttributeName
    this.rightAttributeName = rightTableAttributeName
    this.constant = constant
    this.includeLeftBound = includeLeftBound
    this.includeRightBound = includeRightBound
    this.timeIntervalType = Some(timeIntervalType)
  }

}
