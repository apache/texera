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

package org.apache.texera.amber.operator.split

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaString,
  JsonSchemaTitle
}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.annotations.HideAnnotation
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

class SplitOpDesc extends LogicalOp with StandaloneCodeGenerator {

  @JsonSchemaTitle("Split Percentage")
  @JsonProperty(defaultValue = "80")
  @JsonPropertyDescription("percentage of data going to the upper port")
  var k: Int = 80

  @JsonSchemaTitle("Auto-Generate Seed")
  @JsonPropertyDescription("Shuffle the data based on a random seed")
  @JsonProperty(defaultValue = "true")
  var random: Boolean = true

  @JsonSchemaTitle("Seed")
  @JsonProperty(defaultValue = "1")
  @JsonPropertyDescription("An int for reproducible output across multiple runs")
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "random"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "true")
    )
  )
  var seed: Int = 1

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.split.SplitOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withParallelizable(false)
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas => {
          Preconditions.checkArgument(inputSchemas.size == 1)
          val outputSchema = inputSchemas.values.head
          operatorInfo.outputPorts.map(port => port.id -> outputSchema).toMap
        })
      )
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      userFriendlyName = "Split",
      operatorDescription = "Split data to two different ports",
      operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(
        OutputPort(PortIdentity()),
        OutputPort(PortIdentity(1))
      ),
      dynamicInputPorts = true,
      dynamicOutputPorts = true
    )
  }

  override def generateStandaloneCode(): String = {
    // JVM SplitOpExec uses a Bernoulli per-tuple draw with a single PRNG
    // (auto seed when `random`, fixed seed otherwise). Pandas equivalent:
    // draw a uniform vector of len(in1df), threshold at k/100. The same
    // mask drives both output ports — `out1df` takes the upper k%, `out2df`
    // the remainder, so the two outputs partition the input.
    val seedExpr = if (random) "None" else seed.toString
    val frac = k.toDouble / 100.0
    s"""import numpy as np
       |_split_mask = np.random.RandomState($seedExpr).rand(len(in1df)) < $frac
       |out1df = in1df[_split_mask].reset_index(drop=True)
       |out2df = in1df[~_split_mask].reset_index(drop=True)""".stripMargin
  }
}
