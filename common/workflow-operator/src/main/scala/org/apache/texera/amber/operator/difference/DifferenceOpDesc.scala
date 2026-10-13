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

package org.apache.texera.amber.operator.difference

import com.google.common.base.Preconditions
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator, StandaloneHelpers}

class DifferenceOpDesc extends LogicalOp with StandaloneCodeGenerator {
  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName("org.apache.texera.amber.operator.difference.DifferenceOpExec")
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPartitionRequirement(List(Option(HashPartition()), Option(HashPartition())))
      .withDerivePartition(_ => HashPartition())
      .withPropagateSchema(SchemaPropagationFunc(inputSchemas => {
        Preconditions.checkArgument(inputSchemas.values.toSet.size == 1)
        val outputSchema = inputSchemas.values.head
        operatorInfo.outputPorts.map(port => port.id -> outputSchema).toMap
      }))
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Difference",
      "find the set difference of two inputs",
      OperatorGroupConstants.SET_GROUP,
      inputPorts = List(
        InputPort(PortIdentity(), displayName = "left"),
        InputPort(PortIdentity(1), displayName = "right")
      ),
      outputPorts = List(OutputPort(blocking = true))
    )

  // Distinct left (port 0) rows the right (port 1) does not hold, by the
  // engine's row equality.
  override def generateStandaloneCode(): String =
    """_texera_set_right = set(_texera_set_row_keys(in2df))
      |out1df = _texera_set_select(in1df, lambda key: key not in _texera_set_right)""".stripMargin

  override def standaloneHelpers(): Seq[String] = Seq(StandaloneHelpers.SetRowKeys)
}
