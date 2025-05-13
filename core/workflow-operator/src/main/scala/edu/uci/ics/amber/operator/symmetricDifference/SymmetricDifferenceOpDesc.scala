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

package edu.uci.ics.amber.operator.symmetricDifference

import com.google.common.base.Preconditions
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.workflow.{
  HashPartition,
  InputPort,
  OutputPort,
  PhysicalOp,
  PortIdentity,
  SchemaPropagationFunc
}
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}

class SymmetricDifferenceOpDesc extends LogicalOp {

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
          "edu.uci.ics.amber.operator.symmetricDifference.SymmetricDifferenceOpExec"
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPartitionRequirement(List(Option(HashPartition()), Option(HashPartition())))
      .withDerivePartition(_ => HashPartition(List()))
      .withPropagateSchema(SchemaPropagationFunc(inputSchemas => {
        Preconditions.checkArgument(inputSchemas.values.toSet.size == 1)
        val outputSchema = inputSchemas.values.head
        operatorInfo.outputPorts.map(port => port.id -> outputSchema).toMap
      }))
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "SymmetricDifference",
      "find the symmetric difference (the set of elements which are in either of the sets, but not in their intersection) of two inputs",
      OperatorGroupConstants.SET_GROUP,
      inputPorts = List(InputPort(PortIdentity(0)), InputPort(PortIdentity(1))),
      outputPorts = List(OutputPort(blocking = true))
    )

}
