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

package org.apache.texera.amber.operator.loop

import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalOp}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

/**
  * Shared base for the Loop Start / Loop End operator descriptors. Both are
  * single-worker, non-parallelizable CONTROL operators that code-gen a Python
  * class from user expressions and require MATERIALIZED execution (the loop
  * back-edge is a cross-region materialized state channel). Subclasses supply
  * the operator name/description, the generated Python body, and -- for Loop
  * End -- whether output storage is reused across region re-executions.
  */
abstract class LoopOpDesc extends LogicalOp {

  /** Generated ``ProcessLoop*Operator`` Python class wiring the user expressions. */
  def generatePythonCode(): String

  protected def operatorName: String

  protected def operatorDescription: String

  /**
    * Loop End accumulates output across its iterations and so reuses its output
    * storage on region re-execution; Loop Start does not.
    */
  protected def reusesOutputStorage: Boolean = false

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp =
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithCode(generatePythonCode(), "python")
      )
      .withInputPorts(operatorInfo.inputPorts)
      // Loop End reuses its output storage across region re-executions (it
      // accumulates across the iterations of its own loop), so the flag rides
      // its output port; the region scheduler reads it per output port.
      .withOutputPorts(
        operatorInfo.outputPorts.map(_.copy(reusesOutputStorage = reusesOutputStorage))
      )
      .withSuggestedWorkerNum(1)
      .withParallelizable(false)

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      operatorName,
      operatorDescription,
      OperatorGroupConstants.CONTROL_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  // A loop's back-edge is the cross-region materialized state channel, which
  // only exists under MATERIALIZED execution mode.
  override def requiresMaterializedExecution: Boolean = true
}
