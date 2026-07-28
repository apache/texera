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

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalOp}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

/**
  * Shared base for the Loop Start / Loop End operator descriptors. Both are
  * single-worker, non-parallelizable CONTROL operators that code-gen a Python
  * class from user expressions. The loop's outer boundary is pinned by two
  * materialized links: the link INTO Loop Start (the loop back-edge writes
  * the next iteration's state into its storage; forced by the scheduler via
  * PhysicalOp.isLoopStart) and the link OUT OF Loop End (a blocking output
  * port: downstream may only read the accumulated result after the loop
  * completes). Everything between them -- Loop Start, the body, and Loop End
  * -- may be scheduled as ordinary, possibly pipelined, regions that are
  * re-executed per iteration. Subclasses supply the operator
  * name/description, the generated Python body, and -- for Loop End --
  * whether output storage is reused across region re-executions.
  */
abstract class LoopOpDesc extends LogicalOp {

  /**
    * Generated ``ProcessLoop*Operator`` Python class wiring the user expressions.
    *
    * The user-supplied expressions are interpolated via the `pyb` builder, which
    * base64-encodes each EncodableString and renders it as a
    * `self.decode_python_template('<b64>')` expression. So an arbitrary user
    * string -- including quotes, newlines, or backslashes -- can never break the
    * surrounding Python syntax, because the text is no longer pasted in raw.
    */
  def generatePythonCode(): String

  protected def operatorName: String

  protected def operatorDescription: String

  /**
    * Loop End accumulates output across its iterations and so reuses its output
    * storage on region re-execution; Loop Start does not.
    */
  protected def reuseStorage: Boolean = false

  /**
    * Loop End's output port is blocking: its accumulated result is complete
    * only once the loop has finished, so downstream operators must not
    * consume it earlier. The scheduler therefore always materializes the
    * link out of Loop End, making it the loop's downstream boundary. Loop
    * Start's output is NOT blocking -- it may pipeline into the body.
    */
  protected def outputBlocking: Boolean = false

  /**
    * Marks the Loop Start operator; the scheduler resolves each Loop Start's
    * loop-back state write address from this flag (see PhysicalOp.isLoopStart).
    * JsonIgnore: Jackson's is-getter convention would otherwise serialize this
    * as a "loopStart" JSON property, which then fails deserialization as an
    * unrecognized field (it is engine wiring, not a user-facing property).
    */
  @JsonIgnore
  protected def isLoopStart: Boolean = false

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
      .withOutputPorts(operatorInfo.outputPorts)
      // WorkerConfig forces workerCount = 1 for non-parallelizable ops, which
      // keeps the loop state and accumulated table on a single worker.
      .withParallelizable(false)
      .withIsLoopStart(isLoopStart)

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      operatorName,
      operatorDescription,
      OperatorGroupConstants.CONTROL_GROUP,
      inputPorts = List(InputPort()),
      // Loop End reuses its output storage across region re-executions (it
      // accumulates across the iterations of its own loop) and declares its
      // output blocking; both flags are declared on the output port and the
      // scheduler reads them there.
      outputPorts = List(OutputPort(blocking = outputBlocking, reuseStorage = reuseStorage))
    )
}
