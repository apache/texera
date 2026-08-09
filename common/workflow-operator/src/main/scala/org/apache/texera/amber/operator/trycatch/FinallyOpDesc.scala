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

package org.apache.texera.amber.operator.trycatch

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

/**
  * Reconvergence point of a TryCatch frame: emits exactly one branch's complete
  * results — the try side's when the attempt succeeded, the catch side's when it
  * failed (all-or-nothing; never a mix). `From Catch` depends on `From Try`, so
  * the try side resolves fully before the catch side is consumed — the release
  * decision is deterministic, never a race.
  *
  * The winner leaves through the port named after it: `Try Result` on success,
  * `Catch Result` on failure. Rows never cross ports, so each result port
  * carries its own branch's schema — the branches need not agree. The outcome
  * is therefore observable downstream — connect only one port to react to
  * that outcome, or (when the branches do share a schema) connect both to the
  * same downstream input (a Union) to get "the winner, whichever it was".
  */
class FinallyOpDesc extends LogicalOp {

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
          "org.apache.texera.amber.operator.trycatch.FinallyMergerOpExec",
          objectMapper.writeValueAsString(new FinallyMergerConfig())
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas => {
          // Each result port adopts its own branch's schema: try rows only
          // ever leave through Try Result and catch rows through Catch
          // Result, so the branches need not agree. Wiring both ports into
          // one downstream input is a Union, which enforces schema
          // compatibility itself, like any other Union.
          Map(
            FinallyOpDesc.TRY_RESULT -> inputSchemas(operatorInfo.inputPorts.head.id),
            FinallyOpDesc.CATCH_RESULT -> inputSchemas(operatorInfo.inputPorts.last.id)
          )
        })
      )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Finally",
      "Emit the winning branch of a Try Catch frame: try results on Try Result when the attempt succeeds, catch results on Catch Result when it fails",
      OperatorGroupConstants.CONTROL_GROUP,
      inputPorts = List(
        InputPort(PortIdentity(), "From Try"),
        InputPort(PortIdentity(1), "From Catch", dependencies = List(PortIdentity()))
      ),
      outputPorts = List(
        OutputPort(FinallyOpDesc.TRY_RESULT, "Try Result"),
        OutputPort(FinallyOpDesc.CATCH_RESULT, "Catch Result")
      )
    )
}

object FinallyOpDesc {
  val TRY_RESULT: PortIdentity = PortIdentity()
  val CATCH_RESULT: PortIdentity = PortIdentity(1)
}
