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
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

/**
  * Block-level try/catch: the subgraph fed by the
  * Try port is one attempt; if any operator in it fails, the same input is
  * replayed from a snapshot through the Catch port into the fallback subgraph.
  * Pair with a Finally operator to reconverge the winning branch's results.
  *
  * Expands to two physical operators:
  *  - splitter: tees the input — live stream out the Try port, eager snapshot
  *    out an internal port toward the gate.
  *  - gate: an If generalized to N conditions. Signal ports (one per try-cone
  *    leaf output port, added by the compiler pass) collect error States; the
  *    snapshot port depends on all of them, so it is consumed only after the
  *    try side fully resolved. Own-cone error seen => forward the snapshot into
  *    the catch subgraph; clean completion or foreign error => drop it.
  */
class TryCatchOpDesc extends LogicalOp {

  override def getPhysicalPlan(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalPlan = {
    val dataInput = operatorInfo.inputPorts.head // Data (0)
    val tryOutput = operatorInfo.outputPorts.head // Try (0, external)
    val catchOutput = operatorInfo.outputPorts(1) // Catch (1, external, on the gate)
    val errorInfoOutput = operatorInfo.outputPorts(2) // Error Info (2, on the gate)

    val snapshotOut = OutputPort(TryCatchOpDesc.SNAPSHOT_OUT, "snapshot")
    val snapshotIn = InputPort(TryCatchOpDesc.SNAPSHOT_IN, "snapshot")

    val splitter = PhysicalOp
      .oneToOnePhysicalOp(
        PhysicalOpIdentity(operatorIdentifier, TryCatchOpDesc.SPLITTER_LAYER),
        workflowId,
        executionId,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.trycatch.TrySplitterOpExec",
          // non-empty: ExecFactory routes empty descStrings to a legacy
          // (int, int) constructor signature
          "{}"
        )
      )
      .withInputPorts(List(dataInput))
      .withOutputPorts(List(tryOutput, snapshotOut))
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas => {
          val inputSchema = inputSchemas(dataInput.id)
          Map(tryOutput.id -> inputSchema, snapshotOut.id -> inputSchema)
        })
      )

    // Signal ports and the snapshot port's dependencies on them are added by
    // the compiler pass once the try cone is known (they are one-per-leaf).
    val gate = PhysicalOp
      .oneToOnePhysicalOp(
        PhysicalOpIdentity(operatorIdentifier, TryCatchOpDesc.GATE_LAYER),
        workflowId,
        executionId,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.trycatch.CatchGateOpExec",
          objectMapper.writeValueAsString(new CatchGateConfig())
        )
      )
      .withInputPorts(List(snapshotIn))
      .withOutputPorts(List(catchOutput, errorInfoOutput))
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas =>
          Map(
            catchOutput.id -> inputSchemas(snapshotIn.id),
            errorInfoOutput.id -> TryCatchOpDesc.ERROR_INFO_SCHEMA
          )
        )
      )

    PhysicalPlan(
      operators = Set(splitter, gate),
      links = Set(PhysicalLink(splitter.id, snapshotOut.id, gate.id, snapshotIn.id))
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Try Catch",
      "Run the Try subgraph as one attempt; on any failure, replay the same input through the Catch subgraph",
      OperatorGroupConstants.CONTROL_GROUP,
      inputPorts = List(InputPort(PortIdentity(), "Data")),
      outputPorts = List(
        OutputPort(PortIdentity(), "Try"),
        OutputPort(PortIdentity(1), "Catch"),
        OutputPort(PortIdentity(2), "Error Info")
      )
    )
}

object TryCatchOpDesc {
  val SPLITTER_LAYER = "splitter"
  val GATE_LAYER = "gate"

  // splitter-side snapshot output port
  val SNAPSHOT_OUT: PortIdentity = PortIdentity(1, internal = true)
  // gate-side snapshot input port; signal ports use internal ids 1..N
  val SNAPSHOT_IN: PortIdentity = PortIdentity(0, internal = true)
  // gate-side error report output port (one row per caught own-cone failure,
  // deduplicated on (operatorId, workerId); catch-cone failures escalate to
  // the enclosing frame's report instead)
  val ERROR_INFO_PORT: PortIdentity = PortIdentity(2)

  val ERROR_INFO_SCHEMA: Schema = Schema()
    .add("errorType", AttributeType.STRING)
    .add("message", AttributeType.STRING)
    .add("operatorId", AttributeType.STRING)
    .add("workerId", AttributeType.STRING)
}
