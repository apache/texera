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

package org.apache.texera.amber.operator.union

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalOp}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}

class UnionOpDesc extends LogicalOp with StandaloneCodeGenerator {

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName("org.apache.texera.amber.operator.union.UnionOpExec")
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Union",
      "Unions the output rows from multiple input operators",
      OperatorGroupConstants.SET_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  // UNION ALL semantics: UnionOpExec passes every input tuple straight through
  // (no dedup), so we concatenate and keep duplicates.
  //
  // KNOWN LIMITATION: Union has a single *variadic* input port that accepts N
  // upstream links, with N unknown at codegen time. The translator's
  // in1df/in2df placeholder scheme can only express a fixed arity, so we cover
  // the 2-input case here. A 3rd+ upstream maps to an unreferenced in3df and is
  // silently dropped. A general fix (a variadic placeholder) is integration-
  // branch work — see the project Open Questions.
  override def generateStandaloneCode(): String =
    "out1df = pd.concat([in1df, in2df], ignore_index=True)"
}
