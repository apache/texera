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

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalOp}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext

class LoopEndOpDesc extends LogicalOp {
  @JsonProperty(required = true, defaultValue = "i += 1")
  @JsonSchemaTitle("Update")
  var update: EncodableString = ""

  @JsonProperty(required = true, defaultValue = "i < len(table)")
  @JsonSchemaTitle("Condition")
  var condition: EncodableString = ""

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
      .withSuggestedWorkerNum(1)
      .withParallelizable(false)
      .withIsLoopEnd(true)

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Loop End",
      "Close a loop body and decide whether to iterate again based on a condition; pairs with Loop Start.",
      OperatorGroupConstants.CONTROL_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  // User-supplied `update` and `condition` are interpolated via the `pyb`
  // builder, which base64-encodes each EncodableString and renders it as
  // a `self.decode_python_template('<b64>')` expression. This means an
  // arbitrary user string -- including quotes, newlines, or backslashes
  // -- can never break the surrounding Python syntax, because the user
  // text is no longer pasted in as a raw quoted literal.
  def generatePythonCode(): String = {
    pyb"""
       |from pytexera import *
       |class ProcessLoopEndOperator(LoopEndOperator):
       |    @overrides
       |    def process_state(self, state: State, port: int) -> Optional[State]:
       |      self.state = dict(state)
       |      from pickle import loads
       |      self.state["table"] = loads(self.state["table"])
       |      exec($update, {}, self.state)
       |      return None
       |
       |    @overrides
       |    def condition(self) -> bool:
       |      exec("output = " + $condition, {}, self.state)
       |      return self.state["output"]
       |""".encode
  }
}
