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

import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class LoopEndOpDescSpec extends AnyFlatSpec with LoopOpDescSpecMixin {

  private def desc(
      update: String = "i += 1",
      condition: String = "i < len(table)"
  ): LoopEndOpDesc = {
    val d = new LoopEndOpDesc()
    d.update = update
    d.condition = condition
    d
  }

  "LoopEndOpDesc.operatorInfo" should "advertise the user-friendly name and Control group" in {
    val info = desc().operatorInfo
    info.userFriendlyName shouldBe "Loop End"
    info.operatorGroupName shouldBe OperatorGroupConstants.CONTROL_GROUP
    info.operatorDescription should include("loop")
  }

  it should "expose exactly one input port and one output port" in {
    val info = desc().operatorInfo
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "LoopEndOpDesc.generatePythonCode" should "wrap user inputs in the base64 decode template" in {
    // Distinct sentinels so we know the codegen wires the right user
    // field into the right `decode_python_template` site. If `condition`
    // were accidentally pasted in place of `update`, a generic
    // `code.contains("i")` would still pass -- these sentinels force the
    // asymmetry.
    val code = desc(update = "UPDATE_SENT", condition = "COND_SENT").generatePythonCode()
    assertUserInputIsBase64Wrapped(code, "UPDATE_SENT")
    assertUserInputIsBase64Wrapped(code, "COND_SENT")
  }

  it should "subclass LoopEndOperator from pytexera" in {
    // Runtime branch `isinstance(executor, LoopEndOperator)` in
    // main_loop gates the loop-end reset_storage path; a rename of
    // either side must break this assertion.
    val code = desc().generatePythonCode()
    code should include("from pytexera import *")
    code should include("class ProcessLoopEndOperator(LoopEndOperator)")
  }

  it should "declare condition() as returning bool, matching the abstract base" in {
    // The abstract base in operator.py is `-> bool`; the generator
    // template must agree. A `-> None` slip would produce a class that
    // disagrees with the abstract contract.
    val code = desc().generatePythonCode()
    code should include("def condition(self) -> bool:")
  }

  it should "delegate the user update to run_update with no loop_counter handling" in {
    // loop_counter is owned by the runtime; the nested-loop pass-through happens
    // in main_loop._process_state_frame before the operator is invoked, so the
    // generated LoopEnd only runs the matching-loop (consume) path. The user
    // `update` runs through the guarded run_update helper (which keeps the
    // reserved `table` out of self.state), not inline against self.state.
    val code = desc(update = "i = i + 7").generatePythonCode()
    code should not include "loop_counter"
    code should include(s"self.run_update(${decodeExpr("i = i + 7")}, state)")
  }

  it should "not exec user code inline against self.state (the guard lives in the base helpers)" in {
    // The table decode (Arrow IPC) and the user update/condition exec run in
    // the LoopEnd base helpers (run_update / eval_condition) against a
    // throwaway namespace, so the reserved `table` never persists in the loop
    // state (a user rebind raises). The generated operator must not touch it
    // directly or exec user code against self.state.
    val code = desc(update = "i = i + 7", condition = "i < 3").generatePythonCode()
    code should not include "exec("
    code should not include "self.state[\"table\"]"
    code should not include "self.state[\"output\"]"
  }

  it should "delegate the user condition to eval_condition" in {
    val code = desc(condition = "i < 3").generatePythonCode()
    code should include(s"return self.eval_condition(${decodeExpr("i < 3")})")
  }

  // Tricky-input round-trips (quotes / newlines / backslashes / unicode)
  // through the base64 codegen are pinned exhaustively by PythonTemplateBuilderSpec.

  "LoopEndOpDesc" should "round-trip its user expressions through the polymorphic base" in {
    // Pins the @JsonSubTypes "LoopEnd" registration on LogicalOp -- the only
    // thing that makes this desc deserializable from a workflow JSON.
    val d = desc(update = "j += 2", condition = "j < 10")
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[LoopEndOpDesc]
    val r = restored.asInstanceOf[LoopEndOpDesc]
    r.update shouldBe "j += 2"
    r.condition shouldBe "j < 10"
  }

  // ---- PhysicalOp wiring --------------------------------------------------

  "LoopEndOpDesc.getPhysicalOp" should "produce a non-parallelizable PhysicalOp" in {
    assertNonParallelizable(desc().getPhysicalOp(workflowId, executionId))
  }

  "LoopEndOpDesc.getPhysicalOp" should "require materialized execution" in {
    // The loop back-edge is the cross-region materialized state channel, so the
    // scheduler forces a fully-materialized schedule (PhysicalOp.requiresMaterializedExecution).
    desc().getPhysicalOp(workflowId, executionId).requiresMaterializedExecution shouldBe true
  }

  it should "not mark the physical op as a loop start" in {
    // Only Loop Start is a jump / write-back target; the scheduler must not
    // mint a loop-back entry for a Loop End.
    desc().getPhysicalOp(workflowId, executionId).isLoopStart shouldBe false
  }

  it should "reuse its output storage across re-execution so RegionExecutionManager skips iceberg recreation" in {
    // The output port's `reuseStorage` flag drives the create-or-reuse
    // decision in DocumentFactory.createOrReuseDocument (called by
    // RegionExecutionManager). Without it, every loop iteration would
    // unconditionally recreate the result/state tables and lose accumulated
    // data. The flag must be set on Loop End's output port.
    val physical = desc().getPhysicalOp(workflowId, executionId)
    physical.outputPorts.values.head._1.reuseStorage shouldBe true
  }

  it should "carry the generated Python code via OpExecWithCode" in {
    assertOpExecWithPythonCodeForClass(
      desc().getPhysicalOp(workflowId, executionId),
      "class ProcessLoopEndOperator(LoopEndOperator)"
    )
  }

  it should "carry forward the operatorInfo input/output ports onto the PhysicalOp" in {
    val opDesc = desc()
    assertPortsCarriedForward(opDesc, opDesc.getPhysicalOp(workflowId, executionId))
  }
}
