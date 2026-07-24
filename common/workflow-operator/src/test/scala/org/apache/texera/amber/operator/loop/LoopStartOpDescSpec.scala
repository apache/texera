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

class LoopStartOpDescSpec extends AnyFlatSpec with LoopOpDescSpecMixin {

  private def desc(init: String = "i = 0", out: String = "table.iloc[i]"): LoopStartOpDesc = {
    val d = new LoopStartOpDesc()
    d.initialization = init
    d.output = out
    d
  }

  "LoopStartOpDesc.operatorInfo" should "advertise the user-friendly name and Control group" in {
    val info = desc().operatorInfo
    info.userFriendlyName shouldBe "Loop Start"
    info.operatorGroupName shouldBe OperatorGroupConstants.CONTROL_GROUP
    info.operatorDescription should include("loop")
  }

  it should "expose exactly one input port and one output port" in {
    val info = desc().operatorInfo
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "LoopStartOpDesc.generatePythonCode" should "wrap user inputs in the base64 decode template" in {
    // Distinct sentinels prove the codegen routes the right user field
    // through the encode pipeline (not accidentally swapped) and that
    // each appears at exactly the expected `decode_python_template`
    // call site.
    val code = desc(init = "INIT_SENT", out = "OUT_SENT").generatePythonCode()
    assertUserInputIsBase64Wrapped(code, "INIT_SENT")
    assertUserInputIsBase64Wrapped(code, "OUT_SENT")
  }

  it should "subclass LoopStartOperator from pytexera" in {
    // Runtime branch `isinstance(executor, LoopStartOperator)` in
    // main_loop gates the loop-start state-attach path; a rename of
    // either side must break this assertion.
    val code = desc().generatePythonCode()
    code should include("from pytexera import *")
    code should include("class ProcessLoopStartOperator(LoopStartOperator)")
  }

  it should "place the decoded initialization expression inside open() and the decoded output expression inside process_table()" in {
    // The user's `initialization` is run in open() to seed self.state;
    // the user's `output` is run in process_table() against the buffered
    // table. Pin both call sites so a future refactor that swaps the two
    // does not silently produce a runnable-looking class that loops over
    // nothing.
    val code = desc(init = "i = 0", out = "table.iloc[i]").generatePythonCode()
    code should include("def open(self)")
    // The initialization runs through the guarded run_initialization helper
    // (exec against a globals namespace so comprehensions/lambdas resolve,
    // then `__builtins__` stripped). loop_counter is runtime-owned and must
    // not be seeded into the operator's state.
    code should not include "loop_counter"
    code should include(s"self.run_initialization(${decodeExpr("i = 0")})")
    code should include("def process_table(self, table: Table, port: int)")
    // The output expression runs through the guarded eval_output helper so
    // the reserved `table` stays out of the persistent loop state.
    code should include(s"yield self.eval_output(${decodeExpr("table.iloc[i]")}, table)")
  }

  // Tricky-input round-trips (quotes / newlines / backslashes / unicode)
  // through the base64 codegen are pinned exhaustively by PythonTemplateBuilderSpec.

  "LoopStartOpDesc" should "round-trip its user expressions through the polymorphic base" in {
    // Pins the @JsonSubTypes "LoopStart" registration on LogicalOp -- the only
    // thing that makes this desc deserializable from a workflow JSON.
    val d = desc(init = "j = 10", out = "table.head(j)")
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[LoopStartOpDesc]
    val r = restored.asInstanceOf[LoopStartOpDesc]
    r.initialization shouldBe "j = 10"
    r.output shouldBe "table.head(j)"
  }

  // ---- PhysicalOp wiring --------------------------------------------------

  "LoopStartOpDesc.getPhysicalOp" should "produce a non-parallelizable PhysicalOp" in {
    assertNonParallelizable(desc().getPhysicalOp(workflowId, executionId))
  }

  "LoopStartOpDesc.getPhysicalOp" should "require materialized execution" in {
    // The loop back-edge is the cross-region materialized state channel, so the
    // scheduler forces a fully-materialized schedule (PhysicalOp.requiresMaterializedExecution).
    desc().getPhysicalOp(workflowId, executionId).requiresMaterializedExecution shouldBe true
  }

  it should "mark the physical op as the loop start" in {
    // The scheduler resolves each Loop Start's loop-back write address (the
    // state URI of its input port) from this flag and delivers it to workers
    // at setup via InitializeExecutorRequest.loopStartStateUris.
    desc().getPhysicalOp(workflowId, executionId).isLoopStart shouldBe true
  }

  it should "not reuse output storage across re-execution" in {
    // The output port's `reuseStorage` flag is consumed by
    // RegionExecutionManager (via DocumentFactory.createOrReuseDocument) to
    // skip recreating result/state tables across loop iterations. LoopStart's
    // port must NOT carry it -- only LoopEnd (which accumulates output) does.
    val physical = desc().getPhysicalOp(workflowId, executionId)
    physical.outputPorts.values.head._1.reuseStorage shouldBe false
  }

  it should "carry the generated Python code via OpExecWithCode" in {
    assertOpExecWithPythonCodeForClass(
      desc().getPhysicalOp(workflowId, executionId),
      "class ProcessLoopStartOperator(LoopStartOperator)"
    )
  }

  it should "carry forward the operatorInfo input/output ports onto the PhysicalOp" in {
    val opDesc = desc()
    assertPortsCarriedForward(opDesc, opDesc.getPhysicalOp(workflowId, executionId))
  }
}
