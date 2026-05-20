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
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LoopStartOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

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

  "LoopStartOpDesc.generatePythonCode" should "embed the user-supplied initialization and output expressions" in {
    // The init + output strings are interpolated directly into the generated
    // class so the Python `exec` calls at runtime see the user-provided code.
    // Use distinct sentinels so we know each field is wired through and not
    // accidentally swapped (e.g. init pasted in place of output).
    val code = desc(init = "INIT_SENT", out = "OUT_SENT").generatePythonCode()
    code should include("INIT_SENT")
    code should include("OUT_SENT")
  }

  it should "subclass LoopStartOperator from pytexera" in {
    // The generated class must extend the base LoopStartOperator (defined
    // in core.models.operator) so the runtime's
    // `isinstance(executor, LoopStartOperator)` branch in main_loop fires
    // for it. A rename of either side should break this assertion.
    val code = desc().generatePythonCode()
    code should include("from pytexera import *")
    code should include("class ProcessLoopStartOperator(LoopStartOperator)")
  }

  it should "wire the initialization expression into open() and the output expression into process_table()" in {
    // The user's `initialization` runs once in `open()` to seed self.state
    // (specifically self.state['loop_counter'] = 0 plus user vars); the
    // user's `output` runs in `process_table()` against the buffered table.
    // Pin both call sites so a future refactor that swaps the two doesn't
    // silently produce a runnable-looking class that loops over nothing.
    val code = desc(init = "i = 0", out = "table.iloc[i]").generatePythonCode()
    code should include("def open(self)")
    code should include("\"loop_counter\": 0")
    code should include("exec(\"i = 0\"")
    code should include("def process_table(self, table: Table, port: int)")
    code should include("exec(\"output = table.iloc[i]\"")
  }

  "LoopStartOpDesc.getPhysicalOp" should "produce a non-parallelizable PhysicalOp pinned to a single worker" in {
    // LoopStart must run on exactly one worker because the loop state
    // (self.state, the accumulated table) is per-instance, not distributed.
    // Parallelizing it would fan-out the table and break the loop body's
    // per-iteration invariants.
    val physical = desc().getPhysicalOp(workflowId, executionId)
    physical.parallelizable shouldBe false
    physical.suggestedWorkerNum shouldBe Some(1)
  }

  it should "not be tagged as a loop end" in {
    // The isLoopEnd flag is consumed by RegionExecutionCoordinator to skip
    // recreating result/state tables across loop iterations. LoopStart
    // must NOT carry the flag — only LoopEnd does.
    val physical = desc().getPhysicalOp(workflowId, executionId)
    physical.isLoopEnd shouldBe false
  }

  it should "carry the generated Python code via OpExecWithCode" in {
    val physical = desc().getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithCode(code, language) =>
        language shouldBe "python"
        code should include("class ProcessLoopStartOperator(LoopStartOperator)")
      case other =>
        fail(s"expected OpExecWithCode, got $other")
    }
  }

  it should "carry forward the operatorInfo input/output ports onto the PhysicalOp" in {
    val physical = desc().getPhysicalOp(workflowId, executionId)
    physical.inputPorts.size shouldBe desc().operatorInfo.inputPorts.size
    physical.outputPorts.size shouldBe desc().operatorInfo.outputPorts.size
  }
}
