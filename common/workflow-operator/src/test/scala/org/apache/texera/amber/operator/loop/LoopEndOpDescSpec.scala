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

class LoopEndOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

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

  "LoopEndOpDesc.generatePythonCode" should "embed the user-supplied update and condition expressions" in {
    // Distinct sentinels so we know the codegen wires the right user field
    // into the right `exec(...)` site. If `condition` were accidentally
    // pasted in place of `update`, a generic `code.contains("i")` check
    // would still pass — these sentinels force the asymmetry.
    val code = desc(update = "UPDATE_SENT", condition = "COND_SENT").generatePythonCode()
    code should include("UPDATE_SENT")
    code should include("COND_SENT")
  }

  it should "subclass LoopEndOperator from pytexera" in {
    // Runtime branch `isinstance(executor, LoopEndOperator)` in main_loop
    // gates the loop-end reset path; a rename of either side must break
    // this assertion.
    val code = desc().generatePythonCode()
    code should include("from pytexera import *")
    code should include("class ProcessLoopEndOperator(LoopEndOperator)")
  }

  it should "declare condition() as returning bool, matching the abstract base" in {
    // The abstract base in operator.py was fixed to `-> bool`; the
    // generator template must agree. A `-> None` slip here would produce
    // a class that disagrees with the abstract contract.
    val code = desc().generatePythonCode()
    code should include("def condition(self) -> bool:")
  }

  it should "decrement loop_counter and pass state through when loop_counter > 0 (nested-loop case)" in {
    // For nested loops, the inner LoopEnd sees state belonging to an
    // outer loop. The generated process_state recognises this by a
    // positive loop_counter and just decrements + returns the state,
    // leaving the actual loop-control work to the outer LoopEnd.
    // This branch is critical for nested-for-loop correctness so pin
    // its shape explicitly.
    val code = desc().generatePythonCode()
    code should include("loop_counter = int(state.get(\"loop_counter\", 0))")
    code should include("if loop_counter > 0:")
    code should include("state[\"loop_counter\"] = loop_counter - 1")
  }

  it should "stash state, deserialize the pickled table, and run the user update on the matching-loop branch" in {
    val code = desc(update = "i = i + 7").generatePythonCode()
    // The matching-loop branch is the path the user's `update` expression
    // runs on. Pin the pickle round-trip and the exec call so a refactor
    // of either is intentional.
    code should include("self.state = dict(state)")
    code should include("from pickle import loads")
    code should include("self.state[\"table\"] = loads(self.state[\"table\"])")
    code should include("exec(\"i = i + 7\"")
  }

  it should "evaluate the user condition in process-shared state" in {
    val code = desc(condition = "i < 3").generatePythonCode()
    // condition() must read from self.state (populated by the matching-
    // loop branch above) and assign into self.state["output"] before
    // returning it. Pinning both the exec format and the assignment
    // keeps a future "just return the expr" refactor from silently
    // dropping the state side-effect main_loop.complete() depends on.
    code should include("exec(\"output = i < 3\"")
    code should include("self.state[\"output\"]")
  }

  "LoopEndOpDesc.getPhysicalOp" should "produce a non-parallelizable PhysicalOp pinned to a single worker" in {
    // Same reasoning as LoopStart: the loop body's per-iteration state
    // is per-instance, and the accumulated table must be a single buffer.
    val physical = desc().getPhysicalOp(workflowId, executionId)
    physical.parallelizable shouldBe false
    physical.suggestedWorkerNum shouldBe Some(1)
  }

  it should "be tagged as a loop end so RegionExecutionCoordinator skips iceberg recreation" in {
    // The isLoopEnd flag drives the
    // `if (!isLoopEndRegion || !DocumentFactory.documentExists(...))`
    // branch in RegionExecutionCoordinator. Without the tag, every loop
    // iteration would unconditionally recreate the result/state tables
    // and lose accumulated data. The flag must be set.
    val physical = desc().getPhysicalOp(workflowId, executionId)
    physical.isLoopEnd shouldBe true
  }

  it should "carry the generated Python code via OpExecWithCode" in {
    val physical = desc().getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithCode(code, language) =>
        language shouldBe "python"
        code should include("class ProcessLoopEndOperator(LoopEndOperator)")
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
