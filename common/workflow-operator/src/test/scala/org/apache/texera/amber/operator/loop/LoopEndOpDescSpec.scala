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

import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
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

  it should "generate a consume-only process_state with no loop_counter handling" in {
    // loop_counter is owned by the worker runtime now, not the operator. The
    // nested-loop pass-through (decrement + forward) happens in
    // main_loop._process_state_frame before the operator is invoked, so the
    // generated LoopEnd only ever runs the matching-loop (consume) path and
    // must not read or mutate loop_counter. Pin the absence so a regression
    // that re-introduces operator-side counter handling is caught.
    val code = desc().generatePythonCode()
    code should not include "loop_counter"
    code should include("self.state = dict(state)")
  }

  it should "stash state, deserialize the pickled table, and run the decoded update on the matching-loop branch" in {
    val code = desc(update = "i = i + 7").generatePythonCode()
    // The matching-loop branch is the path the user's `update`
    // expression runs on. Pin the pickle round-trip and the exec call
    // so a refactor of either is intentional.
    code should include("self.state = dict(state)")
    code should include("from pickle import loads")
    code should include("self.state[\"table\"] = loads(self.state[\"table\"])")
    code should include(s"exec(${decodeExpr("i = i + 7")}, {}, self.state)")
  }

  it should "evaluate the decoded user condition in process-shared state" in {
    val code = desc(condition = "i < 3").generatePythonCode()
    // condition() must read from self.state (populated by the matching-
    // loop branch above) and assign into self.state["output"] before
    // returning it. Pinning both the exec format and the assignment
    // keeps a future "just return the expr" refactor from silently
    // dropping the state side-effect main_loop.complete() depends on.
    code should include(s"""exec("output = " + ${decodeExpr("i < 3")}, {}, self.state)""")
    code should include("self.state[\"output\"]")
  }

  // ---- codegen robustness -------------------------------------------------
  //
  // These tests address PR #4206 reviewer feedback that the old s"..."
  // template was vulnerable to user input containing quotes / newlines /
  // backslashes. With the `pyb` interpolator and an `EncodableString`-typed
  // field, the raw user text never appears in the generated source -- only
  // its base64-encoded form does -- so quotes etc. can never break the
  // surrounding Python syntax.

  it should "encode a user update containing double quotes" in {
    val tricky = """name = "alice""""
    val code = desc(update = tricky).generatePythonCode()
    assertUserInputIsBase64Wrapped(code, tricky)
  }

  it should "encode a user condition containing single quotes" in {
    val tricky = "name != 'bob'"
    val code = desc(condition = tricky).generatePythonCode()
    assertUserInputIsBase64Wrapped(code, tricky)
  }

  it should "encode a user update containing newlines" in {
    val tricky = "i += 1\nj += 1"
    val code = desc(update = tricky).generatePythonCode()
    assertUserInputIsBase64Wrapped(code, tricky)
  }

  it should "encode a user condition containing backslashes" in {
    val tricky = """i < len("a\\b")"""
    val code = desc(condition = tricky).generatePythonCode()
    assertUserInputIsBase64Wrapped(code, tricky)
  }

  // ---- PhysicalOp wiring --------------------------------------------------

  "LoopEndOpDesc.getPhysicalOp" should "produce a non-parallelizable PhysicalOp pinned to a single worker" in {
    assertNonParallelizableSingleWorker(desc().getPhysicalOp(workflowId, executionId))
  }

  it should "be tagged as a loop end so RegionExecutionCoordinator skips iceberg recreation" in {
    // The isLoopEnd flag drives the
    // `if (!isLoopEndRegion || !DocumentFactory.documentExists(...))`
    // branch in RegionExecutionCoordinator. Without the tag, every
    // loop iteration would unconditionally recreate the result/state
    // tables and lose accumulated data. The flag must be set.
    val physical = desc().getPhysicalOp(workflowId, executionId)
    physical.isLoopEnd shouldBe true
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
