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
import org.apache.texera.amber.core.workflow.PhysicalOp
import org.apache.texera.amber.operator.LogicalOp
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

/**
  * Shared scaffolding for `LoopStartOpDescSpec` and `LoopEndOpDescSpec`. Both
  * specs assert the same PhysicalOp invariants (non-parallelizable, ports
  * carried forward, OpExecWithCode wiring) and both need a base64 helper
  * for verifying that user-supplied Python expressions are emitted through the
  * `self.decode_python_template('...')` wrapper instead of inlined as raw text.
  *
  * Extracting these into a mixin keeps the two specs focused on the parts that
  * actually differ — operator name, group, generated class name, the output
  * port's reuseStorage flag, which user fields exist, and what
  * statements the generated code must contain.
  */
trait LoopOpDescSpecMixin extends Matchers {

  protected val workflowId: WorkflowIdentity = WorkflowIdentity(1L)
  protected val executionId: ExecutionIdentity = ExecutionIdentity(1L)

  /** Base64-encode a sentinel the way `pyb` would, so we can pin the exact
    * `self.decode_python_template('<b64>')` substring in generated code.
    */
  protected def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  /** The Python decode wrapper that `pyb` renders around each `EncodableString`
    * argument. Used by the per-spec generator-output assertions.
    */
  protected def decodeExpr(rawInput: String): String =
    s"self.decode_python_template('${b64(rawInput)}')"

  protected def assertNonParallelizable(physical: PhysicalOp): Assertion = {
    // LoopStart and LoopEnd both keep iteration state and the accumulated
    // table on a single worker; fanning out would break the loop body.
    // WorkerConfig forces workerCount = 1 for non-parallelizable ops, so
    // this flag alone pins the single worker.
    physical.parallelizable shouldBe false
  }

  protected def assertPortsCarriedForward(
      opDesc: LogicalOp,
      physical: PhysicalOp
  ): Assertion = {
    physical.inputPorts.size shouldBe opDesc.operatorInfo.inputPorts.size
    physical.outputPorts.size shouldBe opDesc.operatorInfo.outputPorts.size
  }

  protected def assertOpExecWithPythonCodeForClass(
      physical: PhysicalOp,
      expectedSubclassDecl: String
  ): Assertion = {
    physical.opExecInitInfo match {
      case OpExecWithCode(code, language) =>
        language shouldBe "python"
        code should include(expectedSubclassDecl)
      case other =>
        fail(s"expected OpExecWithCode, got $other")
    }
  }

  /** Verify that a user-supplied expression is emitted through the base64
    * decode wrapper, **not** inlined as a raw substring. This is the
    * codegen-robustness invariant: a `"`/`\n`/`\\` in user input must not
    * appear in the generated source unescaped, because then it would break
    * the surrounding Python syntax (or worse, change what gets executed).
    */
  protected def assertUserInputIsBase64Wrapped(code: String, rawInput: String): Assertion = {
    code should include(decodeExpr(rawInput))
    // The raw input must NOT appear verbatim anywhere in the generated
    // source. (Allowing it would mean a `"` in user input could leak past
    // the wrapper and break the template.)
    code should not include rawInput
  }
}
