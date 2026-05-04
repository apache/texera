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

package org.apache.texera.amber.core.workflow

import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WorkflowContextSpec extends AnyFlatSpec with Matchers {

  "WorkflowContext companion" should "expose pinned defaults" in {
    // These constants seed every default-constructed WorkflowContext, and the
    // engine's bootstrap path relies on the exact 1L identifiers for both
    // workflow and execution. Pinning so a renumber is reviewed deliberately.
    WorkflowContext.DEFAULT_WORKFLOW_ID shouldBe WorkflowIdentity(1L)
    WorkflowContext.DEFAULT_EXECUTION_ID shouldBe ExecutionIdentity(1L)
    WorkflowContext.DEFAULT_WORKFLOW_SETTINGS shouldBe WorkflowSettings()
  }

  "default WorkflowContext" should "use the companion-object defaults" in {
    val ctx = new WorkflowContext()
    ctx.workflowId shouldBe WorkflowContext.DEFAULT_WORKFLOW_ID
    ctx.executionId shouldBe WorkflowContext.DEFAULT_EXECUTION_ID
    ctx.workflowSettings shouldBe WorkflowContext.DEFAULT_WORKFLOW_SETTINGS
  }

  "WorkflowContext fields" should "be reassignable through their var accessors" in {
    val ctx = new WorkflowContext()
    ctx.workflowId = WorkflowIdentity(42L)
    ctx.executionId = ExecutionIdentity(7L)
    ctx.workflowId shouldBe WorkflowIdentity(42L)
    ctx.executionId shouldBe ExecutionIdentity(7L)
  }

  "WorkflowContext constructor" should "accept overridden defaults at construction time" in {
    val ctx = new WorkflowContext(
      workflowId = WorkflowIdentity(99L),
      executionId = ExecutionIdentity(123L)
    )
    ctx.workflowId shouldBe WorkflowIdentity(99L)
    ctx.executionId shouldBe ExecutionIdentity(123L)
    // Settings argument was not overridden, so the companion default holds.
    ctx.workflowSettings shouldBe WorkflowContext.DEFAULT_WORKFLOW_SETTINGS
  }
}
