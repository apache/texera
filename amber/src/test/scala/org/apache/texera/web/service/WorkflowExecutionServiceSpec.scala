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

package org.apache.texera.web.service

import org.apache.texera.amber.core.workflow.{ExecutionMode, WorkflowSettings}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.loop.{LoopEndOpDesc, LoopStartOpDesc}
import org.apache.texera.amber.operator.sleep.SleepOpDesc
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WorkflowExecutionServiceSpec extends AnyFlatSpec with Matchers {

  private val pipelined = WorkflowSettings(executionMode = ExecutionMode.PIPELINED)
  private val materialized = WorkflowSettings(executionMode = ExecutionMode.MATERIALIZED)

  // The flag is what validateExecutionMode keys off, replacing the old
  // isInstanceOf[LoopStartOpDesc] check. Pin it on the operators so a
  // regression in either the loop ops or the base default is caught.
  "LogicalOp.requiresMaterializedExecution" should "be set on both loop operators and unset otherwise" in {
    new LoopStartOpDesc().requiresMaterializedExecution shouldBe true
    new LoopEndOpDesc().requiresMaterializedExecution shouldBe true
    new SleepOpDesc().requiresMaterializedExecution shouldBe false
  }

  "WorkflowExecutionService.validateExecutionMode" should "reject a loop workflow submitted with a non-MATERIALIZED mode" in {
    val ops: Seq[LogicalOp] = Seq(new SleepOpDesc(), new LoopStartOpDesc())
    val ex = intercept[IllegalArgumentException] {
      WorkflowExecutionService.validateExecutionMode(ops, pipelined)
    }
    ex.getMessage should include("MATERIALIZED")
  }

  it should "also reject when only a Loop End operator is present (no Loop Start)" in {
    // The old isInstanceOf[LoopStartOpDesc] check missed this case: a plan
    // with a LoopEnd but no LoopStart would have skipped the guard.
    intercept[IllegalArgumentException] {
      WorkflowExecutionService.validateExecutionMode(Seq(new LoopEndOpDesc()), pipelined)
    }
  }

  it should "accept a loop workflow already set to MATERIALIZED" in {
    noException should be thrownBy {
      WorkflowExecutionService.validateExecutionMode(Seq(new LoopStartOpDesc()), materialized)
    }
  }

  it should "accept a non-loop workflow in PIPELINED mode" in {
    noException should be thrownBy {
      WorkflowExecutionService.validateExecutionMode(Seq(new SleepOpDesc()), pipelined)
    }
  }

  it should "accept an empty plan in PIPELINED mode" in {
    noException should be thrownBy {
      WorkflowExecutionService.validateExecutionMode(Seq.empty, pipelined)
    }
  }
}
