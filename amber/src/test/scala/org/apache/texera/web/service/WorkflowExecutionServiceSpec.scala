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

import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{ExecutionMode, PhysicalOp, WorkflowSettings}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorInfo
import org.apache.texera.amber.operator.sleep.SleepOpDesc
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WorkflowExecutionServiceSpec extends AnyFlatSpec with Matchers {

  private val pipelined = WorkflowSettings(executionMode = ExecutionMode.PIPELINED)
  private val materialized = WorkflowSettings(executionMode = ExecutionMode.MATERIALIZED)

  // A minimal operator that requires MATERIALIZED execution. validateExecutionMode
  // only reads `requiresMaterializedExecution`, so the other members are never
  // exercised here and are left unimplemented on purpose.
  private class MaterializedOnlyOp extends LogicalOp {
    override def requiresMaterializedExecution: Boolean = true
    override def operatorInfo: OperatorInfo = throw new UnsupportedOperationException
    override def getPhysicalOp(
        workflowId: WorkflowIdentity,
        executionId: ExecutionIdentity
    ): PhysicalOp = throw new UnsupportedOperationException
  }

  "LogicalOp.requiresMaterializedExecution" should "default to false" in {
    new SleepOpDesc().requiresMaterializedExecution shouldBe false
  }

  "WorkflowExecutionService.validateExecutionMode" should
    "reject a non-MATERIALIZED submission that contains an operator requiring materialization" in {
    val ex = intercept[IllegalArgumentException] {
      WorkflowExecutionService.validateExecutionMode(
        Seq(new SleepOpDesc(), new MaterializedOnlyOp()),
        pipelined
      )
    }
    ex.getMessage should include("MATERIALIZED")
  }

  it should "accept that submission when the mode is already MATERIALIZED" in {
    noException should be thrownBy {
      WorkflowExecutionService.validateExecutionMode(Seq(new MaterializedOnlyOp()), materialized)
    }
  }

  it should "accept a non-MATERIALIZED submission when no operator requires materialization" in {
    noException should be thrownBy {
      WorkflowExecutionService.validateExecutionMode(Seq(new SleepOpDesc()), pipelined)
    }
  }
}
