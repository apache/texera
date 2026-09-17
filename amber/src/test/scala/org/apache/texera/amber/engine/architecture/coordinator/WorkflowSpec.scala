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

package org.apache.texera.amber.engine.architecture.coordinator

import org.apache.texera.amber.core.workflow.{PortIdentity, WorkflowContext}
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.apache.texera.common.compiler.model.{LogicalLink, LogicalPlanPojo}
import org.apache.texera.common.compiler.{CompilationErrorHandling, WorkflowCompiler}
import org.scalatest.flatspec.AnyFlatSpec

/**
  * Direct unit coverage for [[Workflow.fromCompilationResult]]. The adapter owns the
  * execution path's two obligations after a Strict compile: writing the result's
  * `outputPortsNeedingStorage` back onto the workflow context (the schedule generators
  * read it from there to materialize terminal results) and unwrapping the physical
  * plan. The context write used to live inside amber's compiler and was pinned by its
  * spec; these cases keep that behavior directly anchored after the move.
  */
class WorkflowSpec extends AnyFlatSpec {

  "Workflow.fromCompilationResult" should
    "write outputPortsNeedingStorage onto the context and unwrap the physical plan" in {
    val csv = TestOperators.smallCsvScanOpDesc()
    val keyword = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val context = new WorkflowContext()

    val result = new WorkflowCompiler(context).compile(
      LogicalPlanPojo(
        List(csv, keyword),
        List(
          LogicalLink(
            csv.operatorIdentifier,
            PortIdentity(),
            keyword.operatorIdentifier,
            PortIdentity()
          )
        ),
        List(),
        List()
      ),
      CompilationErrorHandling.Strict
    )
    val workflow = Workflow.fromCompilationResult(context, result)

    assert(context.workflowSettings.outputPortsNeedingStorage == result.outputPortsNeedingStorage)
    assert(context.workflowSettings.outputPortsNeedingStorage.nonEmpty)
    assert(workflow.physicalPlan eq result.physicalPlan.get)
    assert(workflow.logicalPlan eq result.logicalPlan)
    assert(workflow.context eq context)
  }

  it should "reject a result without a physical plan with a clear error" in {
    val context = new WorkflowContext()
    // Lenient default: the unset fileName becomes a per-operator error and physicalPlan = None.
    val failed = new WorkflowCompiler(context).compile(
      LogicalPlanPojo(List(new CSVScanSourceOpDesc()), List(), List(), List())
    )
    assert(failed.physicalPlan.isEmpty)

    val ex = intercept[IllegalStateException] {
      Workflow.fromCompilationResult(context, failed)
    }
    assert(ex.getMessage.contains("physicalPlan"))
  }
}
