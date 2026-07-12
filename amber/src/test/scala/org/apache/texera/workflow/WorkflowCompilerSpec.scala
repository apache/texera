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

package org.apache.texera.workflow

import org.apache.texera.amber.core.workflow.{PortIdentity, WorkflowContext}
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.apache.texera.web.model.websocket.request.LogicalPlanPojo
import org.scalatest.flatspec.AnyFlatSpec

/**
  * Direct unit coverage for [[WorkflowCompiler]]. Today the compiler is only
  * exercised transitively by e2e/scheduler specs through
  * [[org.apache.texera.amber.engine.e2e.TestUtils.buildWorkflow]]. The cases
  * below pin its contract — physical-plan shape, storage-port collection, and
  * strict-mode error behavior — so future refactors (notably the planned
  * merge with workflow-compiling-service's compiler) have a direct anchor.
  *
  * Not yet covered: the Python codegen `#EXCEPTION DURING CODE GENERATION:`
  * regex branch. Triggering it requires a `PythonOperatorDescriptor` subclass
  * whose `generatePythonCode()` throws; left for a follow-up so this initial
  * spec stays focused on plumbing the compiler boundary itself.
  */
class WorkflowCompilerSpec extends AnyFlatSpec {

  private def pojo(
      operators: List[org.apache.texera.amber.operator.LogicalOp],
      links: List[LogicalLink],
      opsToViewResult: List[String] = List.empty
  ): LogicalPlanPojo =
    LogicalPlanPojo(operators, links, opsToViewResult, List.empty)

  // -------------------- physical-plan shape --------------------

  "WorkflowCompiler" should "produce a physical plan that contains at least one physical op per logical op" in {
    val csv = TestOperators.smallCsvScanOpDesc()
    val keyword = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val ctx = new WorkflowContext()

    val workflow = new WorkflowCompiler(ctx).compile(
      pojo(
        List(csv, keyword),
        List(
          LogicalLink(
            csv.operatorIdentifier,
            PortIdentity(),
            keyword.operatorIdentifier,
            PortIdentity()
          )
        )
      )
    )

    assert(workflow.logicalPlan.operators.size == 2)
    assert(workflow.physicalPlan.getPhysicalOpsOfLogicalOp(csv.operatorIdentifier).nonEmpty)
    assert(workflow.physicalPlan.getPhysicalOpsOfLogicalOp(keyword.operatorIdentifier).nonEmpty)
  }

  it should "translate a logical link into a physical link between the two logical ops' physical ops" in {
    val csv = TestOperators.smallCsvScanOpDesc()
    val keyword = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val ctx = new WorkflowContext()

    val workflow = new WorkflowCompiler(ctx).compile(
      pojo(
        List(csv, keyword),
        List(
          LogicalLink(
            csv.operatorIdentifier,
            PortIdentity(),
            keyword.operatorIdentifier,
            PortIdentity()
          )
        )
      )
    )

    val csvPhysIds =
      workflow.physicalPlan.getPhysicalOpsOfLogicalOp(csv.operatorIdentifier).map(_.id).toSet
    val keywordPhysIds =
      workflow.physicalPlan.getPhysicalOpsOfLogicalOp(keyword.operatorIdentifier).map(_.id).toSet

    val bridging = workflow.physicalPlan.links.filter(l =>
      csvPhysIds.contains(l.fromOpId) && keywordPhysIds.contains(l.toOpId)
    )
    assert(bridging.nonEmpty, "expected at least one physical link from csv to keyword")
  }

  // -------------------- storage-port collection --------------------

  // The compiler walks `logicalPlan.getTerminalOperatorIds` (logical ops with
  // out-degree 0) plus `opsToViewResult`, and for every physical op of those
  // logical ops collects every non-internal output port into
  // `outputPortsNeedingStorage`, which it then writes back onto the
  // workflow context. These tests pin that the *mutation* lands on the
  // context (not just a side value), and that both the terminal-default and
  // the opsToViewResult-additive paths populate it.

  "WorkflowCompiler" should "mark the terminal op's output port as needing storage on the context" in {
    val csv = TestOperators.smallCsvScanOpDesc()
    val keyword = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val ctx = new WorkflowContext()

    new WorkflowCompiler(ctx).compile(
      pojo(
        List(csv, keyword),
        List(
          LogicalLink(
            csv.operatorIdentifier,
            PortIdentity(),
            keyword.operatorIdentifier,
            PortIdentity()
          )
        )
      )
    )

    val storage = ctx.workflowSettings.outputPortsNeedingStorage
    assert(
      storage.exists(_.opId.logicalOpId == keyword.operatorIdentifier),
      s"expected keyword to be marked for storage, got ${storage.map(_.opId.logicalOpId)}"
    )
    assert(
      !storage.exists(_.opId.logicalOpId == csv.operatorIdentifier),
      "csv is not terminal and was not requested via opsToViewResult; it should not be in storage"
    )
  }

  it should "also mark a non-terminal op for storage when it is named in opsToViewResult" in {
    val csv = TestOperators.smallCsvScanOpDesc()
    val keyword = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val ctx = new WorkflowContext()

    new WorkflowCompiler(ctx).compile(
      pojo(
        List(csv, keyword),
        List(
          LogicalLink(
            csv.operatorIdentifier,
            PortIdentity(),
            keyword.operatorIdentifier,
            PortIdentity()
          )
        ),
        opsToViewResult = List(csv.operatorIdentifier.id)
      )
    )

    val storage = ctx.workflowSettings.outputPortsNeedingStorage
    val logicalOpsInStorage = storage.map(_.opId.logicalOpId)
    assert(
      logicalOpsInStorage.contains(csv.operatorIdentifier),
      s"opsToViewResult should add csv to storage, got $logicalOpsInStorage"
    )
    assert(
      logicalOpsInStorage.contains(keyword.operatorIdentifier),
      s"terminal keyword should remain in storage, got $logicalOpsInStorage"
    )
  }

  it should "treat a single source op as terminal and mark its output port for storage" in {
    val csv = TestOperators.smallCsvScanOpDesc()
    val ctx = new WorkflowContext()

    new WorkflowCompiler(ctx).compile(pojo(List(csv), List.empty))

    val storage = ctx.workflowSettings.outputPortsNeedingStorage
    assert(
      storage.exists(_.opId.logicalOpId == csv.operatorIdentifier),
      "single op has out-degree 0, so its output port should land in storage"
    )
    assert(
      storage.forall(!_.portId.internal),
      "compiler must filter out internal ports; storage should expose only user-visible outputs"
    )
  }

  // -------------------- strict-mode error semantics --------------------

  // Re-anchor the subject after the sub-section above.
  "WorkflowCompiler in strict mode (no errorList)" should
    "throw when a scan source has no fileName set" in {
    // CSVScanSourceOpDesc defaults fileName to None; `resolveScanSourceOpFileName(None)`
    // hits the "No file selected" RuntimeException thrown from `scanOp.fileName.getOrElse`
    // and surfaces that exception out of `compile` because the compiler passes
    // `None` for the errorList (i.e. fail-fast on the execution path).
    val orphanCsv = new CSVScanSourceOpDesc()
    val ctx = new WorkflowContext()

    val ex = intercept[RuntimeException] {
      new WorkflowCompiler(ctx).compile(pojo(List(orphanCsv), List.empty))
    }
    assert(ex.getMessage.contains("No file selected"))
  }
}
