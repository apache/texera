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

package org.apache.texera.amber.compiler

import org.apache.texera.amber.compiler.model.{LogicalLink, LogicalPlanPojo}
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType}
import org.apache.texera.amber.core.virtualidentity.WorkflowIdentity
import org.apache.texera.amber.core.workflow.{PortIdentity, WorkflowContext}
import org.apache.texera.amber.core.workflowruntimestate.FatalErrorType.COMPILATION_ERROR
import org.apache.texera.amber.operator.filter.{
  ComparisonType,
  FilterPredicate,
  SpecializedFilterOpDesc
}
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.operator.projection.{AttributeUnit, ProjectionOpDesc}
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.scalatest.flatspec.AnyFlatSpec

/**
  * Direct unit coverage for the editing-time [[WorkflowCompiler]].
  *
  * Owns *compiler-behavior* tests — schema propagation through multi-op
  * chains, lenient-mode error accumulation, terminal-storage selection.
  * `WorkflowCompilationResourceSpec` owns *resource-layer* tests — HTTP
  * status, response type discriminator, JSON envelope. Drawing the line
  * here keeps each spec focused on a single failure axis.
  *
  * Bypassing the resource layer also sidesteps a separate NPE in response
  * serialization (apache/texera#5021); these compiler-level tests stay
  * green once that bug is fixed.
  */
class WorkflowCompilerSpec extends AnyFlatSpec {

  private def newContext(): WorkflowContext =
    new WorkflowContext(workflowId = WorkflowIdentity(0))

  private def csvOp(fileName: String): CSVScanSourceOpDesc = {
    val op = new CSVScanSourceOpDesc()
    op.fileName = Some(fileName)
    op.customDelimiter = Some(",")
    op.hasHeader = true
    op
  }

  private def csvOpNoFile(): CSVScanSourceOpDesc = {
    val op = new CSVScanSourceOpDesc()
    op.customDelimiter = Some(",")
    op.hasHeader = true
    op
  }

  private def projectOp(columns: List[String]): ProjectionOpDesc = {
    val op = new ProjectionOpDesc()
    op.attributes = columns.map(name => new AttributeUnit(name, ""))
    op.isDrop = false
    op
  }

  private def filterOp(predicates: FilterPredicate*): SpecializedFilterOpDesc = {
    val op = new SpecializedFilterOpDesc
    op.predicates = predicates.toList
    op
  }

  private def limitOp(limit: Int): LimitOpDesc = {
    val op = new LimitOpDesc
    op.limit = limit
    op
  }

  private val realCsvPath =
    "workflow-compiling-service/src/test/resources/country_sales_small.csv"

  // -------------------- happy path --------------------

  "WorkflowCompiler" should "produce a populated physicalPlan and no errors for a well-formed plan" in {
    val csv = csvOp(realCsvPath)
    val proj = projectOp(List("Region", "Total Profit"))
    val result = new WorkflowCompiler(newContext()).compile(
      LogicalPlanPojo(
        operators = List(csv, proj),
        links = List(
          LogicalLink(
            csv.operatorIdentifier,
            PortIdentity(0),
            proj.operatorIdentifier,
            PortIdentity(0)
          )
        ),
        opsToViewResult = List.empty,
        opsToReuseResult = List.empty
      )
    )

    assert(result.physicalPlan.isDefined, "happy path should yield a physical plan")
    assert(result.operatorIdToError.isEmpty, s"unexpected errors: ${result.operatorIdToError}")
    // Schema for both operators' output ports should be populated and non-null —
    // this is the property whose violation triggers the resource-level NPE.
    val projSchemas = result.operatorIdToOutputSchemas(proj.operatorIdentifier)
    assert(projSchemas.values.forall(s => s.isDefined && s.get != null))
  }

  it should "propagate schemas through a csv -> projection -> limit -> filter -> filter -> limit chain" in {
    // Real-world editing-shape: source then filter/limit/project ops. Asserts
    // the compiler threads schema through every link so the frontend sees the
    // projected columns at every downstream port. Previously this lived in
    // WorkflowCompilationResourceSpec as an HTTP test, but the property being
    // pinned is compiler-level (schema propagation) — the REST envelope adds
    // no signal.
    val csv = csvOp(realCsvPath)
    val proj = projectOp(List("Region", "Total Profit"))
    val limit1 = limitOp(10)
    val filter1 =
      filterOp(new FilterPredicate("Total Profit", ComparisonType.GREATER_THAN, "10000"))
    val filter2 = filterOp(new FilterPredicate("Region", ComparisonType.NOT_EQUAL_TO, "JPN"))
    val limit2 = limitOp(5)

    val result = new WorkflowCompiler(newContext()).compile(
      LogicalPlanPojo(
        operators = List(csv, proj, limit1, filter1, filter2, limit2),
        links = List(
          LogicalLink(
            csv.operatorIdentifier,
            PortIdentity(0),
            proj.operatorIdentifier,
            PortIdentity(0)
          ),
          LogicalLink(
            proj.operatorIdentifier,
            PortIdentity(0),
            limit1.operatorIdentifier,
            PortIdentity(0)
          ),
          LogicalLink(
            limit1.operatorIdentifier,
            PortIdentity(0),
            filter1.operatorIdentifier,
            PortIdentity(0)
          ),
          LogicalLink(
            filter1.operatorIdentifier,
            PortIdentity(0),
            filter2.operatorIdentifier,
            PortIdentity(0)
          ),
          LogicalLink(
            filter2.operatorIdentifier,
            PortIdentity(0),
            limit2.operatorIdentifier,
            PortIdentity(0)
          )
        ),
        opsToViewResult = List.empty,
        opsToReuseResult = List.empty
      )
    )

    assert(result.physicalPlan.isDefined)
    assert(result.operatorIdToError.isEmpty, s"unexpected errors: ${result.operatorIdToError}")
    // Projection narrowed [Region, Country, ..., Total Profit] down to two
    // columns; every downstream op should see exactly those two attributes.
    val filter2Schemas = result.operatorIdToOutputSchemas(filter2.operatorIdentifier)
    val outputAttrs = filter2Schemas(PortIdentity(0)).get.attributes
    assert(
      outputAttrs == List(
        new Attribute("Region", AttributeType.STRING),
        new Attribute("Total Profit", AttributeType.DOUBLE)
      ),
      s"projected schema should reach filter2 unchanged, got $outputAttrs"
    )
  }

  // -------------------- lenient-mode error accumulation --------------------

  // The frontend relies on `compile` *never throwing*: a user mid-edit
  // routinely produces an inconsistent plan and the editing UI must render
  // structured per-operator errors. These tests pin the contract.

  "WorkflowCompiler" should "accumulate, not throw, when a scan source has no fileName" in {
    val orphan = csvOpNoFile()

    val result = new WorkflowCompiler(newContext()).compile(
      LogicalPlanPojo(
        operators = List(orphan),
        links = List.empty,
        opsToViewResult = List.empty,
        opsToReuseResult = List.empty
      )
    )

    assert(result.physicalPlan.isEmpty, "any error must clear the physical plan")
    val err = result.operatorIdToError(orphan.operatorIdentifier)
    assert(err.`type` == COMPILATION_ERROR)
    assert(err.operatorId == orphan.operatorIdentifier.id)
    assert(err.message.contains("No file selected"), s"unexpected message: ${err.message}")
    assert(err.details.nonEmpty, "stack-trace details should be populated for UI display")
  }

  it should "accumulate when a scan source's fileName points to a non-existent path" in {
    val broken = csvOp("/does/not/exist/missing.csv")

    val result = new WorkflowCompiler(newContext()).compile(
      LogicalPlanPojo(
        operators = List(broken),
        links = List.empty,
        opsToViewResult = List.empty,
        opsToReuseResult = List.empty
      )
    )

    assert(result.physicalPlan.isEmpty)
    assert(result.operatorIdToError.contains(broken.operatorIdentifier))
    // FileResolver.resolve falls through both resolvers and rethrows
    // org.apache.commons.vfs2.FileNotFoundException(fileName); its message bundle
    // renders as `Could not read from "<path>" because it is not a file.`, so the
    // only stable substring across that wording and any java.io.FileNotFoundException
    // fallback is the bad path itself.
    assert(
      result.operatorIdToError(broken.operatorIdentifier).message.contains("missing.csv"),
      s"unexpected message: ${result.operatorIdToError(broken.operatorIdentifier).message}"
    )
  }

  it should "accumulate a per-operator error when projection references a non-existent attribute" in {
    val csv = csvOp(realCsvPath)
    val badProjection = projectOp(List("DoesNotExist"))

    val result = new WorkflowCompiler(newContext()).compile(
      LogicalPlanPojo(
        operators = List(csv, badProjection),
        links = List(
          LogicalLink(
            csv.operatorIdentifier,
            PortIdentity(0),
            badProjection.operatorIdentifier,
            PortIdentity(0)
          )
        ),
        opsToViewResult = List.empty,
        opsToReuseResult = List.empty
      )
    )

    assert(result.physicalPlan.isEmpty)
    assert(
      result.operatorIdToError.contains(badProjection.operatorIdentifier),
      s"projection should be in errors, got ${result.operatorIdToError.keySet}"
    )
    // The upstream csv ran fine, so its output schema should still be present
    // — partial progress is the whole point of lenient mode.
    assert(
      result.operatorIdToOutputSchemas.contains(csv.operatorIdentifier),
      "upstream csv's schemas should be retained even when downstream fails"
    )
  }

  it should "not throw when given an empty plan" in {
    val result = new WorkflowCompiler(newContext()).compile(
      LogicalPlanPojo(
        operators = List.empty,
        links = List.empty,
        opsToViewResult = List.empty,
        opsToReuseResult = List.empty
      )
    )
    assert(result.operatorIdToError.isEmpty)
    assert(result.operatorIdToOutputSchemas.isEmpty)
    assert(result.physicalPlan.isDefined, "an empty plan compiles to an empty physical plan")
    assert(result.physicalPlan.get.operators.isEmpty)
    assert(result.physicalPlan.get.links.isEmpty)
  }

  // -------------------- multi-error accumulation --------------------

  // Re-anchor the subject after the sub-section.
  "WorkflowCompiler" should "accumulate errors for multiple unrelated failing ops in one compile" in {
    val orphan1 = csvOpNoFile()
    val orphan2 = csvOpNoFile()

    val result = new WorkflowCompiler(newContext()).compile(
      LogicalPlanPojo(
        operators = List(orphan1, orphan2),
        links = List.empty,
        opsToViewResult = List.empty,
        opsToReuseResult = List.empty
      )
    )

    assert(result.physicalPlan.isEmpty)
    // Both ops must appear in the error map — the frontend renders per-op
    // diagnostics in parallel, so swallowing all-but-one would silently break
    // multi-error workflows.
    assert(
      result.operatorIdToError.contains(orphan1.operatorIdentifier) &&
        result.operatorIdToError.contains(orphan2.operatorIdentifier),
      s"expected both csvs in errors, got ${result.operatorIdToError.keySet}"
    )
  }
}
