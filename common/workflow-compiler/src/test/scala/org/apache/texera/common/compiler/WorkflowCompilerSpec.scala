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

package org.apache.texera.common.compiler

import org.apache.texera.common.compiler.model.{LogicalLink, LogicalPlanPojo}
import org.apache.texera.amber.core.executor.{ExecFactory, LateBoundExecutor, OpExecWithClassName}
import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.WorkflowIdentity
import org.apache.texera.amber.core.workflow.{OutputPort, PortIdentity, WorkflowContext}
import org.apache.texera.amber.core.workflowruntimestate.FatalErrorType.COMPILATION_ERROR
import org.apache.texera.amber.operator.filter.{
  ComparisonType,
  FilterPredicate,
  SpecializedFilterOpDesc,
  SpecializedFilterOpExec
}
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.operator.loop.{LoopEndOpDesc, LoopStartOpDesc}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.projection.{AttributeUnit, ProjectionOpDesc}
import org.apache.texera.amber.operator.sort.{SortCriteriaUnit, SortOpDesc, SortPreference}
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.apache.texera.amber.operator.source.scan.text.TextInputSourceOpDesc
import org.apache.texera.amber.operator.{LogicalOp, PythonOperatorDescriptor, TestOperators}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

/**
  * Direct unit coverage for the unified [[WorkflowCompiler]].
  *
  * Owns *compiler-behavior* tests across both paths: lenient (editing-time —
  * accumulate per-operator errors, schema propagation) and strict
  * (pre-execution — fail-fast), plus physical-plan shape and the set of
  * output ports needing storage. `WorkflowCompilationResourceSpec` owns
  * *resource-layer* tests — HTTP status, response type discriminator, JSON
  * envelope. Drawing the line here keeps each spec focused.
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

  // Sort is a real, shipped `PythonOperatorDescriptor` whose `generatePythonCode`
  // rejects an unconfigured operator, so `sortOp()` (no sort keys) and
  // `sortOp("" -> ASC)` (a key with no attribute) are genuine ways for a user to
  // land in the `#EXCEPTION DURING CODE GENERATION:` state.
  private def sortOp(criteria: (String, SortPreference)*): SortOpDesc = {
    val op = new SortOpDesc
    op.attributes = criteria.map {
      case (attributeName, preference) =>
        val unit = new SortCriteriaUnit
        unit.attributeName = attributeName
        unit.sortPreference = preference
        unit
    }.toList
    op
  }

  /**
    * A test-only Python operator whose code generation fails with a
    * whitespace-padded message. No shipped operator raises a padded message, so
    * this is the only way to pin the compiler's regex-group + `trim` extraction
    * of the marker's payload.
    */
  private class PaddedFailurePyOp extends PythonOperatorDescriptor {
    override def asSource(): Boolean = true
    override def generatePythonCode(): String =
      throw new RuntimeException("   padded codegen failure   ")
    override def getOutputSchemas(
        inputSchemas: Map[PortIdentity, Schema]
    ): Map[PortIdentity, Schema] = Map(PortIdentity() -> Schema())
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "padded",
        "raises a padded message during code generation",
        OperatorGroupConstants.PYTHON_GROUP,
        List.empty,
        List(OutputPort())
      )
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

  // -------------------- Python code-generation error path --------------------

  // A `PythonOperatorDescriptor` whose `generatePythonCode` throws does not
  // propagate the failure: it embeds `#EXCEPTION DURING CODE GENERATION: <msg>`
  // in the generated code so schema propagation can still run. The compiler is
  // the consumer that turns that marker back into a per-operator error, so these
  // tests pin the marker -> error translation from the compiler's side.

  // Re-anchor the subject after the sub-section.
  "WorkflowCompiler" should "accumulate a per-operator error when a Python operator's code generation fails" in {
    val csv = csvOp(realCsvPath)
    val unconfiguredSort = sortOp() // no sort keys -> generatePythonCode throws

    val result = new WorkflowCompiler(newContext()).compile(
      LogicalPlanPojo(
        operators = List(csv, unconfiguredSort),
        links = List(
          LogicalLink(
            csv.operatorIdentifier,
            PortIdentity(0),
            unconfiguredSort.operatorIdentifier,
            PortIdentity(0)
          )
        ),
        opsToViewResult = List.empty,
        opsToReuseResult = List.empty
      )
    )

    assert(result.physicalPlan.isEmpty, "any error must clear the physical plan")
    val err = result.operatorIdToError(unconfiguredSort.operatorIdentifier)
    assert(err.`type` == COMPILATION_ERROR)
    assert(err.operatorId == unconfiguredSort.operatorIdentifier.id)
    assert(
      err.message.contains(
        "Operator is not configured properly: " +
          "requirement failed: Sort operator requires at least one sort key."
      ),
      s"unexpected message: ${err.message}"
    )
    // The failure belongs to the Python operator alone; the upstream csv compiled.
    assert(
      !result.operatorIdToError.contains(csv.operatorIdentifier),
      s"only the Python op should have errored, got ${result.operatorIdToError.keySet}"
    )
    // Lenient mode records the error and keeps going *within* the same operator:
    // the terminal sort's output port is still collected for storage, which only
    // happens if the marker check did not abort the operator's expansion.
    assert(
      result.outputPortsNeedingStorage.exists(
        _.opId.logicalOpId == unconfiguredSort.operatorIdentifier
      ),
      s"expected the sort's port to still be collected, got ${result.outputPortsNeedingStorage}"
    )
  }

  it should "attribute each Python code-generation failure to its own logical operator" in {
    val csv = csvOp(realCsvPath)
    val noKeys = sortOp()
    val blankKey = sortOp("" -> SortPreference.ASC)

    val result = new WorkflowCompiler(newContext()).compile(
      LogicalPlanPojo(
        operators = List(csv, noKeys, blankKey),
        links = List(
          LogicalLink(
            csv.operatorIdentifier,
            PortIdentity(0),
            noKeys.operatorIdentifier,
            PortIdentity(0)
          ),
          LogicalLink(
            csv.operatorIdentifier,
            PortIdentity(0),
            blankKey.operatorIdentifier,
            PortIdentity(0)
          )
        ),
        opsToViewResult = List.empty,
        opsToReuseResult = List.empty
      )
    )

    assert(
      result.operatorIdToError.keySet ==
        Set(noKeys.operatorIdentifier, blankKey.operatorIdentifier),
      s"expected exactly the two Python ops in errors, got ${result.operatorIdToError.keySet}"
    )
    // Each operator carries the message its *own* code generation raised — a
    // mixed-up mapping would put the wrong diagnostic on the wrong UI node.
    assert(
      result
        .operatorIdToError(noKeys.operatorIdentifier)
        .message
        .contains("Operator is not configured properly: requirement failed: Sort operator requires")
    )
    assert(
      result
        .operatorIdToError(blankKey.operatorIdentifier)
        .message
        .contains(
          "Operator is not configured properly: " +
            "requirement failed: Each sort key must have an attribute selected."
        )
    )
    // The rest of the plan still compiled: the csv's schemas survive.
    assert(
      result.operatorIdToOutputSchemas.contains(csv.operatorIdentifier),
      "upstream csv's schemas should be retained even when downstream Python ops fail"
    )
  }

  it should "trim the marker's message and report it as a plain RuntimeException" in {
    val padded = new PaddedFailurePyOp

    val result = new WorkflowCompiler(newContext()).compile(
      LogicalPlanPojo(
        operators = List(padded),
        links = List.empty,
        opsToViewResult = List.empty,
        opsToReuseResult = List.empty
      )
    )

    // `message` is the RuntimeException's toString, so the extracted payload is
    // the tail of it: exactly the raised message with its padding removed, and
    // with the marker itself stripped off by the regex.
    val message = result.operatorIdToError(padded.operatorIdentifier).message
    assert(
      message.endsWith("Operator is not configured properly: padded codegen failure"),
      s"unexpected message: [$message]"
    )
    // The head of it is the exception's class name: the compiler wraps the
    // extracted payload in a plain `RuntimeException` and the error map stores
    // `err.toString`, so the type is part of what the UI renders. Pinning it
    // here keeps the wrapper type from silently drifting.
    assert(
      message.startsWith("java.lang.RuntimeException: "),
      s"expected a plain RuntimeException to be reported, got: [$message]"
    )
    assert(
      !message.contains("#EXCEPTION DURING CODE GENERATION"),
      s"the marker itself must not leak into the user-facing message: [$message]"
    )
  }

  it should "report no code-generation error for a well-formed Python operator" in {
    val csv = csvOp(realCsvPath)
    val configuredSort = sortOp("Region" -> SortPreference.ASC)

    val result = new WorkflowCompiler(newContext()).compile(
      LogicalPlanPojo(
        operators = List(csv, configuredSort),
        links = List(
          LogicalLink(
            csv.operatorIdentifier,
            PortIdentity(0),
            configuredSort.operatorIdentifier,
            PortIdentity(0)
          )
        ),
        opsToViewResult = List.empty,
        opsToReuseResult = List.empty
      )
    )

    assert(result.operatorIdToError.isEmpty, s"unexpected errors: ${result.operatorIdToError}")
    assert(result.physicalPlan.isDefined)
    // Same operator, same Python code path — the only difference is that code
    // generation succeeded, so no marker is present to be turned into an error.
    val sortPhysicalOps =
      result.physicalPlan.get.getPhysicalOpsOfLogicalOp(configuredSort.operatorIdentifier)
    assert(sortPhysicalOps.nonEmpty)
    assert(sortPhysicalOps.forall(_.isPythonBased), "Sort must still be a Python-based operator")
    assert(sortPhysicalOps.forall(!_.getCode.contains("#EXCEPTION DURING CODE GENERATION")))
  }

  it should "not subject non-Python operators to the code-generation check" in {
    // Non-Python operators carry no code at all — `getCode` throws
    // IllegalAccessError on them — so the check must stay behind the
    // `isPythonBased` guard or every Scala operator would fail to compile.
    val csv = csvOp(realCsvPath)
    val filter = filterOp(new FilterPredicate("Region", ComparisonType.EQUAL_TO, "Asia"))

    val result = new WorkflowCompiler(newContext()).compile(
      LogicalPlanPojo(
        operators = List(csv, filter),
        links = List(
          LogicalLink(
            csv.operatorIdentifier,
            PortIdentity(0),
            filter.operatorIdentifier,
            PortIdentity(0)
          )
        ),
        opsToViewResult = List.empty,
        opsToReuseResult = List.empty
      )
    )

    assert(result.operatorIdToError.isEmpty, s"unexpected errors: ${result.operatorIdToError}")
    val physicalOps = result.physicalPlan.get.operators
    assert(
      physicalOps.forall(!_.isPythonBased),
      "this plan must contain no Python-based op, otherwise the test proves nothing"
    )
  }

  // -------------------- physical-plan shape --------------------

  private def pojo(
      operators: List[org.apache.texera.amber.operator.LogicalOp],
      links: List[LogicalLink],
      opsToViewResult: List[String] = List.empty
  ): LogicalPlanPojo =
    LogicalPlanPojo(operators, links, opsToViewResult, List.empty)

  // Re-anchor the subject after the sub-section.
  "WorkflowCompiler" should "produce a physical plan that contains at least one physical op per logical op" in {
    val csv = TestOperators.smallCsvScanOpDesc()
    val keyword = TestOperators.keywordSearchOpDesc("Region", "Asia")

    val result = new WorkflowCompiler(newContext()).compile(
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

    assert(result.logicalPlan.operators.size == 2)
    val physicalPlan = result.physicalPlan.get
    assert(physicalPlan.getPhysicalOpsOfLogicalOp(csv.operatorIdentifier).nonEmpty)
    assert(physicalPlan.getPhysicalOpsOfLogicalOp(keyword.operatorIdentifier).nonEmpty)
  }

  it should "translate a logical link into a physical link between the two logical ops' physical ops" in {
    val csv = TestOperators.smallCsvScanOpDesc()
    val keyword = TestOperators.keywordSearchOpDesc("Region", "Asia")

    val result = new WorkflowCompiler(newContext()).compile(
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

    val physicalPlan = result.physicalPlan.get
    val csvPhysIds =
      physicalPlan.getPhysicalOpsOfLogicalOp(csv.operatorIdentifier).map(_.id).toSet
    val keywordPhysIds =
      physicalPlan.getPhysicalOpsOfLogicalOp(keyword.operatorIdentifier).map(_.id).toSet

    val bridging = physicalPlan.links.filter(l =>
      csvPhysIds.contains(l.fromOpId) && keywordPhysIds.contains(l.toOpId)
    )
    assert(bridging.nonEmpty, "expected at least one physical link from csv to keyword")
  }

  // -------------------- storage-port collection --------------------

  // The compiler walks `logicalPlan.getTerminalOperatorIds` (logical ops with
  // out-degree 0) plus `opsToViewResult`, and for every physical op of those
  // logical ops collects every non-internal output port into the result's
  // `outputPortsNeedingStorage`. These tests pin both the terminal-default and
  // the opsToViewResult-additive paths, and that internal ports are filtered.

  "WorkflowCompiler" should "mark the terminal op's output port as needing storage" in {
    val csv = TestOperators.smallCsvScanOpDesc()
    val keyword = TestOperators.keywordSearchOpDesc("Region", "Asia")

    val result = new WorkflowCompiler(newContext()).compile(
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

    val storage = result.outputPortsNeedingStorage
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

    val result = new WorkflowCompiler(newContext()).compile(
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

    val logicalOpsInStorage = result.outputPortsNeedingStorage.map(_.opId.logicalOpId)
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

    val result = new WorkflowCompiler(newContext()).compile(pojo(List(csv), List.empty))

    val storage = result.outputPortsNeedingStorage
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

  // Re-anchor the subject after the sub-section.
  "WorkflowCompiler in strict mode" should "throw when a scan source has no fileName set" in {
    // Strict passes no error buffer, so `resolveScanSourceOpFileName` rethrows
    // the first failure instead of accumulating it (the execution path's
    // fail-fast contract). The lenient counterpart above accumulates the same
    // failure without throwing.
    val orphanCsv = new CSVScanSourceOpDesc()

    val ex = intercept[RuntimeException] {
      new WorkflowCompiler(newContext())
        .compile(pojo(List(orphanCsv), List.empty), CompilationErrorHandling.Strict)
    }
    assert(ex.getMessage.contains("No file selected"))
  }

  it should "return a defined physicalPlan for a well-formed plan" in {
    // The execution path calls `physicalPlan.get` on the result, so a strict
    // success must always carry a plan.
    val csv = csvOp(realCsvPath)
    val proj = projectOp(List("Region", "Total Profit"))

    val result = new WorkflowCompiler(newContext()).compile(
      pojo(
        List(csv, proj),
        List(
          LogicalLink(
            csv.operatorIdentifier,
            PortIdentity(0),
            proj.operatorIdentifier,
            PortIdentity(0)
          )
        )
      ),
      CompilationErrorHandling.Strict
    )

    assert(result.physicalPlan.isDefined, "strict success must yield a physical plan")
    assert(result.operatorIdToError.isEmpty)
    assert(result.outputPortsNeedingStorage.nonEmpty, "terminal ports still collected in strict")
  }

  it should "throw on schema-propagation errors" in {
    // A projection on a missing column fails schema *propagation*, not plan
    // expansion: `propagateSchema` stores a Left on the output port instead of
    // throwing, so this error only becomes visible when output schemas are
    // collected. Strict must fail fast on it too — otherwise the plan would be
    // launched and only fail at runtime. The lenient counterpart above turns
    // the same failure into a per-operator error instead.
    val csv = csvOp(realCsvPath)
    val badProjection = projectOp(List("DoesNotExist"))

    val ex = intercept[Throwable] {
      new WorkflowCompiler(newContext()).compile(
        pojo(
          List(csv, badProjection),
          List(
            LogicalLink(
              csv.operatorIdentifier,
              PortIdentity(0),
              badProjection.operatorIdentifier,
              PortIdentity(0)
            )
          )
        ),
        CompilationErrorHandling.Strict
      )
    }
    assert(
      ex.getMessage != null && ex.getMessage.contains("DoesNotExist"),
      s"the thrown schema error should name the missing attribute, got: $ex"
    )
  }

  it should "throw immediately when a Python operator's code generation failed" in {
    // The execution path passes no error buffer, so the marker found in the
    // generated code must abort the compile instead of being collected. The
    // lenient counterpart above turns the same marker into a per-operator error.
    val csv = csvOp(realCsvPath)
    val unconfiguredSort = sortOp()

    val ex = intercept[RuntimeException] {
      new WorkflowCompiler(newContext()).compile(
        pojo(
          List(csv, unconfiguredSort),
          List(
            LogicalLink(
              csv.operatorIdentifier,
              PortIdentity(0),
              unconfiguredSort.operatorIdentifier,
              PortIdentity(0)
            )
          )
        ),
        CompilationErrorHandling.Strict
      )
    }
    assert(
      ex.getMessage == "Operator is not configured properly: " +
        "requirement failed: Sort operator requires at least one sort key.",
      s"unexpected message: ${ex.getMessage}"
    )
  }

  it should "not throw for a well-formed Python operator" in {
    val csv = csvOp(realCsvPath)
    val configuredSort = sortOp("Region" -> SortPreference.DESC)

    val result = new WorkflowCompiler(newContext()).compile(
      pojo(
        List(csv, configuredSort),
        List(
          LogicalLink(
            csv.operatorIdentifier,
            PortIdentity(0),
            configuredSort.operatorIdentifier,
            PortIdentity(0)
          )
        )
      ),
      CompilationErrorHandling.Strict
    )

    assert(result.physicalPlan.isDefined)
    assert(result.operatorIdToError.isEmpty)
  }

  // -------------------- loop-variable references --------------------

  /** An operator as the frontend sends it: its properties, then its type, "$..." whatever the type. */
  private def parsed(json: String): LogicalOp = objectMapper.readValue(json, classOf[LogicalOp])

  private def linked(upstream: LogicalOp, downstream: LogicalOp, toPort: Int = 0): LogicalLink =
    LogicalLink(
      upstream.operatorIdentifier,
      PortIdentity(0),
      downstream.operatorIdentifier,
      PortIdentity(toPort)
    )

  private def chain(operators: LogicalOp*): List[LogicalLink] =
    operators.sliding(2).map(pair => linked(pair.head, pair.last)).toList

  private def textInputOp(text: String): TextInputSourceOpDesc = {
    val op = new TextInputSourceOpDesc()
    op.textInput = text
    op
  }

  private def loopStartOp(): LoopStartOpDesc = {
    val op = new LoopStartOpDesc()
    op.initialization = "i = 0"
    op.output = "table.iloc[i]"
    op
  }

  private def loopEndOp(): LoopEndOpDesc = {
    val op = new LoopEndOpDesc()
    op.update = "i += 1"
    op.condition = "i < len(table)"
    op
  }

  /** The class name and descString the worker builds `op`'s executor from. */
  private def executorInit(result: WorkflowCompilationResult, op: LogicalOp): (String, String) =
    result.physicalPlan.get
      .getPhysicalOpsOfLogicalOp(op.operatorIdentifier)
      .head
      .opExecInitInfo match {
      case OpExecWithClassName(className, descString) => (className, descString)
      case other                                      => fail(s"unexpected opExecInitInfo: $other")
    }

  /** The `stateReferences` sidecar a descString carries to the worker. */
  private def sidecarOf(descString: String): Map[String, String] =
    objectMapper.convertValue(
      objectMapper.readTree(descString).get("stateReferences"),
      classOf[Map[String, String]]
    )

  private def row(attribute: String, value: String): Tuple = {
    val schema = Schema().add(new Attribute(attribute, AttributeType.STRING))
    Tuple.builder(schema).add(schema.getAttribute(attribute), value).build()
  }
  private def line(text: String): Tuple = row("line", text)

  "WorkflowCompiler" should "late-bind a frontend '$n' in Limit's Int property inside a loop block, end to end" in {
    // TextInput -> LoopStart -> Limit("$n") -> LoopEnd, the Limit parsed as the frontend sends it.
    val src = textInputOp("0\n1")
    val start = loopStartOp()
    val limit = parsed("""{"limit":"$n","operatorType":"Limit"}""")
    val end = loopEndOp()

    val result = new WorkflowCompiler(newContext()).compile(
      pojo(List(src, start, limit, end), chain(src, start, limit, end))
    )

    assert(result.operatorIdToError.isEmpty, s"unexpected errors: ${result.operatorIdToError}")
    val (className, descString) = executorInit(result, limit)
    assert(sidecarOf(descString) == Map("/limit" -> "n"))
    val exec = ExecFactory.newExecFromJavaClassName(className, descString)
    assert(exec.isInstanceOf[LateBoundExecutor])
    exec.open()
    exec.processState(State(Map("n" -> 2L)), 0)
    val passed = (1 to 5).flatMap(i => exec.processTuple(line(i.toString), 0))
    assert(passed.size == 2)
  }

  it should "keep a '$AAPL' on a Filter outside every loop block the literal it is on main" in {
    val csv = csvOp(realCsvPath)
    val filter = parsed(
      """{"predicates":[{"attribute":"Region","condition":"=","value":"$AAPL"}],"operatorType":"Filter"}"""
    )

    val result = new WorkflowCompiler(newContext()).compile(
      pojo(List(csv, filter), List(linked(csv, filter)))
    )

    assert(result.operatorIdToError.isEmpty, s"unexpected errors: ${result.operatorIdToError}")
    val (className, descString) = executorInit(result, filter)
    assert(sidecarOf(descString).isEmpty)
    val exec = ExecFactory.newExecFromJavaClassName(className, descString)
    assert(exec.isInstanceOf[SpecializedFilterOpExec])
    exec.open()
    assert(exec.processTuple(row("Region", "$AAPL"), 0).toList == List(row("Region", "$AAPL")))
    assert(exec.processTuple(row("Region", "Asia"), 0).isEmpty)
  }

  it should "report a typed '$n' outside every loop block, naming the property and the variable" in {
    val csv = csvOp(realCsvPath)
    val limit = parsed("""{"limit":"$n","operatorType":"Limit"}""")
    val plan = pojo(List(csv, limit), List(linked(csv, limit)))
    val message = "property /limit refers to loop variable n, but Limit is not inside a loop block"

    val result = new WorkflowCompiler(newContext()).compile(plan)

    assert(result.physicalPlan.isEmpty)
    assert(result.operatorIdToError.keySet == Set(limit.operatorIdentifier))
    assert(result.operatorIdToError(limit.operatorIdentifier).message.contains(message))
    val ex = intercept[IllegalArgumentException] {
      new WorkflowCompiler(newContext()).compile(plan, CompilationErrorHandling.Strict)
    }
    assert(ex.getMessage == message)
  }

  it should "find a '$i' literal on a descriptor built in Scala inside a loop block" in {
    // No parse step ran: the compiler finds the whole-string '$i' in the operator's own JSON.
    val src = textInputOp("0\n1")
    val start = loopStartOp()
    val filter = filterOp(new FilterPredicate("line", ComparisonType.EQUAL_TO, "$i"))
    val end = loopEndOp()

    val result = new WorkflowCompiler(newContext()).compile(
      pojo(List(src, start, filter, end), chain(src, start, filter, end))
    )

    assert(result.operatorIdToError.isEmpty, s"unexpected errors: ${result.operatorIdToError}")
    val (className, descString) = executorInit(result, filter)
    assert(sidecarOf(descString) == Map("/predicates/0/value" -> "i"))
    val exec = ExecFactory.newExecFromJavaClassName(className, descString)
    exec.processState(State(Map("i" -> 1L)), 0)
    assert(exec.processTuple(line("1"), 0).toList == List(line("1")))
    assert(exec.processTuple(line("$i"), 0).isEmpty)
  }

  it should "report a reference inside a loop block on an operator whose code is generated" in {
    // Sort generates its Python from its properties before the loop runs; outside every block
    // "$USD" is the column name it is on main.
    val csv = csvOp(realCsvPath)
    val start = loopStartOp()
    val sortJson =
      """{"attributes":[{"attribute":"$%s","sortPreference":"ASC"}],"operatorType":"Sort"}"""
    val sort = parsed(sortJson.format("col"))
    val end = loopEndOp()

    val result = new WorkflowCompiler(newContext()).compile(
      pojo(List(csv, start, sort, end), chain(csv, start, sort, end))
    )

    assert(result.physicalPlan.isEmpty)
    assert(result.operatorIdToError.keySet == Set(sort.operatorIdentifier))
    assert(
      result
        .operatorIdToError(sort.operatorIdentifier)
        .message
        .contains("cannot refer to loop variables yet (/attributes/0/attribute -> $col)")
    )

    val outside = parsed(sortJson.format("USD"))
    val outsideResult = new WorkflowCompiler(newContext()).compile(
      pojo(List(csv, outside), List(linked(csv, outside)))
    )
    assert(
      outsideResult.operatorIdToError.isEmpty,
      s"unexpected: ${outsideResult.operatorIdToError}"
    )
  }

  private def intervalJoinOp(): LogicalOp =
    parsed(
      """{"leftAttributeName":"line","rightAttributeName":"line","constant":"$i",
        |"includeLeftBound":true,"includeRightBound":true,"operatorType":"IntervalJoin"}""".stripMargin
    )

  it should "report a reference inside a loop block on an operator whose first input comes from outside the block" in {
    // Its right input waits for the left one, so every left tuple arrives before the loop state.
    val outside = textInputOp("0")
    val src = textInputOp("0\n1")
    val start = loopStartOp()
    val join = intervalJoinOp()
    val end = loopEndOp()
    val plan = pojo(
      List(outside, src, start, join, end),
      List(linked(outside, join), linked(src, start), linked(start, join, 1), linked(join, end))
    )

    val result = new WorkflowCompiler(newContext()).compile(plan)

    assert(result.operatorIdToError.keySet == Set(join.operatorIdentifier))
    assert(
      result
        .operatorIdToError(join.operatorIdentifier)
        .message
        .contains(
          "Interval Join refers to loop variables (/constant -> $i), but its input 'left table' " +
            "is fed from outside the loop block"
        )
    )
  }

  it should "late-bind an operator inside a loop block whose input from outside waits for one from inside" in {
    val outside = textInputOp("0")
    val src = textInputOp("0\n1")
    val start = loopStartOp()
    val join = intervalJoinOp()
    val end = loopEndOp()

    val result = new WorkflowCompiler(newContext()).compile(
      pojo(
        List(outside, src, start, join, end),
        List(linked(src, start), linked(start, join), linked(outside, join, 1), linked(join, end))
      )
    )

    assert(result.operatorIdToError.isEmpty, s"unexpected errors: ${result.operatorIdToError}")
    val (_, descString) = executorInit(result, join)
    assert(sidecarOf(descString) == Map("/constant" -> "i"))
  }
}
