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

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.texera.common.compiler.WorkflowCompilerSpec.{
  FlagWritingPyOp,
  IntWritingPyOp,
  RatioWritingPyOp
}
import org.apache.texera.common.compiler.model.{LogicalLink, LogicalPlanPojo}
import org.apache.texera.amber.core.executor.{
  ExecFactory,
  LateBoundExecutor,
  OpExecWithClassName,
  OpExecWithCode
}
import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.WorkflowIdentity
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity, WorkflowContext}
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
import org.apache.texera.amber.operator.udf.python.{
  LambdaAttributeUnit,
  PythonLambdaFunctionOpDesc,
  PythonUDFOpDescV2
}
import org.apache.texera.amber.operator.{LogicalOp, PythonOperatorDescriptor, TestOperators}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.decoderExpression
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

  /** The code the worker runs for `op`, an operator whose executor runs code. */
  private def codeOf(result: WorkflowCompilationResult, op: LogicalOp): String =
    result.physicalPlan.get
      .getPhysicalOpsOfLogicalOp(op.operatorIdentifier)
      .head
      .opExecInitInfo match {
      case OpExecWithCode(code, _) => code
      case other                   => fail(s"unexpected opExecInitInfo: $other")
    }

  private def lookup(name: String): String = s"self.loop_variable_text('$name')"

  /** The one compile error of `op`, lenient; the strict compile must throw exactly `message`. */
  private def assertOnlyError(plan: LogicalPlanPojo, op: LogicalOp, message: String): Unit = {
    val result = new WorkflowCompiler(newContext()).compile(plan)
    assert(result.physicalPlan.isEmpty)
    assert(result.operatorIdToError.keySet == Set(op.operatorIdentifier))
    val reported = result.operatorIdToError(op.operatorIdentifier).message
    assert(reported.contains(message), s"unexpected message: $reported")
    val ex = intercept[UnsupportedOperationException] {
      new WorkflowCompiler(newContext()).compile(plan, CompilationErrorHandling.Strict)
    }
    assert(ex.getMessage == message)
  }

  private def valueLookup(name: String, kind: String): String =
    s"self.loop_variable_value('$name', '$kind')"

  private val generatedFromValueReason = "its code is generated from the value before the loop runs"

  private val unreadValueReason = "its code does not read the value when the loop runs"

  private val valueInTextReason =
    "its value lands inside a text or a comment of the generated code"

  private val checkedValueReason = "it checks or adjusts the value before the loop runs"

  private val embeddedTextReason = "its code embeds the text before the loop runs"

  private val unreadTextReason = "its code does not read the text when the loop runs"

  private val outputColumnReason =
    "the text names an output column, which is set before the loop runs"

  private val udfReason = "its code and properties are fixed before the loop runs"

  it should "read a text reference inside a loop block from the loop state in an operator whose code is generated" in {
    // Sort generates its Python from its properties before the loop runs, but pyb renders the
    // "$col" key as an expression evaluated inside the operator, which now reads `col` instead.
    // Outside every block "$USD" is the column name it is on main.
    val csv = csvOp(realCsvPath)
    val start = loopStartOp()
    val sortJson =
      """{"attributes":[{"attribute":"$%s","sortPreference":"ASC"}],"operatorType":"Sort"}"""
    val sort = parsed(sortJson.format("col"))
    val end = loopEndOp()

    val result = new WorkflowCompiler(newContext()).compile(
      pojo(List(csv, start, sort, end), chain(csv, start, sort, end))
    )

    assert(result.operatorIdToError.isEmpty, s"unexpected errors: ${result.operatorIdToError}")
    assert(sort.stateReferences == Map("/attributes/0/attribute" -> "col"))
    val code = codeOf(result, sort)
    assert(code.contains(s"sort_columns = [${lookup("col")}]"), code)
    assert(!code.contains(decoderExpression("$col")))
    assert(!code.contains("$col"))

    val outside = parsed(sortJson.format("USD"))
    val outsideResult = new WorkflowCompiler(newContext()).compile(
      pojo(List(csv, outside), List(linked(csv, outside)))
    )
    assert(
      outsideResult.operatorIdToError.isEmpty,
      s"unexpected: ${outsideResult.operatorIdToError}"
    )
    assert(outside.stateReferences.isEmpty)
    val outsideCode = codeOf(outsideResult, outside)
    assert(outsideCode.contains(s"sort_columns = [${decoderExpression("$USD")}]"), outsideCode)
    assert(!outsideCode.contains("loop_variable_text"))
  }

  /** The SVR trainer as the frontend sends it, with one hyperparameter row. */
  private def svrTrainerOp(value: String, parametersSource: String): LogicalOp =
    parsed(
      s"""{"groundTruthAttribute":"line","Selected Features":["line"],
         |"paraList":[{"parameter":"C","value":"$value","attribute":"line",
         |"parametersSource":$parametersSource}],
         |"operatorType":"SVRTrainer"}""".stripMargin
    )

  /** TextInput -> LoopStart -> both inputs of `body` -> LoopEnd. */
  private def twoInputLoop(body: LogicalOp): LogicalPlanPojo = {
    val src = textInputOp("1\n2")
    val start = loopStartOp()
    val end = loopEndOp()
    pojo(
      List(src, start, body, end),
      List(linked(src, start), linked(start, body), linked(start, body, 1), linked(body, end))
    )
  }

  it should "read a text hyperparameter reference inside a loop block from the loop state" in {
    val trainer = svrTrainerOp("$c", "false")

    val result = new WorkflowCompiler(newContext()).compile(twoInputLoop(trainer))

    assert(result.operatorIdToError.isEmpty, s"unexpected errors: ${result.operatorIdToError}")
    val code = codeOf(result, trainer)
    assert(code.contains(s"C = float (${lookup("c")})"), code)
    assert(!code.contains(decoderExpression("$c")))
  }

  it should "report a typed reference on an operator whose code is generated, naming its pointer" in {
    // The Boolean `parametersSource` picks the code's shape: generated with true, the row reads its
    // parameter from the second input, and with false from its value, so no one place in the code
    // is the value. Whether the code reads the text "$c" depends on it too, so the error does not
    // judge the text: the code it was generated with, from true, does not.
    val trainer = svrTrainerOp("$c", "\"$fromPort\"")
    assertOnlyError(
      twoInputLoop(trainer),
      trainer,
      s"SVM Regressor cannot refer to loop variables in /paraList/0/parametersSource yet: " +
        generatedFromValueReason
    )
  }

  it should "report a text reference that generated code never reads, not that it embeds it" in {
    // With the parameter taken from the second input, the row's "$c" value is hidden and unused:
    // the code holds neither the lookup nor the text.
    val trainer = svrTrainerOp("$c", "true")
    assertOnlyError(
      twoInputLoop(trainer),
      trainer,
      s"SVM Regressor cannot refer to loop variables in /paraList/0/value yet: $unreadTextReason"
    )

    // Sort has no use for its dummy property at all.
    val sort = parsed(
      """{"attributes":[{"attribute":"line","sortPreference":"ASC"}],
        |"dummyPropertyList":[{"dummyProperty":"p","dummyValue":"$i"}],
        |"operatorType":"Sort"}""".stripMargin
    )
    assertOnlyError(
      oneInputLoop(sort),
      sort,
      s"Sort cannot refer to loop variables in /dummyPropertyList/0/dummyValue yet: " +
        unreadTextReason
    )
  }

  /** TextInput -> LoopStart -> `body` -> LoopEnd. */
  private def oneInputLoop(body: LogicalOp): LogicalPlanPojo = {
    val src = textInputOp("1\n2")
    val start = loopStartOp()
    val end = loopEndOp()
    pojo(List(src, start, body, end), chain(src, start, body, end))
  }

  it should "read a typed reference and a text one of Radar Chart inside a loop block from the loop state" in {
    // Its code is generated from probe values, and each place a probe landed reads the variable.
    val radar = parsed(
      """{"nameColumn":"$col","valueColumns":["line"],"fillOpacity":"$r",
        |"operatorType":"RadarChart"}""".stripMargin
    )

    val result = new WorkflowCompiler(newContext()).compile(oneInputLoop(radar))

    assert(result.operatorIdToError.isEmpty, s"unexpected errors: ${result.operatorIdToError}")
    val code = codeOf(result, radar)
    assert(code.contains(s"opacity=${valueLookup("r", "number")}\n"), code)
    assert(code.contains(s"name=str(row[${lookup("col")}])"), code)
    assert(!code.contains("EXCEPTION DURING CODE GENERATION"), code)
  }

  it should "report the bins of Histogram2D inside a loop block, which it checks before the loop runs" in {
    // "X Bins must be > 0" never sees the loop's value: were it read from the loop state, a 0
    // would reach plotly unchecked. The "$col" text is judged once they are bound.
    val histogram = parsed(
      """{"xColumn":"$col","yColumn":"line","xBins":"$n","yBins":"$m",
        |"operatorType":"Histogram2D"}""".stripMargin
    )
    assertOnlyError(
      oneInputLoop(histogram),
      histogram,
      s"Histogram2D cannot refer to loop variables in /xBins, /yBins yet: $checkedValueReason"
    )
  }

  it should "report why the code generation failed, not a typed reference, when no probe value helps" in {
    // Y Bins is 0, which Histogram2D rejects whatever $n is: that is the error, and neither the
    // "$n" nor the "$col" is said to be the problem.
    val histogram = parsed(
      """{"xColumn":"$col","yColumn":"line","xBins":"$n","yBins":0,
        |"operatorType":"Histogram2D"}""".stripMargin
    )

    val result = new WorkflowCompiler(newContext()).compile(oneInputLoop(histogram))

    assert(result.operatorIdToError.keySet == Set(histogram.operatorIdentifier))
    val message = result.operatorIdToError(histogram.operatorIdentifier).message
    assert(
      message.contains(
        "Operator is not configured properly: assertion failed: Y Bins must be > 0, but got 0"
      ),
      s"unexpected message: $message"
    )
    assert(!message.contains("loop variables"), s"unexpected message: $message")
  }

  it should "report why the code generation failed, not a text reference in it" in {
    // The second key has no attribute, so Sort generates no code: the "$col" key is not the
    // problem, and the error names the one that is.
    val sort = parsed(
      """{"attributes":[{"attribute":"$col","sortPreference":"ASC"},
        |{"attribute":"","sortPreference":"ASC"}],"operatorType":"Sort"}""".stripMargin
    )

    val result = new WorkflowCompiler(newContext()).compile(oneInputLoop(sort))

    assert(result.operatorIdToError.keySet == Set(sort.operatorIdentifier))
    val message = result.operatorIdToError(sort.operatorIdentifier).message
    assert(
      message.contains(
        "Operator is not configured properly: requirement failed: " +
          "Each sort key must have an attribute selected."
      ),
      s"unexpected message: $message"
    )
    assert(!message.contains("loop variables"), s"unexpected message: $message")
  }

  it should "report a text reference on a Python UDF inside a loop block, and a typed one next to it" in {
    // A UDF's code is the user's, not generated: nothing rewrites it, and its Boolean picks the
    // output schema, so neither reference is bound, for the one reason.
    val udf = parsed("""{"code":"$i","retainInputColumns":"$keep","operatorType":"PythonUDFV2"}""")
    assertOnlyError(
      oneInputLoop(udf),
      udf,
      s"Python UDF cannot refer to loop variables in /code, /retainInputColumns yet: $udfReason"
    )
  }

  it should "report a text reference on a Python UDF whose own code reads the loop state" in {
    // The code calls the lookup itself, where the state has arrived, and holds no "$i": only
    // that a UDF's code is not generated from its properties tells this one apart.
    val code =
      """from pytexera import *
        |class ProcessTupleOperator(UDFOperatorV2):
        |    @overrides
        |    def process_tuple(self, tuple_, port):
        |        yield {'env': self.loop_variable_text('i')}
        |""".stripMargin
    val udf = new PythonUDFOpDescV2
    udf.code = code
    udf.envName = "$i"
    udf.retainInputColumns = true
    assertOnlyError(
      oneInputLoop(udf),
      udf,
      s"Python UDF cannot refer to loop variables in /envName yet: $udfReason"
    )
  }

  it should "report a text reference on a Java UDF or an R UDF inside a loop block" in {
    // The Java UDF's code carries the failed-generation marker as a comment: it is the user's
    // code all the same, not a generation that failed, so its reference is still reported.
    val java = parsed(
      """{"code":"// #EXCEPTION DURING CODE GENERATION: not really\nimport x;",
        |"retainInputColumns":true,
        |"outputColumns":[{"attributeName":"$i","attributeType":"string"}],
        |"operatorType":"JavaUDF"}""".stripMargin
    )
    assertOnlyError(
      oneInputLoop(java),
      java,
      s"Java UDF cannot refer to loop variables in /outputColumns/0/attributeName yet: $udfReason"
    )

    val r = parsed("""{"code":"$i","retainInputColumns":true,"operatorType":"RUDF"}""")
    assertOnlyError(
      oneInputLoop(r),
      r,
      s"R UDF cannot refer to loop variables in /code yet: $udfReason"
    )
  }

  it should "report a text reference that an operator whose code is generated pastes in as it is" in {
    // Python Lambda Function writes its expressions into its code verbatim, not through pyb.
    val src = textInputOp("1\n2")
    val start = loopStartOp()
    val lambda = new PythonLambdaFunctionOpDesc
    lambda.lambdaAttributeUnits =
      List(new LambdaAttributeUnit("line", "$i", null, AttributeType.STRING))
    val end = loopEndOp()
    assertOnlyError(
      pojo(List(src, start, lambda, end), chain(src, start, lambda, end)),
      lambda,
      s"Python Lambda Function cannot refer to loop variables in " +
        s"/lambdaAttributeUnits/0/expression yet: $embeddedTextReason"
    )
  }

  /**
    * A test-only generated operator that renders `label` as pyb renders an Encodable string (the
    * `pyb` macro itself only expands under `org.apache.texera.amber`) but pastes `raw` into its
    * code as it is. No shipped operator does both with one variable, so this is the only way to
    * pin that a lookup elsewhere in the code does not excuse a pasted `$name`.
    */
  private class DecodedAndPastedPyOp extends PythonOperatorDescriptor {
    @JsonProperty var label: String = ""
    @JsonProperty var raw: String = ""
    override def generatePythonCode(): String =
      s"""class ProcessTupleOperator(UDFOperatorV2):
         |    def process_tuple(self, tuple_, port):
         |        yield {"label": ${decoderExpression(label)}, "raw": '$raw'}
         |""".stripMargin
    override def getOutputSchemas(
        inputSchemas: Map[PortIdentity, Schema]
    ): Map[PortIdentity, Schema] = Map(PortIdentity() -> inputSchemas.values.head)
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "Decoded and pasted",
        "renders one property through pyb and pastes another",
        OperatorGroupConstants.PYTHON_GROUP,
        List(InputPort()),
        List(OutputPort())
      )
  }

  it should "report a text reference that generated code both reads and pastes in as it is" in {
    val src = textInputOp("1\n2")
    val start = loopStartOp()
    val both = new DecodedAndPastedPyOp
    both.label = "$i"
    both.raw = "$i"
    val end = loopEndOp()
    val plan = pojo(List(src, start, both, end), chain(src, start, both, end))
    assertOnlyError(
      plan,
      both,
      s"Decoded and pasted cannot refer to loop variables in /label, /raw yet: $embeddedTextReason"
    )

    // Pasted text alone, a different variable's: the decoded "$i" is read from the loop state.
    both.raw = "$j"
    both.stateReferences = Map.empty
    val result = new WorkflowCompiler(newContext()).compile(plan)
    assert(
      result
        .operatorIdToError(both.operatorIdentifier)
        .message
        .contains(
          s"Decoded and pasted cannot refer to loop variables in /raw yet: $embeddedTextReason"
        ),
      s"unexpected errors: ${result.operatorIdToError}"
    )
  }

  it should "report a decoded text reference whose '$name' begins a longer text the code pastes" in {
    // "cost $index" is no reference, and the decoded "$i" is read from the loop state; but the code
    // holds "$i" as written, which a "$i" pasted next to letters looks the same as. The check does
    // not tell the two apart, so it rejects rather than bind a pasted "$i" as the literal.
    val both = new DecodedAndPastedPyOp
    both.label = "$i"
    both.raw = "cost $index"
    assertOnlyError(
      oneInputLoop(both),
      both,
      s"Decoded and pasted cannot refer to loop variables in /label yet: $embeddedTextReason"
    )
    assert(both.stateReferences == Map("/label" -> "i"))
  }

  /**
    * A test-only generated operator whose code is `template`, each `LABEL` in it the expression
    * pyb renders `label` as: where the code evaluates that expression decides whether the
    * iteration's state has arrived by then.
    */
  private class LabelReadingPyOp extends PythonOperatorDescriptor {
    @JsonProperty var label: String = "$i"
    @JsonProperty var template: String = ""
    override def generatePythonCode(): String =
      template.replace("LABEL", decoderExpression(label))
    override def getOutputSchemas(
        inputSchemas: Map[PortIdentity, Schema]
    ): Map[PortIdentity, Schema] = Map(PortIdentity() -> inputSchemas.values.head)
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "Label reading",
        "reads one property where its template says",
        OperatorGroupConstants.PYTHON_GROUP,
        List(InputPort()),
        List(OutputPort())
      )
  }

  private def labelReading(template: String): LabelReadingPyOp = {
    val op = new LabelReadingPyOp
    op.template = template.stripMargin
    op
  }

  // A read in process_tuple runs once the port's state messages are in: the reader replays them
  // ahead of the port's tuples.
  private val readsInProcessTuple =
    """class ProcessTupleOperator(UDFOperatorV2):
      |    @overrides
      |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
      |        yield {"label": LABEL}
      |"""

  it should "read a text reference in the code a test-only generated operator writes" in {
    val body = labelReading(readsInProcessTuple)

    val result = new WorkflowCompiler(newContext()).compile(oneInputLoop(body))

    assert(result.operatorIdToError.isEmpty, s"unexpected errors: ${result.operatorIdToError}")
    val code = codeOf(result, body)
    assert(code.contains(lookup("i")), code)
    assert(!code.contains(decoderExpression("$i")))
  }

  it should "report a text reference that names a new output column of generated code" in {
    // The code writes the column the loop state names, but the schema, computed before the loop
    // runs, declares one named "$r". The "$a" it reads its input from is bound as usual.
    val summarize = parsed(
      """{"attribute":"$a","Result attribute name":"$r",
        |"operatorType":"HuggingFaceTextSummarization"}""".stripMargin
    )
    assertOnlyError(
      oneInputLoop(summarize),
      summarize,
      s"Hugging Face Text Summarization cannot refer to loop variables in " +
        s"/Result attribute name yet: $outputColumnReason"
    )
  }

  it should "read a text reference in generated code that passes on an input column of that name" in {
    // The column "$col" is named outside the block, where the text is a name like any other. The
    // operator passes it on rather than making it, so its schema does not depend on the loop.
    val src = textInputOp("1\n2")
    val rename = new ProjectionOpDesc()
    rename.attributes = List(new AttributeUnit("line", "$col"))
    rename.isDrop = false
    val start = loopStartOp()
    val body = labelReading(readsInProcessTuple)
    body.label = "$col"
    val end = loopEndOp()

    val result = new WorkflowCompiler(newContext()).compile(
      pojo(List(src, rename, start, body, end), chain(src, rename, start, body, end))
    )

    assert(result.operatorIdToError.isEmpty, s"unexpected errors: ${result.operatorIdToError}")
    assert(
      result
        .operatorIdToOutputSchemas(body.operatorIdentifier)(PortIdentity())
        .get
        .containsAttribute("$col")
    )
    assert(codeOf(result, body).contains(lookup("col")))
  }

  /**
    * An `IntWritingPyOp` whose `n` refers to `n`, as parsed: the placeholder 0, recorded. Its
    * generation rejects every `n` in `rejected`.
    */
  private def intWriting(template: String, rejected: Int*): IntWritingPyOp = {
    val op = new IntWritingPyOp
    op.n = 0
    op.template = template
    op.rejected = rejected.toList
    op.stateReferences = Map("/n" -> "n")
    op
  }

  /** Code whose process_tuple yields each tuple's line and `expression` next to it. */
  private def yielding(expression: String): String =
    s"""class ProcessTupleOperator(UDFOperatorV2):
       |    @overrides
       |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
       |        yield {"line": tuple_["line"], "v": $expression}
       |""".stripMargin

  it should "read an integer that generated code holds from the loop state" in {
    val body = intWriting(yielding("<n>"))

    val result = new WorkflowCompiler(newContext()).compile(oneInputLoop(body))

    assert(result.operatorIdToError.isEmpty, s"unexpected errors: ${result.operatorIdToError}")
    assert(codeOf(result, body) == yielding(valueLookup("n", "integer")))
  }

  it should "report an integer reference whose descriptor rejects a value it is generated with" in {
    // A probe (3, 5, then 70, 90) or an edge value (0, 1, -1, 1000000) rejected is a check the
    // iteration's value would skip: rejecting 3 fails the code every variant is compared with,
    // rejecting 5 one variant, and either way the next pair generates, but the check remains.
    Seq(Seq(3), Seq(5), Seq(70), Seq(0), Seq(1), Seq(-1), Seq(1000000)).foreach { rejected =>
      val body = intWriting(yielding("<n>"), rejected: _*)
      assertOnlyError(
        oneInputLoop(body),
        body,
        s"Int writing cannot refer to loop variables in /n yet: $checkedValueReason"
      )
    }
  }

  it should "report an integer reference whose descriptor adjusts the value before it lands" in {
    // At most 10: the probes land as they are, 1000000 does not.
    val body = intWriting(yielding("<n min 10>"))
    assertOnlyError(
      oneInputLoop(body),
      body,
      s"Int writing cannot refer to loop variables in /n yet: $checkedValueReason"
    )
  }

  it should "report why the code generation failed when it fails for every probe value" in {
    // No code is generated from any probe, so whether the failure is the value's cannot be told:
    // the descriptor's own reason, for the first probe, is the error.
    val body = intWriting(yielding("<n>"), 0, 3, 70)

    val result = new WorkflowCompiler(newContext()).compile(oneInputLoop(body))

    assert(result.operatorIdToError.keySet == Set(body.operatorIdentifier))
    val message = result.operatorIdToError(body.operatorIdentifier).message
    assert(
      message.contains("Operator is not configured properly: requirement failed: n cannot be 3"),
      s"unexpected message: $message"
    )
    assert(!message.contains("loop variables"), s"unexpected message: $message")
  }

  it should "report an integer reference that generated code computes with before the loop runs" in {
    val body = intWriting(yielding("<2n>"))
    assertOnlyError(
      oneInputLoop(body),
      body,
      s"Int writing cannot refer to loop variables in /n yet: $generatedFromValueReason"
    )
  }

  it should "report an integer reference that lands in a text, a comment or next to a name" in {
    Seq(
      yielding("\"bins=<n>\""),
      yielding("f'{<n>}'"),
      yielding("1  # at most <n>"),
      yielding("bins<n>"),
      yielding("<n>j"),
      yielding("<n>.0"),
      yielding("1.<n>"),
      """class ProcessTupleOperator(UDFOperatorV2):
        |    '''
        |    Writes at most <n> bins.
        |    '''
        |""".stripMargin
    ).foreach { template =>
      val body = intWriting(template)
      assertOnlyError(
        oneInputLoop(body),
        body,
        s"Int writing cannot refer to loop variables in /n yet: $valueInTextReason"
      )
    }
  }

  it should "report an integer reference that generated code does not hold" in {
    val body = intWriting(yielding("1"))
    assertOnlyError(
      oneInputLoop(body),
      body,
      s"Int writing cannot refer to loop variables in /n yet: $unreadValueReason"
    )
  }

  /** `intWriting`, with `m` referring to `m` as well, and rejecting every `m` in `rejectedM`. */
  private def intsWriting(template: String, rejected: Int*)(rejectedM: Int*): IntWritingPyOp = {
    val op = intWriting(template, rejected: _*)
    op.m = 0
    op.rejectedM = rejectedM.toList
    op.stateReferences += "/m" -> "m"
    op
  }

  it should "bind the references it can when every variant of another one is rejected" in {
    // Rejecting 5 and 90 fails n's variant in either pair, never the baseline, so the first
    // baseline is the code: m is read from the loop state, and n keeps the probe it was generated
    // with, reported (the operator never runs).
    val body = intsWriting(yielding("(<n>, <m>)"), 5, 90)()
    assertOnlyError(
      oneInputLoop(body),
      body,
      s"Int writing cannot refer to loop variables in /n yet: $checkedValueReason"
    )
    assert(body.loopVariableBinding.unbound == Map("/n" -> checkedValueReason))
    assert(body.loopVariableBinding.code == yielding(s"(3, ${valueLookup("m", "integer")})"))
  }

  it should "bind a reference with the next probe pair when another one rejects the first" in {
    // m rejects 3, so no code is generated with both at 3: both are generated from the next pair,
    // 70 and 90, and n is read from the loop state. m, which rejects a value, is reported.
    val body = intsWriting(yielding("(<n>, <m>)"))(3)
    assertOnlyError(
      oneInputLoop(body),
      body,
      s"Int writing cannot refer to loop variables in /m yet: $checkedValueReason"
    )
    assert(body.loopVariableBinding.code == yielding(s"(${valueLookup("n", "integer")}, 70)"))
  }

  it should "report two references whose values seem to land in one place" in {
    // n + m - 3 lands as either one alone, the other one at its first probe, 3: it is neither.
    val body = intsWriting(yielding("<n+m-3>"))()
    assertOnlyError(
      oneInputLoop(body),
      body,
      s"Int writing cannot refer to loop variables in /m, /n yet: $generatedFromValueReason"
    )
    assert(body.loopVariableBinding.code == yielding("3"))
  }

  it should "report two references whose values the descriptor adjusts with each other" in {
    // The larger of n and m is at least the other one's probe: 0 and -1 do not land as they are.
    val body = intsWriting(yielding("<max>"))()
    assertOnlyError(
      oneInputLoop(body),
      body,
      s"Int writing cannot refer to loop variables in /m, /n yet: $checkedValueReason"
    )
  }

  it should "leave generated code as it is when none of its properties refers to a loop variable" in {
    // Outside every loop block, and inside one: the code holds the value itself.
    def writingFour(): IntWritingPyOp = {
      val op = new IntWritingPyOp
      op.n = 4
      op.template = yielding("<n>")
      op
    }
    val src = textInputOp("1\n2")
    val outside = writingFour()
    val inside = writingFour()

    val outsideResult = new WorkflowCompiler(newContext()).compile(
      pojo(List(src, outside), List(linked(src, outside)))
    )
    val insideResult = new WorkflowCompiler(newContext()).compile(oneInputLoop(inside))

    Seq(outsideResult -> outside, insideResult -> inside).foreach {
      case (result, op) =>
        assert(result.operatorIdToError.isEmpty, s"unexpected errors: ${result.operatorIdToError}")
        assert(op.stateReferences.isEmpty)
        assert(codeOf(result, op) == yielding("4"))
    }
  }

  it should "read a boolean that generated code holds as a Python literal from the loop state" in {
    def flagWriting(template: String): FlagWritingPyOp = {
      val op = new FlagWritingPyOp
      op.flag = false
      op.template = template
      op.stateReferences = Map("/flag" -> "f")
      op
    }
    val body = flagWriting(yielding("<Flag>"))

    val result = new WorkflowCompiler(newContext()).compile(oneInputLoop(body))

    assert(result.operatorIdToError.isEmpty, s"unexpected errors: ${result.operatorIdToError}")
    assert(codeOf(result, body) == yielding(valueLookup("f", "boolean")))

    // As Scala writes it, "true" is no Python boolean.
    val scala = flagWriting(yielding("<flag>"))
    assertOnlyError(
      oneInputLoop(scala),
      scala,
      s"Flag writing cannot refer to loop variables in /flag yet: $generatedFromValueReason"
    )
  }

  private def ratioWriting(namesColumn: Boolean, rejected: Double*): RatioWritingPyOp = {
    val op = new RatioWritingPyOp
    op.ratio = 0.0
    op.namesColumn = namesColumn
    op.rejected = rejected.toList
    op.stateReferences = Map("/ratio" -> "r")
    op
  }

  it should "generate a number's code from its next probe pair, and report the value rejected" in {
    // 0.25 rejected, the code is generated from the next pair, 2.5 and 7.5: they land as they
    // are, but the check that rejects 0.25 remains.
    val body = ratioWriting(namesColumn = false, 0.25)
    assertOnlyError(
      oneInputLoop(body),
      body,
      s"Ratio writing cannot refer to loop variables in /ratio yet: $checkedValueReason"
    )
    assert(body.loopVariableBinding.code.contains("""float(tuple_["line"]) * 2.5}"""))
  }

  it should "read a number that generated code holds from the loop state, its schema from a probe" in {
    // The placeholder 0.0 would fail the schema's "ratio must be > 0"; the probes pass it.
    val body = ratioWriting(namesColumn = false)

    val result = new WorkflowCompiler(newContext()).compile(oneInputLoop(body))

    assert(result.operatorIdToError.isEmpty, s"unexpected errors: ${result.operatorIdToError}")
    val code = codeOf(result, body)
    assert(code.contains(s"""float(tuple_["line"]) * ${valueLookup("r", "number")}"""), code)
    assert(
      result.operatorIdToOutputSchemas(body.operatorIdentifier)(PortIdentity()).get ==
        Schema().add(new Attribute("line", AttributeType.STRING))
    )
  }

  it should "report a number reference that decides the output schema" in {
    val body = ratioWriting(namesColumn = true)

    val result = new WorkflowCompiler(newContext()).compile(oneInputLoop(body))

    assert(result.physicalPlan.isEmpty)
    val message = result.operatorIdToError(body.operatorIdentifier).message
    assert(
      message.contains(
        "Ratio writing cannot refer to loop variables in /ratio yet: the value decides its " +
          "output schema"
      ),
      s"unexpected message: $message"
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

/**
  * Test-only generated operators whose numeric or boolean properties refer to loop variables.
  * The descriptor generates their code from copies it parses from their JSON, and Jackson builds
  * no inner class, so these live here rather than in the spec class.
  */
object WorkflowCompilerSpec {

  /**
    * A test-only generated operator that writes its Ints `n` and `m` into its code where
    * `template` says: each `<n>` / `<m>` as Scala writes it, each `<2n>` twice `n`, each
    * `<n min 10>` `n`, at most 10, each `<max>` the larger of the two, each `<n+m-3>` their sum
    * less 3. Generating rejects every `n` in `rejected` and every `m` in `rejectedM`, as a
    * descriptor's own validation would.
    */
  private class IntWritingPyOp extends PythonOperatorDescriptor {
    @JsonProperty var n: Int = 1
    @JsonProperty var m: Int = 1
    @JsonProperty var template: String = ""
    @JsonProperty var rejected: List[Int] = List.empty
    @JsonProperty var rejectedM: List[Int] = List.empty
    override def generatePythonCode(): String = {
      require(!rejected.contains(n), s"n cannot be $n")
      require(!rejectedM.contains(m), s"m cannot be $m")
      template
        .replace("<2n>", (2 * n).toString)
        .replace("<n min 10>", n.min(10).toString)
        .replace("<max>", n.max(m).toString)
        .replace("<n+m-3>", (n + m - 3).toString)
        .replace("<n>", n.toString)
        .replace("<m>", m.toString)
    }
    override def getOutputSchemas(
        inputSchemas: Map[PortIdentity, Schema]
    ): Map[PortIdentity, Schema] = Map(PortIdentity() -> inputSchemas.values.head)
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "Int writing",
        "writes its n into its code where its template says",
        OperatorGroupConstants.PYTHON_GROUP,
        List(InputPort()),
        List(OutputPort())
      )
  }

  /**
    * A test-only generated operator that writes its Boolean `flag` into its code where `template`
    * says: each `<Flag>` as Python writes it (True / False), each `<flag>` as Scala does.
    */
  private class FlagWritingPyOp extends PythonOperatorDescriptor {
    @JsonProperty var flag: Boolean = true
    @JsonProperty var template: String = ""
    override def generatePythonCode(): String =
      template.replace("<Flag>", if (flag) "True" else "False").replace("<flag>", flag.toString)
    override def getOutputSchemas(
        inputSchemas: Map[PortIdentity, Schema]
    ): Map[PortIdentity, Schema] = Map(PortIdentity() -> inputSchemas.values.head)
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "Flag writing",
        "writes its flag into its code where its template says",
        OperatorGroupConstants.PYTHON_GROUP,
        List(InputPort()),
        List(OutputPort())
      )
  }

  /**
    * A test-only generated operator that scales each line by its Double `ratio`, which must be
    * positive; with `namesColumn` its output adds a column named after the ratio. Generating
    * rejects every `ratio` in `rejected`.
    */
  private class RatioWritingPyOp extends PythonOperatorDescriptor {
    @JsonProperty var ratio: Double = 1.0
    @JsonProperty var namesColumn: Boolean = false
    @JsonProperty var rejected: List[Double] = List.empty
    override def generatePythonCode(): String = {
      require(!rejected.contains(ratio), s"ratio cannot be $ratio")
      s"""class ProcessTupleOperator(UDFOperatorV2):
         |    @overrides
         |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
         |        yield {"line": tuple_["line"], "v": float(tuple_["line"]) * $ratio}
         |""".stripMargin
    }
    override def getOutputSchemas(
        inputSchemas: Map[PortIdentity, Schema]
    ): Map[PortIdentity, Schema] = {
      require(ratio > 0, s"ratio must be > 0, but got $ratio")
      val input = inputSchemas.values.head
      Map(
        PortIdentity() ->
          (if (namesColumn) input.add(s"times $ratio", AttributeType.DOUBLE) else input)
      )
    }
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "Ratio writing",
        "scales each line by its ratio",
        OperatorGroupConstants.PYTHON_GROUP,
        List(InputPort()),
        List(OutputPort())
      )
  }
}
