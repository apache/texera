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

package org.apache.texera.web.resource

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.timestamp.Timestamp
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.workflowruntimestate.FatalErrorType.EXECUTION_FAILURE
import org.apache.texera.amber.core.workflowruntimestate.WorkflowFatalError
import org.apache.texera.amber.core.virtualidentity.ExecutionIdentity
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.common.executionruntimestate.ExecutionMetadataStore
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

/**
  * Unit tests for the pure, wire-shape parts of [[SyncExecutionResource]] that this PR
  * introduced: the execution-summary case classes (SampleRow / OperatorResultSummary /
  * OperatorConsoleLogsSummary / OperatorExecutionSummary / WorkflowExecutionSummary), the
  * `handleExecutionError` error-classification branch, and the two behavior-preserving
  * extract-method helpers `sampleAndTruncateTuples` (the result sampling/truncation logic
  * lifted out of `collectOperatorResult`) and `buildOperatorExecutionSummary` (the
  * per-operator summary construction lifted out of `collectOperatorInfos`).
  *
  * The remaining changed code paths (`executeWorkflowSync`, and the storage/DB plumbing in
  * `collectOperatorResult` / `collectOperatorInfos` around these helpers) require a live
  * Pekko execution engine, an Iceberg-backed result store, and DB-persisted port executions,
  * so they are exercised by integration tests rather than here.
  */
class SyncExecutionResourceSpec extends AnyFlatSpec with Matchers with PrivateMethodTester {

  private val mapper = new ObjectMapper()
  private val resource = new SyncExecutionResource

  private def sampleRow(idx: Int, k: String, v: String): SampleRow = {
    val node = mapper.createObjectNode()
    node.put(k, v)
    SampleRow(rowIndex = idx, tuple = node)
  }

  "SampleRow" should "carry the row index and the processed JSON tuple" in {
    val row = sampleRow(3, "col", "value")
    row.rowIndex shouldBe 3
    row.tuple.get("col").asText() shouldBe "value"
  }

  "OperatorResultSummary" should "carry the result mode, sample tuples, and total count" in {
    val rows = List(sampleRow(0, "a", "1"), sampleRow(1, "a", "2"))
    val summary = OperatorResultSummary(
      resultMode = "table",
      sampleTuples = rows,
      tuplesCount = 42
    )
    summary.resultMode shouldBe "table"
    summary.sampleTuples should have size 2
    summary.sampleTuples.head.rowIndex shouldBe 0
    summary.tuplesCount shouldBe 42
  }

  it should "support the visualization result mode" in {
    val summary =
      OperatorResultSummary(
        resultMode = "visualization",
        sampleTuples = List.empty,
        tuplesCount = 1
      )
    summary.resultMode shouldBe "visualization"
    summary.sampleTuples shouldBe empty
    summary.tuplesCount shouldBe 1
  }

  "OperatorConsoleLogsSummary" should "carry the console message list" in {
    val messages = List(
      ConsoleMessageInfo(msgType = "ERROR", title = "boom", message = "stack trace"),
      ConsoleMessageInfo(msgType = "PRINT", title = "hello", message = "")
    )
    val summary = OperatorConsoleLogsSummary(messages = messages)
    summary.messages should have size 2
    summary.messages.head.msgType shouldBe "ERROR"
    summary.messages.head.title shouldBe "boom"
    summary.messages(1).message shouldBe ""
  }

  "OperatorExecutionSummary" should "carry state, error messages, and the optional sub-summaries" in {
    val error =
      WorkflowFatalError(EXECUTION_FAILURE, Timestamp(Instant.now), "op failed", "", "op-1")
    val resultSummary =
      OperatorResultSummary(resultMode = "table", sampleTuples = List.empty, tuplesCount = 0)
    val consoleSummary = OperatorConsoleLogsSummary(messages = List.empty)

    val summary = OperatorExecutionSummary(
      state = "Completed",
      errorMessages = List(error),
      resultSummary = Some(resultSummary),
      consoleLogsSummary = Some(consoleSummary)
    )

    summary.state shouldBe "Completed"
    summary.errorMessages should have size 1
    summary.errorMessages.head.`type` shouldBe EXECUTION_FAILURE
    summary.errorMessages.head.message shouldBe "op failed"
    summary.errorMessages.head.operatorId shouldBe "op-1"
    summary.resultSummary shouldBe Some(resultSummary)
    summary.consoleLogsSummary shouldBe Some(consoleSummary)
  }

  it should "represent a not-failed operator with an empty error list and no sub-summaries" in {
    val summary = OperatorExecutionSummary(
      state = "Running",
      errorMessages = List.empty,
      resultSummary = None,
      consoleLogsSummary = None
    )
    summary.errorMessages shouldBe empty
    summary.resultSummary shouldBe None
    summary.consoleLogsSummary shouldBe None
  }

  "WorkflowExecutionSummary" should "carry success, state, per-operator summaries, and errors" in {
    val opSummary = OperatorExecutionSummary(
      state = "Completed",
      errorMessages = List.empty,
      resultSummary = None,
      consoleLogsSummary = None
    )
    val summary = WorkflowExecutionSummary(
      success = true,
      state = "Completed",
      operators = Map("op-1" -> opSummary),
      errors = List.empty
    )
    summary.success shouldBe true
    summary.state shouldBe "Completed"
    summary.operators.keySet shouldBe Set("op-1")
    summary.operators("op-1").state shouldBe "Completed"
    summary.errors shouldBe empty
  }

  // handleExecutionError is private; reflectively invoke it (no production change needed).
  private val handleExecutionError =
    PrivateMethod[WorkflowExecutionSummary](Symbol("handleExecutionError"))

  private def classify(message: String): WorkflowExecutionSummary =
    resource invokePrivate handleExecutionError(new RuntimeException(message))

  "handleExecutionError" should "classify lowercase 'compilation' messages as CompilationFailed" in {
    val summary = classify("compilation failed for the plan")
    summary.success shouldBe false
    summary.state shouldBe "CompilationFailed"
    summary.operators shouldBe empty
    summary.errors shouldBe List("compilation failed for the plan")
  }

  it should "classify capitalized 'Compilation' messages as CompilationFailed" in {
    classify("Compilation error near line 3").state shouldBe "CompilationFailed"
  }

  it should "classify 'operator' messages as CompilationFailed" in {
    classify("unknown operator reference").state shouldBe "CompilationFailed"
  }

  it should "classify 'schema' messages as CompilationFailed" in {
    classify("invalid schema on input port").state shouldBe "CompilationFailed"
  }

  it should "classify unrecognized messages as a generic Error" in {
    val summary = classify("something unexpected happened")
    summary.success shouldBe false
    summary.state shouldBe "Error"
    summary.operators shouldBe empty
    summary.errors shouldBe List("something unexpected happened")
  }

  it should "fall back to 'Unknown error' when the exception has a null message" in {
    val summary = resource invokePrivate handleExecutionError(
      new RuntimeException(null.asInstanceOf[String])
    )
    summary.state shouldBe "Error"
    summary.errors shouldBe List("Unknown error")
  }

  // --- sampleAndTruncateTuples (extracted from collectOperatorResult) ---------------------

  private val tableSchema = new Schema(List(new Attribute("col", AttributeType.STRING)))
  private def tableTuple(v: String): Tuple =
    Tuple.builder(tableSchema).add("col", AttributeType.STRING, v).build()

  "sampleAndTruncateTuples" should "report an empty table for a zero-count / empty iterator" in {
    val (mode, rows, total, returned, truncated) =
      resource.sampleAndTruncateTuples(Iterator.empty, 0, 100000, 100000)
    mode shouldBe "table"
    rows shouldBe Some(List.empty[SampleRow])
    total shouldBe Some(0)
    returned shouldBe Some(0)
    truncated shouldBe Some(false)
  }

  it should "report an empty table when the iterator is empty despite a positive count" in {
    // Exercises the `!tupleIterator.hasNext` half of the guard (count > 0, no rows).
    val (mode, rows, total, returned, truncated) =
      resource.sampleAndTruncateTuples(Iterator.empty, 5, 100000, 100000)
    mode shouldBe "table"
    rows shouldBe Some(List.empty[SampleRow])
    total shouldBe Some(0)
    returned shouldBe Some(0)
    truncated shouldBe Some(false)
  }

  it should "return visualization mode for a single visualization tuple" in {
    val vizSchema = new Schema(List(new Attribute("html-content", AttributeType.STRING)))
    val vizTuple =
      Tuple.builder(vizSchema).add("html-content", AttributeType.STRING, "<div>viz</div>").build()

    val (mode, rows, total, returned, truncated) =
      resource.sampleAndTruncateTuples(Iterator(vizTuple), 1, 100000, 100000)
    mode shouldBe "visualization"
    rows.get should have size 1
    rows.get.head.rowIndex shouldBe 0
    total shouldBe Some(1)
    returned shouldBe Some(1)
    truncated shouldBe Some(false)
  }

  it should "return every row untruncated when the whole table fits (front-only path)" in {
    val tuples = List(tableTuple("a"), tableTuple("b"), tableTuple("c"))
    val (mode, rows, total, returned, truncated) =
      resource.sampleAndTruncateTuples(tuples.iterator, tuples.size, 100000, 100000)
    mode shouldBe "table"
    rows.get should have size 3
    rows.get.map(_.rowIndex) shouldBe List(0, 1, 2)
    rows.get.map(_.tuple.get("col").asText()) shouldBe List("a", "b", "c")
    total shouldBe Some(3)
    returned shouldBe Some(3)
    truncated shouldBe Some(false)
  }

  it should "keep only the first (truncated) row when a single tuple exceeds the char limit" in {
    // Tiny char limit so the first tuple's estimated size already exceeds it. The single
    // tuple is not a visualization tuple, so the oversized-first early return fires.
    val (mode, rows, total, returned, truncated) =
      resource.sampleAndTruncateTuples(Iterator(tableTuple("hello world")), 1, 10, 100000)
    mode shouldBe "table"
    rows.get should have size 1
    rows.get.head.rowIndex shouldBe 0
    total shouldBe Some(1)
    returned shouldBe Some(1)
    truncated shouldBe Some(true)
  }

  it should "drop the middle rows when the front fills and a sliding back-window runs" in {
    // ~31 chars/tuple, halfLimit = 100: the front fills after a few rows, then the inner
    // else switches to the sliding back-window and drops the middle rows.
    val tuples = (0 until 12).map(i => tableTuple("v" * 20)).toList
    val (mode, rows, total, returned, truncated) =
      resource.sampleAndTruncateTuples(tuples.iterator, tuples.size, 200, 100000)
    mode shouldBe "table"
    total shouldBe Some(12)
    truncated shouldBe Some(true)
    returned.get should be < 12
    rows.get.size shouldBe returned.get
    // Front rows keep their original positions; the tail keeps the most recent rows.
    rows.get.head.rowIndex shouldBe 0
    rows.get.last.rowIndex shouldBe 11
  }

  it should "run the trailing back-window when the first row alone fills the front half" in {
    // First tuple's string is truncated by convertTuplesToJson to 103 chars, giving an
    // estimated size >= halfLimit (100) but < the char limit (200), so the front while-loop
    // never runs and the trailing back-window path handles the remaining rows.
    val tuples = tableTuple("x" * 150) :: (0 until 5).map(_ => tableTuple("y" * 20)).toList
    val (mode, rows, total, returned, truncated) =
      resource.sampleAndTruncateTuples(tuples.iterator, tuples.size, 200, 100000)
    mode shouldBe "table"
    total shouldBe Some(6)
    truncated shouldBe Some(true)
    returned.get should be < 6
    rows.get.size shouldBe returned.get
    rows.get.head.rowIndex shouldBe 0
    rows.get.last.rowIndex shouldBe 5
  }

  // --- buildOperatorExecutionSummary (extracted from collectOperatorInfos) ----------------

  "buildOperatorExecutionSummary" should "wire a result summary and no errors when only a result is present" in {
    val rows = List(sampleRow(0, "col", "v"))
    val summary = resource.buildOperatorExecutionSummary(
      opId = "op-1",
      state = "Completed",
      resultMode = "table",
      result = Some(rows),
      tuplesCount = Some(7),
      consoleLogs = None
    )
    summary.state shouldBe "Completed"
    summary.errorMessages shouldBe empty
    summary.consoleLogsSummary shouldBe None
    summary.resultSummary.get.resultMode shouldBe "table"
    summary.resultSummary.get.sampleTuples shouldBe rows
    summary.resultSummary.get.tuplesCount shouldBe 7
  }

  it should "surface a console ERROR as one EXECUTION_FAILURE error using the longer of title/message" in {
    val logs = List(
      ConsoleMessageInfo(msgType = "PRINT", title = "noise", message = "ignored"),
      ConsoleMessageInfo(msgType = "ERROR", title = "short", message = "a much longer message")
    )
    val summary = resource.buildOperatorExecutionSummary(
      opId = "op-9",
      state = "Failed",
      resultMode = "table",
      result = None,
      tuplesCount = None,
      consoleLogs = Some(logs)
    )
    summary.errorMessages should have size 1
    summary.errorMessages.head.`type` shouldBe EXECUTION_FAILURE
    summary.errorMessages.head.message shouldBe "a much longer message"
    summary.errorMessages.head.operatorId shouldBe "op-9"
    summary.consoleLogsSummary.get.messages should have size 2
  }

  it should "keep the ERROR title when it is longer than the message" in {
    val logs =
      List(ConsoleMessageInfo(msgType = "ERROR", title = "a long descriptive title", message = ""))
    val summary = resource.buildOperatorExecutionSummary(
      opId = "op-2",
      state = "Failed",
      resultMode = "table",
      result = None,
      tuplesCount = None,
      consoleLogs = Some(logs)
    )
    summary.errorMessages.head.message shouldBe "a long descriptive title"
  }

  it should "keep the ERROR title when the message is non-empty but shorter" in {
    // Exercises `message.nonEmpty` true AND `message.length > title.length` false.
    val logs =
      List(
        ConsoleMessageInfo(
          msgType = "ERROR",
          title = "a fairly long error title",
          message = "short"
        )
      )
    val summary = resource.buildOperatorExecutionSummary(
      opId = "op-5",
      state = "Failed",
      resultMode = "table",
      result = None,
      tuplesCount = None,
      consoleLogs = Some(logs)
    )
    summary.errorMessages.head.message shouldBe "a fairly long error title"
  }

  it should "leave the result summary empty when no result was materialized" in {
    val summary = resource.buildOperatorExecutionSummary(
      opId = "op-3",
      state = "Uninitialized",
      resultMode = "table",
      result = None,
      tuplesCount = None,
      consoleLogs = None
    )
    summary.resultSummary shouldBe None
    summary.consoleLogsSummary shouldBe None
    summary.errorMessages shouldBe empty
  }

  // --- assembleExecutionSummary (extracted from executeWorkflowSync) ----------------------

  private def metadataStore(
      state: WorkflowAggregatedState,
      fatalErrors: Seq[WorkflowFatalError] = Seq.empty
  ): ExecutionMetadataStore =
    ExecutionMetadataStore(
      state = state,
      fatalErrors = fatalErrors,
      executionId = ExecutionIdentity(0L)
    )

  private def failingOperatorSummary: OperatorExecutionSummary =
    OperatorExecutionSummary(
      state = "Failed",
      errorMessages =
        List(WorkflowFatalError(EXECUTION_FAILURE, Timestamp(Instant.now), "err", "", "op1")),
      resultSummary = None,
      consoleLogsSummary = None
    )

  "assembleExecutionSummary" should "report success for a COMPLETED run with no errors" in {
    val summary = resource.assembleExecutionSummary(
      finalState = metadataStore(WorkflowAggregatedState.COMPLETED),
      operatorInfos = Map.empty,
      terminatedByConsoleError = false,
      terminatedByTargetResults = false
    )
    summary.success shouldBe true
    summary.state shouldBe "Completed"
    summary.errors shouldBe empty
    summary.operators shouldBe empty
  }

  it should "map a non-terminal-success final state through stateToString" in {
    val summary = resource.assembleExecutionSummary(
      finalState = metadataStore(WorkflowAggregatedState.FAILED),
      operatorInfos = Map.empty,
      terminatedByConsoleError = false,
      terminatedByTargetResults = false
    )
    summary.success shouldBe false
    summary.state shouldBe "Failed"
  }

  it should "force a Failed state when terminated by a console error, regardless of final state" in {
    val summary = resource.assembleExecutionSummary(
      finalState = metadataStore(WorkflowAggregatedState.COMPLETED),
      operatorInfos = Map.empty,
      terminatedByConsoleError = true,
      terminatedByTargetResults = false
    )
    summary.state shouldBe "Failed"
    summary.success shouldBe false
  }

  it should "override to Completed/success when terminated by target results on a non-completed state" in {
    val summary = resource.assembleExecutionSummary(
      finalState = metadataStore(WorkflowAggregatedState.RUNNING),
      operatorInfos = Map.empty,
      terminatedByConsoleError = false,
      terminatedByTargetResults = true
    )
    summary.state shouldBe "Completed"
    summary.success shouldBe true
  }

  it should "mark the run unsuccessful when any operator reports console errors" in {
    val summary = resource.assembleExecutionSummary(
      finalState = metadataStore(WorkflowAggregatedState.COMPLETED),
      operatorInfos = Map("op1" -> failingOperatorSummary),
      terminatedByConsoleError = false,
      terminatedByTargetResults = false
    )
    summary.success shouldBe false
    summary.state shouldBe "Completed"
  }

  it should "surface each final-state fatal error as a formatted error string" in {
    val summary = resource.assembleExecutionSummary(
      finalState = metadataStore(
        WorkflowAggregatedState.COMPLETED,
        Seq(WorkflowFatalError(EXECUTION_FAILURE, Timestamp(Instant.now), "boom", "", "op1"))
      ),
      operatorInfos = Map.empty,
      terminatedByConsoleError = false,
      terminatedByTargetResults = false
    )
    summary.errors should have size 1
    summary.errors.head should endWith("boom")
  }
}
