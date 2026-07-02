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
import org.apache.texera.amber.core.workflowruntimestate.FatalErrorType.EXECUTION_FAILURE
import org.apache.texera.amber.core.workflowruntimestate.WorkflowFatalError
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

/**
  * Unit tests for the pure, wire-shape parts of [[SyncExecutionResource]] that this PR
  * introduced: the execution-summary case classes (SampleRow / OperatorResultSummary /
  * OperatorConsoleLogsSummary / OperatorExecutionSummary / WorkflowExecutionSummary) and
  * the `handleExecutionError` error-classification branch.
  *
  * The remaining changed code paths (`executeWorkflowSync`, `collectOperatorInfos`, and the
  * result-truncation loop inside `collectOperatorResult`) require a live Pekko execution
  * engine, an Iceberg-backed result store, and DB-persisted port executions, so they are
  * exercised by integration tests rather than here.
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
}
