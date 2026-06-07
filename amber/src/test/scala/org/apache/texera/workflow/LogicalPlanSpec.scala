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

import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.apache.texera.web.model.websocket.request.LogicalPlanPojo
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.IteratorHasAsScala

class LogicalPlanSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Fixture: a small operator chain `csv → keyword` (and richer variants below)
  // ---------------------------------------------------------------------------

  private def csv(): CSVScanSourceOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()
  private def keyword(): org.apache.texera.amber.operator.keywordSearch.KeywordSearchOpDesc =
    TestOperators.keywordSearchOpDesc("column-1", "Asia")

  private def link(
      from: OperatorIdentity,
      to: OperatorIdentity,
      fromPort: Int = 0,
      toPort: Int = 0
  ): LogicalLink =
    LogicalLink(from, PortIdentity(fromPort), to, PortIdentity(toPort))

  // ---------------------------------------------------------------------------
  // Construction
  // ---------------------------------------------------------------------------

  "LogicalPlan" should "expose the operators and links it was constructed with" in {
    val a = csv()
    val b = keyword()
    val l = link(a.operatorIdentifier, b.operatorIdentifier)
    val plan = LogicalPlan(List(a, b), List(l))
    assert(plan.operators == List(a, b))
    assert(plan.links == List(l))
  }

  "LogicalPlan.apply(LogicalPlanPojo)" should
    "lift the POJO's operators and links, ignoring opsToViewResult / opsToReuseResult" in {
    val a = csv()
    val b = keyword()
    val l = link(a.operatorIdentifier, b.operatorIdentifier)
    val pojo = LogicalPlanPojo(
      operators = List(a, b),
      links = List(l),
      // `operatorId` is private — use the public OperatorIdentity wrapper.
      opsToViewResult = List(b.operatorIdentifier.id), // intentionally non-empty
      opsToReuseResult = List(a.operatorIdentifier.id)
    )
    val plan = LogicalPlan(pojo)
    assert(plan.operators == List(a, b))
    assert(plan.links == List(l))
  }

  // ---------------------------------------------------------------------------
  // getTopologicalOpIds
  // ---------------------------------------------------------------------------

  "LogicalPlan.getTopologicalOpIds" should "yield a topological order on a linear chain" in {
    val a = csv()
    val b = keyword()
    val plan = LogicalPlan(List(a, b), List(link(a.operatorIdentifier, b.operatorIdentifier)))
    val order = plan.getTopologicalOpIds.asScala.toList
    assert(order == List(a.operatorIdentifier, b.operatorIdentifier))
  }

  it should "respect edge directionality across a fan-out shape (a → b, a → c)" in {
    val a = csv()
    val b = keyword()
    val c = TestOperators.keywordSearchOpDesc("column-1", "Africa")
    val plan = LogicalPlan(
      List(a, b, c),
      List(
        link(a.operatorIdentifier, b.operatorIdentifier),
        link(a.operatorIdentifier, c.operatorIdentifier)
      )
    )
    val order = plan.getTopologicalOpIds.asScala.toList
    assert(order.head == a.operatorIdentifier, s"expected a first, got $order")
    assert(order.tail.toSet == Set(b.operatorIdentifier, c.operatorIdentifier))
  }

  // ---------------------------------------------------------------------------
  // getOperator
  // ---------------------------------------------------------------------------

  "LogicalPlan.getOperator" should "return the operator with the requested identifier" in {
    val a = csv()
    val b = keyword()
    val plan = LogicalPlan(List(a, b), List.empty)
    assert(plan.getOperator(a.operatorIdentifier) eq a)
    assert(plan.getOperator(b.operatorIdentifier) eq b)
  }

  it should "throw NoSuchElementException for an unknown operator id" in {
    val a = csv()
    val plan = LogicalPlan(List(a), List.empty)
    intercept[NoSuchElementException] {
      plan.getOperator(OperatorIdentity("not-in-plan"))
    }
  }

  // ---------------------------------------------------------------------------
  // getTerminalOperatorIds
  // ---------------------------------------------------------------------------

  "LogicalPlan.getTerminalOperatorIds" should "return the single sink in a linear chain" in {
    val a = csv()
    val b = keyword()
    val plan = LogicalPlan(List(a, b), List(link(a.operatorIdentifier, b.operatorIdentifier)))
    val sinks = plan.getTerminalOperatorIds
    assert(sinks == List(b.operatorIdentifier))
  }

  it should "return every operator with out-degree 0 in a fan-out plan" in {
    val a = csv()
    val b = keyword()
    val c = TestOperators.keywordSearchOpDesc("column-1", "Africa")
    val plan = LogicalPlan(
      List(a, b, c),
      List(
        link(a.operatorIdentifier, b.operatorIdentifier),
        link(a.operatorIdentifier, c.operatorIdentifier)
      )
    )
    assert(plan.getTerminalOperatorIds.toSet == Set(b.operatorIdentifier, c.operatorIdentifier))
  }

  it should "return every operator when there are no links" in {
    // An isolated set of operators with no edges — every operator has
    // out-degree 0 and is therefore terminal.
    val a = csv()
    val b = keyword()
    val plan = LogicalPlan(List(a, b), List.empty)
    assert(plan.getTerminalOperatorIds.toSet == Set(a.operatorIdentifier, b.operatorIdentifier))
  }

  it should "return an empty list for an empty plan" in {
    val plan = LogicalPlan(List.empty, List.empty)
    assert(plan.getTerminalOperatorIds.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // getUpstreamLinks
  // ---------------------------------------------------------------------------

  "LogicalPlan.getUpstreamLinks" should "return every link whose toOpId matches" in {
    val a = csv()
    val b = keyword()
    val c = TestOperators.keywordSearchOpDesc("column-1", "Africa")
    val ab = link(a.operatorIdentifier, b.operatorIdentifier)
    val cb = link(c.operatorIdentifier, b.operatorIdentifier, fromPort = 0, toPort = 1)
    val plan = LogicalPlan(List(a, b, c), List(ab, cb))
    val upstreams = plan.getUpstreamLinks(b.operatorIdentifier)
    assert(upstreams.size == 2)
    assert(upstreams.toSet == Set(ab, cb))
  }

  it should "preserve construction order when multiple links flow into the same target" in {
    val a = csv()
    val b = keyword()
    val c = TestOperators.keywordSearchOpDesc("column-1", "Africa")
    val ab = link(a.operatorIdentifier, b.operatorIdentifier)
    val cb = link(c.operatorIdentifier, b.operatorIdentifier, fromPort = 0, toPort = 1)
    val plan = LogicalPlan(List(a, b, c), List(ab, cb))
    assert(plan.getUpstreamLinks(b.operatorIdentifier) == List(ab, cb))
  }

  it should "return an empty list when nothing flows into the requested target" in {
    val a = csv()
    val plan = LogicalPlan(List(a), List.empty)
    assert(plan.getUpstreamLinks(a.operatorIdentifier).isEmpty)
  }

  // ---------------------------------------------------------------------------
  // resolveScanSourceOpFileName
  // ---------------------------------------------------------------------------
  //
  // A successful resolution of a real file path is environment-dependent
  // (resolved through FileResolver, which can reach LakeFS / dataset
  // service), so we exclusively pin the FAILURE behavior here — that
  // matches the production contract worth pinning:
  //   1. Some(errorList) — failures are appended to errorList by opId
  //   2. None — the first failure rethrows
  // To force a failure deterministically, we set a non-existent input
  // path on the ScanSource fixture and assert the error surfaces both
  // ways.

  private def csvWithMissingFile(): CSVScanSourceOpDesc = {
    val op = TestOperators.headerlessSmallCsvScanOpDesc()
    op.fileName = Some("/definitely/does/not/exist.csv")
    op
  }

  "LogicalPlan.resolveScanSourceOpFileName" should
    "append per-operator errors to the provided errorList instead of throwing" in {
    val brokenOp = csvWithMissingFile()
    val plan = LogicalPlan(List(brokenOp), List.empty)
    val errors = ArrayBuffer.empty[(OperatorIdentity, Throwable)]
    plan.resolveScanSourceOpFileName(Some(errors))
    assert(errors.size == 1, s"expected one captured error, got: $errors")
    val (opId, _) = errors.head
    assert(opId == brokenOp.operatorIdentifier)
  }

  it should "rethrow the first failure when no errorList is provided" in {
    val brokenOp = csvWithMissingFile()
    val plan = LogicalPlan(List(brokenOp), List.empty)
    intercept[Throwable] {
      plan.resolveScanSourceOpFileName(None)
    }
  }

  it should "leave non-ScanSourceOpDesc operators untouched (no errors, no resolution)" in {
    // KeywordSearch is not a ScanSourceOpDesc — the `case _` branch
    // skips it entirely. An errorList provided here must remain empty.
    val k = keyword()
    val plan = LogicalPlan(List(k), List.empty)
    val errors = ArrayBuffer.empty[(OperatorIdentity, Throwable)]
    plan.resolveScanSourceOpFileName(Some(errors))
    assert(errors.isEmpty)
  }
}
