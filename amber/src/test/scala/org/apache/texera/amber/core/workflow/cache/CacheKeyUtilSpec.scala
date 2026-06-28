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

package org.apache.texera.amber.core.workflow.cache

import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.{
  GlobalPortIdentity,
  PhysicalPlan,
  PortIdentity,
  WorkflowContext
}
import org.apache.texera.amber.engine.e2e.TestUtils.buildWorkflow
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.operator.aggregate.{AggregateOpDesc, AggregationFunction}
import org.apache.texera.amber.operator.keywordSearch.KeywordSearchOpDesc
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.apache.texera.workflow.LogicalLink
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CacheKeyUtilSpec extends AnyFlatSpec with Matchers {

  private def newCsv(): CSVScanSourceOpDesc =
    TestOperators.headerlessSmallCsvScanOpDesc()

  private def newKeyword(
      pattern: String = "Asia",
      logicalId: Option[String] = None
  ): KeywordSearchOpDesc = {
    val op = TestOperators.keywordSearchOpDesc("column-1", pattern)
    logicalId.foreach(id => op.setOperatorId(id))
    op
  }

  private def newGroupBy(): AggregateOpDesc =
    TestOperators.aggregateAndGroupByDesc("column-1", AggregationFunction.COUNT, List[String]())

  private def buildSimpleWorkflow(csv: CSVScanSourceOpDesc, keyword: KeywordSearchOpDesc) =
    buildWorkflow(
      List(csv, keyword),
      List(
        LogicalLink(
          csv.operatorIdentifier,
          PortIdentity(0),
          keyword.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      new WorkflowContext()
    )

  /** The first external output port of the (single) physical op for a logical operator. */
  private def outputPort(plan: PhysicalPlan, opId: OperatorIdentity): GlobalPortIdentity =
    GlobalPortIdentity(
      plan.getPhysicalOpsOfLogicalOp(opId).head.id,
      PortIdentity(0, internal = false)
    )

  "CacheKeyUtil" should "produce a stable cache key for identical workflows" in {
    val csv = newCsv()
    val keyword = newKeyword()
    val plan = buildSimpleWorkflow(csv, keyword).physicalPlan
    val target = outputPort(plan, keyword.operatorIdentifier)
    val k1 = CacheKeyUtil.computeCacheKey(plan, target)
    val k2 = CacheKeyUtil.computeCacheKey(plan, target)
    k1.hash shouldEqual k2.hash
    k1.json shouldEqual k2.json
    k1.hash should have length 64
  }

  it should "change the cache key when operator configuration changes" in {
    val csv = newCsv()
    val keyword = newKeyword()
    val plan = buildSimpleWorkflow(csv, keyword).physicalPlan
    val target = outputPort(plan, keyword.operatorIdentifier)
    val modifiedKeyword = newKeyword("Europe", logicalId = Some(keyword.operatorIdentifier.id))
    val modifiedPlan = buildWorkflow(
      List(csv, modifiedKeyword),
      List(
        LogicalLink(
          csv.operatorIdentifier,
          PortIdentity(0),
          modifiedKeyword.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      new WorkflowContext()
    ).physicalPlan
    val k1 = CacheKeyUtil.computeCacheKey(plan, target)
    val k2 = CacheKeyUtil.computeCacheKey(modifiedPlan, target)
    k1.hash should not equal k2.hash
  }

  it should "produce different cache keys for different ports (wiring)" in {
    val csv = newCsv()
    val keyword = newKeyword()
    val groupBy = newGroupBy()
    val plan = buildWorkflow(
      List(csv, keyword, groupBy),
      List(
        LogicalLink(
          csv.operatorIdentifier,
          PortIdentity(0),
          keyword.operatorIdentifier,
          PortIdentity(0)
        ),
        LogicalLink(
          keyword.operatorIdentifier,
          PortIdentity(0),
          groupBy.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      new WorkflowContext()
    ).physicalPlan
    val kKeyword = CacheKeyUtil.computeCacheKey(plan, outputPort(plan, keyword.operatorIdentifier))
    val kGroupBy = CacheKeyUtil.computeCacheKey(plan, outputPort(plan, groupBy.operatorIdentifier))
    kKeyword.hash should not equal kGroupBy.hash
  }

  it should "produce a stable cache key for a source operator with no upstream" in {
    val csv = newCsv()
    val keyword = newKeyword()
    val plan = buildSimpleWorkflow(csv, keyword).physicalPlan
    val sourceTarget = outputPort(plan, csv.operatorIdentifier)
    val k1 = CacheKeyUtil.computeCacheKey(plan, sourceTarget)
    val k2 = CacheKeyUtil.computeCacheKey(plan, sourceTarget)
    k1.hash shouldEqual k2.hash
    k1.hash should not equal
      CacheKeyUtil.computeCacheKey(plan, outputPort(plan, keyword.operatorIdentifier)).hash
  }

  it should "include only upstream operators in the cache key, not downstream ones" in {
    val csv = newCsv()
    val keyword = newKeyword()
    val groupBy = newGroupBy()
    val plan = buildWorkflow(
      List(csv, keyword, groupBy),
      List(
        LogicalLink(
          csv.operatorIdentifier,
          PortIdentity(0),
          keyword.operatorIdentifier,
          PortIdentity(0)
        ),
        LogicalLink(
          keyword.operatorIdentifier,
          PortIdentity(0),
          groupBy.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      new WorkflowContext()
    ).physicalPlan
    val csvOpId = plan.getPhysicalOpsOfLogicalOp(csv.operatorIdentifier).head.id.toString
    val groupByOpId = plan.getPhysicalOpsOfLogicalOp(groupBy.operatorIdentifier).head.id.toString
    val key = CacheKeyUtil.computeCacheKey(plan, outputPort(plan, keyword.operatorIdentifier))
    key.json should include(csvOpId)
    key.json should not include groupByOpId
  }

  "CacheKeyUtil.sameComputation" should "treat two keys with the same hash and JSON as a match" in {
    val csv = newCsv()
    val keyword = newKeyword()
    val plan = buildSimpleWorkflow(csv, keyword).physicalPlan
    val target = outputPort(plan, keyword.operatorIdentifier)
    CacheKeyUtil.sameComputation(
      CacheKeyUtil.computeCacheKey(plan, target),
      CacheKeyUtil.computeCacheKey(plan, target)
    ) shouldBe true
  }

  it should "reject a hash collision by comparing the full JSON" in {
    // Two different computations that hash to the same value (fabricated, since a
    // real SHA-256 collision is infeasible to construct): the JSON differs, so the
    // match is rejected and a cached result is never reused for the wrong port.
    CacheKeyUtil.sameComputation(
      CacheKey("upstream-A", "same-hash"),
      CacheKey("upstream-B", "same-hash")
    ) shouldBe false
  }

  it should "reject keys with different hashes" in {
    CacheKeyUtil.sameComputation(
      CacheKey("upstream-A", "hash-1"),
      CacheKey("upstream-A", "hash-2")
    ) shouldBe false
  }
}
