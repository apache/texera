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

import org.apache.texera.amber.core.executor.{OpExecWithClassName, OpExecWithCode}
import org.apache.texera.amber.core.workflow.{
  HashPartition,
  PartitionInfo,
  PhysicalPlan,
  PortIdentity,
  ToHash,
  ToUnknown,
  UnknownPartition,
  WorkflowContext
}
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.operator.aggregate.AggregationFunction
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.amber.compiler.model.{LogicalLink, LogicalPlanPojo}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Thorough round-trip coverage over a VARIETY of real, compiler-produced physical plans —
  * hash join (`ToHash` on the probe key, two inputs, partition requirement), Python UDF
  * (`OpExecWithCode`, `ToUnknown`), a keyword-search filter (one-to-one / `Passthrough`),
  * and a multi-operator chain. For each it asserts full structural + runtime equivalence
  * after a JSON round-trip, and that a second round-trip is a fixed point (stable).
  *
  * This complements the single-workflow `PhysicalPlanCompiledRoundTripSpec` and the
  * hand-built `PhysicalPlanSerializationSpec`.
  */
class PhysicalPlanRoundTripThoroughSpec extends AnyFlatSpec with Matchers {

  private def pojo(
      operators: List[org.apache.texera.amber.operator.LogicalOp],
      links: List[LogicalLink]
  ): LogicalPlanPojo = LogicalPlanPojo(operators, links, List.empty, List.empty)

  private def compile(
      operators: List[org.apache.texera.amber.operator.LogicalOp],
      links: List[LogicalLink]
  ): PhysicalPlan =
    new WorkflowCompiler(new WorkflowContext()).compile(pojo(operators, links)).physicalPlan

  private def roundTrip(plan: PhysicalPlan): PhysicalPlan =
    objectMapper.readValue(objectMapper.writeValueAsString(plan), classOf[PhysicalPlan])

  /** Deep equivalence of every serialized + runtime-critical field, plus rehydrated links. */
  private def assertEquivalent(orig: PhysicalPlan, back: PhysicalPlan): Unit = {
    back.operators.map(_.id) shouldBe orig.operators.map(_.id)
    back.links shouldBe orig.links

    orig.operators.foreach { o =>
      val r = back.getOperator(o.id)
      withClue(s"operator ${o.id}: ") {
        r.opExecInitInfo shouldBe o.opExecInitInfo
        r.partitionDeriveSpec shouldBe o.partitionDeriveSpec
        r.partitionRequirement shouldBe o.partitionRequirement
        r.locationPreference shouldBe o.locationPreference
        r.parallelizable shouldBe o.parallelizable
        r.isOneToManyOp shouldBe o.isOneToManyOp
        r.suggestedWorkerNum shouldBe o.suggestedWorkerNum
        r.pveName shouldBe o.pveName

        o.outputPorts.foreach {
          case (pid, (_, _, schema)) =>
            r.outputPorts(pid)._3.toOption shouldBe schema.toOption
        }
        o.inputPorts.foreach {
          case (pid, (_, _, schema)) =>
            r.inputPorts(pid)._3.toOption shouldBe schema.toOption
        }

        // the reconstructed derivePartition reproduces the original output exactly
        val inputs =
          o.inputPorts.keys.toList.indices.map(_ => UnknownPartition(): PartitionInfo).toList
        val sample = if (inputs.isEmpty) List(UnknownPartition(): PartitionInfo) else inputs
        r.derivePartition(sample) shouldBe o.derivePartition(sample)

        // per-port links rehydrated from plan.links
        r.getInputLinks() should contain theSameElementsAs o.getInputLinks()
        o.outputPorts.keys.foreach { pid =>
          r.getOutputLinks(pid) should contain theSameElementsAs o.getOutputLinks(pid)
        }
      }
    }
  }

  /** A second round-trip must be a fixed point. */
  private def assertStable(plan: PhysicalPlan): Unit = {
    val once = roundTrip(plan)
    assertEquivalent(once, roundTrip(once))
  }

  "A hash-join plan" should "round-trip ToHash(probe key), the partition requirement, and two inputs" in {
    val build = TestOperators.smallCsvScanOpDesc()
    val probe = TestOperators.smallCsvScanOpDesc()
    val join = TestOperators.joinOpDesc("Country", "Country")
    val plan = compile(
      List(build, probe, join),
      List(
        LogicalLink(build.operatorIdentifier, PortIdentity(), join.operatorIdentifier, PortIdentity()),
        LogicalLink(probe.operatorIdentifier, PortIdentity(), join.operatorIdentifier, PortIdentity(1))
      )
    )
    val back = roundTrip(plan)
    assertEquivalent(plan, back)

    // A hash join expands into several physical ops; its hash distribution lives in a
    // ToHash derivePartition and/or a HashPartition requirement on some of them. Assert
    // that hash distribution exists (so this is a real hash-join fixture) — the round-trip
    // of every field on every op is already asserted by assertEquivalent above.
    val joinOps = plan.getPhysicalOpsOfLogicalOp(join.operatorIdentifier)
    joinOps should not be empty
    joinOps.exists(jo =>
      jo.partitionDeriveSpec.isInstanceOf[ToHash] ||
        jo.partitionRequirement.flatten.exists(_.isInstanceOf[HashPartition])
    ) shouldBe true
    assertStable(plan)
  }

  "A Python-UDF plan" should "round-trip OpExecWithCode and ToUnknown" in {
    val csv = TestOperators.smallCsvScanOpDesc()
    val python = TestOperators.pythonOpDesc()
    val plan = compile(
      List(csv, python),
      List(
        LogicalLink(csv.operatorIdentifier, PortIdentity(), python.operatorIdentifier, PortIdentity())
      )
    )
    val back = roundTrip(plan)
    assertEquivalent(plan, back)

    val pyOps = plan.getPhysicalOpsOfLogicalOp(python.operatorIdentifier)
    pyOps should not be empty
    pyOps.foreach { p =>
      p.opExecInitInfo shouldBe a[OpExecWithCode]
      p.partitionDeriveSpec shouldBe ToUnknown()
      val rb = back.getOperator(p.id)
      rb.opExecInitInfo shouldBe a[OpExecWithCode]
      rb.opExecInitInfo shouldBe p.opExecInitInfo
      rb.partitionDeriveSpec shouldBe ToUnknown()
    }
  }

  "A filter (keyword-search) plan" should "round-trip a one-to-one operator" in {
    val csv = TestOperators.smallCsvScanOpDesc()
    val filter = TestOperators.keywordSearchOpDesc("Country", "United States")
    val plan = compile(
      List(csv, filter),
      List(
        LogicalLink(csv.operatorIdentifier, PortIdentity(), filter.operatorIdentifier, PortIdentity())
      )
    )
    assertEquivalent(plan, roundTrip(plan))
    // the scan source keeps its class-name executor descriptor
    plan
      .getPhysicalOpsOfLogicalOp(csv.operatorIdentifier)
      .foreach(_.opExecInitInfo shouldBe a[OpExecWithClassName])
    assertStable(plan)
  }

  "A multi-operator chain (scan -> filter -> group-by aggregate)" should "round-trip end to end" in {
    val csv = TestOperators.smallCsvScanOpDesc()
    val filter = TestOperators.keywordSearchOpDesc("Country", "United States")
    val agg =
      TestOperators.aggregateAndGroupByDesc("Units Sold", AggregationFunction.SUM, List("Country"))
    val plan = compile(
      List(csv, filter, agg),
      List(
        LogicalLink(csv.operatorIdentifier, PortIdentity(), filter.operatorIdentifier, PortIdentity()),
        LogicalLink(filter.operatorIdentifier, PortIdentity(), agg.operatorIdentifier, PortIdentity())
      )
    )
    assertEquivalent(plan, roundTrip(plan))
    assertStable(plan)
  }
}
