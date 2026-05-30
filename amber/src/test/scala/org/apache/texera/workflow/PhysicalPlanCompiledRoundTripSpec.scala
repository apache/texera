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
import org.apache.texera.amber.core.workflow.{PartitionInfo, PhysicalPlan, PortIdentity, UnknownPartition, WorkflowContext}
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.operator.aggregate.AggregationFunction
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.web.model.websocket.request.LogicalPlanPojo
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * End-to-end proof that a `PhysicalPlan` produced by the real [[WorkflowCompiler]] (the
  * exact code path the engine uses) survives a JSON round-trip and comes back structurally
  * and runtime-equivalent. This complements the workflow-core `PhysicalPlanSerializationSpec`
  * (which builds a plan by hand) by exercising a compiler-generated plan, including the
  * multi-physical-op expansion of an Aggregate and its hash partitioning.
  */
class PhysicalPlanCompiledRoundTripSpec extends AnyFlatSpec with Matchers {

  private def pojo(
      operators: List[org.apache.texera.amber.operator.LogicalOp],
      links: List[LogicalLink]
  ): LogicalPlanPojo =
    LogicalPlanPojo(operators, links, List.empty, List.empty)

  /** Compiles CSV scan -> group-by aggregate into a physical plan. */
  private def compiledPlan(): PhysicalPlan = {
    val csv = TestOperators.smallCsvScanOpDesc()
    val agg = TestOperators.aggregateAndGroupByDesc(
      "Units Sold",
      AggregationFunction.SUM,
      List("Country")
    )
    val ctx = new WorkflowContext()
    val workflow = new WorkflowCompiler(ctx).compile(
      pojo(
        List(csv, agg),
        List(
          LogicalLink(csv.operatorIdentifier, PortIdentity(), agg.operatorIdentifier, PortIdentity())
        )
      )
    )
    workflow.physicalPlan
  }

  private def roundTrip(plan: PhysicalPlan): PhysicalPlan = {
    val json = objectMapper.writeValueAsString(plan)
    objectMapper.readValue(json, classOf[PhysicalPlan])
  }

  "A compiler-produced PhysicalPlan" should "round-trip its operator id set and links" in {
    val plan = compiledPlan()
    val back = roundTrip(plan)
    back.operators.map(_.id) shouldBe plan.operators.map(_.id)
    back.links shouldBe plan.links
  }

  it should "round-trip every operator's opExecInitInfo, partition spec, requirement, and location" in {
    val plan = compiledPlan()
    val back = roundTrip(plan)

    val sampleInputs = List(UnknownPartition(): PartitionInfo)

    plan.operators.foreach { orig =>
      val restored = back.getOperator(orig.id)

      // opExecInitInfo: the runtime-critical executor descriptor must be identical, and a
      // className/code value must survive as the same concrete subtype the engine builds from.
      restored.opExecInitInfo shouldBe orig.opExecInitInfo
      orig.opExecInitInfo match {
        case _: OpExecWithClassName => restored.opExecInitInfo shouldBe a[OpExecWithClassName]
        case _: OpExecWithCode      => restored.opExecInitInfo shouldBe a[OpExecWithCode]
        case _                      => // OpExecSource / Empty: equality assertion above suffices
      }

      restored.partitionRequirement shouldBe orig.partitionRequirement
      restored.partitionDeriveSpec shouldBe orig.partitionDeriveSpec
      restored.locationPreference shouldBe orig.locationPreference

      // the rebuilt derivePartition function reproduces the original output
      val padded = orig.inputPorts.keys.toList.indices.map(_ => UnknownPartition(): PartitionInfo).toList
      val inputs = if (padded.isEmpty) sampleInputs else padded
      restored.derivePartition(inputs) shouldBe orig.derivePartition(inputs)
    }
  }

  it should "round-trip per-port output schemas and rehydrate per-port links" in {
    val plan = compiledPlan()
    val back = roundTrip(plan)

    plan.operators.foreach { orig =>
      val restored = back.getOperator(orig.id)

      // per-port output schemas
      orig.outputPorts.foreach {
        case (portId, (_, _, schemaEither)) =>
          restored.outputPorts(portId)._3.toOption shouldBe schemaEither.toOption
      }

      // per-port input/output link lists rehydrated from plan.links
      restored.getInputLinks() should contain theSameElementsAs orig.getInputLinks()
      orig.outputPorts.keys.foreach { portId =>
        restored.getOutputLinks(portId) should contain theSameElementsAs orig.getOutputLinks(portId)
      }
    }
  }

  it should "produce an opExecInitInfo that still pattern-matches as a runnable descriptor" in {
    val plan = compiledPlan()
    val back = roundTrip(plan)
    // Mirror the SerializationManager match: every op's restored opExecInitInfo must be a
    // kind the engine knows how to instantiate an executor from.
    back.operators.foreach { op =>
      val runnable = op.opExecInitInfo match {
        case OpExecWithClassName(className, _) => className.nonEmpty
        case OpExecWithCode(code, _)           => code.nonEmpty
        case other                             => fail(s"unexpected opExecInitInfo after round-trip: $other")
      }
      assert(runnable, s"op ${op.id} lost its executor descriptor")
    }
  }

  "A compiler-produced PhysicalPlan round-trip" should "reject malformed JSON" in {
    assertThrows[Exception] {
      objectMapper.readValue("{ not valid ", classOf[PhysicalPlan])
    }
  }
}
