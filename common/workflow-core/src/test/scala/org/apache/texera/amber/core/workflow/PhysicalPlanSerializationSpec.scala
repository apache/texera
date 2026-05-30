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

package org.apache.texera.amber.core.workflow

import org.apache.texera.amber.core.executor.{
  OpExecInitInfo,
  OpExecSource,
  OpExecWithClassName,
  OpExecWithCode
}
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Proves that a compiled [[PhysicalPlan]] can be serialized to JSON and deserialized
  * back into an equivalent, runnable plan. The plan is assembled the way operator
  * descriptors assemble it: via the [[PhysicalOp]] factory methods + `withInputPorts` /
  * `withOutputPorts` / `withDerivePartition` / `PhysicalPlan.addLink`, exercising every
  * field that used to be `@JsonIgnore`d because it held a function closure.
  */
class PhysicalPlanSerializationSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(42L)
  private val executionId = ExecutionIdentity(7L)

  private def opId(name: String): PhysicalOpIdentity =
    PhysicalOpIdentity(OperatorIdentity(name), "main")

  private val scanSchema: Schema =
    Schema(List(new Attribute("id", AttributeType.INTEGER), new Attribute("name", AttributeType.STRING)))
  private val projectedSchema: Schema =
    Schema(List(new Attribute("name", AttributeType.STRING)))
  private val aggSchema: Schema =
    Schema(List(new Attribute("name", AttributeType.STRING), new Attribute("cnt", AttributeType.LONG)))

  /**
    * Builds a realistic 4-operator plan: scan (source, className, PreferController) ->
    * projection (one-to-one, ProjectionPartition) -> aggregate (hash, ToHash) ->
    * sink (many-to-one, code, ToSingle), with output schemas attached on every port so
    * the per-port schema round-trip is exercised.
    */
  private def buildPlan(): PhysicalPlan = {
    val scanOp = PhysicalOp
      .sourcePhysicalOp(
        opId("scan"),
        workflowId,
        executionId,
        OpExecWithClassName("org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpExec")
      )
      .withOutputPorts(List(OutputPort(PortIdentity(0))))
    // sources have no input to propagate from, so set the output schema directly
    val scanWithSchema = setOutputSchema(scanOp, PortIdentity(0), scanSchema)

    val projectionOp = PhysicalOp
      .oneToOnePhysicalOp(
        opId("projection"),
        workflowId,
        executionId,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.projection.ProjectionOpExec",
          """{"attributes":[],"isDrop":false}"""
        )
      )
      .withInputPorts(List(InputPort(PortIdentity(0))))
      .withOutputPorts(List(OutputPort(PortIdentity(0))))
      .withDerivePartition(ProjectionPartition())
      .withPropagateSchema(SchemaPropagationFunc(_ => Map(PortIdentity(0) -> projectedSchema)))

    val aggregateOp = PhysicalOp
      .oneToOnePhysicalOp(
        opId("aggregate"),
        workflowId,
        executionId,
        OpExecWithClassName("org.apache.texera.amber.operator.aggregate.AggregateOpExec", "{}")
      )
      .withInputPorts(List(InputPort(PortIdentity(0))))
      .withOutputPorts(List(OutputPort(PortIdentity(0))))
      .withPartitionRequirement(List(Option(HashPartition(List("name")))))
      .withDerivePartition(ToHash(List("name")))
      .withParallelizable(false)
      .withPropagateSchema(SchemaPropagationFunc(_ => Map(PortIdentity(0) -> aggSchema)))

    val sinkOp = PhysicalOp
      .manyToOnePhysicalOp(
        opId("sink"),
        workflowId,
        executionId,
        OpExecWithCode("def process(): pass", "python")
      )
      .withInputPorts(List(InputPort(PortIdentity(0))))
      .withOutputPorts(List(OutputPort(PortIdentity(0))))
      .withPropagateSchema(SchemaPropagationFunc(_ => Map(PortIdentity(0) -> aggSchema)))

    var plan = PhysicalPlan(
      operators = Set(scanWithSchema, projectionOp, aggregateOp, sinkOp),
      links = Set.empty
    )
    plan = plan.addLink(PhysicalLink(opId("scan"), PortIdentity(0), opId("projection"), PortIdentity(0)))
    plan = plan.addLink(
      PhysicalLink(opId("projection"), PortIdentity(0), opId("aggregate"), PortIdentity(0))
    )
    plan = plan.addLink(PhysicalLink(opId("aggregate"), PortIdentity(0), opId("sink"), PortIdentity(0)))
    plan
  }

  /** Helper: force a Right schema onto a specific output port. */
  private def setOutputSchema(op: PhysicalOp, portId: PortIdentity, schema: Schema): PhysicalOp = {
    val (port, links, _) = op.outputPorts(portId)
    op.copy(outputPorts = op.outputPorts + (portId -> ((port, links, Right(schema)))))
  }

  private def roundTrip(plan: PhysicalPlan): PhysicalPlan = {
    val json = objectMapper.writeValueAsString(plan)
    objectMapper.readValue(json, classOf[PhysicalPlan])
  }

  // ---------------------------------------------------------------------------
  // Structural equivalence
  // ---------------------------------------------------------------------------

  "PhysicalPlan JSON round-trip" should "preserve the operator id set" in {
    val plan = buildPlan()
    val back = roundTrip(plan)
    back.operators.map(_.id) shouldBe plan.operators.map(_.id)
  }

  it should "preserve the link set" in {
    val plan = buildPlan()
    val back = roundTrip(plan)
    back.links shouldBe plan.links
  }

  it should "rehydrate per-port input/output link lists from the plan's links" in {
    val plan = buildPlan()
    val back = roundTrip(plan)
    // every op's input/output link lists must match the original op's lists
    plan.operators.foreach { orig =>
      val restored = back.getOperator(orig.id)
      restored.getInputLinks() should contain theSameElementsAs orig.getInputLinks()
      orig.outputPorts.keys.foreach { portId =>
        restored.getOutputLinks(portId) should contain theSameElementsAs orig.getOutputLinks(portId)
      }
    }
    // sanity: the projection op really does have 1 incoming + 1 outgoing link
    val proj = back.getOperator(opId("projection"))
    proj.getInputLinks() should have size 1
    proj.getOutputLinks(PortIdentity(0)) should have size 1
  }

  // ---------------------------------------------------------------------------
  // Runtime-critical, per-operator equivalence
  // ---------------------------------------------------------------------------

  it should "preserve opExecInitInfo (className/descString) on the scan operator" in {
    val plan = buildPlan()
    val back = roundTrip(plan)
    back.getOperator(opId("scan")).opExecInitInfo shouldBe a[OpExecWithClassName]
    back.getOperator(opId("scan")).opExecInitInfo shouldBe
      plan.getOperator(opId("scan")).opExecInitInfo
  }

  it should "preserve opExecInitInfo (code/language) on the sink operator" in {
    val plan = buildPlan()
    val back = roundTrip(plan)
    val info = back.getOperator(opId("sink")).opExecInitInfo
    info shouldBe a[OpExecWithCode]
    info shouldBe plan.getOperator(opId("sink")).opExecInitInfo
    info.asInstanceOf[OpExecWithCode].language shouldBe "python"
  }

  it should "round-trip an OpExecSource (with workflowIdentity) standalone" in {
    val source: OpExecInitInfo = OpExecSource("storage-key-1", workflowId)
    val json = objectMapper.writeValueAsString(source)
    val back = objectMapper.readValue(json, classOf[OpExecInitInfo])
    back shouldBe source
    back.asInstanceOf[OpExecSource].workflowIdentity shouldBe workflowId
  }

  it should "preserve partitionRequirement on every operator" in {
    val plan = buildPlan()
    val back = roundTrip(plan)
    plan.operators.foreach { orig =>
      back.getOperator(orig.id).partitionRequirement shouldBe orig.partitionRequirement
    }
    // the aggregate's hash requirement survives with its attribute names
    back.getOperator(opId("aggregate")).partitionRequirement shouldBe
      List(Option(HashPartition(List("name"))))
  }

  it should "preserve partitionDeriveSpec and reproduce the same derivePartition output" in {
    val plan = buildPlan()
    val back = roundTrip(plan)

    val sampleInputs = List(HashPartition(List("name")), UnknownPartition())

    plan.operators.foreach { orig =>
      val restored = back.getOperator(orig.id)
      restored.partitionDeriveSpec shouldBe orig.partitionDeriveSpec
      restored.derivePartition(sampleInputs) shouldBe orig.derivePartition(sampleInputs)
    }

    // spot-check the concrete spec types and outputs
    back.getOperator(opId("scan")).partitionDeriveSpec shouldBe Passthrough()
    back.getOperator(opId("projection")).partitionDeriveSpec shouldBe ProjectionPartition()
    back.getOperator(opId("aggregate")).partitionDeriveSpec shouldBe ToHash(List("name"))
    back.getOperator(opId("sink")).partitionDeriveSpec shouldBe ToSingle()

    back.getOperator(opId("aggregate")).derivePartition(sampleInputs) shouldBe
      HashPartition(List("name"))
    back.getOperator(opId("sink")).derivePartition(sampleInputs) shouldBe SinglePartition()
    // projection passes a hash partition (with attrs) through unchanged
    back.getOperator(opId("projection")).derivePartition(sampleInputs) shouldBe
      HashPartition(List("name"))
    // projection collapses an attribute-less hash partition to unknown
    back
      .getOperator(opId("projection"))
      .derivePartition(List(HashPartition(List.empty))) shouldBe UnknownPartition()
  }

  it should "preserve locationPreference (PreferController) and its singleton identity" in {
    val plan = buildPlan()
    val back = roundTrip(plan)
    val pref = back.getOperator(opId("scan")).locationPreference
    pref shouldBe Some(PreferController)
    // singleton identity is preserved by the custom deserializer
    assert(pref.get eq PreferController)
  }

  it should "preserve absent locationPreference on non-source operators" in {
    val plan = buildPlan()
    val back = roundTrip(plan)
    back.getOperator(opId("aggregate")).locationPreference shouldBe None
  }

  it should "preserve per-port output schemas on every operator" in {
    val plan = buildPlan()
    val back = roundTrip(plan)
    plan.operators.foreach { orig =>
      orig.outputPorts.foreach {
        case (portId, (_, _, schemaEither)) =>
          val restoredSchema = back.getOperator(orig.id).outputPorts(portId)._3
          restoredSchema.toOption shouldBe schemaEither.toOption
      }
    }
    // concrete schema check
    back.getOperator(opId("scan")).outputPorts(PortIdentity(0))._3.toOption shouldBe Some(scanSchema)
    back.getOperator(opId("aggregate")).outputPorts(PortIdentity(0))._3.toOption shouldBe
      Some(aggSchema)
  }

  it should "preserve input-port schemas propagated across links" in {
    val plan = buildPlan()
    val back = roundTrip(plan)
    // the projection received the scan's output schema on its input port
    plan.getOperator(opId("projection")).inputPorts(PortIdentity(0))._3.toOption shouldBe
      Some(scanSchema)
    back.getOperator(opId("projection")).inputPorts(PortIdentity(0))._3.toOption shouldBe
      Some(scanSchema)
  }

  it should "preserve parallelizable, isOneToManyOp, suggestedWorkerNum, and pveName" in {
    val plan = buildPlan().setOperator(
      buildPlan()
        .getOperator(opId("projection"))
        .withSuggestedWorkerNum(5)
        .withPveName("my-pve")
        .withIsOneToManyOp(true)
    )
    val back = roundTrip(plan)
    val proj = back.getOperator(opId("projection"))
    proj.suggestedWorkerNum shouldBe Some(5)
    proj.pveName shouldBe "my-pve"
    proj.isOneToManyOp shouldBe true
    back.getOperator(opId("aggregate")).parallelizable shouldBe false
    back.getOperator(opId("scan")).parallelizable shouldBe false
  }

  it should "preserve port metadata (displayName, dependencies, blocking, mode)" in {
    val inPort = InputPort(PortIdentity(1), displayName = "in1", dependencies = List(PortIdentity(0)))
    val outPort = OutputPort(PortIdentity(0), displayName = "out0", blocking = true)
    val op = PhysicalOp
      .oneToOnePhysicalOp(opId("x"), workflowId, executionId, OpExecWithClassName("X"))
      .withInputPorts(List(InputPort(PortIdentity(0)), inPort))
      .withOutputPorts(List(outPort))
    val plan = PhysicalPlan(Set(op), Set.empty)
    val back = roundTrip(plan)
    val restored = back.getOperator(opId("x"))
    restored.inputPorts(PortIdentity(1))._1 shouldBe inPort
    restored.outputPorts(PortIdentity(0))._1 shouldBe outPort
  }

  // ---------------------------------------------------------------------------
  // Negative test
  // ---------------------------------------------------------------------------

  "PhysicalPlan deserialization" should "fail on malformed JSON" in {
    assertThrows[Exception] {
      objectMapper.readValue("{ this is not valid json ", classOf[PhysicalPlan])
    }
  }

  it should "fail when a required structural field is missing/garbled" in {
    // operators present but with a bogus shape for an operator entry
    val badJson = """{"operators":[{"id":"not-an-object"}],"links":[]}"""
    assertThrows[Exception] {
      objectMapper.readValue(badJson, classOf[PhysicalPlan])
    }
  }

  "OpExecInitInfo deserialization" should "fail on an unknown kind" in {
    val badJson = """{"kind":"bogus"}"""
    val ex = intercept[Exception] {
      objectMapper.readValue(badJson, classOf[OpExecInitInfo])
    }
    // unwrap to confirm it is our explicit rejection, not an unrelated failure
    val msg = Option(ex.getCause).map(_.getMessage).getOrElse(ex.getMessage)
    msg should include("Unknown OpExecInitInfo kind")
  }
}
