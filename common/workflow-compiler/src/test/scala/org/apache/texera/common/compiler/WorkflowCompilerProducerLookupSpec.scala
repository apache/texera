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

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.storage.ScanFileResolver
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.aggregate.{
  AggregateOpDesc,
  AggregationFunction,
  AggregationOperation
}
import org.apache.texera.amber.operator.hashJoin.HashJoinOpDesc
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.source.scan.text.TextInputSourceOpDesc
import org.apache.texera.common.compiler.model.{LogicalLink, LogicalPlanPojo}
import org.scalatest.flatspec.AnyFlatSpec

import java.net.URI

/**
  * How the compiler picks the physical op at the upstream end of each logical link: the op of
  * the upstream logical op that produces the link's output port.
  */
class WorkflowCompilerProducerLookupSpec extends AnyFlatSpec {
  private val p0 = PortIdentity(0)
  private val p1 = PortIdentity(1)
  private val i0 = PortIdentity(0, internal = true)
  private val valueSchema = Schema().add("value", AttributeType.INTEGER)
  private val noFiles = new ScanFileResolver {
    override def resolve(name: String): URI =
      throw new AssertionError("test cannot read files: " + name)
  }
  private val modes = Seq(CompilationErrorHandling.Lenient, CompilationErrorHandling.Strict)

  private case class Stage(name: String, inputs: List[PortIdentity], outputs: List[PortIdentity])
  private case class Edge(
      fromStage: String,
      fromPort: PortIdentity,
      toStage: String,
      toPort: PortIdentity
  )

  /** A logical op whose physical plan is the given stages, each emitting `valueSchema`. */
  private class FixtureOp(label: String, stages: List[Stage], edges: List[Edge] = Nil)
      extends LogicalOp {
    setOperatorId(label)
    override def getPhysicalPlan(wid: WorkflowIdentity, eid: ExecutionIdentity): PhysicalPlan = {
      def id(stage: String) = PhysicalOpIdentity(operatorIdentifier, stage)
      val ops = stages.map { stage =>
        PhysicalOp(id(stage.name), wid, eid, OpExecWithClassName("fixture.NeverRun", "{}"))
          .withInputPorts(stage.inputs.map(p => InputPort(p)))
          .withOutputPorts(stage.outputs.map(p => OutputPort(p)))
          .withPropagateSchema(
            SchemaPropagationFunc(_ => stage.outputs.map(_ -> valueSchema).toMap)
          )
      }
      val links = edges.map(e => PhysicalLink(id(e.fromStage), e.fromPort, id(e.toStage), e.toPort))
      PhysicalPlan(ops.toSet, links.toSet)
    }
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        label,
        "test fixture",
        OperatorGroupConstants.UTILITY_GROUP,
        stages.flatMap(_.inputs).filterNot(_.internal).distinct.map(p => InputPort(p)),
        stages.flatMap(_.outputs).filterNot(_.internal).distinct.map(p => OutputPort(p))
      )
  }

  private def op(id: String, inputs: List[PortIdentity] = List(p0)): FixtureOp =
    new FixtureOp(id, List(Stage("main", inputs, List(p0))))

  private def link(
      from: LogicalOp,
      to: LogicalOp,
      fromPort: PortIdentity = p0,
      toPort: PortIdentity = p0
  ): LogicalLink =
    LogicalLink(from.operatorIdentifier, fromPort, to.operatorIdentifier, toPort)

  private def compile(
      ops: List[LogicalOp],
      links: List[LogicalLink],
      mode: CompilationErrorHandling,
      view: List[String] = Nil
  ): WorkflowCompilationResult =
    new WorkflowCompiler(new WorkflowContext(workflowId = WorkflowIdentity(0)), noFiles)
      .compile(LogicalPlanPojo(ops, links, view, Nil), mode)

  private def textInput(id: String): TextInputSourceOpDesc = {
    val op = new TextInputSourceOpDesc
    op.setOperatorId(id)
    op.textInput = "a\nb"
    op.attributeName = "line"
    op
  }

  private def limit(id: String): LimitOpDesc = {
    val op = new LimitOpDesc
    op.setOperatorId(id)
    op.limit = 5
    op
  }

  "WorkflowCompiler" should "link from the stage of a multi-stage operator that produces the port" in {
    val text = textInput("text")
    val aggregate = new AggregateOpDesc
    aggregate.setOperatorId("aggregate")
    val count = new AggregationOperation
    count.aggFunction = AggregationFunction.COUNT
    count.attribute = "line"
    count.resultAttribute = "count"
    aggregate.aggregations = List(count)
    val afterAggregate = limit("after-aggregate")

    val right = textInput("right")
    val join = new HashJoinOpDesc[String]
    join.setOperatorId("join")
    join.buildAttributeName = "line"
    join.probeAttributeName = "line"
    val afterJoin = limit("after-join")

    val ops = List(text, aggregate, afterAggregate, right, join, afterJoin)
    val links = List(
      link(text, aggregate),
      link(aggregate, afterAggregate),
      link(text, join),
      link(right, join, toPort = p1),
      link(join, afterJoin)
    )
    val localAgg = PhysicalOpIdentity(aggregate.operatorIdentifier, "localAgg")
    val globalAgg = PhysicalOpIdentity(aggregate.operatorIdentifier, "globalAgg")
    val build = PhysicalOpIdentity(join.operatorIdentifier, "build")
    val probe = PhysicalOpIdentity(join.operatorIdentifier, "probe")
    modes.foreach { mode =>
      val result = compile(ops, links, mode, view = List("aggregate"))
      val plan = result.physicalPlan.get
      def sourcesOfLinksInto(op: LogicalOp): Set[(PhysicalOpIdentity, PortIdentity)] =
        plan.links
          .filter(_.toOpId.logicalOpId == op.operatorIdentifier)
          .map(l => (l.fromOpId, l.fromPortId))
      // The first stage of each writes to its internal port 0; the operator's output is port 0.
      assert(sourcesOfLinksInto(aggregate).contains((localAgg, i0)))
      assert(sourcesOfLinksInto(join).contains((build, i0)))
      assert(sourcesOfLinksInto(afterAggregate) == Set((globalAgg, p0)))
      assert(sourcesOfLinksInto(afterJoin) == Set((probe, p0)))
      assert(
        result.outputPortsNeedingStorage.filter(
          _.opId.logicalOpId == aggregate.operatorIdentifier
        ) ==
          Set(GlobalPortIdentity(globalAgg, p0))
      )
    }
  }

  it should "keep a stage that was added before a later internal link of its operator failed" in {
    val partial = new FixtureOp(
      "partial",
      List(Stage("first", Nil, List(p0)), Stage("second", List(i0), List(p1))),
      List(Edge("first", PortIdentity(9), "second", i0))
    )
    val target = op("target")
    val terminal = op("terminal")
    val ops = List(partial, target, terminal)
    val links = List(link(partial, target), link(target, terminal))

    val result = compile(ops, links, CompilationErrorHandling.Lenient)
    assert(result.physicalPlan.isEmpty)
    assert(result.operatorIdToError(partial.operatorIdentifier).message.contains("key not found"))
    assert(result.operatorIdToOutputSchemas(target.operatorIdentifier)(p0).contains(valueSchema))

    val thrown = intercept[NoSuchElementException] {
      compile(ops, links, CompilationErrorHandling.Strict)
    }
    assert(thrown.getMessage.contains("key not found"))
  }

  it should "add no link from an output port the upstream operator does not have" in {
    val source = op("source", Nil)
    val target = op("target")
    val terminal = op("terminal")
    val ops = List(source, target, terminal)
    val links = List(link(source, target, fromPort = PortIdentity(7)), link(target, terminal))

    val result = compile(ops, links, CompilationErrorHandling.Lenient)
    assert(result.physicalPlan.isEmpty)
    assert(result.operatorIdToOutputSchemas(target.operatorIdentifier)(p0).isEmpty)
    assert(
      result
        .operatorIdToError(target.operatorIdentifier)
        .message
        .endsWith("schema is not available")
    )

    val thrown = intercept[SchemaNotAvailableException] {
      compile(ops, links, CompilationErrorHandling.Strict)
    }
    assert(thrown.getMessage == "schema is not available")
  }

  it should "keep the order of the links into an input port, repeats included" in {
    val a = op("a", Nil)
    val c = op("c", Nil)
    val target = op("target")
    val ops = List(a, target, c)
    val links = List(link(c, target), link(a, target), link(c, target))
    modes.foreach { mode =>
      val plan = compile(ops, links, mode).physicalPlan.get
      val targetOp = plan.getPhysicalOpsOfLogicalOp(target.operatorIdentifier).head
      assert(targetOp.inputPorts(p0)._2.map(_.fromOpId.logicalOpId.id) == List("c", "a", "c"))
    }
  }
}
