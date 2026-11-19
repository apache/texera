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

package org.apache.texera.amber.engine.architecture.scheduling

import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.storage.VFSURIFactory
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{
  CachedResult,
  GlobalPortIdentity,
  InputPort,
  OutputPort,
  PhysicalLink,
  PhysicalOp,
  PhysicalPlan,
  PortIdentity,
  WorkflowContext,
  WorkflowSettings
}
import org.apache.texera.amber.engine.architecture.scheduling.EdgeType._
import org.apache.texera.amber.engine.architecture.scheduling.config.OutputPortConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI
import scala.util.Random

/**
  * [[SearchState]] is pure, so the plans here are built by hand: no compiler, no engine. As in a
  * compiled plan, every operator lists its own links; `SearchState` reads links from the plan,
  * leaving the operators unchanged.
  */
class SearchStateSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(7L)
  private val executionId = ExecutionIdentity(2L)
  // The execution that saved the results.
  private val sourceExecutionId = ExecutionIdentity(1L)

  private def opId(name: String): PhysicalOpIdentity =
    PhysicalOpIdentity(OperatorIdentity(name), "main")

  private def op(name: String, inputs: Int = 1, outputs: Int = 1): PhysicalOp =
    PhysicalOp
      .oneToOnePhysicalOp(opId(name), workflowId, executionId, OpExecInitInfo.Empty)
      .withInputPorts((0 until inputs).map(i => InputPort(PortIdentity(i))).toList)
      .withOutputPorts((0 until outputs).map(i => OutputPort(PortIdentity(i))).toList)

  private def source(name: String, outputs: Int = 1): PhysicalOp = op(name, inputs = 0, outputs)

  private def link(from: String, to: String, fromPort: Int = 0, toPort: Int = 0): PhysicalLink =
    PhysicalLink(opId(from), PortIdentity(fromPort), opId(to), PortIdentity(toPort))

  /** A plan whose operators list their own links, as the compiler builds it. */
  private def planOf(ops: Set[PhysicalOp], links: Set[PhysicalLink]): PhysicalPlan =
    links.foldLeft(PhysicalPlan(ops, Set.empty))(_.addLink(_))

  private def out(name: String, port: Int = 0): GlobalPortIdentity =
    GlobalPortIdentity(opId(name), PortIdentity(port))

  private def in(name: String, port: Int = 0): GlobalPortIdentity =
    GlobalPortIdentity(opId(name), PortIdentity(port), input = true)

  private def baseUri(
      port: GlobalPortIdentity,
      warehouse: Option[String] = None,
      wid: WorkflowIdentity = workflowId
  ): URI = VFSURIFactory.createPortBaseURI(wid, sourceExecutionId, port, warehouse)

  private def entry(
      port: GlobalPortIdentity,
      count: Option[Long] = Some(10L),
      warehouse: Option[String] = None,
      wid: WorkflowIdentity = workflowId
  ): (GlobalPortIdentity, CachedResult) =
    port -> CachedResult(baseUri(port, warehouse, wid), count)

  private def context(
      matchedResults: Map[GlobalPortIdentity, CachedResult],
      requiredOutputs: Set[GlobalPortIdentity],
      warehouse: Option[String] = None
  ): WorkflowContext =
    new WorkflowContext(
      workflowId,
      executionId,
      WorkflowSettings(outputPortsNeedingStorage = requiredOutputs),
      warehouse = warehouse,
      matchedResults = matchedResults
    )

  private def ids(plan: PhysicalPlan): Set[PhysicalOpIdentity] = plan.operators.map(_.id)

  /** a -> b -> c, with c's output the terminal port. */
  private def chain(): PhysicalPlan =
    planOf(Set(source("a"), op("b"), op("c")), Set(link("a", "b"), link("b", "c")))

  /**
    * src -> split; split's port 0 -> x and port 1 -> y, with x and y terminal. Split has one
    * input and two outputs, like the Split operator.
    */
  private def split(): PhysicalPlan =
    planOf(
      Set(source("src"), op("split", outputs = 2), op("x"), op("y")),
      Set(link("src", "split"), link("split", "x", fromPort = 0), link("split", "y", fromPort = 1))
    )

  private case class EagerSetup(
      matched: Map[GlobalPortIdentity, CachedResult],
      required: Set[GlobalPortIdentity],
      state: SearchState
  )

  private def eagerSetup(
      plan: PhysicalPlan,
      ctx: WorkflowContext,
      allowPipelined: Boolean = true
  ): EagerSetup = {
    val matched = ctx.matchedResults
    val required = SearchState.requiredOutputPorts(ctx, plan)
    val state = SearchState.eagerSeed(plan, matched, required, allowPipelined = allowPipelined)
    EagerSetup(matched, required, state)
  }

  /** Independent backward-reachability oracle for resolving input requirements. */
  private def referenceResolveInputRequirements(
      plan: PhysicalPlan,
      requiredOutputs: Set[GlobalPortIdentity],
      matchedResults: Map[GlobalPortIdentity, CachedResult],
      allowPipelined: Boolean,
      defaultActiveType: PhysicalLink => EdgeType,
      state: SearchState
  ): SearchState = {
    def sourcePort(l: PhysicalLink): GlobalPortIdentity =
      GlobalPortIdentity(l.fromOpId, l.fromPortId)

    val allowedLinks = SearchState.allowedLinkTypes(plan, matchedResults, allowPipelined)
    def willPropagateToProducer(l: PhysicalLink): Boolean =
      state(l) == Materialized || state(l) == Pipelined ||
        (state(l) == Unused && !matchedResults.contains(sourcePort(l)))

    var runOps = plan.operators.filter(_.outputPorts.isEmpty).map(_.id) ++
      requiredOutputs.diff(state.reusedOutputs).map(_.opId)
    var changed = true
    while (changed) {
      val next = runOps ++ plan.links.collect {
        case l if runOps.contains(l.toOpId) && willPropagateToProducer(l) => l.fromOpId
      }
      changed = next != runOps
      runOps = next
    }

    val expectedLinks = plan.links.map { l =>
      val edgeType =
        if (!runOps.contains(l.toOpId)) Unused
        else if (state(l) != Unused) state(l)
        else if (matchedResults.contains(sourcePort(l))) CacheRead
        else {
          val candidate = defaultActiveType(l)
          if (
            candidate != Unused && candidate != CacheRead && allowedLinks(l).contains(candidate)
          ) {
            candidate
          } else Materialized
        }
      l -> edgeType
    }.toMap
    state.copy(linkTypes = expectedLinks)
  }

  private def allMaterialized(physicalPlan: PhysicalPlan): SearchState =
    SearchState(physicalPlan.links.map(_ -> Materialized).toMap)

  private def withReusedOutput(
      state: SearchState,
      port: GlobalPortIdentity,
      reused: Boolean
  ): SearchState =
    if (reused) state.copy(reusedOutputs = state.reusedOutputs + port)
    else state.copy(reusedOutputs = state.reusedOutputs - port)

  "SearchState.eagerSeed" should "mark nothing when the cache is empty" in {
    val plan = chain()
    val setup = eagerSetup(plan, context(Map.empty, Set(out("c"))))
    val s = setup.state
    s.skipsAnything(plan, setup.required) shouldBe false
    s.skippedOperators(plan, setup.required) shouldBe empty
    s.linksOfType(Unused) shouldBe empty
    s.linksOfType(CacheRead) shouldBe empty
    s.retainedPlan(plan, setup.required) shouldBe plan
    s.readerUrisByInputPort(plan, setup.matched, setup.required) shouldBe empty
    s.cacheReadPorts(plan, setup.matched, setup.required) shouldBe empty
  }

  it should "skip everything when the terminal port is matched" in {
    val plan = chain()
    val cached = Map(entry(out("c"), count = Some(3L)))
    val setup = eagerSetup(plan, context(cached, Set(out("c"))))
    val s = setup.state
    s.skipsAnything(plan, setup.required) shouldBe true
    s.skippedOperators(plan, setup.required) shouldBe ids(plan)
    s.linksOfType(Unused) shouldBe plan.links
    s.linksOfType(CacheRead) shouldBe empty
    s.retainedPlan(plan, setup.required).operators shouldBe empty
    s.readerUrisByInputPort(plan, setup.matched, setup.required) shouldBe empty
    s.cacheReadPorts(plan, setup.matched, setup.required) shouldBe
      Map(out("c") -> CachedResult(baseUri(out("c")), Some(3L)))
    val regions = s.skipRegions(plan, setup.matched, setup.required, firstRegionId = 5)
    regions.map(_.id.id) shouldBe Set(5L)
    val region = regions.head
    region.skipped shouldBe true
    region.physicalOps.map(_.id) shouldBe ids(plan)
    region.physicalLinks shouldBe plan.links
    region.ports shouldBe Set(out("a"), in("b"), out("b"), in("c"), out("c"))
    // The saved result becomes the port's config: its location and row count.
    region.resourceConfig.get.portConfigs shouldBe Map(
      out("c") -> OutputPortConfig(baseUri(out("c")), Some(3L))
    )
  }

  it should "run only the operators downstream of a match in the middle" in {
    val plan = chain()
    val cached = Map(entry(out("b")))
    val setup = eagerSetup(plan, context(cached, Set(out("c"))))
    val s = setup.state
    // a is skipped too: nothing needs its output once b's output is read from the cache.
    s.skippedOperators(plan, setup.required) shouldBe Set(opId("a"), opId("b"))
    s.linksOfType(Unused) shouldBe Set(link("a", "b"))
    s.linksOfType(CacheRead) shouldBe Set(link("b", "c"))
    ids(s.retainedPlan(plan, setup.required)) shouldBe Set(opId("c"))
    s.retainedPlan(plan, setup.required).links shouldBe empty
    s.readerUrisByInputPort(plan, setup.matched, setup.required) shouldBe Map(
      in("c") -> List(cached(out("b")).storageUri)
    )
    s.cacheReadPorts(plan, setup.matched, setup.required) shouldBe cached
    val region = s.skipRegions(plan, setup.matched, setup.required, 0).head
    region.resourceConfig.get.portConfigs.keySet shouldBe Set(out("b"))
  }

  it should "run an operator whose results the user views when its port has no saved result" in {
    // a -> b -> c, and the user views b, so the run must store b's port, which has no saved
    // result: b runs, and so does a, which b reads. Only c is skipped, on its own saved result.
    val plan = chain()
    val setup = eagerSetup(plan, context(Map(entry(out("c"))), Set(out("b"), out("c"))))
    val s = setup.state
    s.skippedOperators(plan, setup.required) shouldBe Set(opId("c"))
    ids(s.retainedPlan(plan, setup.required)) shouldBe Set(opId("a"), opId("b"))
    s.linksOfType(Unused) shouldBe Set(link("b", "c"))
    s.linksOfType(CacheRead) shouldBe empty
    s.cacheReadPorts(plan, setup.matched, setup.required).keySet shouldBe Set(out("c"))
  }

  it should "read a join's build-side input from the cache and run the rest" in {
    // s1 -> j (port 0, build), s2 -> j (port 1, probe), j's output is terminal.
    val plan = planOf(
      Set(source("s1"), source("s2"), op("j", inputs = 2)),
      Set(link("s1", "j", toPort = 0), link("s2", "j", toPort = 1))
    )
    val cached = Map(entry(out("s1")))
    val setup = eagerSetup(plan, context(cached, Set(out("j"))))
    val s = setup.state
    s.skippedOperators(plan, setup.required) shouldBe Set(opId("s1"))
    s.linksOfType(Unused) shouldBe empty
    s.linksOfType(CacheRead) shouldBe Set(link("s1", "j", toPort = 0))
    ids(s.retainedPlan(plan, setup.required)) shouldBe Set(opId("s2"), opId("j"))
    s.retainedPlan(plan, setup.required).links shouldBe Set(link("s2", "j", toPort = 1))
    s.readerUrisByInputPort(plan, setup.matched, setup.required) shouldBe Map(
      in("j", 0) -> List(cached(out("s1")).storageUri)
    )
    s.cacheReadPorts(plan, setup.matched, setup.required) shouldBe cached
  }

  it should "give each input port the saved location of its own link" in {
    // s1 -> j's port 0 and s2 -> j's port 1, both from port 0 and both saved: each of j's
    // input ports reads the saved result of the link into it.
    val plan = planOf(
      Set(source("s1"), source("s2"), op("j", inputs = 2)),
      Set(link("s1", "j", toPort = 0), link("s2", "j", toPort = 1))
    )
    val cached = Map(entry(out("s1")), entry(out("s2")))
    val setup = eagerSetup(plan, context(cached, Set(out("j"))))
    setup.state.readerUrisByInputPort(plan, setup.matched, setup.required) shouldBe Map(
      in("j", 0) -> List(cached(out("s1")).storageUri),
      in("j", 1) -> List(cached(out("s2")).storageUri)
    )
  }

  it should "list the saved locations of one input port in a fixed order" in {
    // s1, s2 and s3 all feed u's one input port and all have saved results. The locations are
    // sorted by link, so the same plan configures the port the same way whatever order its links
    // were added in.
    val ops = Set(source("s1"), source("s2"), source("s3"), op("u"))
    val links = List(link("s1", "u"), link("s2", "u"), link("s3", "u"))
    val cached = Map(entry(out("s1")), entry(out("s2")), entry(out("s3")))
    val expected = List("s1", "s2", "s3").map(name => cached(out(name)).storageUri)
    List(links, links.reverse).foreach { linkOrder =>
      val p = planOf(ops, linkOrder.toSet)
      val setup = eagerSetup(p, context(cached, Set(out("u"))))
      setup.state.readerUrisByInputPort(p, setup.matched, setup.required) shouldBe Map(
        in("u") -> expected
      )
    }
  }

  it should "run a multi-output operator as a whole when one needed output has no saved result" in {
    // m has two outputs: m.0 -> x, m.1 -> y; x and y are terminal.
    val plan = planOf(
      Set(source("m", outputs = 2), op("x"), op("y")),
      Set(link("m", "x", fromPort = 0), link("m", "y", fromPort = 1))
    )
    val cached = Map(entry(out("m", 0)), entry(out("x")))
    val setup = eagerSetup(plan, context(cached, Set(out("x"), out("y"))))
    val s = setup.state
    // y needs m.1, which has no saved result, so m runs and computes m.0 again too.
    ids(s.retainedPlan(plan, setup.required)) shouldBe Set(opId("m"), opId("y"))
    s.retainedPlan(plan, setup.required).links shouldBe Set(link("m", "y", fromPort = 1))
    // x is skipped on its own saved result, so m.0's saved result is read by nothing.
    s.skippedOperators(plan, setup.required) shouldBe Set(opId("x"))
    s.linksOfType(CacheRead) shouldBe empty
    s.readerUrisByInputPort(plan, setup.matched, setup.required) shouldBe empty
    s.cacheReadPorts(plan, setup.matched, setup.required).keySet shouldBe Set(out("x"))
  }

  it should "skip every link into a skipped operator, from a retained operator too" in {
    // m.0 -> x -> w and m.1 -> y, with w and y terminal. w's saved result skips w and x; y has
    // none, so y and m run.
    val plan = planOf(
      Set(source("m", outputs = 2), op("x"), op("w"), op("y")),
      Set(link("m", "x", fromPort = 0), link("x", "w"), link("m", "y", fromPort = 1))
    )
    val setup = eagerSetup(plan, context(Map(entry(out("w"))), Set(out("w"), out("y"))))
    val s = setup.state
    s.skippedOperators(plan, setup.required) shouldBe Set(opId("x"), opId("w"))
    // The link from the retained m into the skipped x is Unused, like the one between x and w.
    s.linksOfType(Unused) shouldBe Set(link("m", "x", fromPort = 0), link("x", "w"))
    // The link between the retained m and y is not.
    s.retainedPlan(plan, setup.required).links shouldBe Set(link("m", "y", fromPort = 1))
    s.linksOfType(CacheRead) shouldBe empty
    // The skip region keeps only the skipped link inside it.
    val regions = s.skipRegions(plan, setup.matched, setup.required, 0)
    regions.map(_.physicalOps.map(_.id)) shouldBe Set(Set(opId("x"), opId("w")))
    regions.head.physicalLinks shouldBe Set(link("x", "w"))
  }

  it should "read a saved port from storage even when its operator runs" in {
    // split.0 has a saved result and split.1 has none. y needs split.1, so split runs, and x
    // reads split.0's saved result instead of waiting for split.
    val plan = split()
    val cached = Map(entry(out("split", 0)))
    val setup = eagerSetup(plan, context(cached, Set(out("x"), out("y"))))
    val s = setup.state
    s.skipsAnything(plan, setup.required) shouldBe false
    s.skippedOperators(plan, setup.required) shouldBe empty
    s.linksOfType(Unused) shouldBe empty
    s.linksOfType(CacheRead) shouldBe Set(link("split", "x", fromPort = 0))
    ids(s.retainedPlan(plan, setup.required)) shouldBe ids(plan)
    s.retainedPlan(plan, setup.required).links shouldBe plan.links - link(
      "split",
      "x",
      fromPort = 0
    )
    s.readerUrisByInputPort(plan, setup.matched, setup.required) shouldBe Map(
      in("x") -> List(cached(out("split", 0)).storageUri)
    )
    s.cacheReadPorts(plan, setup.matched, setup.required) shouldBe cached
    // split.1 has no saved result, so y still reads it over its link.
    s.retainedPlan(plan, setup.required).links should contain(link("split", "y", fromPort = 1))
  }

  it should "change nothing for a matched saved result that no retained operator reads" in {
    // m.0 -> x and m.1 has no link. m.1 has a matched saved result while x does not. m runs for
    // m.0 -> x, and nothing reads m.1.
    val plan = planOf(
      Set(source("m", outputs = 2), op("x")),
      Set(link("m", "x", fromPort = 0))
    )
    val cached = Map(entry(out("m", 1)))
    val setup = eagerSetup(plan, context(cached, Set(out("x"))))
    val s = setup.state
    s.skippedOperators(plan, setup.required) shouldBe empty
    s.linksOfType(CacheRead) shouldBe empty
    s.cacheReadPorts(plan, setup.matched, setup.required) shouldBe empty
    s.retainedPlan(plan, setup.required) shouldBe plan
  }

  it should "leave the plan's own operators in the retained plan and the skip regions" in {
    val plan = chain()
    val setup = eagerSetup(plan, context(Map(entry(out("b"))), Set(out("c"))))
    val s = setup.state
    val retained = s.retainedPlan(plan, setup.required)
    retained.operators.foreach(op => op should be theSameInstanceAs plan.getOperator(op.id))
    // c still lists its input link from b, which the retained plan no longer holds.
    retained.getOperator(opId("c")).getInputLinks() shouldBe List(link("b", "c"))
    retained.links should not contain link("b", "c")
    val region = s.skipRegions(plan, setup.matched, setup.required, 0).head
    region.physicalOps.foreach(op => op should be theSameInstanceAs plan.getOperator(op.id))
    // b still lists its output link to c, which is in no region.
    region.getOperator(opId("b")).getOutputLinks(PortIdentity(0)) shouldBe List(link("b", "c"))
    region.physicalLinks shouldBe Set(link("a", "b"))
  }

  it should "run an operator whose unconnected output port has no saved result" in {
    // m.0 -> x (x has its own saved result), m.1 has no link and no saved result: an output
    // port with no link is a required output, so m still runs, as it would without the cache.
    val plan = planOf(
      Set(source("m", outputs = 2), op("x")),
      Set(link("m", "x", fromPort = 0))
    )
    val cached = Map(entry(out("x")))
    val setup = eagerSetup(plan, context(cached, Set(out("x"))))
    val s = setup.state
    ids(s.retainedPlan(plan, setup.required)) shouldBe Set(opId("m"))
    s.skippedOperators(plan, setup.required) shouldBe Set(opId("x"))
    s.linksOfType(Unused) shouldBe Set(link("m", "x", fromPort = 0))
    s.linksOfType(CacheRead) shouldBe empty
    s.readerUrisByInputPort(plan, setup.matched, setup.required) shouldBe empty
    s.cacheReadPorts(plan, setup.matched, setup.required).keySet shouldBe Set(out("x"))
  }

  it should "list a skipped operator's unconnected port among the saved results read" in {
    // The same plan with a saved result for m.1 too: m is skipped, and m.1, a required output
    // of a skipped operator, is read from its saved result. m.0's saved result is not listed:
    // it feeds only the skipped x.
    val plan = planOf(
      Set(source("m", outputs = 2), op("x")),
      Set(link("m", "x", fromPort = 0))
    )
    val cached = Map(entry(out("m", 0)), entry(out("m", 1), count = Some(4L)), entry(out("x")))
    // The compiler stores the ports of operators with no outgoing links: only x's here.
    val setup = eagerSetup(plan, context(cached, Set(out("x"))))
    val s = setup.state
    s.skippedOperators(plan, setup.required) shouldBe Set(opId("m"), opId("x"))
    s.cacheReadPorts(plan, setup.matched, setup.required) shouldBe Map(
      out("m", 1) -> CachedResult(baseUri(out("m", 1)), Some(4L)),
      out("x") -> CachedResult(baseUri(out("x")), Some(10L))
    )
    s.skipRegions(plan, setup.matched, setup.required, 0)
      .head
      .resourceConfig
      .get
      .portConfigs shouldBe Map(
      out("m", 1) -> OutputPortConfig(baseUri(out("m", 1)), Some(4L)),
      out("x") -> OutputPortConfig(baseUri(out("x")), Some(10L))
    )
  }

  it should "keep a /wh/ warehouse on the URIs it hands out" in {
    val plan = chain()
    val cached = Map(entry(out("b"), warehouse = Some("wh1")))
    val setup = eagerSetup(plan, context(cached, Set(out("c")), warehouse = Some("wh1")))
    val s = setup.state
    s.skippedOperators(plan, setup.required) shouldBe Set(opId("a"), opId("b"))
    val uri = s.cacheReadPorts(plan, setup.matched, setup.required)(out("b")).storageUri
    uri.toString should include("/wh/wh1/")
    uri shouldBe cached(out("b")).storageUri
    s.readerUrisByInputPort(plan, setup.matched, setup.required)(in("c")) shouldBe List(uri)
  }

  it should "reject matchedResults naming an input port or a port outside the plan" in {
    val plan = chain()
    val required = SearchState.requiredOutputPorts(context(Map.empty, Set(out("c"))), plan)
    val state = allMaterialized(plan)
    val invalidEntries = Map(
      "input port" -> Map(in("b") -> CachedResult(baseUri(out("b")), None)),
      "unknown operator" -> Map(out("zz") -> CachedResult(baseUri(out("zz")), None)),
      "unknown output port" -> Map(out("b", 9) -> CachedResult(baseUri(out("b", 9)), None)),
      "mixed valid and invalid port" -> Map(
        entry(out("b")),
        out("zz") -> CachedResult(baseUri(out("zz")), None)
      )
    )
    invalidEntries.foreach {
      case (label, badMatched) =>
        withClue(s"$label: ") {
          an[IllegalArgumentException] should be thrownBy
            SearchState.allowedLinkTypes(plan, badMatched, allowPipelined = true)
          an[IllegalArgumentException] should be thrownBy
            SearchState.eagerSeed(plan, badMatched, required)
          an[IllegalArgumentException] should be thrownBy
            state.cacheReadPorts(plan, badMatched, required)
          an[IllegalArgumentException] should be thrownBy
            state.readerUrisByInputPort(plan, badMatched, required)
          an[IllegalArgumentException] should be thrownBy
            state.skipRegions(plan, badMatched, required, 0)
        }
    }
  }

  it should "run an operator with no output ports even when its input has a saved result" in {
    // a -> s, and s has no output ports: nothing in the workflow reads s, so it may be there
    // for what it does outside the workflow; it runs and reads a's saved result.
    val plan = planOf(Set(source("a"), op("s", outputs = 0)), Set(link("a", "s")))
    val cached = Map(entry(out("a")))
    val setup = eagerSetup(plan, context(cached, Set.empty))
    val s = setup.state
    ids(s.retainedPlan(plan, setup.required)) shouldBe Set(opId("s"))
    s.skippedOperators(plan, setup.required) shouldBe Set(opId("a"))
    s.linksOfType(CacheRead) shouldBe Set(link("a", "s"))
    s.readerUrisByInputPort(plan, setup.matched, setup.required) shouldBe Map(
      in("s") -> List(cached(out("a")).storageUri)
    )
    s.cacheReadPorts(plan, setup.matched, setup.required) shouldBe cached
  }

  it should "number skip regions from the given id, one per connected component" in {
    // Two chains that never meet: a1 -> b1, a2 -> b2; both terminal ports matched.
    val plan = planOf(
      Set(source("a1"), op("b1"), source("a2"), op("b2")),
      Set(link("a1", "b1"), link("a2", "b2"))
    )
    val cached = Map(entry(out("b1")), entry(out("b2")))
    val setup = eagerSetup(plan, context(cached, Set(out("b1"), out("b2"))))
    val regions = setup.state.skipRegions(plan, setup.matched, setup.required, firstRegionId = 3)
    regions.map(_.id.id) shouldBe Set(3L, 4L)
    regions.map(_.physicalOps.map(_.id)) shouldBe Set(
      Set(opId("a1"), opId("b1")),
      Set(opId("a2"), opId("b2"))
    )
    regions.forall(_.skipped) shouldBe true
    // Deterministic: the same ids for the same input.
    setup.state
      .skipRegions(plan, setup.matched, setup.required, 3)
      .map(r => r.id -> r.physicalOps.map(_.id)) shouldBe
      regions.map(r => r.id -> r.physicalOps.map(_.id))
  }

  it should "give each skip region only its own group's saved results" in {
    // Two chains that never meet, s1 -> t1 and s2 -> t2, are skipped on t1's and t2's saved
    // results. Beside them m runs for y, and x reads m's port 0 from its saved result: that
    // port is read too, and its operator runs, so it is in no skip region.
    val plan = planOf(
      Set(
        source("s1"),
        op("t1"),
        source("s2"),
        op("t2"),
        source("m", outputs = 2),
        op("x"),
        op("y")
      ),
      Set(
        link("s1", "t1"),
        link("s2", "t2"),
        link("m", "x", fromPort = 0),
        link("m", "y", fromPort = 1)
      )
    )
    val cached = Map(entry(out("t1")), entry(out("t2")), entry(out("m", 0)))
    val setup = eagerSetup(plan, context(cached, Set(out("t1"), out("t2"), out("x"), out("y"))))
    setup.state.cacheReadPorts(plan, setup.matched, setup.required).keySet shouldBe Set(
      out("t1"),
      out("t2"),
      out("m", 0)
    )
    setup.state
      .skipRegions(plan, setup.matched, setup.required, 0)
      .map(region =>
        region.physicalOps.map(_.id) -> region.resourceConfig.get.portConfigs.keySet
      ) shouldBe
      Set(
        Set(opId("s1"), opId("t1")) -> Set(out("t1")),
        Set(opId("s2"), opId("t2")) -> Set(out("t2"))
      )
  }

  // ---------------------------------------------------------------------------
  // Four-edge-state SearchState checks
  // ---------------------------------------------------------------------------

  "SearchState" should "reject reusedOutputs containing an input port" in {
    an[IllegalArgumentException] should be thrownBy SearchState(Map.empty, Set(in("b")))
  }

  "SearchState.allowedLinkTypes" should
    "distinguish pipelined, materialized, blocking and saved edges" in {
    val blockingSource = PhysicalOp
      .oneToOnePhysicalOp(opId("a"), workflowId, executionId, OpExecInitInfo.Empty)
      .withOutputPorts(List(OutputPort(PortIdentity(0), blocking = true)))
    val plan = planOf(Set(blockingSource, op("b"), op("c")), Set(link("a", "b"), link("b", "c")))
    val cached = Map(entry(out("a")), entry(out("b")), entry(out("c")))
    val ctx = context(cached, Set(out("b")))
    val matched = ctx.matchedResults

    // Pipelined mode: blocking link a->b drops Pipelined; non-blocking b->c allows all four.
    val pipelinedLinks = SearchState.allowedLinkTypes(plan, matched, allowPipelined = true)
    pipelinedLinks(link("a", "b")) shouldBe Set(Materialized, CacheRead, Unused)
    pipelinedLinks(link("b", "c")) shouldBe Set(Materialized, Pipelined, CacheRead, Unused)

    // Without saved results, CacheRead is absent.
    val uncachedLinks = SearchState.allowedLinkTypes(plan, Map.empty, allowPipelined = true)
    uncachedLinks(link("b", "c")) shouldBe Set(Materialized, Pipelined, Unused)

    // Materialized mode: no link allows Pipelined.
    val materializedLinks = SearchState.allowedLinkTypes(plan, matched, allowPipelined = false)
    materializedLinks.values.foreach(types => types should not contain Pipelined)
  }

  "SearchState" should "check the consumption condition and resolve both skipped and required inputs" in {
    val plan = chain()
    val cached = Map(entry(out("c")))
    val ctx = context(cached, Set(out("c")))
    val matched = ctx.matchedResults
    val required = SearchState.requiredOutputPorts(ctx, plan)

    // Every link Materialized, nothing reused: every operator runs and no link is Unused.
    val allMat = allMaterialized(plan)
    allMat.satisfiesConsumptionCondition(plan, required) shouldBe true

    // Reuse c's required output without resolving yet: c stops running while b->c is still
    // Materialized, so the consumption condition fails until resolveInputRequirements runs.
    val beforeResolve = withReusedOutput(allMat, out("c"), reused = true)
    beforeResolve.satisfiesConsumptionCondition(plan, required) shouldBe false

    val resolved = beforeResolve.resolveInputRequirements(plan, required, matched)
    resolved.satisfiesConsumptionCondition(plan, required) shouldBe true
    resolved(link("a", "b")) shouldBe Unused
    resolved(link("b", "c")) shouldBe Unused
    resolved.reusedOutputs shouldBe Set(out("c"))
    SearchState.eagerSeed(plan, matched, required, defaultActiveType = _ => Unused) shouldBe
      resolved

    // Making an incoming link of a running operator Unused violates the consumption condition,
    // and resolveInputRequirements supplies its required inputs.
    val unusedIntoRunning = allMat.updated(link("b", "c"), Unused)
    unusedIntoRunning.satisfiesConsumptionCondition(plan, required) shouldBe false
    val supplied = unusedIntoRunning.resolveInputRequirements(plan, required, matched)
    supplied.satisfiesConsumptionCondition(plan, required) shouldBe true
    supplied(link("b", "c")) shouldBe Materialized

    // Naming a non-required output in reusedOutputs fails the precondition.
    val unknownReused = withReusedOutput(allMat, out("b"), reused = true)
    unknownReused.satisfiesConsumptionCondition(plan, required) shouldBe false
    an[IllegalArgumentException] should be thrownBy
      unknownReused.resolveInputRequirements(plan, required, matched)

    // Mismatched link keys or invalid matchedResults port fail fast.
    val missingLink = allMat.copy(linkTypes = allMat.linkTypes - link("b", "c"))
    an[IllegalArgumentException] should be thrownBy
      missingLink.resolveInputRequirements(plan, required, matched)
    an[IllegalArgumentException] should be thrownBy
      allMat.resolveInputRequirements(plan, required, Map(entry(out("missing"))))
  }

  it should "cascade Unused backward across a dead chain and resolve upstream inputs when un-reusing" in {
    val plan = planOf(
      Set(source("a"), op("b"), op("c"), op("d"), op("e")),
      Set(link("a", "b"), link("b", "c"), link("c", "d"), link("d", "e"))
    )
    val cached = Map(entry(out("b")), entry(out("d")))
    val required = Set(out("e"))

    // Skipping all upstream work from e: cascading Unused clears every link in one pass.
    val allMat = allMaterialized(plan).updated(link("d", "e"), CacheRead)
    val skippedState = allMat.resolveInputRequirements(plan, required, cached)
    skippedState.retainedOperators(plan, required) shouldBe Set(opId("e"))
    skippedState(link("a", "b")) shouldBe Unused
    skippedState(link("b", "c")) shouldBe Unused
    skippedState(link("c", "d")) shouldBe Unused
    skippedState(link("d", "e")) shouldBe CacheRead
    skippedState.satisfiesConsumptionCondition(plan, required) shouldBe true

    // Switching d->e back to Materialized requires d and c to run, supplying c->d (Pipelined)
    // and stopping at matched b->c (CacheRead) while leaving a->b Unused and b skipped.
    val unReused = skippedState
      .updated(link("d", "e"), Materialized)
      .resolveInputRequirements(
        plan,
        required,
        cached,
        allowPipelined = true,
        defaultActiveType = _ => Pipelined
      )
    unReused.retainedOperators(plan, required) shouldBe Set(opId("c"), opId("d"), opId("e"))
    unReused(link("d", "e")) shouldBe Materialized
    unReused(link("c", "d")) shouldBe Pipelined
    unReused(link("b", "c")) shouldBe CacheRead
    unReused(link("a", "b")) shouldBe Unused
    unReused.satisfiesConsumptionCondition(plan, required) shouldBe true

    // Empty plan and Materialized-only / CacheRead fallback on defaultActiveType.
    val empty = PhysicalPlan(Set.empty, Set.empty)
    SearchState(Map.empty).resolveInputRequirements(empty, Set.empty) shouldBe SearchState(
      Map.empty
    )

    val unReusedMatFallback = skippedState
      .updated(link("d", "e"), Materialized)
      .resolveInputRequirements(
        plan,
        required,
        cached,
        allowPipelined = false,
        defaultActiveType = _ => Pipelined
      )
    unReusedMatFallback(link("c", "d")) shouldBe Materialized

    val unReusedInvalidDefaultFallback = skippedState
      .updated(link("d", "e"), Materialized)
      .resolveInputRequirements(
        plan,
        required,
        cached,
        allowPipelined = true,
        defaultActiveType = _ => CacheRead
      )
    unReusedInvalidDefaultFallback(link("c", "d")) shouldBe Materialized
  }

  it should "derive retained operators, skipped operators, and cache-read ports from SearchState" in {
    // split.0 is saved AND viewed; split.1 is unsaved, so split runs for y.
    val plan = split()
    val cached = Map(entry(out("split", 0)))
    val ctx = context(cached, Set(out("split", 0), out("x"), out("y")))
    val matched = ctx.matchedResults
    val required = SearchState.requiredOutputPorts(ctx, plan)

    val mixedState = SearchState.eagerSeed(
      plan,
      matched,
      required,
      allowPipelined = true,
      defaultActiveType = l => if (l == link("src", "split")) Pipelined else Materialized
    )
    mixedState.satisfiesConsumptionCondition(plan, required) shouldBe true
    mixedState.retainedOperators(plan, required) shouldBe
      Set(opId("src"), opId("split"), opId("x"), opId("y"))
    mixedState.skippedOperators(plan, required) shouldBe empty
    mixedState.linksOfType(Pipelined) shouldBe Set(link("src", "split"))
    mixedState.linksOfType(CacheRead) shouldBe Set(link("split", "x", fromPort = 0))
    mixedState.linksOfType(Materialized) shouldBe Set(link("split", "y", fromPort = 1))
    mixedState.reusedOutputs shouldBe Set(out("split", 0))
    mixedState.cacheReadPorts(plan, matched, required) shouldBe cached

    // Materializing split.0's required output keeps x reading split.0's saved result while split stores split.0.
    val storeSplitOut0 = withReusedOutput(mixedState, out("split", 0), reused = false)
    storeSplitOut0.satisfiesConsumptionCondition(plan, required) shouldBe true
    storeSplitOut0.reusedOutputs shouldBe empty
    storeSplitOut0.cacheReadPorts(plan, matched, required) shouldBe cached

    // Materialized mode forbids Pipelined and falls back to Materialized on defaultActiveType.
    val matLinks = SearchState.allowedLinkTypes(plan, matched, allowPipelined = false)
    matLinks(link("src", "split")) should not contain Pipelined
    val matSeed = SearchState.eagerSeed(
      plan,
      matched,
      required,
      allowPipelined = false,
      defaultActiveType = _ => Pipelined
    )
    matSeed(link("src", "split")) shouldBe Materialized

    // Rejects a SearchState violating the consumption condition.
    val invalid = allMaterialized(plan).updated(link("src", "split"), Unused)
    invalid.satisfiesConsumptionCondition(plan, required) shouldBe false
    an[IllegalArgumentException] should be thrownBy invalid.cacheReadPorts(plan, matched, required)
    an[IllegalArgumentException] should be thrownBy invalid.retainedPlan(plan, required)
    an[IllegalArgumentException] should be thrownBy
      invalid.readerUrisByInputPort(plan, matched, required)
    an[IllegalArgumentException] should be thrownBy invalid.skipRegions(plan, matched, required, 0)
  }

  /** What a reference computation expects of one search state. */
  private case class Expected(
      retained: Set[PhysicalOpIdentity],
      skippedLinks: Set[PhysicalLink],
      cacheReadLinks: Set[PhysicalLink],
      retainedLinks: Set[PhysicalLink],
      readPorts: Set[GlobalPortIdentity]
  )

  /**
    * A slow reference computation of the rules: a fixed point over all operators, in any order.
    *
    * Every required output gets an operator that always runs and reads it (the user, in
    * effect), and an output port with no link is a required output. An operator runs when it
    * has a link from a port without a saved result into an operator that runs, or when it has
    * no output ports. A link into an operator that runs is a cache-read link when its port has
    * a saved result, and a retained link otherwise; every other link is skipped.
    */
  private def reference(
      plan: PhysicalPlan,
      savedPorts: Set[GlobalPortIdentity],
      mustStore: Set[GlobalPortIdentity]
  ): Expected = {
    def portOf(l: PhysicalLink) = GlobalPortIdentity(l.fromOpId, l.fromPortId)
    val required = plan.operators
      .flatMap(o => o.outputPorts.keys.map(p => GlobalPortIdentity(o.id, p)))
      .filter(p => mustStore.contains(p) || !plan.links.exists(l => portOf(l) == p))
    var retained = plan.operators.filter(_.outputPorts.isEmpty).map(_.id)
    var changed = true
    while (changed) {
      val next = retained ++
        required.filterNot(savedPorts.contains).map(_.opId) ++
        plan.links
          .filter(l => retained.contains(l.toOpId) && !savedPorts.contains(portOf(l)))
          .map(_.fromOpId)
      changed = next != retained
      retained = next
    }
    val skipped = plan.operators.map(_.id) -- retained
    val intoRetained = plan.links.filter(l => retained.contains(l.toOpId))
    val cacheRead = intoRetained.filter(l => savedPorts.contains(portOf(l)))
    Expected(
      retained = retained,
      skippedLinks = plan.links.filter(l => skipped.contains(l.toOpId)),
      cacheReadLinks = cacheRead,
      retainedLinks = intoRetained -- cacheRead,
      // The saved results read: behind every cache-read link, and every required output of a
      // skipped operator. A required output of a retained operator is stored and shown from this
      // run, not read from its saved result.
      readPorts = cacheRead.map(portOf) ++ required.filter(p => skipped.contains(p.opId))
    )
  }

  /** The connected groups of `ops` over the links between them, the slow way. */
  private def groupsOf(
      ops: Set[PhysicalOpIdentity],
      links: Set[PhysicalLink]
  ): Set[Set[PhysicalOpIdentity]] = {
    val inside = links.filter(l => ops.contains(l.fromOpId) && ops.contains(l.toOpId))
    ops.map { start =>
      var group = Set(start)
      var changed = true
      while (changed) {
        val next = group ++ inside.collect {
          case l if group.contains(l.fromOpId) => l.toOpId
          case l if group.contains(l.toOpId)   => l.fromOpId
        }
        changed = next != group
        group = next
      }
      group
    }
  }

  /** Up to six operators with two input ports and zero to two output ports, linked at random. */
  private def randomPlan(random: Random): PhysicalPlan = {
    val n = 1 + random.nextInt(6)
    val outputs = (0 until n).map(_ => if (random.nextInt(8) == 0) 0 else 1 + random.nextInt(2))
    val ops = (0 until n).map(i => op(s"o$i", inputs = 2, outputs = outputs(i)))
    val links = for {
      i <- 0 until n
      j <- i + 1 until n
      if outputs(i) > 0 && random.nextInt(3) == 0
    } yield link(s"o$i", s"o$j", fromPort = random.nextInt(outputs(i)), toPort = random.nextInt(2))
    planOf(ops.toSet, links.toSet)
  }

  it should "agree with a slow reference computation on random small plans" in {
    val random = new Random(42)
    (1 to 400).foreach { _ =>
      val plan = randomPlan(random)
      val allOutputs =
        plan.operators.flatMap(o => o.outputPorts.keys.map(p => GlobalPortIdentity(o.id, p)))
      // Close to the compiler, which stores the ports of operators with no outgoing links and
      // of operators whose results the user views: here most ports with no link, and a few
      // others. A port with no link left out of the set is a required output all the same.
      val noLink = allOutputs.filter(p =>
        !plan.links.exists(l => l.fromOpId == p.opId && l.fromPortId == p.portId)
      )
      val mustStore =
        noLink.filter(_ => random.nextInt(4) != 0) ++ allOutputs.filter(_ => random.nextInt(6) == 0)
      val savedPorts = allOutputs.filter(_ => random.nextBoolean())
      val cached = savedPorts.map(p => entry(p)).toMap
      val ctx = context(cached, mustStore)
      val setup = eagerSetup(plan, ctx)
      val s = setup.state
      val expected = reference(plan, savedPorts, mustStore)
      val expectedSkipped = ids(plan) -- expected.retained
      withClue(s"plan ${plan.links} stores $mustStore saved $savedPorts: ") {
        s.retainedOperators(plan, setup.required) shouldBe expected.retained
        s.skippedOperators(plan, setup.required) shouldBe expectedSkipped
        s.skipsAnything(plan, setup.required) shouldBe expectedSkipped.nonEmpty
        s.linksOfType(Unused) shouldBe expected.skippedLinks
        s.linksOfType(CacheRead) shouldBe expected.cacheReadLinks
        val retained = s.retainedPlan(plan, setup.required)
        ids(retained) shouldBe expected.retained
        retained.links shouldBe expected.retainedLinks
        retained.operators.foreach(o => o should be theSameInstanceAs plan.getOperator(o.id))
        expected.retainedLinks.foreach(l => expected.retained should contain(l.fromOpId))
        val readerUris = s.readerUrisByInputPort(plan, setup.matched, setup.required)
        readerUris.keySet shouldBe
          expected.cacheReadLinks.map(l => GlobalPortIdentity(l.toOpId, l.toPortId, input = true))
        readerUris.foreach {
          case (inputPort, uris) =>
            val links = expected.cacheReadLinks
              .filter(l => l.toOpId == inputPort.opId && l.toPortId == inputPort.portId)
            uris.size shouldBe links.size
            uris.toSet shouldBe links.map(l =>
              cached(GlobalPortIdentity(l.fromOpId, l.fromPortId)).storageUri
            )
        }
        s.cacheReadPorts(plan, setup.matched, setup.required).keySet shouldBe expected.readPorts
        s.cacheReadPorts(plan, setup.matched, setup.required).foreach {
          case (port, saved) => saved shouldBe cached(port)
        }
        val regions = s.skipRegions(plan, setup.matched, setup.required, 10)
        regions.map(_.physicalOps.map(_.id)) shouldBe groupsOf(
          expectedSkipped,
          expected.skippedLinks
        )
        regions.map(_.id.id) shouldBe (10L until 10L + regions.size).toSet
        regions.foreach { region =>
          val group = region.physicalOps.map(_.id)
          region.physicalLinks shouldBe expected.skippedLinks.filter(l =>
            group.contains(l.fromOpId) && group.contains(l.toOpId)
          )
          region.resourceConfig.get.portConfigs shouldBe
            expected.readPorts
              .filter(port => group.contains(port.opId))
              .map { port =>
                port -> OutputPortConfig(cached(port).storageUri, cached(port).tupleCount)
              }
              .toMap
          region.physicalOps.foreach(o => o should be theSameInstanceAs plan.getOperator(o.id))
        }

        // Verify single-pass reverse-topological resolveInputRequirements against the independent
        // backward-reachability oracle across arbitrary starting link states, and confirm both the
        // consumption condition and seed equivalence.
        val allowedLinks = SearchState.allowedLinkTypes(plan, setup.matched, allowPipelined = true)
        val preResolve = SearchState(
          linkTypes = plan.links.map { l =>
            val opts = allowedLinks(l).toVector
            l -> opts(random.nextInt(opts.size))
          }.toMap,
          reusedOutputs =
            setup.required.filter(p => setup.matched.contains(p) && random.nextBoolean())
        )
        val usePipelinedDefault = random.nextBoolean()
        val defaultActive =
          (_: PhysicalLink) => if (usePipelinedDefault) Pipelined else Materialized
        val allowPipe = random.nextBoolean()
        val resolved = preResolve.resolveInputRequirements(
          plan,
          setup.required,
          setup.matched,
          allowPipelined = allowPipe,
          defaultActiveType = defaultActive
        )
        resolved shouldBe referenceResolveInputRequirements(
          plan,
          setup.required,
          setup.matched,
          allowPipelined = allowPipe,
          defaultActiveType = defaultActive,
          preResolve
        )
        resolved.satisfiesConsumptionCondition(plan, setup.required) shouldBe true

        val seed = SearchState.eagerSeed(
          plan,
          setup.matched,
          setup.required,
          defaultActiveType = _ => if (random.nextBoolean()) Pipelined else Materialized
        )
        seed.satisfiesConsumptionCondition(plan, setup.required) shouldBe true
        seed.skippedOperators(plan, setup.required) shouldBe expectedSkipped
        seed.linksOfType(Unused) shouldBe expected.skippedLinks
        seed.linksOfType(CacheRead) shouldBe expected.cacheReadLinks
        seed.retainedPlan(plan, setup.required) shouldBe retained
        seed.readerUrisByInputPort(plan, setup.matched, setup.required) shouldBe readerUris
        seed.cacheReadPorts(plan, setup.matched, setup.required) shouldBe
          s.cacheReadPorts(plan, setup.matched, setup.required)
      }
    }
  }
}
