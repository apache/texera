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
import org.apache.texera.amber.engine.architecture.scheduling.config.OutputPortConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI
import scala.util.Random

/**
  * The skeleton generator is pure, so the plans here are built by hand: no compiler, no
  * engine. As in a compiled plan, every operator lists its own links; the generator reads links
  * from the plan, and the skeleton must leave the operators as they are.
  */
class SkeletonGeneratorSpec extends AnyFlatSpec with Matchers {

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
      warehouse: Option[String] = None
  ): (GlobalPortIdentity, CachedResult) = port -> CachedResult(baseUri(port, warehouse), count)

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

  private def skeletonOf(plan: PhysicalPlan, ctx: WorkflowContext): RunSkeleton =
    SkeletonGenerator.generate(ctx, plan)

  "SkeletonGenerator.generate" should "mark nothing when the cache is empty" in {
    val plan = chain()
    val skeleton = skeletonOf(plan, context(Map.empty, Set(out("c"))))
    skeleton.skipsAnything shouldBe false
    skeleton.skippedOps shouldBe empty
    skeleton.skippedLinks shouldBe empty
    skeleton.cacheReadLinks shouldBe empty
    skeleton.retainedPart(plan) shouldBe plan
    skeleton.cacheReadInputs.readerUris shouldBe empty
    skeleton.cacheReadPorts shouldBe empty
    skeleton.unusable shouldBe empty
  }

  it should "skip everything when the terminal port is matched" in {
    val plan = chain()
    val cached = Map(entry(out("c"), count = Some(3L)))
    val skeleton = skeletonOf(plan, context(cached, Set(out("c"))))
    skeleton.skipsAnything shouldBe true
    skeleton.skippedOps shouldBe ids(plan)
    skeleton.skippedLinks shouldBe plan.links
    skeleton.cacheReadLinks shouldBe empty
    skeleton.retainedPart(plan).operators shouldBe empty
    skeleton.cacheReadInputs.readerUris shouldBe empty
    skeleton.cacheReadPorts shouldBe Map(out("c") -> CachedResult(baseUri(out("c")), Some(3L)))
    val regions = skeleton.skipRegions(plan, firstRegionId = 5)
    regions.map(_.id.id) shouldBe Set(5L)
    val region = regions.head
    region.skipped shouldBe true
    region.physicalOps.map(_.id) shouldBe ids(plan)
    region.physicalLinks shouldBe plan.links
    region.ports shouldBe Set(out("a"), in("b"), out("b"), in("c"), out("c"))
    // the saved result becomes the port's config: its location and row count
    region.resourceConfig.get.portConfigs shouldBe Map(
      out("c") -> OutputPortConfig(baseUri(out("c")), Some(3L))
    )
  }

  it should "run only the operators downstream of a match in the middle" in {
    val plan = chain()
    val cached = Map(entry(out("b")))
    val skeleton = skeletonOf(plan, context(cached, Set(out("c"))))
    // a is skipped too: nothing needs its output once b's output is read from the cache
    skeleton.skippedOps shouldBe Set(opId("a"), opId("b"))
    skeleton.skippedLinks shouldBe Set(link("a", "b"))
    skeleton.cacheReadLinks shouldBe Set(link("b", "c"))
    ids(skeleton.retainedPart(plan)) shouldBe Set(opId("c"))
    skeleton.retainedPart(plan).links shouldBe empty
    skeleton.cacheReadInputs.readerUris shouldBe Map(
      in("c") -> List(cached(out("b")).storageUri)
    )
    skeleton.cacheReadInputs.operatorsReadingFromCache shouldBe Set(opId("c"))
    skeleton.cacheReadPorts shouldBe cached
    val region = skeleton.skipRegions(plan, 0).head
    region.resourceConfig.get.portConfigs.keySet shouldBe Set(out("b"))
  }

  it should "run an operator whose results the user views when its port has no saved result" in {
    // a -> b -> c, and the user views b, so the run must store b's port, which has no saved
    // result: b runs, and so does a, which b reads. Only c is skipped, on its own saved result.
    val plan = chain()
    val skeleton = skeletonOf(plan, context(Map(entry(out("c"))), Set(out("b"), out("c"))))
    skeleton.skippedOps shouldBe Set(opId("c"))
    ids(skeleton.retainedPart(plan)) shouldBe Set(opId("a"), opId("b"))
    skeleton.skippedLinks shouldBe Set(link("b", "c"))
    skeleton.cacheReadLinks shouldBe empty
    skeleton.cacheReadPorts.keySet shouldBe Set(out("c"))
  }

  it should "read a join's build-side input from the cache and run the rest" in {
    // s1 -> j (port 0, build), s2 -> j (port 1, probe), j's output is terminal
    val plan = planOf(
      Set(source("s1"), source("s2"), op("j", inputs = 2)),
      Set(link("s1", "j", toPort = 0), link("s2", "j", toPort = 1))
    )
    val cached = Map(entry(out("s1")))
    val skeleton = skeletonOf(plan, context(cached, Set(out("j"))))
    skeleton.skippedOps shouldBe Set(opId("s1"))
    skeleton.skippedLinks shouldBe empty
    skeleton.cacheReadLinks shouldBe Set(link("s1", "j", toPort = 0))
    ids(skeleton.retainedPart(plan)) shouldBe Set(opId("s2"), opId("j"))
    skeleton.retainedPart(plan).links shouldBe Set(link("s2", "j", toPort = 1))
    skeleton.cacheReadInputs.readerUris shouldBe Map(
      in("j", 0) -> List(cached(out("s1")).storageUri)
    )
    skeleton.cacheReadPorts shouldBe cached
  }

  it should "give each input port the saved location of its own link" in {
    // s1 -> j's port 0 and s2 -> j's port 1, both from port 0 and both saved: each of j's
    // input ports reads the saved result of the link into it.
    val plan = planOf(
      Set(source("s1"), source("s2"), op("j", inputs = 2)),
      Set(link("s1", "j", toPort = 0), link("s2", "j", toPort = 1))
    )
    val cached = Map(entry(out("s1")), entry(out("s2")))
    val skeleton = skeletonOf(plan, context(cached, Set(out("j"))))
    skeleton.cacheReadInputs.readerUris shouldBe Map(
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
      val skeleton = skeletonOf(planOf(ops, linkOrder.toSet), context(cached, Set(out("u"))))
      skeleton.cacheReadInputs.readerUris shouldBe Map(in("u") -> expected)
    }
  }

  it should "run a multi-output operator as a whole when one needed output has no saved result" in {
    // m has two outputs: m.0 -> x, m.1 -> y; x and y are terminal
    val plan = planOf(
      Set(source("m", outputs = 2), op("x"), op("y")),
      Set(link("m", "x", fromPort = 0), link("m", "y", fromPort = 1))
    )
    val cached = Map(entry(out("m", 0)), entry(out("x")))
    val skeleton = skeletonOf(plan, context(cached, Set(out("x"), out("y"))))
    // y needs m.1, which has no saved result, so m runs and computes m.0 again too
    ids(skeleton.retainedPart(plan)) shouldBe Set(opId("m"), opId("y"))
    skeleton.retainedPart(plan).links shouldBe Set(link("m", "y", fromPort = 1))
    // x is skipped on its own saved result, so m.0's saved result is read by nothing
    skeleton.skippedOps shouldBe Set(opId("x"))
    skeleton.cacheReadLinks shouldBe empty
    skeleton.cacheReadInputs.readerUris shouldBe empty
    skeleton.cacheReadPorts.keySet shouldBe Set(out("x"))
  }

  it should "skip every link into a skipped operator, from a retained operator too" in {
    // m.0 -> x -> w and m.1 -> y, with w and y terminal. w's saved result skips w and x; y has
    // none, so y and m run.
    val plan = planOf(
      Set(source("m", outputs = 2), op("x"), op("w"), op("y")),
      Set(link("m", "x", fromPort = 0), link("x", "w"), link("m", "y", fromPort = 1))
    )
    val skeleton = skeletonOf(plan, context(Map(entry(out("w"))), Set(out("w"), out("y"))))
    skeleton.skippedOps shouldBe Set(opId("x"), opId("w"))
    // the link from the retained m into the skipped x is skipped, like the one between x and w
    skeleton.skippedLinks shouldBe Set(link("m", "x", fromPort = 0), link("x", "w"))
    // the link between the retained m and y is not
    skeleton.retainedPart(plan).links shouldBe Set(link("m", "y", fromPort = 1))
    skeleton.cacheReadLinks shouldBe empty
    // the skip region keeps only the skipped link inside it
    val regions = skeleton.skipRegions(plan, 0)
    regions.map(_.physicalOps.map(_.id)) shouldBe Set(Set(opId("x"), opId("w")))
    regions.head.physicalLinks shouldBe Set(link("x", "w"))
  }

  it should "read a saved port from storage even when its operator runs" in {
    // split.0 has a saved result and split.1 has none. y needs split.1, so split runs, and x
    // reads split.0's saved result instead of waiting for split.
    val plan = split()
    val cached = Map(entry(out("split", 0)))
    val skeleton = skeletonOf(plan, context(cached, Set(out("x"), out("y"))))
    skeleton.skipsAnything shouldBe false
    skeleton.skippedOps shouldBe empty
    skeleton.skippedLinks shouldBe empty
    skeleton.cacheReadLinks shouldBe Set(link("split", "x", fromPort = 0))
    ids(skeleton.retainedPart(plan)) shouldBe ids(plan)
    skeleton.retainedPart(plan).links shouldBe plan.links - link("split", "x", fromPort = 0)
    skeleton.cacheReadInputs.readerUris shouldBe Map(
      in("x") -> List(cached(out("split", 0)).storageUri)
    )
    skeleton.cacheReadPorts shouldBe cached
    // split.1 has no saved result, so y still reads it over its link
    skeleton.retainedPart(plan).links should contain(link("split", "y", fromPort = 1))
  }

  it should "change nothing for a usable saved result that no retained operator reads" in {
    // m.0 -> x and m.1 has no link. m.1's saved result is usable, but m runs for m.0, which x
    // reads and which has no saved result, and nothing reads m.1: nothing is skipped and no
    // link reads a saved result, so every operator and link runs.
    val plan = planOf(
      Set(source("m", outputs = 2), op("x")),
      Set(link("m", "x", fromPort = 0))
    )
    val skeleton = skeletonOf(plan, context(Map(entry(out("m", 1))), Set(out("x"))))
    skeleton.skippedOps shouldBe empty
    skeleton.cacheReadLinks shouldBe empty
    skeleton.cacheReadPorts shouldBe empty
    skeleton.retainedPart(plan) shouldBe plan
    skeleton.unusable shouldBe empty
  }

  it should "leave the plan's own operators in the retained part and the skip regions" in {
    val plan = chain()
    val skeleton = skeletonOf(plan, context(Map(entry(out("b"))), Set(out("c"))))
    val retained = skeleton.retainedPart(plan)
    retained.operators.foreach(op => op should be theSameInstanceAs plan.getOperator(op.id))
    // c still lists its input link from b, which the retained part no longer holds
    retained.getOperator(opId("c")).getInputLinks() shouldBe List(link("b", "c"))
    retained.links should not contain link("b", "c")
    val region = skeleton.skipRegions(plan, 0).head
    region.physicalOps.foreach(op => op should be theSameInstanceAs plan.getOperator(op.id))
    // b still lists its output link to c, which is in no region
    region.getOperator(opId("b")).getOutputLinks(PortIdentity(0)) shouldBe List(link("b", "c"))
    region.physicalLinks shouldBe Set(link("a", "b"))
  }

  it should "run an operator whose unconnected output port has no saved result" in {
    // m.0 -> x (x has its own saved result), m.1 has no link and no saved result: an output
    // port with no link is a required output, so m still runs, as it would without the cache
    val plan = planOf(
      Set(source("m", outputs = 2), op("x")),
      Set(link("m", "x", fromPort = 0))
    )
    val cached = Map(entry(out("x")))
    val skeleton = skeletonOf(plan, context(cached, Set(out("x"))))
    ids(skeleton.retainedPart(plan)) shouldBe Set(opId("m"))
    skeleton.skippedOps shouldBe Set(opId("x"))
    skeleton.skippedLinks shouldBe Set(link("m", "x", fromPort = 0))
    skeleton.cacheReadLinks shouldBe empty
    skeleton.cacheReadInputs.readerUris shouldBe empty
    skeleton.cacheReadPorts.keySet shouldBe Set(out("x"))
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
    val skeleton = skeletonOf(plan, context(cached, Set(out("x"))))
    skeleton.skippedOps shouldBe Set(opId("m"), opId("x"))
    skeleton.cacheReadPorts shouldBe Map(
      out("m", 1) -> CachedResult(baseUri(out("m", 1)), Some(4L)),
      out("x") -> CachedResult(baseUri(out("x")), Some(10L))
    )
    skeleton.skipRegions(plan, 0).head.resourceConfig.get.portConfigs shouldBe Map(
      out("m", 1) -> OutputPortConfig(baseUri(out("m", 1)), Some(4L)),
      out("x") -> OutputPortConfig(baseUri(out("x")), Some(10L))
    )
  }

  it should "keep a /wh/ warehouse on the URIs it hands out" in {
    val plan = chain()
    val cached = Map(entry(out("b"), warehouse = Some("wh1")))
    val skeleton = skeletonOf(plan, context(cached, Set(out("c")), warehouse = Some("wh1")))
    // a saved result in the run's own warehouse is usable
    skeleton.skippedOps shouldBe Set(opId("a"), opId("b"))
    val uri = skeleton.cacheReadPorts(out("b")).storageUri
    uri.toString should include("/wh/wh1/")
    uri shouldBe cached(out("b")).storageUri
    skeleton.cacheReadInputs.readerUris(in("c")) shouldBe List(uri)
  }

  it should "mark each bad saved result unusable and say why" in {
    val plan = chain()
    val bad = Map(
      in("b") -> CachedResult(baseUri(out("b")), None),
      out("zz") -> CachedResult(baseUri(out("zz")), None),
      out("a") -> CachedResult(new URI("http://example.com/x"), None),
      out("b") -> CachedResult(VFSURIFactory.resultURI(baseUri(out("b"))), None),
      out("c") -> CachedResult(baseUri(out("c"), wid = WorkflowIdentity(99L)), None)
    )
    val skeleton = skeletonOf(plan, context(bad, Set(out("c"))))
    skeleton.skipsAnything shouldBe false
    skeleton.cacheReadLinks shouldBe empty
    skeleton.unusable.keySet shouldBe bad.keySet
    skeleton.unusable(in("b")) should include("not an output port")
    skeleton.unusable(out("zz")) should include("not an output port")
    skeleton.unusable(out("a")) should include("not a port base URI")
    skeleton.unusable(out("b")) should include("not the base URI")
    skeleton.unusable(out("c")) should include("another workflow")
  }

  it should "mark a saved result at another port's URI or in another warehouse unusable" in {
    val plan = chain()
    val bad = Map(
      out("b") -> CachedResult(baseUri(out("c")), None),
      out("c") -> CachedResult(baseUri(out("c"), warehouse = Some("theirs")), None)
    )
    val skeleton = skeletonOf(plan, context(bad, Set(out("c")), warehouse = Some("mine")))
    skeleton.skipsAnything shouldBe false
    skeleton.cacheReadLinks shouldBe empty
    skeleton.unusable(out("b")) should include("not the base URI")
    skeleton.unusable(out("c")) should include("another warehouse")
  }

  it should "reuse the usable saved results and not the unusable ones in one plan" in {
    val plan = chain()
    val cached = Map(entry(out("b")), out("a") -> CachedResult(new URI("http://x"), None))
    val skeleton = skeletonOf(plan, context(cached, Set(out("c"))))
    skeleton.skippedOps shouldBe Set(opId("a"), opId("b"))
    skeleton.unusable.keySet shouldBe Set(out("a"))
  }

  it should "give a plan with a loop no reuse, whichever loop flag an operator has" in {
    // Loop Start and Loop End both set requiresMaterializedExecution, and Loop Start also
    // sets isLoopStart; either flag alone must turn reuse off. Without the check, c's saved
    // result would skip the whole chain.
    val cached = Map(entry(out("b")), entry(out("c")))
    List(
      "requiresMaterializedExecution" -> op("b").withRequiresMaterializedExecution(true),
      "isLoopStart" -> op("b").withIsLoopStart(true)
    ).foreach {
      case (flag, loopOp) =>
        withClue(s"$flag: ") {
          val plan =
            planOf(Set(source("a"), loopOp, op("c")), Set(link("a", "b"), link("b", "c")))
          val skeleton = skeletonOf(plan, context(cached, Set(out("c"))))
          skeleton.skipsAnything shouldBe false
          skeleton.cacheReadLinks shouldBe empty
          skeleton.unusable shouldBe Map(
            out("b") -> "the plan contains a loop",
            out("c") -> "the plan contains a loop"
          )
        }
    }
  }

  it should "run an operator with no output ports even when its input has a saved result" in {
    // a -> s, and s has no output ports: nothing in the workflow reads s, so it may be there
    // for what it does outside the workflow; it runs and reads a's saved result
    val plan = planOf(Set(source("a"), op("s", outputs = 0)), Set(link("a", "s")))
    val cached = Map(entry(out("a")))
    val skeleton = skeletonOf(plan, context(cached, Set.empty))
    ids(skeleton.retainedPart(plan)) shouldBe Set(opId("s"))
    skeleton.skippedOps shouldBe Set(opId("a"))
    skeleton.cacheReadLinks shouldBe Set(link("a", "s"))
    skeleton.cacheReadInputs.readerUris shouldBe Map(in("s") -> List(cached(out("a")).storageUri))
    skeleton.cacheReadPorts shouldBe cached
  }

  it should "number skip regions from the given id, one per connected component" in {
    // two chains that never meet: a1 -> b1, a2 -> b2; both terminal ports matched
    val plan = planOf(
      Set(source("a1"), op("b1"), source("a2"), op("b2")),
      Set(link("a1", "b1"), link("a2", "b2"))
    )
    val cached = Map(entry(out("b1")), entry(out("b2")))
    val skeleton = skeletonOf(plan, context(cached, Set(out("b1"), out("b2"))))
    val regions = skeleton.skipRegions(plan, firstRegionId = 3)
    regions.map(_.id.id) shouldBe Set(3L, 4L)
    regions.map(_.physicalOps.map(_.id)) shouldBe Set(
      Set(opId("a1"), opId("b1")),
      Set(opId("a2"), opId("b2"))
    )
    regions.forall(_.skipped) shouldBe true
    // deterministic: the same ids for the same input
    skeleton.skipRegions(plan, 3).map(r => r.id -> r.physicalOps.map(_.id)) shouldBe
      regions.map(r => r.id -> r.physicalOps.map(_.id))
  }

  it should "give each skip region only its own group's saved results" in {
    // Two chains that never meet, s1 -> t1 and s2 -> t2, are skipped on t1's and t2's saved
    // results. Beside them m runs for y, and x reads m's port 0 from its saved result: that
    // port is read too, but its operator runs, so it is in no skip region.
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
    val skeleton =
      skeletonOf(plan, context(cached, Set(out("t1"), out("t2"), out("x"), out("y"))))
    skeleton.cacheReadPorts.keySet shouldBe Set(out("t1"), out("t2"), out("m", 0))
    skeleton
      .skipRegions(plan, 0)
      .map(region =>
        region.physicalOps.map(_.id) -> region.resourceConfig.get.portConfigs.keySet
      ) shouldBe
      Set(
        Set(opId("s1"), opId("t1")) -> Set(out("t1")),
        Set(opId("s2"), opId("t2")) -> Set(out("t2"))
      )
  }

  /** What a reference computation expects of the skeleton of one plan. */
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
      val skeleton = skeletonOf(plan, context(cached, mustStore))
      val expected = reference(plan, savedPorts, mustStore)
      val expectedSkipped = ids(plan) -- expected.retained
      withClue(s"plan ${plan.links} stores $mustStore saved $savedPorts: ") {
        skeleton.unusable shouldBe empty
        skeleton.skippedOps shouldBe expectedSkipped
        skeleton.skipsAnything shouldBe expectedSkipped.nonEmpty
        skeleton.skippedLinks shouldBe expected.skippedLinks
        skeleton.cacheReadLinks shouldBe expected.cacheReadLinks
        val retained = skeleton.retainedPart(plan)
        ids(retained) shouldBe expected.retained
        retained.links shouldBe expected.retainedLinks
        retained.operators.foreach(o => o should be theSameInstanceAs plan.getOperator(o.id))
        // a retained link's producer runs too
        expected.retainedLinks.foreach(l => expected.retained should contain(l.fromOpId))
        // every reader URI is the saved location of a cache-read link's source port
        skeleton.cacheReadInputs.readerUris.keySet shouldBe
          expected.cacheReadLinks.map(l => GlobalPortIdentity(l.toOpId, l.toPortId, input = true))
        skeleton.cacheReadInputs.readerUris.foreach {
          case (inputPort, uris) =>
            val links = expected.cacheReadLinks
              .filter(l => l.toOpId == inputPort.opId && l.toPortId == inputPort.portId)
            uris.size shouldBe links.size
            uris.toSet shouldBe links.map(l =>
              cached(GlobalPortIdentity(l.fromOpId, l.fromPortId)).storageUri
            )
        }
        skeleton.cacheReadPorts.keySet shouldBe expected.readPorts
        skeleton.cacheReadPorts.foreach {
          case (port, saved) => saved shouldBe cached(port)
        }
        val regions = skeleton.skipRegions(plan, 10)
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
          // the group's ports whose saved results are read, each with its location and count
          region.resourceConfig.get.portConfigs shouldBe
            expected.readPorts
              .filter(port => group.contains(port.opId))
              .map { port =>
                port -> OutputPortConfig(cached(port).storageUri, cached(port).tupleCount)
              }
              .toMap
          region.physicalOps.foreach(o => o should be theSameInstanceAs plan.getOperator(o.id))
        }
      }
    }
  }
}
