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
    skeleton.cacheReadPorts shouldBe Map(out("c") -> CachedResult(baseUri(out("c")), Some(3L)))
  }

  it should "run only the operators downstream of a match in the middle" in {
    val plan = chain()
    val cached = Map(entry(out("b")))
    val skeleton = skeletonOf(plan, context(cached, Set(out("c"))))
    // a is skipped too: nothing needs its output once b's output is read from the cache
    skeleton.skippedOps shouldBe Set(opId("a"), opId("b"))
    skeleton.skippedLinks shouldBe Set(link("a", "b"))
    skeleton.cacheReadLinks shouldBe Set(link("b", "c"))
    skeleton.cacheReadPorts shouldBe cached
  }

  it should "run an operator whose results the user views when its port has no saved result" in {
    // a -> b -> c, and the user views b, so the run must store b's port, which has no saved
    // result: b runs, and so does a, which b reads. Only c is skipped, on its own saved result.
    val plan = chain()
    val skeleton = skeletonOf(plan, context(Map(entry(out("c"))), Set(out("b"), out("c"))))
    skeleton.skippedOps shouldBe Set(opId("c"))
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
    skeleton.cacheReadPorts shouldBe cached
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
    // x is skipped on its own saved result, so m.0's saved result is read by nothing
    skeleton.skippedOps shouldBe Set(opId("x"))
    skeleton.cacheReadLinks shouldBe empty
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
    skeleton.cacheReadLinks shouldBe empty
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
    skeleton.cacheReadPorts shouldBe cached
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
    skeleton.unusable shouldBe empty
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
    skeleton.skippedOps shouldBe Set(opId("x"))
    skeleton.skippedLinks shouldBe Set(link("m", "x", fromPort = 0))
    skeleton.cacheReadLinks shouldBe empty
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
    skeleton.skippedOps shouldBe Set(opId("a"))
    skeleton.cacheReadLinks shouldBe Set(link("a", "s"))
    skeleton.cacheReadPorts shouldBe cached
  }

  /** What a reference computation expects of the skeleton of one plan. */
  private case class Expected(
      retained: Set[PhysicalOpIdentity],
      skippedLinks: Set[PhysicalLink],
      cacheReadLinks: Set[PhysicalLink],
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
      // The saved results read: behind every cache-read link, and every required output of a
      // skipped operator. A required output of a retained operator is stored and shown from this
      // run, not read from its saved result.
      readPorts = cacheRead.map(portOf) ++ required.filter(p => skipped.contains(p.opId))
    )
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
        skeleton.cacheReadPorts.keySet shouldBe expected.readPorts
        skeleton.cacheReadPorts.foreach {
          case (port, saved) => saved shouldBe cached(port)
        }
      }
    }
  }
}
