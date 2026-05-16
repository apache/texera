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

package org.apache.texera.amber.compiler.macroOp

import org.apache.texera.amber.compiler.WorkflowCompiler
import org.apache.texera.amber.compiler.model.{LogicalLink, LogicalPlan, LogicalPlanPojo}
import org.apache.texera.amber.core.virtualidentity.{OperatorIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PortIdentity, WorkflowContext}
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.operator.macroOp._
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MacroExpanderSpec extends AnyFlatSpec with Matchers {

  "MacroExpander" should "leave non-macro plans unchanged" in {
    val src = limit("src", 0)
    val sink = limit("sink", 1)
    val plan = LogicalPlan(
      operators = List(src, sink),
      links = List(
        LogicalLink(src.operatorIdentifier, PortIdentity(0), sink.operatorIdentifier, PortIdentity(0))
      )
    )
    val out = MacroExpander.expand(plan, MacroRegistry.Empty)
    out.operators.map(_.operatorIdentifier.id).toSet shouldBe Set("src", "sink")
    out.links.size shouldBe 1
  }

  it should "inline a single-port SNAPSHOT macro and prefix inner-op IDs" in {
    val body = MacroBody(
      operators = List(inMarker(0, "in"), limit("inner", 10), outMarker(0, "out")),
      links = List(
        MacroLink("in", PortIdentity(0), "inner", PortIdentity(0)),
        MacroLink("inner", PortIdentity(0), "out", PortIdentity(0))
      )
    )
    val inst = snapshotInstance("MyMacro-1", "macro-A", body)
    val src = limit("src", 0); val sink = limit("sink", 1)
    val plan = LogicalPlan(
      operators = List(src, inst, sink),
      links = List(
        LogicalLink(src.operatorIdentifier, PortIdentity(0), inst.operatorIdentifier, PortIdentity(0)),
        LogicalLink(inst.operatorIdentifier, PortIdentity(0), sink.operatorIdentifier, PortIdentity(0))
      )
    )
    val out = MacroExpander.expand(plan, MacroRegistry.Empty)

    out.operators.exists(_.isInstanceOf[MacroOpDesc]) shouldBe false
    out.operators.exists(_.isInstanceOf[MacroInputOp]) shouldBe false
    out.operators.exists(_.isInstanceOf[MacroOutputOp]) shouldBe false

    out.operators.collect { case l: LimitOpDesc => l.operatorIdentifier.id }.toSet shouldBe
      Set("src", "sink", "MyMacro-1--inner")

    val edges = out.links.map(l => (l.fromOpId.id, l.toOpId.id)).toSet
    edges shouldBe Set("src" -> "MyMacro-1--inner", "MyMacro-1--inner" -> "sink")
  }

  it should "fetch a LIVE-linked macro body from the registry" in {
    val body = MacroBody(
      operators = List(inMarker(0, "in"), limit("inner", 3), outMarker(0, "out")),
      links = List(
        MacroLink("in", PortIdentity(0), "inner", PortIdentity(0)),
        MacroLink("inner", PortIdentity(0), "out", PortIdentity(0))
      )
    )
    val registry = MacroRegistry.inMemory(Map(("live-id", 4) -> body))

    val inst = new MacroOpDesc
    inst.macroId = "live-id"
    inst.macroVersion = 4
    inst.linkMode = MacroOpDesc.LIVE
    inst.inputPortCount = 1
    inst.outputPortCount = 1
    inst.setOperatorId("L-inst")
    val src = limit("src", 0); val sink = limit("sink", 1)
    val plan = LogicalPlan(
      operators = List(src, inst, sink),
      links = List(
        LogicalLink(src.operatorIdentifier, PortIdentity(0), inst.operatorIdentifier, PortIdentity(0)),
        LogicalLink(inst.operatorIdentifier, PortIdentity(0), sink.operatorIdentifier, PortIdentity(0))
      )
    )
    val out = MacroExpander.expand(plan, registry)
    out.operators.collect { case l: LimitOpDesc => l.operatorIdentifier.id }.toSet shouldBe
      Set("src", "sink", "L-inst--inner")
  }

  it should "expand nested macros with concatenated ID prefixes" in {
    val innerBody = MacroBody(
      operators = List(inMarker(0, "in"), limit("inner-inner", 7), outMarker(0, "out")),
      links = List(
        MacroLink("in", PortIdentity(0), "inner-inner", PortIdentity(0)),
        MacroLink("inner-inner", PortIdentity(0), "out", PortIdentity(0))
      )
    )
    val innerInst = snapshotInstance("Inner", "macro-inner", innerBody)
    val outerBody = MacroBody(
      operators = List(inMarker(0, "oin"), innerInst, outMarker(0, "oout")),
      links = List(
        MacroLink("oin", PortIdentity(0), "Inner", PortIdentity(0)),
        MacroLink("Inner", PortIdentity(0), "oout", PortIdentity(0))
      )
    )
    val outer = snapshotInstance("Outer", "macro-outer", outerBody)
    val src = limit("src", 0); val sink = limit("sink", 1)
    val plan = LogicalPlan(
      operators = List(src, outer, sink),
      links = List(
        LogicalLink(src.operatorIdentifier, PortIdentity(0), outer.operatorIdentifier, PortIdentity(0)),
        LogicalLink(outer.operatorIdentifier, PortIdentity(0), sink.operatorIdentifier, PortIdentity(0))
      )
    )
    val out = MacroExpander.expand(plan, MacroRegistry.Empty)
    val ids = out.operators.collect { case l: LimitOpDesc => l.operatorIdentifier.id }.toSet
    ids should contain("Outer--Inner--inner-inner")
    ids should contain("src")
    ids should contain("sink")
    val edges = out.links.map(l => (l.fromOpId.id, l.toOpId.id)).toSet
    edges should contain("src" -> "Outer--Inner--inner-inner")
    edges should contain("Outer--Inner--inner-inner" -> "sink")
  }

  it should "detect a self-referential macro cycle" in {
    val cycleId = "loop"
    // A body that references the same macro again.
    val recurInst = new MacroOpDesc
    recurInst.macroId = cycleId
    recurInst.macroVersion = 1
    recurInst.linkMode = MacroOpDesc.LIVE
    recurInst.inputPortCount = 1
    recurInst.outputPortCount = 1
    recurInst.setOperatorId("self")
    val body = MacroBody(
      operators = List(inMarker(0, "in"), recurInst, outMarker(0, "out")),
      links = List(
        MacroLink("in", PortIdentity(0), "self", PortIdentity(0)),
        MacroLink("self", PortIdentity(0), "out", PortIdentity(0))
      )
    )
    val registry = MacroRegistry.inMemory(Map((cycleId, 1) -> body))

    val outer = new MacroOpDesc
    outer.macroId = cycleId
    outer.macroVersion = 1
    outer.linkMode = MacroOpDesc.LIVE
    outer.inputPortCount = 1
    outer.outputPortCount = 1
    outer.setOperatorId("outer")
    val src = limit("src", 0); val sink = limit("sink", 1)
    val plan = LogicalPlan(
      operators = List(src, outer, sink),
      links = List(
        LogicalLink(src.operatorIdentifier, PortIdentity(0), outer.operatorIdentifier, PortIdentity(0)),
        LogicalLink(outer.operatorIdentifier, PortIdentity(0), sink.operatorIdentifier, PortIdentity(0))
      )
    )
    val ex = intercept[IllegalStateException] { MacroExpander.expand(plan, registry) }
    ex.getMessage.toLowerCase should include("cycle")
  }

  it should "fail with a depth-limit error on a long non-cyclic macro chain" in {
    // Build a chain chain-0 → chain-1 → ... → chain-N (where each chain-i's body
    // contains a macro instance referencing chain-(i+1)). Distinct macroIds, so the
    // cycle guard cannot fire; depth guard must.
    val n = MacroCompileContext.MaxDepth + 5
    val bodies: Map[(String, Int), MacroBody] = (0 until n).map { i =>
      val nextId = s"chain-${i + 1}"
      val innerOp =
        if (i < n - 1) {
          val m = new MacroOpDesc
          m.macroId = nextId
          m.macroVersion = 1
          m.linkMode = MacroOpDesc.LIVE
          m.inputPortCount = 1
          m.outputPortCount = 1
          m.setOperatorId(s"inst-$i")
          m
        } else {
          limit(s"leaf-$i", 1)
        }
      val body = MacroBody(
        operators = List(inMarker(0, s"in-$i"), innerOp, outMarker(0, s"out-$i")),
        links = List(
          MacroLink(s"in-$i", PortIdentity(0), innerOp.operatorIdentifier.id, PortIdentity(0)),
          MacroLink(innerOp.operatorIdentifier.id, PortIdentity(0), s"out-$i", PortIdentity(0))
        )
      )
      (s"chain-$i", 1) -> body
    }.toMap

    val registry = MacroRegistry.inMemory(bodies)
    val outer = new MacroOpDesc
    outer.macroId = "chain-0"
    outer.macroVersion = 1
    outer.linkMode = MacroOpDesc.LIVE
    outer.inputPortCount = 1
    outer.outputPortCount = 1
    outer.setOperatorId("outer")
    val src = limit("src", 0); val sink = limit("sink", 1)
    val plan = LogicalPlan(
      operators = List(src, outer, sink),
      links = List(
        LogicalLink(src.operatorIdentifier, PortIdentity(0), outer.operatorIdentifier, PortIdentity(0)),
        LogicalLink(outer.operatorIdentifier, PortIdentity(0), sink.operatorIdentifier, PortIdentity(0))
      )
    )
    val ex = intercept[IllegalStateException] { MacroExpander.expand(plan, registry) }
    ex.getMessage.toLowerCase should include("depth")
  }

  it should "give each instance its own prefix when the same macro is used twice" in {
    val body = MacroBody(
      operators = List(inMarker(0, "in"), limit("inner", 9), outMarker(0, "out")),
      links = List(
        MacroLink("in", PortIdentity(0), "inner", PortIdentity(0)),
        MacroLink("inner", PortIdentity(0), "out", PortIdentity(0))
      )
    )
    val inst1 = snapshotInstance("first", "shared", body)
    val inst2 = snapshotInstance("second", "shared", body)
    val src = limit("src", 0); val sink = limit("sink", 1)
    val plan = LogicalPlan(
      operators = List(src, inst1, inst2, sink),
      links = List(
        LogicalLink(src.operatorIdentifier, PortIdentity(0), inst1.operatorIdentifier, PortIdentity(0)),
        LogicalLink(inst1.operatorIdentifier, PortIdentity(0), inst2.operatorIdentifier, PortIdentity(0)),
        LogicalLink(inst2.operatorIdentifier, PortIdentity(0), sink.operatorIdentifier, PortIdentity(0))
      )
    )
    val out = MacroExpander.expand(plan, MacroRegistry.Empty)
    val ids = out.operators.collect { case l: LimitOpDesc => l.operatorIdentifier.id }.toSet
    ids should contain("first--inner")
    ids should contain("second--inner")
    val edges = out.links.map(l => (l.fromOpId.id, l.toOpId.id)).toSet
    edges shouldBe Set(
      "src" -> "first--inner",
      "first--inner" -> "second--inner",
      "second--inner" -> "sink"
    )
  }

  it should "fan out a single external input port to multiple inner consumers" in {
    val body = MacroBody(
      operators = List(
        inMarker(0, "in"),
        limit("consumerA", 1),
        limit("consumerB", 2),
        outMarker(0, "out")
      ),
      links = List(
        MacroLink("in", PortIdentity(0), "consumerA", PortIdentity(0)),
        MacroLink("in", PortIdentity(0), "consumerB", PortIdentity(0)),
        MacroLink("consumerA", PortIdentity(0), "out", PortIdentity(0))
      )
    )
    val inst = snapshotInstance("FanOut", "macro-fan", body)
    val src = limit("src", 0); val sink = limit("sink", 1)
    val plan = LogicalPlan(
      operators = List(src, inst, sink),
      links = List(
        LogicalLink(src.operatorIdentifier, PortIdentity(0), inst.operatorIdentifier, PortIdentity(0)),
        LogicalLink(inst.operatorIdentifier, PortIdentity(0), sink.operatorIdentifier, PortIdentity(0))
      )
    )
    val out = MacroExpander.expand(plan, MacroRegistry.Empty)
    val srcOutTargets =
      out.links.filter(_.fromOpId == src.operatorIdentifier).map(_.toOpId.id).toSet
    srcOutTargets shouldBe Set("FanOut--consumerA", "FanOut--consumerB")
  }

  it should "fail clearly when a LIVE macro is missing from the registry" in {
    val inst = new MacroOpDesc
    inst.macroId = "missing"
    inst.macroVersion = 5
    inst.linkMode = MacroOpDesc.LIVE
    inst.inputPortCount = 1
    inst.outputPortCount = 1
    inst.setOperatorId("inst")
    val src = limit("src", 0)
    val plan = LogicalPlan(
      operators = List(src, inst),
      links = List(
        LogicalLink(src.operatorIdentifier, PortIdentity(0), inst.operatorIdentifier, PortIdentity(0))
      )
    )
    val ex = intercept[IllegalArgumentException] {
      MacroExpander.expand(plan, MacroRegistry.Empty)
    }
    ex.getMessage.toLowerCase should include("not found")
    ex.getMessage should include("missing")
  }

  it should "leave the persisted snapshot body unmutated across two expansions" in {
    val body = MacroBody(
      operators = List(inMarker(0, "in"), limit("inner", 1), outMarker(0, "out")),
      links = List(
        MacroLink("in", PortIdentity(0), "inner", PortIdentity(0)),
        MacroLink("inner", PortIdentity(0), "out", PortIdentity(0))
      )
    )
    val inst = snapshotInstance("once", "m", body)
    val src = limit("src", 0); val sink = limit("sink", 1)
    val plan = LogicalPlan(
      operators = List(src, inst, sink),
      links = List(
        LogicalLink(src.operatorIdentifier, PortIdentity(0), inst.operatorIdentifier, PortIdentity(0)),
        LogicalLink(inst.operatorIdentifier, PortIdentity(0), sink.operatorIdentifier, PortIdentity(0))
      )
    )

    val first = MacroExpander.expand(plan, MacroRegistry.Empty)
    val innerInBodyAfterFirst =
      body.operators.collectFirst { case l: LimitOpDesc => l.operatorIdentifier.id }
    innerInBodyAfterFirst shouldBe Some("inner") // not "once--inner" — body wasn't mutated.

    // Re-expand a fresh plan that reuses the SAME body object: must still inline cleanly.
    val inst2 = snapshotInstance("twice", "m", body)
    val plan2 = LogicalPlan(
      operators = List(src, inst2, sink),
      links = List(
        LogicalLink(src.operatorIdentifier, PortIdentity(0), inst2.operatorIdentifier, PortIdentity(0)),
        LogicalLink(inst2.operatorIdentifier, PortIdentity(0), sink.operatorIdentifier, PortIdentity(0))
      )
    )
    val second = MacroExpander.expand(plan2, MacroRegistry.Empty)
    val secondIds = second.operators.collect { case l: LimitOpDesc => l.operatorIdentifier.id }.toSet
    secondIds should contain("twice--inner")

    val firstIds = first.operators.collect { case l: LimitOpDesc => l.operatorIdentifier.id }.toSet
    firstIds should contain("once--inner")
  }

  // ---------- full-compile path: schema propagation + error attribution ----------

  it should "compile a workflow whose source feeds a macro body, propagating schemas through the inline" in {
    val body = MacroBody(
      operators = List(inMarker(0, "in"), limit("inner", 10), outMarker(0, "out")),
      links = List(
        MacroLink("in", PortIdentity(0), "inner", PortIdentity(0)),
        MacroLink("inner", PortIdentity(0), "out", PortIdentity(0))
      )
    )
    val inst = snapshotInstance("MyMacro-1", "macro-A", body)

    val csvOp = new CSVScanSourceOpDesc()
    csvOp.fileName = Some("workflow-compiling-service/src/test/resources/country_sales_small.csv")
    csvOp.customDelimiter = Some(",")
    csvOp.hasHeader = true
    csvOp.setOperatorId("CSVScan-A")

    val sink = limit("sink", 5)

    val pojo = LogicalPlanPojo(
      operators = List(csvOp, inst, sink),
      links = List(
        LogicalLink(csvOp.operatorIdentifier, PortIdentity(0), inst.operatorIdentifier, PortIdentity(0)),
        LogicalLink(inst.operatorIdentifier, PortIdentity(0), sink.operatorIdentifier, PortIdentity(0))
      ),
      opsToViewResult = List(),
      opsToReuseResult = List()
    )

    val ctx = new WorkflowContext(workflowId = WorkflowIdentity(0))
    val result = new WorkflowCompiler(ctx).compile(pojo)

    result.operatorIdToError shouldBe empty
    result.physicalPlan should not be empty
    // Inner op got the source's schema propagated through the macro boundary.
    val innerSchema = result.operatorIdToOutputSchemas(OperatorIdentity("MyMacro-1--inner"))
    innerSchema.values.head shouldBe defined
  }

  it should "propagate schemas through a LIVE-mode macro to multiple downstream ops on the parent canvas" in {
    // Mirrors the user-reported failure shape:
    //   CSV → Macro(LIVE, macroId=265) → mid → end
    // where the macro body is a single one-to-one op wrapped in markers.
    val body = MacroBody(
      operators = List(inMarker(0, "in"), limit("inner", 10), outMarker(0, "out")),
      links = List(
        MacroLink("in", PortIdentity(0), "inner", PortIdentity(0)),
        MacroLink("inner", PortIdentity(0), "out", PortIdentity(0))
      )
    )
    val registry = MacroRegistry.inMemory(Map(("265", 1) -> body))

    val inst = new MacroOpDesc
    inst.macroId = "265"
    inst.macroVersion = 1
    inst.linkMode = MacroOpDesc.LIVE
    inst.inputPortCount = 1
    inst.outputPortCount = 1
    inst.setOperatorId("Macro-operator-acc00f1c")

    val csvOp = new CSVScanSourceOpDesc()
    csvOp.fileName = Some("workflow-compiling-service/src/test/resources/country_sales_small.csv")
    csvOp.customDelimiter = Some(",")
    csvOp.hasHeader = true
    csvOp.setOperatorId("CSVScan")

    val mid = limit("mid", 5)
    val end = limit("end", 2)

    val pojo = LogicalPlanPojo(
      operators = List(csvOp, inst, mid, end),
      links = List(
        LogicalLink(csvOp.operatorIdentifier, PortIdentity(0), inst.operatorIdentifier, PortIdentity(0)),
        LogicalLink(inst.operatorIdentifier, PortIdentity(0), mid.operatorIdentifier, PortIdentity(0)),
        LogicalLink(mid.operatorIdentifier, PortIdentity(0), end.operatorIdentifier, PortIdentity(0))
      ),
      opsToViewResult = List(),
      opsToReuseResult = List()
    )

    val ctx = new WorkflowContext(workflowId = WorkflowIdentity(0))
    val result = new WorkflowCompiler(ctx, registry).compile(pojo)

    result.operatorIdToError shouldBe empty
    result.physicalPlan should not be empty
    // Inner op got its schema, and the downstream canvas ops did too.
    val outputKeys = result.operatorIdToOutputSchemas.keys.map(_.id).toSet
    outputKeys should contain("Macro-operator-acc00f1c--inner")
    outputKeys should contain("mid")
    outputKeys should contain("end")
  }

  it should "surface the macro on the canvas as failing when the LIVE macro body's inner op lacks its MacroInput link" in {
    // The macro body has both markers, but the MacroInput → inner-op link is
    // missing. After expansion, the inner op is disconnected from the parent's
    // upstream — so its schema can't be computed, and that cascades to every
    // downstream canvas op. Before the visible-id remap, the macro itself looked
    // fine in `operatorErrors` (only `Macro/inner` was keyed there), so the
    // canvas would only mark the *downstream* ops red — confusing the user since
    // the root cause is the macro.
    val body = MacroBody(
      operators = List(inMarker(0, "in"), limit("inner", 10), outMarker(0, "out")),
      links = List(
        // intentionally NO link from "in" to "inner"
        MacroLink("inner", PortIdentity(0), "out", PortIdentity(0))
      )
    )
    val registry = MacroRegistry.inMemory(Map(("265", 1) -> body))

    val inst = new MacroOpDesc
    inst.macroId = "265"
    inst.macroVersion = 1
    inst.linkMode = MacroOpDesc.LIVE
    inst.inputPortCount = 1
    inst.outputPortCount = 1
    inst.setOperatorId("Macro-operator-acc00f1c")

    val csvOp = new CSVScanSourceOpDesc()
    csvOp.fileName = Some("workflow-compiling-service/src/test/resources/country_sales_small.csv")
    csvOp.customDelimiter = Some(",")
    csvOp.hasHeader = true
    csvOp.setOperatorId("CSVScan")

    val mid = limit("mid", 5)
    val end = limit("end", 2)

    val pojo = LogicalPlanPojo(
      operators = List(csvOp, inst, mid, end),
      links = List(
        LogicalLink(csvOp.operatorIdentifier, PortIdentity(0), inst.operatorIdentifier, PortIdentity(0)),
        LogicalLink(inst.operatorIdentifier, PortIdentity(0), mid.operatorIdentifier, PortIdentity(0)),
        LogicalLink(mid.operatorIdentifier, PortIdentity(0), end.operatorIdentifier, PortIdentity(0))
      ),
      opsToViewResult = List(),
      opsToReuseResult = List()
    )

    val ctx = new WorkflowContext(workflowId = WorkflowIdentity(0))
    val result = new WorkflowCompiler(ctx, registry).compile(pojo)

    result.physicalPlan shouldBe empty
    val keys = result.operatorIdToError.keys.map(_.id).toSet
    keys should contain("Macro-operator-acc00f1c") // the macro instance, not "Macro-operator-acc00f1c--inner"
    keys should contain("mid")
    keys should contain("end")
    result.operatorIdToError(OperatorIdentity("Macro-operator-acc00f1c")).message should
      include("Macro-operator-acc00f1c--inner")
  }

  it should "still compile the outer workflow when a dangling inner op inside the macro has a schema error" in {
    // Main path: in → inner1 → out (valid, schema flows through).
    // Dangling side branch: inner2 has no upstream in the body → schema error.
    // The outer canvas (CSVScan → Macro → sink) should still compile; only the
    // macro shows red. Previously the dangling error set physicalPlan to None.
    val body = MacroBody(
      operators = List(
        inMarker(0, "in"),
        limit("inner1", 10),
        limit("inner2", 5), // disconnected — no link to/from anything
        outMarker(0, "out")
      ),
      links = List(
        MacroLink("in", PortIdentity(0), "inner1", PortIdentity(0)),
        MacroLink("inner1", PortIdentity(0), "out", PortIdentity(0))
      )
    )
    val registry = MacroRegistry.inMemory(Map(("265", 1) -> body))

    val inst = new MacroOpDesc
    inst.macroId = "265"
    inst.macroVersion = 1
    inst.linkMode = MacroOpDesc.LIVE
    inst.inputPortCount = 1
    inst.outputPortCount = 1
    inst.setOperatorId("Macro-operator-acc00f1c")

    val csvOp = new CSVScanSourceOpDesc()
    csvOp.fileName = Some("workflow-compiling-service/src/test/resources/country_sales_small.csv")
    csvOp.customDelimiter = Some(",")
    csvOp.hasHeader = true
    csvOp.setOperatorId("CSVScan")

    val sink = limit("sink", 5)

    val pojo = LogicalPlanPojo(
      operators = List(csvOp, inst, sink),
      links = List(
        LogicalLink(csvOp.operatorIdentifier, PortIdentity(0), inst.operatorIdentifier, PortIdentity(0)),
        LogicalLink(inst.operatorIdentifier, PortIdentity(0), sink.operatorIdentifier, PortIdentity(0))
      ),
      opsToViewResult = List(),
      opsToReuseResult = List()
    )

    val ctx = new WorkflowContext(workflowId = WorkflowIdentity(0))
    val result = new WorkflowCompiler(ctx, registry).compile(pojo)

    // Outer workflow compiles despite the dangling inner error.
    result.physicalPlan should not be empty
    // Error is attributed to the macro on the canvas, not the inner op.
    result.operatorIdToError.keys.map(_.id) should contain("Macro-operator-acc00f1c")
    result.operatorIdToError.keys.map(_.id) should not contain "sink"
  }

  it should "attribute a schema error inside a macro body to the visible macro instance, not the prefixed inner op" in {
    // body: in → limit("inner", 7) → out — limit's input schema can't be computed when
    // the macro has no upstream connection on the parent canvas.
    val body = MacroBody(
      operators = List(inMarker(0, "in"), limit("inner", 7), outMarker(0, "out")),
      links = List(
        MacroLink("in", PortIdentity(0), "inner", PortIdentity(0)),
        MacroLink("inner", PortIdentity(0), "out", PortIdentity(0))
      )
    )
    val inst = snapshotInstance("Lonely", "macro-A", body)

    val pojo = LogicalPlanPojo(
      operators = List(inst),
      links = List(),
      opsToViewResult = List(),
      opsToReuseResult = List()
    )

    val ctx = new WorkflowContext(workflowId = WorkflowIdentity(0))
    val result = new WorkflowCompiler(ctx).compile(pojo)

    // The frontend canvas only shows "Lonely" — error must be keyed under that ID,
    // not the post-expansion "Lonely--inner", or it never reaches the macro node UI.
    result.operatorIdToError.keys.map(_.id).toSet shouldBe Set("Lonely")
    val err = result.operatorIdToError.values.head
    err.operatorId shouldBe "Lonely"
    // Inner op id stays in the message so the developer knows which body op blew up.
    err.message should include("Lonely--inner")
    err.message should include("schema is not available")
  }

  it should "short-circuit standalone macro-body compiles (markers present, no MacroOpDesc) to a clean success" in {
    // Mirrors the drill-down editor: the frontend reloads `workflow.content` for a
    // macro and the body — markers + inner ops, NO MacroOpDesc — gets fed straight
    // into /compile by the singleton WorkflowCompilingService. Pre-fix, the markers
    // threw IllegalStateException, every inner op downstream failed schema
    // propagation, and the resulting "Failed" state would persist across the
    // drill-down → parent navigation in the singleton compile-state, making the
    // parent canvas look broken until the parent's own compile finished.
    val pojo = LogicalPlanPojo(
      operators = List(inMarker(0, "in"), limit("inner", 10), outMarker(0, "out")),
      links = List(
        LogicalLink(OperatorIdentity("in"), PortIdentity(0), OperatorIdentity("inner"), PortIdentity(0)),
        LogicalLink(OperatorIdentity("inner"), PortIdentity(0), OperatorIdentity("out"), PortIdentity(0))
      ),
      opsToViewResult = List(),
      opsToReuseResult = List()
    )

    val ctx = new WorkflowContext(workflowId = WorkflowIdentity(0))
    val result = new WorkflowCompiler(ctx).compile(pojo)

    result.operatorIdToError shouldBe empty
    result.physicalPlan should not be empty
    result.physicalPlan.get.operators shouldBe empty
    result.operatorIdToOutputSchemas shouldBe empty
  }

  it should "still compile a parent that uses a macro instance (short-circuit does NOT apply post-expansion markers)" in {
    // Regression guard: the short-circuit fires on the *raw* plan before
    // MacroExpander runs. A parent canvas legitimately holds a MacroOpDesc
    // (which carries markers in its embedded body) and must take the full
    // compile path. Otherwise we'd silently swallow real parent compiles.
    val body = MacroBody(
      operators = List(inMarker(0, "in"), limit("inner", 10), outMarker(0, "out")),
      links = List(
        MacroLink("in", PortIdentity(0), "inner", PortIdentity(0)),
        MacroLink("inner", PortIdentity(0), "out", PortIdentity(0))
      )
    )
    val inst = snapshotInstance("ParentMacro", "macro-A", body)

    val csvOp = new CSVScanSourceOpDesc()
    csvOp.fileName = Some("workflow-compiling-service/src/test/resources/country_sales_small.csv")
    csvOp.customDelimiter = Some(",")
    csvOp.hasHeader = true
    csvOp.setOperatorId("CSVScan-A")

    val pojo = LogicalPlanPojo(
      operators = List(csvOp, inst),
      links = List(
        LogicalLink(csvOp.operatorIdentifier, PortIdentity(0), inst.operatorIdentifier, PortIdentity(0))
      ),
      opsToViewResult = List(),
      opsToReuseResult = List()
    )

    val ctx = new WorkflowContext(workflowId = WorkflowIdentity(0))
    val result = new WorkflowCompiler(ctx).compile(pojo)

    result.operatorIdToError shouldBe empty
    result.physicalPlan should not be empty
    // The expanded plan should have actually compiled — non-empty physical ops,
    // proving we took the full path, not the short-circuit.
    result.physicalPlan.get.operators should not be empty
  }

  // ---------- helpers ----------

  private def limit(id: String, lim: Int): LimitOpDesc = {
    val l = new LimitOpDesc
    l.limit = lim
    l.setOperatorId(id)
    l
  }

  private def inMarker(idx: Int, id: String): MacroInputOp = {
    val m = new MacroInputOp
    m.portIndex = idx
    m.setOperatorId(id)
    m
  }

  private def outMarker(idx: Int, id: String): MacroOutputOp = {
    val m = new MacroOutputOp
    m.portIndex = idx
    m.setOperatorId(id)
    m
  }

  private def snapshotInstance(
      instanceId: String,
      macroId: String,
      body: MacroBody
  ): MacroOpDesc = {
    val m = new MacroOpDesc
    m.macroId = macroId
    m.macroVersion = 1
    m.linkMode = MacroOpDesc.SNAPSHOT
    m.snapshot = Some(body)
    m.inputPortCount = 1
    m.outputPortCount = 1
    m.setOperatorId(instanceId)
    m
  }
}
