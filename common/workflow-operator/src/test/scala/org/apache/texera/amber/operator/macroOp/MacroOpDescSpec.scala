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

package org.apache.texera.amber.operator.macroOp

import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MacroOpDescSpec extends AnyFlatSpec with Matchers {

  "MacroOpDesc" should "round-trip through Jackson with all fields preserved" in {
    val inner = new LimitOpDesc
    inner.limit = 5
    inner.setOperatorId("inner-limit")

    val body = MacroBody(
      operators = List(makeInputMarker(0, "in"), inner, makeOutputMarker(0, "out")),
      links = List(
        MacroLink("in", PortIdentity(0), "inner-limit", PortIdentity(0)),
        MacroLink("inner-limit", PortIdentity(0), "out", PortIdentity(0))
      ),
      inputs = List(MacroPortSpec(0, "the-input")),
      outputs = List(MacroPortSpec(0, "the-output"))
    )

    val m = new MacroOpDesc
    m.macroId = "wid-42"
    m.macroVersion = 7
    m.linkMode = MacroOpDesc.SNAPSHOT
    m.snapshot = Some(body)
    m.inputPortCount = 1
    m.outputPortCount = 1
    m.displayName = "MyMacro"
    m.setOperatorId("macro-instance-1")

    val json = objectMapper.writeValueAsString(m.asInstanceOf[LogicalOp])
    val restored = objectMapper.readValue(json, classOf[LogicalOp])

    restored shouldBe a[MacroOpDesc]
    val r = restored.asInstanceOf[MacroOpDesc]
    r.macroId shouldBe "wid-42"
    r.macroVersion shouldBe 7
    r.linkMode shouldBe MacroOpDesc.SNAPSHOT
    r.inputPortCount shouldBe 1
    r.outputPortCount shouldBe 1
    r.displayName shouldBe "MyMacro"
    r.operatorIdentifier.id shouldBe "macro-instance-1"

    r.snapshot shouldBe defined
    val rb = r.snapshot.get
    rb.operators should have size 3
    rb.links should have size 2
    rb.inputs shouldBe body.inputs
    rb.outputs shouldBe body.outputs

    // Polymorphic round-trip: inner ops keep their concrete types.
    rb.operators.collect { case l: LimitOpDesc => l.limit } shouldBe List(5)
    rb.operators.collect { case i: MacroInputOp => i.portIndex } shouldBe List(0)
    rb.operators.collect { case o: MacroOutputOp => o.portIndex } shouldBe List(0)
  }

  it should "throw on getPhysicalPlan / getPhysicalOp because expansion must run first" in {
    val m = new MacroOpDesc
    m.macroId = "x"
    val wid = WorkflowIdentity(0L)
    val eid = ExecutionIdentity(0L)
    assertThrows[IllegalStateException] { m.getPhysicalPlan(wid, eid) }
    assertThrows[IllegalStateException] { m.getPhysicalOp(wid, eid) }
  }

  "MacroInputOp / MacroOutputOp" should "round-trip and throw on compile" in {
    val in = makeInputMarker(2, "in-2")
    val out = makeOutputMarker(3, "out-3")
    val inJson = objectMapper.writeValueAsString(in.asInstanceOf[LogicalOp])
    val outJson = objectMapper.writeValueAsString(out.asInstanceOf[LogicalOp])

    val restoredIn =
      objectMapper.readValue(inJson, classOf[LogicalOp]).asInstanceOf[MacroInputOp]
    val restoredOut =
      objectMapper.readValue(outJson, classOf[LogicalOp]).asInstanceOf[MacroOutputOp]
    restoredIn.portIndex shouldBe 2
    restoredOut.portIndex shouldBe 3

    val wid = WorkflowIdentity(0L)
    val eid = ExecutionIdentity(0L)
    assertThrows[IllegalStateException] { restoredIn.getPhysicalPlan(wid, eid) }
    assertThrows[IllegalStateException] { restoredOut.getPhysicalPlan(wid, eid) }
  }

  "MacroOpDesc.operatorInfo" should "expose ports matching inputPortCount/outputPortCount" in {
    val m = new MacroOpDesc
    m.inputPortCount = 2
    m.outputPortCount = 3
    val info = m.operatorInfo
    info.inputPorts.map(_.id.id) shouldBe List(0, 1)
    info.outputPorts.map(_.id.id) shouldBe List(0, 1, 2)
  }

  private def makeInputMarker(idx: Int, id: String): MacroInputOp = {
    val m = new MacroInputOp
    m.portIndex = idx
    m.setOperatorId(id)
    m
  }

  private def makeOutputMarker(idx: Int, id: String): MacroOutputOp = {
    val m = new MacroOutputOp
    m.portIndex = idx
    m.setOperatorId(id)
    m
  }
}
