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
package org.apache.texera.web.resource

import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.util.JSONUtils.objectMapper
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.common.compiler.model.LogicalPlanPojo
import org.apache.texera.common.compiler.MacroTestUtils._
import org.apache.texera.common.compiler.WorkflowCompiler
import org.apache.texera.amber.core.virtualidentity.{OperatorIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PortIdentity, WorkflowContext}
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SyncMacroExecutionSpec extends AnyFlatSpec with Matchers with PrivateMethodTester {
  private val resource = new SyncExecutionResource()

  private def selectedPortPlan: LogicalPlanPojo = {
    val leaf = forwardingBody(
      "leaf",
      operator("split", "Split", List("input-0"), List("output-0", "output-2")),
      List("rows" -> "input-0"),
      List("selected" -> "output-2")
    )
    val child = instance("child", "leaf", List("rows"), List("selected"))
    val parent =
      forwardingBody("parent", child, List("rows" -> "rows"), List("result" -> "selected"))
    val ids = List("selected", "sibling")
    val root = graph(
      textSource("source", "one") :: ids.map(instance(_, "parent", List("rows"), List("result"))),
      ids.map(id => link(s"$id-in", end("source", "output-0"), end(id, "rows")))
    )
    macroPlan(
      pinned(root, definition("parent", "Parent", parent), definition("leaf", "Leaf", leaf)),
      List("source", "selected")
    )
  }

  "Macro result ports" should "retain a nested selected endpoint's dense ordinal and leave sibling pins unchanged" in {
    val plan = selectedPortPlan
    val before = plan.macroContent.get.deepCopy[JsonNode]()
    val leaf = plan.macroExpansion.get.paths.find(_._2 == List("selected", "child", "split")).get._1
    plan.macroResultPorts shouldBe Map("selected" -> (OperatorIdentity(leaf), PortIdentity(1)))
    val browserTarget =
      objectMapper.valueToTree[JsonNode](plan.macroExpansion.get).at("/resultTargets/selected")
    browserTarget.path("operatorID").asText() shouldBe leaf
    browserTarget.path("portIndex").asInt(-1) shouldBe 1
    plan.macroContent.get shouldBe before
    objectMapper
      .valueToTree[JsonNode](plan)
      .has("macroResultPorts") shouldBe false
    plan.copy(opsToViewResult = List("sibling")).macroResultPorts.keys.toList shouldBe List(
      "sibling"
    )
  }

  it should "leave native selections unchanged and reject missing pinned definitions" in {
    LogicalPlanPojo(Nil, Nil, List("native"), Nil).macroResultPorts shouldBe empty
    val plan = selectedPortPlan
    val malformed = plan.macroContent.get.deepCopy[ObjectNode]()
    malformed.remove("macroDependencies")
    intercept[IllegalArgumentException](
      plan.copy(macroContent = Some(malformed)).macroResultPorts
    ).getMessage should include("Missing macro version")
  }

  private def reject(plan: LogicalPlanPojo): String = {
    val result = resource.executeWorkflowSync(
      Long.MaxValue,
      Int.MaxValue,
      SyncExecutionRequest(
        "macro preflight",
        plan,
        None,
        Nil,
        1,
        1000,
        1000
      ),
      // A rejected graph must not reach session, warehouse, database or runtime setup.
      null
    )
    result.success shouldBe false
    result.state shouldBe "CompilationFailed"
    result.errors.get.mkString
  }

  "Execution preflight" should "reject duplicate native IDs before accessing any session or runtime" in {
    val plan = objectMapper.readValue(
      """{"operators":[
        {"operatorID":"same","operatorType":"TextInput","textInput":"one",
         "attributeName":"line","attributeType":"string"},
        {"operatorID":"same","operatorType":"TextInput","textInput":"two",
         "attributeName":"line","attributeType":"string"}
      ],"links":[],"opsToViewResult":[],"opsToReuseResult":[]}""",
      classOf[LogicalPlanPojo]
    )
    reject(plan) should include("Duplicate graph element ID 'same'")
  }

  "Macro execution preflight" should "reject missing pins before initializing or replacing an execution" in {
    reject(macroPlan(graph(List(instance("instance", "missing"))))) should include(
      "Missing macro version"
    )
  }

  it should "reject standalone definitions before initializing or replacing an execution" in {
    reject(macroPlan(macroGraph("definition", Nil))) should include(
      "Run macro definitions inside a workflow"
    )
  }

  it should "reject invalid nested settings before accessing the session or replacing an execution" in {
    val sorting = operator("sort", "StableMergeSort", List("input-0"), List("output-0"))
    val pin = definition("sort", "Sorting", macroGraph("sort", List(sorting)))
    val error = reject(macroPlan(pinned(graph(List(instance("instance", "sort"))), pin)))
    error should include("Sorting / Stable Merge Sort")
    error should include("keys")
  }

  it should "retain nested validation paths when execution selects an upstream sub-DAG" in {
    val projection = operator(
      "project",
      "Projection",
      List("input-0"),
      List("output-0"),
      json("isDrop" -> false, "attributes" -> List(json("originalAttribute" -> "missing")))
    )
    val body = macroGraph(
      "projection",
      List(textSource("text", "one"), projection),
      List(link("link", end("text", "output-0"), end("project", "input-0")))
    )
    val root = pinned(
      graph(List(instance("instance", "projection"))),
      definition("projection", "Projection macro", body)
    )
    val normalized = macroPlan(root).normalized
    val target = normalized.macroExpansion.get.paths.find(_._2.last == "project").get._1
    val select = PrivateMethod[LogicalPlanPojo](Symbol("computeSubDAGIfNeeded"))
    val subDag = resource invokePrivate select(normalized, List(target))
    val compiler = new WorkflowCompiler(
      new WorkflowContext(WorkflowIdentity(0)),
      TestOperators.fixtureFileResolver
    )
    val errors = compiler.compile(subDag).operatorIdToError
    errors(OperatorIdentity("instance")).message should include("Projection macro / Projection")
    errors(OperatorIdentity("instance")).message should include("missing")
    subDag.macroContent shouldBe None
    objectMapper
      .valueToTree[JsonNode](subDag)
      .has("expandedMacros") shouldBe false
  }
}
