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

import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.util.JSONUtils.objectMapper
import com.fasterxml.jackson.databind.JsonNode
import org.apache.texera.common.compiler.MacroTestUtils._
import com.fasterxml.jackson.databind.node.ObjectNode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._
import org.apache.texera.common.compiler.model.{LogicalPlan, LogicalPlanPojo}
import org.apache.texera.amber.core.virtualidentity.WorkflowIdentity
import org.apache.texera.amber.core.workflow.WorkflowContext

class MacroExpansionSpec extends AnyFlatSpec with Matchers {
  private val cases = MacroExpansionCases.cases

  cases.foreach { example =>
    "Backend macro expansion" should example.get("name").asText() in {
      val content = example.get("content")
      val original = content.deepCopy[JsonNode]()
      if (example.has("error")) {
        val error = intercept[IllegalArgumentException](MacroExpansion.expand(content))
        error.getMessage should include(example.get("error").asText())
      } else {
        val expanded = MacroExpansion.expand(content)
        expanded.content shouldBe example.at("/expected/content")
        objectMapper.valueToTree[JsonNode](expanded.paths) shouldBe
          example.at("/expected/paths")
        example.at("/expected/ports").fields().asScala.foreach { port =>
          objectMapper.valueToTree[JsonNode](
            expanded.ports(port.getKey)
          ) shouldBe port.getValue
        }
      }
      content shouldBe original
    }
  }

  private def pinnedSource: ObjectNode = {
    val body =
      forwardingBody("source", textSource("leaf", "original"), Nil, List("out" -> "output-0"))
    body.set[JsonNode]("formBinding", formBinding(field("text", "leaf", "textInput")))
    val usage = instance("macro", "source", outputs = List("out"))
    objectAt(usage, "/operatorProperties").put("text", "changed")
    pinned(graph(List(usage)), definition("source", "Source", body))
  }

  private def firstBody(content: JsonNode): ObjectNode =
    objectAt(content, "/macroDependencies/0/content")

  private def expansionError(content: JsonNode, drafts: List[MacroDraft] = Nil): String = {
    val before = content.deepCopy[JsonNode]()
    val error = intercept[IllegalArgumentException](MacroExpansion.expand(content, drafts))
    content shouldBe before
    error.getMessage
  }

  it should "apply a nested preview only at its occurrence, even at the largest saved revision" in {
    val version = 9007199254740991L
    val leaf = instance("child", "source", outputs = List("out"), version = version)
    val parent = macroGraph("parent", List(leaf))
    val original = firstBody(pinnedSource)
    val root = pinned(
      graph(List(instance("one", "parent"), instance("two", "parent"))),
      definition("parent", "Parent", parent),
      definition("source", "Source", original).put("version", version)
    )
    val before = root.deepCopy[JsonNode]()
    val edited = original.deepCopy[ObjectNode]()
    objectAt(edited, "/operators/0/operatorProperties").put("textInput", "draft")
    val path = List("one", "child")
    val preview = MacroDraft(path, edited)
    val expanded = MacroExpansion.expand(root, List(preview))
    val operators = expanded.content.path("operators").elements().asScala.toList
    operators.map(_.at("/operatorProperties/textInput").asText()) shouldBe List("draft", "original")
    operators.map(op => expanded.paths(op.path("operatorID").asText()).head) shouldBe List(
      "one",
      "two"
    )
    val self = macroGraph(
      "source",
      List(changed(leaf, "operatorID" -> "again")),
      outputs = List("out"),
      links = List(link("self-out", end("again", "out"), end("source__outputs", "out")))
    )
    val nested = MacroExpansion.expand(root, List(preview.copy(content = self)))
    nested.paths.values should contain(path ++ List("again", "leaf"))
    root shouldBe before
    for (
      (drafts, error) <- List(
        List(preview, preview) -> "Duplicate macro draft",
        List(preview.copy(path = List("missing"))) -> "Unknown macro draft",
        List(preview.copy(content = parent)) -> "Macro draft identity"
      )
    )
      expansionError(root, drafts) should include(error)
  }

  private def boundary(body: JsonNode, input: Boolean): ObjectNode =
    body
      .path("operators")
      .elements()
      .asScala
      .find(_.path("operatorType").asText() == (if (input) "MacroInput" else "MacroOutput"))
      .get
      .asInstanceOf[ObjectNode]

  List("null", "content", "version", "name", "macroInterface").foreach { malformed =>
    "External macro definitions" should s"reject malformed $malformed without changing the submitted snapshot" in {
      val root = pinnedSource
      val definition = objectAt(root, "/macroDependencies/0")
      malformed match {
        case "null" =>
          arrayAt(root, "/macroDependencies")
            .set(0, objectMapper.nullNode())
        case "content" => definition.putNull("content")
        case "version" => definition.put("version", 0)
        case "name"    => definition.put("name", 42)
        case "macroInterface" =>
          objectAt(definition, "/content")
            .putNull("macroInterface")
      }
      expansionError(root) should include("Malformed")
    }
  }

  "Native and empty graphs" should "keep their complete content and deterministic native identities" in {
    for (ids <- List(Nil, List("native"))) {
      val content = graph(
        ids.map(id =>
          json(
            "operatorID" -> id,
            "operatorType" -> "Filter",
            "operatorProperties" -> json(),
            "inputPorts" -> Nil,
            "outputPorts" -> Nil
          )
        )
      )
      val expanded = MacroExpansion.expand(content)
      expanded.content shouldBe content
      expanded.paths shouldBe ids.map(id => id -> List(id)).toMap
    }
  }

  "Exposed optional fields" should "accept an override without a stored native value" in {
    val root = pinnedSource
    objectAt(root, "/macroDependencies/0/content/operators/0")
      .putObject("operatorProperties")
    MacroExpansion
      .expand(root)
      .content
      .at("/operators/0/operatorProperties/textInput")
      .asText() shouldBe "changed"
  }

  List("inputs" -> "input", "outputs" -> "output").foreach {
    case (direction, label) =>
      "Macro boundaries" should s"identify an unfinished $label without changing its snapshot" in {
        val content = pinnedSource
        boundary(firstBody(content), input = direction == "inputs")
          .withArray(if (direction == "inputs") "outputPorts" else "inputPorts")
          .add(objectMapper.readTree("""{"portID":"pending","displayName":"Rows"}"""))
        expansionError(content) shouldBe
          s"Source: Connect or remove macro $label 'Rows' before running."
      }
  }

  "Macro boundaries" should "reject multiple targets on one output with an actionable error" in {
    val content = pinnedSource
    val edges = arrayAt(content, "/macroDependencies/0/content/links")
    edges.add(changed(edges.get(0), "linkID" -> "duplicate-output"))
    expansionError(content) shouldBe
      "Source: Macro output 'out' must connect to exactly one operator output."
  }

  "Macro logical plans" should "deserialize pinned graphs and normalize result and reuse selections" in {
    val pojo = LogicalPlanPojo(Nil, Nil, List("macro"), List("macro"), Some(pinnedSource))
    val decoded =
      objectMapper.readValue(objectMapper.writeValueAsString(pojo), classOf[LogicalPlanPojo])
    val normalized = decoded.normalized
    val leaf = normalized.operators.head
    leaf.operatorIdentifier.id shouldBe decoded.macroExpansion.get.paths.keys.head
    normalized.opsToViewResult shouldBe List(leaf.operatorIdentifier.id)
    normalized.opsToReuseResult shouldBe normalized.opsToViewResult
    normalized.macroContent shouldBe None
    LogicalPlan(decoded).operators.size shouldBe 1
  }

  it should "compile macros through the ordinary compiler and leave the snapshot untouched" in {
    val content = pinnedSource
    val original = content.deepCopy[JsonNode]()
    val pojo = macroPlan(content)
    val result = new WorkflowCompiler(
      new WorkflowContext(WorkflowIdentity(0)),
      TestOperators.fixtureFileResolver
    )
      .compile(pojo, CompilationErrorHandling.Strict)
    result.physicalPlan should not be empty
    result.logicalPlan.operators.size shouldBe 1
    content shouldBe original
  }

  it should "filter selected canvas nodes after validating the whole graph" in {
    val pojo = LogicalPlanPojo(Nil, Nil, List("macro"), Nil, Some(pinnedSource), Some(Nil))
    pojo.normalized.operators shouldBe empty
    pojo.normalized.opsToViewResult shouldBe empty
    val invalid = pinnedSource
    objectAt(invalid)
      .putArray("macroDependencies")
    intercept[IllegalArgumentException](
      pojo.copy(macroContent = Some(invalid)).normalized
    ).getMessage should include("Missing macro version")
  }

  it should "report malformed macros as compilation errors in lenient mode and reject them in strict mode" in {
    val invalid = pinnedSource
    objectAt(invalid)
      .putArray("macroDependencies")
    arrayAt(invalid, "/operators")
      .insert(
        0,
        pinnedSource.at("/macroDependencies/0/content/operators/0")
      )
    val pojo = macroPlan(invalid)
    val compiler = new WorkflowCompiler(
      new WorkflowContext(WorkflowIdentity(0)),
      TestOperators.fixtureFileResolver
    )
    val result = compiler.compile(pojo)
    result.physicalPlan shouldBe None
    result.operatorIdToError.keys.map(_.id).toList shouldBe List("macro")
    result.operatorIdToError.values.head.message should include("Missing macro version")
    intercept[IllegalArgumentException](compiler.compile(pojo, CompilationErrorHandling.Strict))
  }

  it should "preserve the declared output order when selecting results" in {
    val content = pinnedSource
    val body = firstBody(content)
    val second = changed(body.at("/operators/0"), "operatorID" -> "second")
    arrayAt(body, "/operators").add(second)
    val terminal = boundary(body, input = false)
    terminal.withArray("inputPorts").add(objectMapper.readTree("""{"portID":"second"}"""))
    arrayAt(body, "/links")
      .add(
        link(
          "second-output",
          end("second", "output-0"),
          end(terminal.path("operatorID").asText(), "second")
        )
      )
    arrayAt(content, "/operators/0/outputPorts")
      .insert(0, objectMapper.readTree("""{"portID":"second"}"""))
    val pojo = macroPlan(content, List("macro"))
    val ids = pojo.macroExpansion.get.paths.map { case (id, path) => path.last -> id }
    pojo.normalized.opsToViewResult shouldBe List(ids("second"), ids("leaf"))
    val ports = objectMapper.valueToTree[JsonNode](pojo.macroExpansion.get).at("/resultPorts/macro")
    ports.get(0).path("operatorID").asText() shouldBe ids("second")
    ports.get(1).path("operatorID").asText() shouldBe ids("leaf")
    ports.get(1).path("portIndex").asInt(-1) shouldBe 0
  }

  it should "reject numeric pins and malformed bundles instead of accepting coerced values" in {
    val content = pinnedSource
    objectAt(content, "/operators/0")
      .put("operatorVersion", 1)
    intercept[IllegalArgumentException](MacroExpansion.expand(content))
    val invalid = objectMapper.readTree("""{"operators":[],"links":[],"macroDependencies":{}}""")
    intercept[IllegalArgumentException](MacroExpansion.expand(invalid))
  }

  "Nested macro expansion" should "return resolved boundaries at every depth for the live inspector" in {
    val example = cases.head
    val expansion = MacroExpansion.expand(example.path("content"))
    val inspectors = objectMapper.valueToTree[JsonNode](expansion).path("inspectors")
    for ((runtime, path) <- expansion.paths if path.length > 1) {
      val scope = objectMapper.writeValueAsString(path.init)
      inspectors.path(scope).path("ids").path(path.last).asText() shouldBe runtime
    }
    expansion.ports.size should be > example.at("/expected/ports").size()
    expansion.ports.values.flatten.foreach { port =>
      expansion.paths should contain key port.path("operatorID").asText()
      expansion.content
        .path("operators")
        .elements()
        .asScala
        .find(_.path("operatorID") == port.path("operatorID")) should not be empty
    }
  }

  it should "keep boundary links and native source ordinals on the correct occurrence" in {
    val example = cases.find(_.path("name").asText() == "input fanout and multiple outputs").get
    val root = example.path("content").deepCopy[ObjectNode]()
    val source =
      root.path("operators").elements().asScala.find(_.path("operatorID").asText() == "source").get
    arrayAt(source, "/outputPorts").insert(0, json("portID" -> "unused"))
    val expansion = MacroExpansion.expand(root)
    val view = expansion.inspectors("""["m"]""")
    val links = expansion.content.path("links")
    List("a", "b").foreach { local =>
      view.inputs(local) should have size 1
      val input = view.inputs(local).head
      input.input shouldBe 0
      input.output shouldBe 1
      links.get(input.link).at("/target/operatorID").asText() shouldBe view.ids(local)
    }
    val exposed = view.terminals.map(p => p.portID -> p.links.map(i => links.get(i))).toMap
    exposed("in").map(_.at("/source/operatorID").asText()) shouldBe List("source", "source")
    exposed("a") shouldBe empty
    exposed("b").map(_.at("/target/operatorID").asText()) shouldBe List("sink")
  }

  it should "distinguish two exposed inputs wired to the same native port" in {
    val ports = List("left", "right")
    val body =
      forwardingBody("join", operator("leaf", "Filter", List("in")), ports.map(_ -> "in"), Nil)
    val root = pinned(
      graph(
        ports.map(id => textSource(id, id)) :+ instance("m", "join", ports),
        ports.map(id => link(s"wire-$id", end(id, "output-0"), end("m", id)))
      ),
      definition("join", "Join", body)
    )
    val expanded = MacroExpansion.expand(root)
    val links = expanded.content.path("links")
    val inputs = expanded.inspectors("[]").inputs("m")
    inputs.map(i =>
      ports(i.input) -> links.get(i.link).at("/source/operatorID").asText()
    ) shouldBe ports.zip(ports)
    expanded.inspectors("""["m"]""").terminals.foreach { port =>
      port.links.map(i => links.get(i).at("/source/operatorID").asText()) shouldBe List(port.portID)
    }
  }

  it should "scale the occurrence index linearly for repeated sibling definitions" in {
    def index(width: Int) = {
      val root = cases.head.path("content")
      val sibling = root.at("/operators/0")
      val usages = (0 until width).map(i => changed(sibling, "operatorID" -> s"copy-$i"))
      MacroExpansion.expand(changed(root, "operators" -> usages)).inspectors
    }
    val one = index(1)
    val many = index(32)
    many.size - 1 shouldBe 32 * (one.size - 1)
    many.values.map(_.ids.size).sum shouldBe 32 * one.values.map(_.ids.size).sum
  }

  it should "allow 32 pinned levels and reject the next level without changing the snapshots" in {
    def nested(depth: Int): JsonNode = {
      val root = pinnedSource
      val definitions = root.withArray("macroDependencies")
      var child = instance("macro", "source", outputs = List("out"))
      for (level <- 1 until depth) {
        val id = s"level-$level"
        val body = macroGraph(id, List(child))
        definitions.add(definition(id, id, body))
        child = instance(id, id)
      }
      root.withArray("operators").removeAll().add(child)
      root
    }
    MacroExpansion.expand(nested(32)).content.path("operators").size() shouldBe 1
    expansionError(nested(33)) should include("32 levels")
  }
  private def terminalBody: ObjectNode = firstBody(passthroughWorkflow)

  "Saved macro terminals" should "lower passthrough links without adding runtime operators or changing pins" in {
    val content = passthroughWorkflow
    val before = content.toString
    val expanded = MacroExpansion.expand(content)
    expanded.paths.keySet shouldBe Set("source", "sink")
    expanded.content.path("operators").size() shouldBe 2
    expanded.content.path("links").size() shouldBe 1
    expanded.content
      .at("/links/0/source/operatorID")
      .asText() shouldBe "source"
    expanded.content.at("/links/0/target/operatorID").asText() shouldBe "sink"
    expanded
      .ports("[\"macro\",\"inputs\",\"rows\"]")
      .head
      .path("operatorID")
      .asText() shouldBe "sink"
    expanded
      .ports("[\"macro\",\"outputs\",\"out\"]")
      .head
      .path("operatorID")
      .asText() shouldBe "source"
    val pojo = macroPlan(content, List("macro"))
    pojo.normalized.operators.size shouldBe 2
    pojo.normalized.opsToViewResult shouldBe List("source")
    new WorkflowCompiler(
      new WorkflowContext(WorkflowIdentity(0)),
      TestOperators.fixtureFileResolver
    )
      .compile(pojo, CompilationErrorHandling.Strict)
      .physicalPlan should not be empty
    content.toString shouldBe before
  }

  it should "deduplicate repeated input routes independently of endpoint JSON key order" in {
    val example = cases.find(_.path("name").asText() == "input fanout and multiple outputs").get
    val content = example.path("content").deepCopy[JsonNode]()
    val body = firstBody(content)
    val input = boundary(body, input = true)
    val links = arrayAt(body, "/links")
    val original =
      links.elements().asScala.find(_.at("/source/operatorID") == input.path("operatorID")).get
    val duplicate = original.deepCopy[ObjectNode]().put("linkID", "again")
    for (direction <- List("source", "target"))
      duplicate.set[JsonNode](
        direction,
        json(
          "portID" -> original.at(s"/$direction/portID"),
          "operatorID" -> original.at(s"/$direction/operatorID")
        )
      )
    links.add(duplicate)
    val before = content.toString
    val result = MacroExpansion.expand(content)
    result.content shouldBe example.at("/expected/content")
    content.toString shouldBe before
  }

  it should "reject missing, duplicate, disabled and out-of-context terminals with clear errors" in {
    val disconnected = passthroughWorkflow
    arrayAt(firstBody(disconnected), "/links").removeAll()
    expansionError(disconnected) should include("Connect or remove")
    val duplicate = terminalBody
    arrayAt(duplicate, "/operators").add(
      changed(duplicate.at("/operators/0"), "operatorID" -> "another-input")
    )
    expansionError(duplicate) should include("exactly one")
    val disabled = terminalBody
    objectAt(disabled, "/operators/0").put("isDisabled", true)
    expansionError(disabled) should include("cannot be disabled")
    val outside = terminalBody
    outside.remove("macroInterface")
    expansionError(outside) should include("macro definition")
  }

  it should "detect cycles made entirely of compile-time boundaries" in {
    val root = passthroughWorkflow
    val first = root.at("/operators/1")
    val second = changed(first, "operatorID" -> "second")
    root.putArray("operators").add(first).add(second)
    root
      .putArray("links")
      .add(link("forward", end("macro", "out"), end("second", "rows")))
      .add(link("back", end("second", "out"), end("macro", "rows")))
    expansionError(root).toLowerCase should include("cycle")
  }

}
