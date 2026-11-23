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

package org.apache.texera.service.resource

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.dropwizard.testing.junit5.ResourceExtension
import jakarta.ws.rs.client.Entity
import jakarta.ws.rs.core.{MediaType, Response}
import org.apache.texera.common.compiler.MacroTestUtils._
import org.apache.texera.common.compiler.MacroExpansion
import org.apache.texera.common.compiler.model.{LogicalLink, LogicalPlanPojo}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.apache.texera.amber.operator.source.scan.file.FileScanSourceOpDesc
import org.apache.texera.amber.operator.source.scan.text.TextInputSourceOpDesc
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.assertj.core.api.Assertions.assertThat
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

/**
  * Resource-layer tests for `/compile`. Owns only what the REST envelope
  * adds on top of compilation itself: HTTP status, the
  * `@JsonTypeInfo` discriminator that lets the frontend route success vs
  * failure, and the JSON shape the resource expects on the wire.
  *
  * All compiler-behavior assertions (schema propagation, lenient-mode
  * error accumulation, multi-op chains) live in
  * `org.apache.texera.common.compiler.WorkflowCompilerSpec` so a regression
  * lands on the right spec.
  */
class WorkflowCompilationResourceSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with LoggedInResourceFixture {

  private val resources: ResourceExtension = resourcesFor(new WorkflowCompilationResource())

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    insertUsersAndPrivateDataset()
    resources.before()
  }

  override protected def afterAll(): Unit = {
    resources.after()
    shutdownDB()
  }

  private def csvOp(fileName: String): CSVScanSourceOpDesc = {
    val op = new CSVScanSourceOpDesc()
    op.fileName = Some(fileName)
    op.customDelimiter = Some(",")
    op.hasHeader = true
    op
  }

  // A source that needs no file, for the tests that are about the envelope only.
  private def textOp(text: String): TextInputSourceOpDesc = {
    val op = new TextInputSourceOpDesc()
    op.textInput = text
    op
  }

  private def limitOp(rows: Int): LimitOpDesc = {
    val op = new LimitOpDesc()
    op.limit = rows
    op
  }

  // Scans a whole file as one value, so compiling it resolves the name without reading the file.
  private def wholeFileScanOp(fileName: String): FileScanSourceOpDesc = {
    val op = new FileScanSourceOpDesc()
    op.fileName = Some(fileName)
    op
  }

  private def planOf(operator: LogicalOp): LogicalPlanPojo =
    LogicalPlanPojo(
      operators = List(operator),
      links = List.empty,
      opsToViewResult = List.empty,
      opsToReuseResult = List.empty
    )

  // The frontend serializes LogicalLink with `fromOpId` / `toOpId` as flat
  // strings, but the Scala case class stores them as nested `OperatorIdentity`
  // records. This helper mirrors the wire shape so the test exercises the
  // resource's actual JSON contract instead of a Scala-only round trip.
  private def encodePojoAsFrontendJson(pojo: LogicalPlanPojo): String = {
    val jsonNode = objectMapper.valueToTree[ObjectNode](pojo)
    val linksArray = jsonNode.withArray("links")
    linksArray.forEach { linkNode =>
      val fromOpIdNode = linkNode.get("fromOpId")
      linkNode.asInstanceOf[ObjectNode].put("fromOpId", fromOpIdNode.get("id").asText())
      val toOpIdNode = linkNode.get("toOpId")
      linkNode.asInstanceOf[ObjectNode].put("toOpId", toOpIdNode.get("id").asText())
    }
    objectMapper.writeValueAsString(jsonNode)
  }

  private def postJson(path: String, body: String, as: User = owner): Response =
    resources
      .target(path)
      .request(MediaType.APPLICATION_JSON)
      .header("Authorization", bearerTokenOf(as))
      .post(Entity.json(body))

  private def postCompile(pojo: LogicalPlanPojo, as: User = owner): Response =
    postJson("/compile", encodePojoAsFrontendJson(pojo), as)

  private def postExpansion(content: String): Response = postJson("/compile/expand", content)

  private def jsonBody(response: Response): JsonNode = {
    assertThat(response.getStatus).isEqualTo(200)
    objectMapper.readTree(response.readEntity(classOf[String]))
  }

  it should "compile a draft through HTTP without changing the executable pin" in {
    val root = passthroughWorkflow
    val before = root.deepCopy[JsonNode]()
    val draft = objectAt(root, "/macroDependencies/0/content").deepCopy[ObjectNode]()
    draft.withArray("links").removeAll()
    val overrides = List(json("path" -> List("macro"), "content" -> draft))
    val preview =
      jsonBody(postJson("/compile/draft", json("content" -> root, "drafts" -> overrides).toString))
    assertThat(preview.path("type").asText()).isEqualTo("failure")
    assertThat(preview.at("/operatorErrors/macro/message").asText()).contains("Connect or remove")
    val plan = objectMapper.readTree(encodePojoAsFrontendJson(macroPlan(root)))
    val execution =
      objectMapper.treeToValue(changed(plan, "drafts" -> overrides), classOf[LogicalPlanPojo])
    assertThat(execution.macroExpansion.get.content)
      .isEqualTo(MacroExpansion.expand(before).content)
    assertThat(root).isEqualTo(before)
  }

  "POST /compile/expand" should "reject colliding canvas IDs with HTTP 400" in {
    val response =
      postExpansion("""{"operators":[{"operatorID":"same"}],"links":[{"linkID":"same"}]}""")
    assertThat(response.getStatus).isEqualTo(400)
    assertThat(response.readEntity(classOf[String])).contains("Duplicate graph element ID 'same'")
  }

  "POST /compile/validate-properties" should "validate a disabled nested instance with draft wiring and preserve pinned defaults" in {
    // Both unconnected native and boundary ports are legitimate during property editing.
    val udf = operator("python", "PythonUDFV2", List("in"), List("out"), json("code" -> "# 東京"))
    val inner = macroGraph("inner", List(udf), inputs = List("rows"))
    inner.set[JsonNode]("formBinding", formBinding(field("workers", "python", "workers")))
    val nested = instance("nested", "inner", inputs = List("rows"))
    val outer = macroGraph("outer", List(nested))
    outer.set[JsonNode]("formBinding", formBinding(field("count", "nested", "workers")))
    val edited = instance("selected", "outer").put("isDisabled", true)
    val unused = macroGraph("unused", List(instance("broken", "missing")))
    val deps = List(
      definition("outer", "Outer", outer),
      definition("inner", "Inner", inner),
      definition("unused", "Unused", unused)
    )
    def post(op: JsonNode) =
      postJson(
        "/compile/validate-properties",
        json("operator" -> op, "macroDependencies" -> deps).toString
      )
    val before = deps.map(_.toString)
    assertThat(post(edited).getStatus).isEqualTo(204)
    val properties = objectAt(edited, "/operatorProperties")
    properties.put("count", 3)
    assertThat(post(edited).getStatus).isEqualTo(204)
    properties.put("count", "invalid")
    val invalid = post(edited)
    assertThat(invalid.getStatus).isEqualTo(400)
    assertThat(invalid.readEntity(classOf[String]))
      .contains("Outer / Inner / Python UDF", "workers")
    properties.put("unknown", false)
    assertThat(post(edited).readEntity(classOf[String]))
      .contains("Outer", "Unknown macro property: unknown")
    assertThat(post(instance("x", "missing")).readEntity(classOf[String]))
      .contains("Missing macro version")
    assertThat(post(json()).getStatus).isEqualTo(400)
    assertThat(deps.map(_.toString)).isEqualTo(before)
  }

  "POST /compile" should "report duplicate native operator IDs through normal validation errors" in {
    val csv = textOp("duplicate")
    csv.setOperatorId("same")
    val response = postCompile(LogicalPlanPojo(List(csv, csv), Nil, Nil, Nil))
    assertThat(response.getStatus).isEqualTo(200)
    val body = response.readEntity(classOf[String])
    assertThat(body).contains("Duplicate graph element ID 'same'")
    assertThat(objectMapper.readTree(body).has("physicalPlan")).isFalse()
  }

  "POST /compile" should "return HTTP 200 for a well-formed plan" in {
    val text = textOp("north\nsouth")
    val limit = limitOp(1)
    val response = postCompile(
      LogicalPlanPojo(
        operators = List(text, limit),
        links = List(
          LogicalLink(
            text.operatorIdentifier,
            PortIdentity(0),
            limit.operatorIdentifier,
            PortIdentity(0)
          )
        ),
        opsToViewResult = List.empty,
        opsToReuseResult = List.empty
      )
    )
    assertThat(response.getStatus).isEqualTo(200)
  }

  it should "reject invalid nested properties at the root and compile valid pins without changing either snapshot" in {
    def content(properties: String) = {
      val text = operator(
        "text",
        "TextInput",
        outputs = List("output-0"),
        properties = objectMapper.readTree(properties)
      )
      val inner = definition("inner", "Inner", macroGraph("inner", List(text)))
      val outer =
        definition("outer", "Outer", macroGraph("outer", List(instance("nested", "inner"))))
      pinned(graph(List(instance("root", "outer"))), outer, inner)
    }
    for (
      (properties, error) <- List(
        "{}" -> Some("textInput"),
        """{"textInput":"hello","attributeName":null}""" -> Some("attributeName"),
        """{"textInput":"hello"}""" -> None,
        """{"textInput":"hello","attributeName":"line","attributeType":"string"}""" -> None
      )
    ) {
      val pinned = content(properties)
      val before = pinned.toString
      val body = jsonBody(postCompile(macroPlan(pinned)))
      assertThat(body.path("type").asText()).isEqualTo(if (error.isEmpty) "success" else "failure")
      assertThat(body.path("macroExpansion").has("names")).isFalse()
      error.foreach { property =>
        assertThat(body.has("physicalPlan")).isFalse()
        assertThat(body.path("operatorErrors").path("root").path("message").asText())
          .contains("Outer / Inner / Text Input", property)
        assertThat(body.path("operatorErrors").size()).isEqualTo(3)
      }
      assertThat(pinned.toString).isEqualTo(before)
    }
  }

  it should "tag the response body with type=success so the frontend can route on the discriminator" in {
    // The @JsonTypeInfo on WorkflowCompilationResponse writes a `type` field.
    // Both polymorphic deserialization (round-tripping through the trait) and
    // a raw-JSON `type == "success"` check need to hold so the Angular client
    // can branch without depending on Scala class names.
    val text = textOp("north\nsouth")
    val limit = limitOp(1)
    val response = postCompile(
      LogicalPlanPojo(
        operators = List(text, limit),
        links = List(
          LogicalLink(
            text.operatorIdentifier,
            PortIdentity(0),
            limit.operatorIdentifier,
            PortIdentity(0)
          )
        ),
        opsToViewResult = List.empty,
        opsToReuseResult = List.empty
      )
    )

    val body = response.readEntity(classOf[String])
    val node = objectMapper.readTree(body)
    assert(
      node.has("type") && node.get("type").asText() == "success",
      s"expected type:success discriminator, got $body"
    )
    val parsed = objectMapper.readValue(body, classOf[WorkflowCompilationResponse])
    assert(parsed.isInstanceOf[WorkflowCompilationSuccess])
    val success = parsed.asInstanceOf[WorkflowCompilationSuccess]
    assert(success.physicalPlan != null)
    assert(success.operatorOutputSchemas.nonEmpty)
  }

  it should "return WorkflowCompilationFailure (not HTTP 500) when a scan source file cannot be resolved" in {
    val response = postCompile(
      LogicalPlanPojo(
        operators = List(csvOp("/does/not/exist/missing.csv")),
        links = List.empty,
        opsToViewResult = List.empty,
        opsToReuseResult = List.empty
      )
    )

    // Must not surface as HTTP 500 — the error must come back as a structured failure.
    assertThat(response.getStatus).isEqualTo(200)

    // Inspect the raw JSON rather than deserializing the full response: WorkflowFatalError
    // is not round-trippable through the test ObjectMapper, but that is unrelated to the
    // bug under test (which is purely about the resource not NPE'ing).
    val responseBody = response.readEntity(classOf[String])
    val rootNode = objectMapper.readTree(responseBody)
    assertThat(rootNode.get("type").asText()).isEqualTo("failure")
    assertThat(rootNode.has("operatorErrors")).isTrue
  }
  it should "return boundary schemas without executable terminal operators and reject disconnected boundaries" in {
    val content = passthroughWorkflow
    objectAt(content, "/operators/0/operatorProperties").put("attributeType", "integer")
    val before = content.toString
    val compiled = jsonBody(postCompile(macroPlan(content, List("macro"))))
    assertThat(compiled.path("type").asText()).isEqualTo("success")
    val schemas = compiled.path("operatorOutputSchemas")
    assertThat(schemas.path("macro").path("0_false"))
      .isEqualTo(schemas.path("source").path("0_false"))
    assertThat(schemas.path("macro").path("0_false").get(0).path("attributeType").asText())
      .isEqualTo("integer")
    val expanded = compiled.path("macroExpansion")
    assertThat(expanded.path("content").path("operators").size()).isEqualTo(2)
    assertThat(expanded.path("paths").size()).isEqualTo(2)
    val inputID = MacroExpansion.operatorID(List("macro"), "identity__inputs")
    assertThat(schemas.path(inputID).path("0_false"))
      .isEqualTo(schemas.path("source").path("0_false"))
    assertThat(content.toString).isEqualTo(before)
    objectAt(content, "/macroDependencies/0/content").putArray("links")
    val failure = jsonBody(postCompile(macroPlan(content)))
    assertThat(failure.path("type").asText()).isEqualTo("failure")
    assertThat(failure.path("operatorErrors").path("macro").path("message").asText())
      .contains("Connect or remove")
  }

  it should "answer 401 to a request that carries no logged-in user" in {
    val response = resources
      .target("/compile")
      .request(MediaType.APPLICATION_JSON)
      .post(Entity.json(encodePojoAsFrontendJson(planOf(wholeFileScanOp(privateDatasetFilePath)))))
    assertThat(response.getStatus).isEqualTo(401)
  }

  it should "compile a scan source whose file the logged-in user can read" in {
    val scan = wholeFileScanOp(privateDatasetFilePath)
    val response = postCompile(planOf(scan), as = owner)

    assertThat(response.getStatus).isEqualTo(200)
    val rootNode = objectMapper.readTree(response.readEntity(classOf[String]))
    assert(rootNode.get("type").asText() == "success", s"expected success, got $rootNode")
    assert(rootNode.get("operatorOutputSchemas").has(scan.operatorIdentifier.id))
  }

  it should "resolve a nested draft's scan with the requesting user's file access" in {
    val leaf = macroGraph("leaf", List(textSource("source", "saved")))
    val parent = macroGraph("parent", List(instance("child", "leaf")))
    val root = pinned(
      graph(List(instance("root", "parent"))),
      definition("parent", "Parent", parent),
      definition("leaf", "Leaf", leaf)
    )
    val scan = operator(
      "source",
      "FileScan",
      outputs = List("out"),
      properties = json("fileName" -> privateDatasetFilePath)
    )
    val draft = json("path" -> List("root", "child"), "content" -> macroGraph("leaf", List(scan)))
    val request = json("content" -> root, "drafts" -> List(draft)).toString
    assertThat(jsonBody(postJson("/compile/draft", request, owner)).path("type").asText())
      .isEqualTo("success")
    val denied = jsonBody(postJson("/compile/draft", request, otherUser))
    assertThat(denied.path("type").asText()).isEqualTo("failure")
    assertThat(denied.at("/operatorErrors/root/message").asText()).contains("Parent / Leaf")
    assertThat(denied.toString)
      .doesNotContain(privateDatasetRepositoryName, privateDatasetVersionHash)
  }

  it should "return WorkflowCompilationFailure when the scan source file is not readable by the user" in {
    val scan = csvOp("/dataset/owner@test.com/private_dataset/v1/table.csv")
    val response = postCompile(planOf(scan), as = otherUser)

    assertThat(response.getStatus).isEqualTo(200)
    val body = response.readEntity(classOf[String])
    val rootNode = objectMapper.readTree(body)
    assert(rootNode.get("type").asText() == "failure", s"expected failure, got $body")
    assert(rootNode.get("operatorErrors").has(scan.operatorIdentifier.id))
    val schemas = rootNode.get("operatorOutputSchemas").get(scan.operatorIdentifier.id)
    assert(
      schemas == null || !schemas.elements().hasNext ||
        schemas.elements().next().isNull,
      s"expected no schema for the operator, got $schemas"
    )
    assert(!body.contains(privateDatasetRepositoryName))
    assert(!body.contains(privateDatasetVersionHash))
  }

  it should "return the same operator error for an unreadable file as for a missing one" in {
    val unreadable = wholeFileScanOp(privateDatasetFilePath)
    val missingPath = "/dataset/owner@test.com/no_such_dataset/v1/notes.txt"
    val missing = wholeFileScanOp(missingPath)

    def errorMessageOf(scan: FileScanSourceOpDesc): String = {
      val rootNode = objectMapper.readTree(
        postCompile(planOf(scan), as = otherUser).readEntity(classOf[String])
      )
      rootNode.get("operatorErrors").get(scan.operatorIdentifier.id).get("message").asText()
    }

    assert(
      errorMessageOf(unreadable).replace(privateDatasetFilePath, "<path>")
        == errorMessageOf(missing).replace(missingPath, "<path>")
    )
  }
}
