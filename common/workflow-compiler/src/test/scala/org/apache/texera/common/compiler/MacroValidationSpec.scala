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
import com.fasterxml.jackson.databind.JsonNode
import org.apache.texera.common.compiler.MacroTestUtils._
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.amber.core.virtualidentity.{OperatorIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.WorkflowContext
import org.apache.texera.amber.operator.udf.python.PythonUDFOpDescV2
import org.apache.texera.common.compiler.model.LogicalPlanPojo
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.jdk.CollectionConverters._

class MacroValidationSpec extends AnyFlatSpec with Matchers {
  private val validKeys = """{"keys":[{"attribute":"line","sortPreference":"ASC"}]}"""
  private def transform(id: String, kind: String, properties: String): ObjectNode =
    operator(id, kind, List("input-0"), List("output-0"), objectMapper.readTree(properties))

  private def workflow(properties: String = "{}", kind: String = "StableMergeSort"): ObjectNode = {
    val out = List("out")
    val inner = macroGraph(
      "inner",
      List(textSource("text", "b\na"), transform("sort", kind, properties)),
      List(
        link("text-sort", end("text", "output-0"), end("sort", "input-0")),
        link("inner__1_out_0", end("sort", "output-0"), end("inner__outputs", "out"))
      ),
      outputs = out
    )
    def named(id: String, macroID: String, name: String): ObjectNode =
      instance(id, macroID, outputs = out).put("customDisplayName", name)
    val outer = macroGraph(
      "outer",
      List(named("first", "inner", "Nested α"), named("second", "inner", "Nested β")),
      List(link("outer__1_out_0", end("first", "out"), end("outer__outputs", "out"))),
      outputs = out
    )
    pinned(
      graph(List(named("outer", "outer", "Reusable 東京"))),
      definition("outer", "Outer", outer),
      definition("inner", "Inner", inner)
    )
  }
  private def body(content: JsonNode) = objectAt(content, "/macroDependencies/1/content")
  private def compile(
      request: LogicalPlanPojo,
      mode: CompilationErrorHandling = CompilationErrorHandling.Lenient
  ): WorkflowCompilationResult = {
    val snapshot = request.macroContent.map(_.toString)
    try new WorkflowCompiler(
      new WorkflowContext(WorkflowIdentity(0)),
      TestOperators.fixtureFileResolver
    ).compile(request, mode)
    finally request.macroContent.map(_.toString) shouldBe snapshot
  }
  private def sortBinding =
    formBinding(field("sort-keys", "sort", "keys").put("displayName", "Sort Keys"))
  private val outerID = OperatorIdentity("outer")

  private def invalidCompilation(content: JsonNode): WorkflowCompilationResult = {
    val result = compile(macroPlan(content))
    result.physicalPlan shouldBe None
    result
  }

  private def rootError(content: JsonNode): String =
    invalidCompilation(content).operatorIdToError(outerID).message

  private def normalizationError(request: LogicalPlanPojo): String =
    intercept[IllegalArgumentException](request.normalized).getMessage

  private def strict(request: LogicalPlanPojo) =
    compile(request, CompilationErrorHandling.Strict)

  private def validCompilation(request: LogicalPlanPojo): WorkflowCompilationResult = {
    val result = strict(request)
    result.physicalPlan should not be empty
    result.operatorIdToError shouldBe empty
    result
  }

  private def checkModes(request: LogicalPlanPojo, valid: Boolean): WorkflowCompilationResult = {
    val editing = compile(request)
    if (valid) {
      editing.operatorIdToError shouldBe empty
      validCompilation(request)
    } else {
      editing.physicalPlan shouldBe None
      intercept[IllegalArgumentException](strict(request))
    }
    editing
  }

  private def checkPaths(message: String, leaf: String): Unit =
    for (instance <- List("Nested α", "Nested β"))
      message should include(s"Reusable 東京 / $instance / $leaf")

  for (single <- List(false, true); count <- List(1, 2)) {
    "Nested macro input validation" should s"honor configured UDF multiplicity single=$single with $count links" in {
      val content = workflow("""{"code":"from pytexera import *"}""", "PythonUDFV2")
      val inner = body(content)
      objectAt(inner, "/operators/1/inputPorts/0")
        .put("disallowMultiInputs", single)
      if (count == 2) {
        arrayAt(inner, "/operators").add(textSource("second-source", "b\na"))
        arrayAt(inner, "/links").add(
          link("second-input", end("second-source", "output-0"), end("sort", "input-0"))
        )
      }
      val valid = !single || count == 1
      val result = checkModes(macroPlan(content), valid)
      if (!valid) {
        val message = result.operatorIdToError(outerID).message
        checkPaths(message, "Python UDF")
        message should include("requires 1 input, has 2")
      }
    }
  }

  for (exposed <- List(false, true); overridden <- List(false, true)) {
    "Forwarded macro properties" should s"validate the declared binding exposed=$exposed overridden=$overridden" in {
      val content = workflow(validKeys)
      body(content).set[JsonNode]("formBinding", if (exposed) sortBinding else formBinding())
      val forwarded = field("forwarded", "first", "sort-keys").put("displayName", "Sort Keys")
      objectAt(content, "/macroDependencies/0/content")
        .set[JsonNode]("formBinding", formBinding(forwarded))
      if (overridden)
        objectAt(content, "/operators/0/operatorProperties")
          .set[JsonNode]("forwarded", objectMapper.readTree(validKeys).path("keys"))
      val before = content.toString
      val imported = macroPlan(objectMapper.readTree(before))
      val result = checkModes(imported, exposed)
      if (!exposed) {
        result.operatorIdToError(outerID).message should include("Missing exposed macro property")
        intercept[IllegalArgumentException](MacroExpansion.expand(content))
      }
      content.toString shouldBe before
      imported.macroContent.get.toString shouldBe before
    }
  }

  "Macro property defaults" should "use the descriptor defaults and honor explicitly configured values" in {
    val defaults = new PythonUDFOpDescV2
    defaults.retainInputColumns shouldBe false // the schema annotation instead advertises true
    val explicit =
      json("code" -> "# 東京", "workers" -> 2, "retainInputColumns" -> true, "defaultEnv" -> false)
    for (properties <- List(json("code" -> "# default"), explicit)) {
      val content = workflow(properties.toString, "PythonUDFV2")
      val before = content.toString
      val operators = macroPlan(content).normalized.operators.collect {
        case op: PythonUDFOpDescV2 => op
      }
      operators should have size 2
      operators.foreach { op =>
        op.workers shouldBe properties.path("workers").asInt(defaults.workers)
        op.retainInputColumns shouldBe properties
          .path("retainInputColumns")
          .asBoolean(defaults.retainInputColumns)
        op.defaultEnv shouldBe properties.path("defaultEnv").asBoolean(defaults.defaultEnv)
        op.code shouldBe properties.get("code").asText()
      }
      content.toString shouldBe before
    }
  }

  List(
    "workers" -> "\"1\"",
    "workers" -> "null",
    "workers" -> "{}",
    "retainInputColumns" -> "\"true\"",
    "retainInputColumns" -> "null",
    "defaultEnv" -> "\"true\"",
    "defaultEnv" -> "null"
  ).foreach {
    case (key, value) =>
      it should s"reject the explicit invalid value $key=$value in both compilation modes" in {
        val content = workflow(
          """{"code":"from pytexera import *","workers":1,"retainInputColumns":false,"defaultEnv":true}""",
          "PythonUDFV2"
        )
        objectAt(body(content), "/operators/1/operatorProperties")
          .set[JsonNode](key, objectMapper.readTree(value))
        val result = invalidCompilation(content)
        val message = result.operatorIdToError(outerID).message
        checkPaths(message, "Python UDF")
        message should include(key)
        val schemas = result.operatorIdToOutputSchemas.values.flatMap(_.values.flatten)
        schemas.flatMap(_.getAttributeNames) should contain("line")
        intercept[IllegalArgumentException](strict(macroPlan(content))).getMessage should include(
          key
        )
      }
  }

  private def castingWorkflow(properties: String): JsonNode = {
    val content = workflow(kind = "Sort")
    val inner = body(content)
    arrayAt(inner, "/operators").add(transform("cast", "TypeCasting", properties))
    arrayAt(inner, "/links").add(link("sort-cast", end("sort", "output-0"), end("cast", "input-0")))
    objectAt(inner, "/links/1/source").put("operatorID", "cast")
    content
  }

  List(
    "{}",
    """{"typeCastingUnits":null}""",
    """{"typeCastingUnits":"bad"}""",
    """{"typeCastingUnits":[{"attribute":"line","resultType":""}]}"""
  ).foreach { properties =>
    "Macro editing schemas" should s"preserve upstream schemas through incomplete Sort and Type Casting settings $properties" in {
      val content = castingWorkflow(properties)
      val before = content.toString
      val request = macroPlan(content)
      val result = compile(request)
      result.physicalPlan shouldBe None
      val upstream = request.macroExpansion.get.paths.filter {
        case (_, path) => path.last == "sort"
      }
      upstream should have size 2
      upstream.keys.foreach { id =>
        result
          .operatorIdToOutputSchemas(OperatorIdentity(id))
          .values
          .flatten
          .flatMap(_.getAttributeNames)
          .toList shouldBe List("line")
      }
      result.operatorIdToError(outerID).message should include("Type Casting")
      intercept[IllegalArgumentException](strict(request))
      intercept[IllegalArgumentException](request.normalized)
      content.toString shouldBe before
    }
  }

  "Macro editing schemas" should "produce complete schemas once the same nested operators are configured" in {
    val content =
      castingWorkflow("""{"typeCastingUnits":[{"attribute":"line","resultType":"string"}]}""")
    objectAt(body(content), "/operators/1")
      .set[JsonNode](
        "operatorProperties",
        objectMapper.readTree("""{"attributes":[{"attribute":"line","sortPreference":"ASC"}]}""")
      )
    validCompilation(macroPlan(content)).operatorIdToOutputSchemas should have size 6
  }

  "Nested macro validation" should "report every missing property at the root with readable instance paths" in {
    val content = workflow()
    val before = content.toString
    val result = invalidCompilation(content)
    val message = result.operatorIdToError(outerID).message
    checkPaths(message, "Stable Merge Sort")
    message should include("keys")
    message should not include "java.lang."
    result.operatorIdToError.size should be >= 5 // both leaves, both nested instances, root
    for (selection <- List(None, Some(Nil))) {
      val request = macroPlan(content).copy(executeOperatorIds = selection)
      val error = normalizationError(request)
      error should include("Nested α")
      error should include("keys")
      intercept[IllegalArgumentException](strict(request))
    }
    content.toString shouldBe before
  }

  List(
    """{"keys":null}""",
    """{"keys":"bad"}""",
    """{"keys":[{}]}""",
    """{"keys":[{"attribute":"line","sortPreference":"INVALID"}]}"""
  ).foreach { properties =>
    it should s"reject malformed nested settings $properties" in {
      rootError(workflow(properties)) should include("Stable Merge Sort")
    }
  }

  it should "compile valid settings and preserve the complete pinned bundle" in {
    val content = workflow(validKeys)
    validCompilation(macroPlan(content))
    objectMapper
      .valueToTree[JsonNode](macroPlan(content).macroExpansion.get)
      .has("names") shouldBe false
  }

  it should "validate effective exposed values after overrides without editing the stored definition" in {
    val content = workflow()
    body(content).set[JsonNode]("formBinding", sortBinding)
    val children = content.at("/macroDependencies/0/content/operators")
    val keys = objectMapper.readTree(validKeys).path("keys")
    children
      .elements()
      .asScala
      .foreach(child => objectAt(child, "/operatorProperties").set[JsonNode]("sort-keys", keys))
    val before = content.toString
    validCompilation(macroPlan(content))
    objectAt(children, "/0/operatorProperties").putNull("sort-keys")
    val message = rootError(content)
    message should include("Nested α")
    message should not include "Nested β"
    body(content).at("/operators/1/operatorProperties").has("keys") shouldBe false
    content.toString should not be before // only the deliberate override changed
  }

  it should "report disconnected inputs with the same nested paths" in {
    val content = workflow(validKeys)
    arrayAt(body(content), "/links")
      .remove(0)
    rootError(content) should include("input-0")
  }

  it should "ignore disabled invalid operators and keep structural expansion available for editing" in {
    val content = workflow()
    MacroExpansion.expand(content).content.path("operators").size() shouldBe 4
    objectAt(body(content), "/operators/1").put("isDisabled", true)
    macroPlan(content).normalized.operators.size shouldBe 2
  }

  it should "use instance and definition names when custom labels are empty" in {
    val content = workflow()
    objectAt(content, "/operators/0").put("customDisplayName", " ")
    objectAt(content, "/macroDependencies/0/content/operators/0")
      .remove("customDisplayName")
    rootError(content) should include("Outer / Inner / Stable Merge Sort")
  }

  it should "propagate ordinary schema failures through every ancestor in both compilation modes" in {
    val content =
      workflow("""{"isDrop":false,"attributes":[{"originalAttribute":"missing"}]}""", "Projection")
    val before = content.toString
    val pinned = macroPlan(content)
    val normalized = pinned.normalized
    for (request <- List(pinned, normalized)) {
      val result = compile(request)
      result.physicalPlan shouldBe None
      result.operatorIdToError.size shouldBe 5
      val message = result.operatorIdToError(outerID).message
      checkPaths(message, "Projection")
      message should include("missing")
      intercept[Throwable](strict(request)).getMessage should include("Nested α / Projection")
    }
    objectMapper.valueToTree[JsonNode](normalized).has("expandedMacros") shouldBe false
    content.toString shouldBe before
  }

  it should "defer typed loop references to Texera's existing state-reference validation" in {
    val content = workflow("""{"limit":"$n"}""", "Limit")
    macroPlan(content).normalized.operators.count(_.stateReferences.nonEmpty) shouldBe 2
    rootError(content) should include("not inside a loop block")
    objectAt(body(content), "/operators/1/operatorProperties")
      .put("limit", "bad")
    normalizationError(macroPlan(content)) should include(
      "integer"
    )
  }

  it should "reject unknown native types and missing required input declarations with readable paths" in {
    val content = workflow(validKeys)
    val sort = objectAt(body(content), "/operators/1")
    sort.put("operatorType", "MissingOperator")
    rootError(content) should include("Nested α / MissingOperator")
    sort.put("operatorType", "StableMergeSort")
    sort.putArray("inputPorts")
    arrayAt(body(content), "/links")
      .remove(0)
    rootError(content) should include("input ports")
  }

  it should "attribute an unfinished nested boundary to its own root instead of an unrelated macro" in {
    val content = workflow(validKeys)
    val ignored =
      changed(content.at("/operators/0"), "operatorID" -> "ignored", "isDisabled" -> true)
    arrayAt(content, "/operators")
      .insert(0, ignored)
    val edges = arrayAt(body(content), "/links")
    edges.remove(edges.elements().asScala.indexWhere(_.path("linkID").asText() == "inner__1_out_0"))
    val result = invalidCompilation(content)
    result.operatorIdToError.keySet shouldBe Set(OperatorIdentity("outer"))
    val message = result.operatorIdToError(outerID).message
    message should startWith("Reusable 東京 / Nested α")
    message should include("Connect or remove")
  }

  it should "identify the operator path when a nested graph contains a cycle" in {
    val content = workflow(validKeys)
    arrayAt(body(content), "/links")
      .add(link("cycle", end("sort", "output-0"), end("sort", "input-0")))
    val error = rootError(content)
    error should include("Reusable 東京 / Nested α / Stable Merge Sort")
    error should include("cycle")
  }

  it should "validate shared snapshots concurrently without changing pins or runtime identities" in {
    import scala.concurrent.{Await, ExecutionContext, Future}
    import scala.concurrent.duration._
    implicit val executionContext: ExecutionContext = ExecutionContext.global
    val content = workflow(validKeys)
    val before = content.toString
    val results =
      Await.result(
        Future.sequence((1 to 8).map(_ => Future(macroPlan(content).normalized))),
        10.seconds
      )
    results.map(_.operators.map(_.operatorIdentifier)).distinct.size shouldBe 1
    content.toString shouldBe before
  }

  it should "reuse conditional operator schemas for COUNT and aggregations that require an attribute" in {
    val content =
      workflow("""{"aggregations":[{"aggFunction":"count","result attribute":"n"}]}""", "Aggregate")
    validCompilation(macroPlan(content))
    val aggregation = objectAt(body(content), "/operators/1/operatorProperties/aggregations/0")
    aggregation.put("aggFunction", "sum")
    normalizationError(macroPlan(content)) should include(
      "attribute"
    )
    aggregation.put("attribute", " ")
    normalizationError(macroPlan(content)) should include(
      "pattern"
    )
    aggregation.put("attribute", "line")
    macroPlan(content).normalized.operators.size shouldBe 4
  }
}
