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

import com.fasterxml.jackson.databind.node.ObjectNode
import io.dropwizard.testing.junit5.ResourceExtension
import jakarta.ws.rs.client.Entity
import jakarta.ws.rs.core.{MediaType, Response}
import org.apache.texera.amber.compiler.model.{LogicalLink, LogicalPlanPojo}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.projection.{AttributeUnit, ProjectionOpDesc}
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.apache.texera.amber.util.JSONUtils.objectMapper
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
  * `org.apache.texera.amber.compiler.WorkflowCompilerSpec` so a regression
  * lands on the right spec.
  */
class WorkflowCompilationResourceSpec extends AnyFlatSpec with BeforeAndAfterAll {

  private val resources: ResourceExtension = ResourceExtension
    .builder()
    .addResource(new WorkflowCompilationResource())
    .setMapper(objectMapper)
    .build()

  override protected def beforeAll(): Unit = resources.before()
  override protected def afterAll(): Unit = resources.after()

  private val realCsvPath =
    "workflow-compiling-service/src/test/resources/country_sales_small.csv"

  private def csvOp(fileName: String): CSVScanSourceOpDesc = {
    val op = new CSVScanSourceOpDesc()
    op.fileName = Some(fileName)
    op.customDelimiter = Some(",")
    op.hasHeader = true
    op
  }

  private def projectOp(columns: List[String]): ProjectionOpDesc = {
    val op = new ProjectionOpDesc()
    op.attributes = columns.map(name => new AttributeUnit(name, ""))
    op.isDrop = false
    op
  }

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

  private def postCompile(pojo: LogicalPlanPojo): Response =
    resources
      .target("/compile")
      .request(MediaType.APPLICATION_JSON)
      .post(Entity.json(encodePojoAsFrontendJson(pojo)))

  "POST /compile" should "return HTTP 200 for a well-formed plan" in {
    val csv = csvOp(realCsvPath)
    val proj = projectOp(List("Region", "Total Profit"))
    val response = postCompile(
      LogicalPlanPojo(
        operators = List(csv, proj),
        links = List(
          LogicalLink(
            csv.operatorIdentifier,
            PortIdentity(0),
            proj.operatorIdentifier,
            PortIdentity(0)
          )
        ),
        opsToViewResult = List.empty,
        opsToReuseResult = List.empty
      )
    )
    assertThat(response.getStatus).isEqualTo(200)
  }

  it should "tag the response body with type=success so the frontend can route on the discriminator" in {
    // The @JsonTypeInfo on WorkflowCompilationResponse writes a `type` field.
    // Both polymorphic deserialization (round-tripping through the trait) and
    // a raw-JSON `type == "success"` check need to hold so the Angular client
    // can branch without depending on Scala class names.
    val csv = csvOp(realCsvPath)
    val proj = projectOp(List("Region", "Total Profit"))
    val response = postCompile(
      LogicalPlanPojo(
        operators = List(csv, proj),
        links = List(
          LogicalLink(
            csv.operatorIdentifier,
            PortIdentity(0),
            proj.operatorIdentifier,
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
}
