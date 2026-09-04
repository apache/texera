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
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.distinct.DistinctOpDesc
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.common.compiler.model.{LogicalLink, LogicalPlanPojo}
import org.assertj.core.api.Assertions.assertThat
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

/**
  * Resource-layer tests for `/workflow-to-python`. Owns only what the REST
  * envelope adds on top of the translation itself: HTTP status, the
  * `@JsonTypeInfo` discriminator the frontend routes on, and the JSON shape
  * the resource expects on the wire.
  *
  * What the translator does with a plan is asserted in
  * `WorkflowToPythonTranslatorSpec`, and what an operator emits in its own
  * spec, so a regression lands where it belongs.
  */
class WorkflowToPythonResourceSpec extends AnyFlatSpec with BeforeAndAfterAll {

  private val resources: ResourceExtension = ResourceExtension
    .builder()
    .addResource(new WorkflowToPythonResource())
    .setMapper(objectMapper)
    .build()

  override protected def beforeAll(): Unit = resources.before()
  override protected def afterAll(): Unit = resources.after()

  private def distinctOp(id: String): DistinctOpDesc = {
    val op = new DistinctOpDesc()
    op.setOperatorId(id)
    op
  }

  private def limitOp(id: String, rows: Int): LimitOpDesc = {
    val op = new LimitOpDesc()
    op.setOperatorId(id)
    op.limit = rows
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

  private def postExport(pojo: LogicalPlanPojo): Response =
    resources
      .target("/workflow-to-python")
      .request(MediaType.APPLICATION_JSON)
      .post(Entity.json(encodePojoAsFrontendJson(pojo)))

  private def chainOf(from: DistinctOpDesc, to: LimitOpDesc): LogicalPlanPojo =
    LogicalPlanPojo(
      operators = List(from, to),
      links = List(
        LogicalLink(
          from.operatorIdentifier,
          PortIdentity(0),
          to.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      opsToViewResult = List.empty,
      opsToReuseResult = List.empty
    )

  "POST /workflow-to-python" should "return HTTP 200 for a well-formed plan" in {
    val response = postExport(chainOf(distinctOp("distinct"), limitOp("limit", 5)))
    assertThat(response.getStatus).isEqualTo(200)
  }

  it should "tag the body with type=success and carry the script the plan translates to" in {
    // The @JsonTypeInfo on WorkflowToPythonResponse writes a `type` field. Both
    // polymorphic deserialization and a raw-JSON `type == "success"` check need
    // to hold, so the Angular client can branch without depending on Scala class
    // names.
    val response = postExport(chainOf(distinctOp("distinct"), limitOp("limit", 5)))
    val body = response.readEntity(classOf[String])

    val node = objectMapper.readTree(body)
    assert(
      node.has("type") && node.get("type").asText() == "success",
      s"expected type:success discriminator, got $body"
    )

    val parsed = objectMapper.readValue(body, classOf[WorkflowToPythonResponse])
    assert(parsed.isInstanceOf[WorkflowToPythonSuccess])
    val code = parsed.asInstanceOf[WorkflowToPythonSuccess].pythonCode
    // Both operators of the chain, and the import every script carries: enough
    // to show the payload is the translated plan rather than an empty string.
    assert(code.contains("import pandas as pd"))
    assert(code.contains("drop_duplicates"))
    assert(code.contains("head(5)"))
  }

  it should "return a failure body rather than HTTP 500 when the plan cannot be read" in {
    // A link naming an operator the plan does not carry: the DAG refuses the
    // edge, and the resource has to answer with a reason rather than a stack
    // trace the frontend cannot render.
    val distinct = distinctOp("distinct")
    val absent = limitOp("absent", 5)
    val response = postExport(
      LogicalPlanPojo(
        operators = List(distinct),
        links = List(
          LogicalLink(
            distinct.operatorIdentifier,
            PortIdentity(0),
            absent.operatorIdentifier,
            PortIdentity(0)
          )
        ),
        opsToViewResult = List.empty,
        opsToReuseResult = List.empty
      )
    )

    assertThat(response.getStatus).isEqualTo(200)
    val node = objectMapper.readTree(response.readEntity(classOf[String]))
    assertThat(node.get("type").asText()).isEqualTo("failure")
    assertThat(node.has("errorMessage")).isTrue
  }
}
