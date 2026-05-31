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

package org.apache.texera.web.service

import com.fasterxml.jackson.databind.JsonNode
import org.apache.texera.amber.config.EnvironmentalVariable
import org.apache.texera.amber.core.workflow.PhysicalPlan
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.amber.compiler.model.LogicalPlanPojo

import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters.IteratorHasAsScala

/**
  * Compiles a workflow (logical plan -> physical plan) by calling the workflow-compiling-service's
  * `POST /api/compile` over HTTP, instead of running [[org.apache.texera.workflow.WorkflowCompiler]]
  * in-process.
  *
  * This lets the computing-unit master offload compilation to a dedicated service: it serializes the
  * [[LogicalPlanPojo]] with [[objectMapper]], posts it with the forwarded `USER_JWT_TOKEN`, and reads
  * back the `WorkflowCompilationResponse` union (`success` / `failure`). On `success` it returns the
  * deserialized [[PhysicalPlan]] (the same JSONUtils serializers are registered on both ends, so the
  * plan round-trips); on `failure` it raises a [[RuntimeException]] carrying the per-operator errors,
  * which the caller's existing catch surfaces to the frontend exactly like an in-process failure.
  *
  * The compiling-service's response classes live in a sibling module that amber does not depend on,
  * so the response is parsed from a JSON tree by its `type` discriminator rather than via those
  * classes.
  */
object CompilingServiceClient {

  private lazy val userJwtToken: String =
    sys.env.getOrElse(EnvironmentalVariable.ENV_USER_JWT_TOKEN, "").trim

  private lazy val endpoint: String =
    sys.env
      .get(EnvironmentalVariable.ENV_WORKFLOW_COMPILING_SERVICE_ENDPOINT)
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse("http://localhost:9090/api/compile")

  /**
    * @throws RuntimeException if compilation fails (message includes the per-operator errors) or the
    *                          service returns a non-2xx response.
    */
  def compile(
      logicalPlan: LogicalPlanPojo,
      workflowId: Long,
      executionId: Long
  ): PhysicalPlan = compile(endpoint, userJwtToken, logicalPlan, workflowId, executionId)

  /** Endpoint/token are injectable so the HTTP round-trip can be unit-tested in isolation. */
  private[service] def compile(
      endpoint: String,
      token: String,
      logicalPlan: LogicalPlanPojo,
      workflowId: Long,
      executionId: Long
  ): PhysicalPlan = {
    val requestUrl = s"$endpoint?workflowId=$workflowId&executionId=$executionId"
    val payload = objectMapper.writeValueAsBytes(logicalPlan)
    val connection = new URL(requestUrl).openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("POST")
    connection.setRequestProperty("Authorization", s"Bearer $token")
    connection.setRequestProperty("Content-Type", "application/json")
    connection.setDoOutput(true)
    try {
      connection.getOutputStream.write(payload)
      val code = connection.getResponseCode
      if (code < 200 || code >= 300) {
        val errorBody = readBody(connection.getErrorStream)
        throw new RuntimeException(
          s"workflow-compiling-service /compile failed (HTTP $code)" +
            (if (errorBody.nonEmpty) s": $errorBody" else "")
        )
      }
      val response =
        objectMapper.readTree(connection.getInputStream.readAllBytes())
      parseResponse(response)
    } finally {
      connection.disconnect()
    }
  }

  /**
    * Interprets the `WorkflowCompilationResponse` union: returns the [[PhysicalPlan]] on `success`,
    * throws with the collected operator errors on `failure`.
    */
  private def parseResponse(response: JsonNode): PhysicalPlan = {
    val responseType = Option(response.get("type")).map(_.asText()).getOrElse("")
    responseType match {
      case "success" =>
        val planNode = response.get("physicalPlan")
        if (planNode == null || planNode.isNull) {
          throw new RuntimeException(
            "workflow-compiling-service returned a success response without a physicalPlan"
          )
        }
        objectMapper.treeToValue(planNode, classOf[PhysicalPlan])

      case "failure" =>
        throw new RuntimeException(
          s"Workflow compilation failed: ${formatOperatorErrors(response.get("operatorErrors"))}"
        )

      case other =>
        throw new RuntimeException(
          s"workflow-compiling-service returned an unrecognized response type '$other'"
        )
    }
  }

  /** Flattens `operatorErrors: Map[operatorId, WorkflowFatalError]` into a readable message. */
  private def formatOperatorErrors(errorsNode: JsonNode): String = {
    if (errorsNode == null || !errorsNode.isObject || errorsNode.isEmpty) {
      "unknown compilation error"
    } else {
      errorsNode
        .fields()
        .asScala
        .map { entry =>
          val opId = entry.getKey
          val err = entry.getValue
          val message = Option(err.get("message")).map(_.asText()).filter(_.nonEmpty)
          val details = Option(err.get("details")).map(_.asText()).filter(_.nonEmpty)
          val text = (message ++ details).mkString(" - ")
          s"$opId: ${if (text.nonEmpty) text else "compilation error"}"
        }
        .mkString("; ")
    }
  }

  private def readBody(stream: java.io.InputStream): String =
    if (stream == null) ""
    else new String(stream.readAllBytes(), StandardCharsets.UTF_8).trim
}
