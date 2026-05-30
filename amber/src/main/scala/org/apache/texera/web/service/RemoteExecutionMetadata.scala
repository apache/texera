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

import org.apache.texera.amber.config.EnvironmentalVariable
import org.apache.texera.amber.core.virtualidentity.ExecutionIdentity
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.net.{HttpURLConnection, URI, URL, URLEncoder}
import java.nio.charset.StandardCharsets

/**
  * Routes execution-metadata operations to the dashboard service over HTTP, instead of querying
  * Postgres directly.
  *
  * This lets a computing-unit pod — which runs user-defined functions and therefore must not hold
  * database credentials (issue #5011) — persist and read execution metadata without a JDBC
  * connection.
  *
  * Remote mode is active exactly when `SqlServer` is NOT initialized in this process: the CU skips
  * initializing it, while the dashboard service keeps initializing it, so the dashboard service
  * always takes the direct-DB path and the metadata endpoints never recurse back onto themselves.
  */
object RemoteExecutionMetadata {

  private lazy val userJwtToken: String =
    sys.env.getOrElse(EnvironmentalVariable.ENV_USER_JWT_TOKEN, "").trim

  private lazy val baseEndpoint: String =
    sys.env
      .get(EnvironmentalVariable.ENV_DASHBOARD_SERVICE_EXECUTION_METADATA_ENDPOINT)
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse("http://localhost:8080/api/internal/execution-metadata")

  /** Remote routing is active only when this process has no database connection of its own. */
  def enabled: Boolean = !org.apache.texera.dao.SqlServer.isInitialized

  def createExecution(
      workflowId: Long,
      uid: Option[Integer],
      executionName: String,
      environmentVersion: String,
      computingUnitId: Integer
  ): ExecutionIdentity = {
    val body = objectMapper.createObjectNode()
    body.put("workflowId", workflowId)
    uid match {
      case Some(value) => body.put("uid", value.intValue())
      case None        => body.putNull("uid")
    }
    body.put("executionName", executionName)
    body.put("environmentVersion", environmentVersion)
    body.put("computingUnitId", computingUnitId.intValue())
    val response = request("POST", "/create", Some(body.toString))
      .getOrElse(
        throw new RuntimeException("dashboard service returned no body for execution create")
      )
    ExecutionIdentity(objectMapper.readTree(response).get("eid").asLong())
  }

  def updateRuntimeStatsUri(wid: Long, eid: Long, uri: URI): Unit = {
    val body = objectMapper.createObjectNode()
    body.put("workflowId", wid)
    body.put("uri", uri.toString)
    request("PUT", s"/$eid/runtime-stats-uri", Some(body.toString))
  }

  def insertOperatorConsoleUri(eid: Long, operatorId: String, uri: URI): Unit = {
    val body = objectMapper.createObjectNode()
    body.put("operatorId", operatorId)
    body.put("uri", uri.toString)
    request("POST", s"/$eid/operator-console", Some(body.toString))
  }

  def insertPortResultUri(eid: Long, globalPortIdSerialized: String, uri: URI): Unit = {
    val body = objectMapper.createObjectNode()
    body.put("globalPortId", globalPortIdSerialized)
    body.put("uri", uri.toString)
    request("POST", s"/$eid/port-result", Some(body.toString))
  }

  def getResultUri(
      eid: Long,
      opId: String,
      portIdId: Int,
      portIdInternal: Boolean
  ): Option[URI] = {
    val path =
      s"/$eid/port-result?opId=${enc(opId)}&portId=$portIdId&internal=$portIdInternal"
    request("GET", path, None)
      .map(response => new URI(objectMapper.readTree(response).get("uri").asText()))
  }

  def getLatestExecutionId(wid: Integer, cuid: Integer): Option[Integer] = {
    request("GET", s"/latest?wid=$wid&cuid=$cuid", None)
      .map(response => Integer.valueOf(objectMapper.readTree(response).get("eid").asInt()))
  }

  private def enc(value: String): String =
    URLEncoder.encode(value, StandardCharsets.UTF_8.name())

  private def request(method: String, path: String, body: Option[String]): Option[String] =
    request(baseEndpoint, userJwtToken, method, path, body)

  /** Endpoint/token are injectable so the HTTP round-trip can be unit-tested in isolation. */
  private[service] def request(
      baseEndpoint: String,
      token: String,
      method: String,
      path: String,
      body: Option[String]
  ): Option[String] = {
    val connection = new URL(baseEndpoint + path).openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod(method)
    connection.setRequestProperty("Authorization", s"Bearer $token")
    connection.setRequestProperty("Content-Type", "application/json")
    try {
      body.foreach { payload =>
        connection.setDoOutput(true)
        connection.getOutputStream.write(payload.getBytes(StandardCharsets.UTF_8))
      }
      val code = connection.getResponseCode
      if (code >= 200 && code < 300) {
        val stream = connection.getInputStream
        Some(new String(stream.readAllBytes(), StandardCharsets.UTF_8))
      } else if (code == HttpURLConnection.HTTP_NOT_FOUND) {
        None
      } else {
        throw new RuntimeException(
          s"dashboard service execution-metadata request failed: $method $path (HTTP $code)"
        )
      }
    } finally {
      connection.disconnect()
    }
  }
}
