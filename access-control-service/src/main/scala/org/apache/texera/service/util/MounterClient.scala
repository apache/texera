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

package org.apache.texera.service.util

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.net.{HttpURLConnection, URI}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.util.Using

/**
  * HTTP client for the per-node `texera-mounter`.
  *
  * The mounter is privileged and listens on a hostPort, so it admits exactly one caller: it
  * requires a service-account token minted for its own audience and checks it with the
  * Kubernetes TokenReview API (see `authenticate_caller` in `bin/mounter/mounter.py`). That
  * caller is this service, which is why the client lives here.
  */
class MounterClient(tokenPath: String = MounterDefaults.ProjectedTokenPath) {

  import MounterClient._

  private val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  private val connectTimeoutMs = 10000
  private val readTimeoutMs = 35000

  private def baseUrl(nodeIp: String, port: Int): String = s"http://$nodeIp:$port"

  // Read per call, not cached: the kubelet rewrites the projected token in place.
  private def mounterToken(): String =
    try Files.readString(Paths.get(tokenPath)).trim
    catch {
      case e: Exception =>
        throw new IllegalStateException(
          s"cannot read the mounter service-account token at $tokenPath; without it this " +
            s"service cannot authenticate to the node mounter: ${e.getMessage}"
        )
    }

  def mount(
      nodeIp: String,
      port: Int,
      cuid: String,
      repositoryName: String,
      commitHash: String,
      jwt: String,
      fileServiceBase: String
  ): String = {
    MountRequestValidation.validate(cuid, repositoryName, commitHash)

    val body = mapper.createObjectNode()
    body.put("cuid", cuid)
    body.put("repositoryName", repositoryName)
    body.put("commitHash", commitHash)
    body.put("jwt", jwt)
    body.put("fileServiceBase", fileServiceBase)

    val response = send("POST", s"${baseUrl(nodeIp, port)}/mount", Some(body.toString))
    Option(response.get("mountPath"))
      .filter(node => node.isTextual && node.asText().nonEmpty)
      .map(_.asText())
      .getOrElse(
        throw new IllegalStateException(
          s"mounter at $nodeIp reported success without a mount path: $response"
        )
      )
  }

  private def send(method: String, url: String, body: Option[String]): JsonNode = {
    val connection = URI.create(url).toURL.openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod(method)
    connection.setRequestProperty("Authorization", s"Bearer ${mounterToken()}")
    connection.setConnectTimeout(connectTimeoutMs)
    connection.setReadTimeout(readTimeoutMs)
    body.foreach { _ =>
      connection.setRequestProperty("Content-Type", "application/json")
      connection.setDoOutput(true)
    }
    try {
      body.foreach(payload =>
        Using(connection.getOutputStream)(_.write(payload.getBytes(StandardCharsets.UTF_8)))
      )
      val code = connection.getResponseCode
      val stream =
        if (code >= 200 && code < 300) connection.getInputStream else connection.getErrorStream
      val responseBody = Option(stream)
        .map(s => new String(s.readAllBytes(), StandardCharsets.UTF_8))
        .getOrElse("")
      if (code < 200 || code >= 300) {
        throw new MounterRequestException(code, s"mounter $method failed: HTTP $code $responseBody")
      }
      if (responseBody.isEmpty) mapper.createObjectNode() else mapper.readTree(responseBody)
    } finally {
      connection.disconnect()
    }
  }
}

object MounterClient extends MounterClient(MounterDefaults.ProjectedTokenPath) {

  class MounterRequestException(val status: Int, message: String) extends RuntimeException(message)
}

private object MounterDefaults {

  /** Where the chart projects the token. Not configurable: the same chart fixes both ends. */
  val ProjectedTokenPath = "/var/run/secrets/texera/mounter/token"
}

/**
  * The shape a mount request has to have before anything acts on it.
  *
  * The mounter joins these into a path and creates the directory, so each has to be a single
  * safe segment: no separator, no "..", and a leading alphanumeric so a value cannot be read
  * as a geesefs flag. The mounter enforces this itself, being privileged; this is the same
  * rule applied earlier, so a malformed request is refused by name rather than by whatever
  * it fails next.
  */
private[service] object MountRequestValidation {

  private val cuidPattern = "^[0-9]+$".r
  private val segmentPattern = "^[A-Za-z0-9][A-Za-z0-9._-]*$".r

  def validate(cuid: String, repositoryName: String, commitHash: String): Unit = {
    if (cuid == null || cuidPattern.findFirstIn(cuid).isEmpty) {
      throw new IllegalArgumentException(s"cuid must be a non-negative integer, got '$cuid'")
    }
    requireSegment(repositoryName, "repositoryName")
    requireSegment(commitHash, "commitHash")
  }

  private def requireSegment(value: String, field: String): Unit =
    if (value == null || segmentPattern.findFirstIn(value).isEmpty) {
      throw new IllegalArgumentException(s"$field must be a single path segment, got '$value'")
    }
}
