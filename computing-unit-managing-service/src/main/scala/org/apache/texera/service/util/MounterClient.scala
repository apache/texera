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

import play.api.libs.json._

import java.net.{HttpURLConnection, URL, URLEncoder}
import java.nio.charset.StandardCharsets
import scala.util.Using

/**
  * Thin HTTP client for the per-node `texera-mounter`. The computing-unit service uses it
  * to mount/unmount/list datasets on a CU's node on the user's behalf: it resolves a
  * dataset path to a repository/commit, finds the CU pod's node IP, and forwards the
  * request here. The mounter holds no credentials; the JWT passed on mount is forwarded to
  * GeeseFS as the S3 access key so authorization stays entirely in file-service.
  */
object MounterClient {

  case class MountEntry(repositoryName: String, commitHash: String, mountPath: String)

  private val connectTimeoutMs = 10000
  // The mounter waits up to 30s for a fresh mount to appear; give it a little headroom.
  private val readTimeoutMs = 35000

  private def baseUrl(nodeIp: String, port: Int): String = s"http://$nodeIp:$port"

  /** Mount `repositoryName:commitHash` for `cuid`; returns the mounter's host mount path. */
  def mount(
      nodeIp: String,
      port: Int,
      cuid: String,
      repositoryName: String,
      commitHash: String,
      jwt: String,
      fileServiceBase: String
  ): String = {
    val body = Json.obj(
      "cuid" -> cuid,
      "repositoryName" -> repositoryName,
      "commitHash" -> commitHash,
      "jwt" -> jwt,
      "fileServiceBase" -> fileServiceBase
    )
    val resp = send("POST", s"${baseUrl(nodeIp, port)}/mount", Some(body))
    (resp \ "mountPath").asOpt[String].getOrElse("")
  }

  /** List the datasets currently mounted for `cuid` on this node. */
  def listMounts(nodeIp: String, port: Int, cuid: String): List[MountEntry] = {
    val resp = send("GET", s"${baseUrl(nodeIp, port)}/mounts?cuid=${enc(cuid)}", None)
    (resp \ "mounts").asOpt[List[JsValue]].getOrElse(Nil).map { m =>
      MountEntry(
        (m \ "repositoryName").as[String],
        (m \ "commitHash").as[String],
        (m \ "mountPath").as[String]
      )
    }
  }

  private def enc(s: String): String = URLEncoder.encode(s, StandardCharsets.UTF_8)

  private def send(method: String, url: String, body: Option[JsObject]): JsValue = {
    val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod(method)
    connection.setConnectTimeout(connectTimeoutMs)
    connection.setReadTimeout(readTimeoutMs)
    body.foreach { _ =>
      connection.setRequestProperty("Content-Type", "application/json")
      connection.setDoOutput(true)
    }
    try {
      body.foreach(b =>
        Using(connection.getOutputStream)(
          _.write(Json.stringify(b).getBytes(StandardCharsets.UTF_8))
        )
      )
      val code = connection.getResponseCode
      val stream =
        if (code >= 200 && code < 300) connection.getInputStream else connection.getErrorStream
      val text = Option(stream)
        .map(s => new String(s.readAllBytes(), StandardCharsets.UTF_8))
        .getOrElse("")
      if (code < 200 || code >= 300) {
        throw new RuntimeException(s"mounter $method $url failed: HTTP $code $text")
      }
      if (text.isEmpty) Json.obj() else Json.parse(text)
    } finally {
      connection.disconnect()
    }
  }
}
