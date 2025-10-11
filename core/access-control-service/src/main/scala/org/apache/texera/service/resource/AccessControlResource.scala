// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.texera.service.resource

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import jakarta.ws.rs.core._
import jakarta.ws.rs.{GET, POST, Path, Produces}
import org.apache.texera.auth.JwtParser.parseToken
import org.apache.texera.auth.SessionUser
import org.apache.texera.auth.util.{ComputingUnitAccess, HeaderField}
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum

import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.util.Optional
import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsScala}
import scala.util.matching.Regex

object AccessControlResource extends LazyLogging {

  private val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  // Regex for the paths that require authorization
  private val wsapiWorkflowWebsocket: Regex = """.*/wsapi/workflow-websocket.*""".r
  private val apiExecutionsStats: Regex = """.*/api/executions/[0-9]+/stats/[0-9]+.*""".r
  private val apiExecutionsResultExport: Regex = """.*/api/executions/result/export.*""".r

  /**
    * Authorize the request based on the path and headers.
    * @param uriInfo URI sent by Envoy or API Gateway
    * @param headers HTTP headers sent by Envoy or API Gateway which include
    *                headers sent by the client (browser)
    * @return HTTP Response with appropriate status code and headers
    */
  def authorize(
      uriInfo: UriInfo,
      headers: HttpHeaders,
      bodyOpt: Option[String] = None
  ): Response = {
    val path = uriInfo.getPath
    logger.info(s"Authorizing request for path: $path")

    path match {
      case wsapiWorkflowWebsocket() | apiExecutionsStats() | apiExecutionsResultExport() =>
        checkComputingUnitAccess(uriInfo, headers, bodyOpt)
      case _ =>
        logger.warn(s"No authorization logic for path: $path. Denying access.")
        Response.status(Response.Status.FORBIDDEN).build()
    }
  }

  private def checkComputingUnitAccess(
      uriInfo: UriInfo,
      headers: HttpHeaders,
      bodyOpt: Option[String]
  ): Response = {
    val queryParams: Map[String, String] = uriInfo
      .getQueryParameters()
      .asScala
      .view
      .mapValues(values => values.asScala.headOption.getOrElse(""))
      .toMap

    logger.info(
      s"Request URI: ${uriInfo.getRequestUri} and headers: ${headers.getRequestHeaders.asScala} and queryParams: $queryParams"
    )

    val token: String = {
      val qToken = queryParams.get("access-token").filter(_.nonEmpty)
      val hToken = Option(headers.getRequestHeader("Authorization"))
        .flatMap(_.asScala.headOption)
        .map(_.replaceFirst("(?i)^Bearer\\s+", "")) // case-insensitive "Bearer "
        .map(_.trim)
        .filter(_.nonEmpty)
      val bToken = bodyOpt.flatMap(extractTokenFromBody)
      qToken.orElse(hToken).orElse(bToken).getOrElse("")
    }
    logger.info(s"token extracted from request $token")
    val cuid = queryParams.getOrElse("cuid", "")
    val cuidInt =
      try {
        cuid.toInt
      } catch {
        case _: NumberFormatException =>
          return Response.status(Response.Status.FORBIDDEN).build()
      }

    var cuAccess: PrivilegeEnum = PrivilegeEnum.NONE
    var userSession: Optional[SessionUser] = Optional.empty()
    try {
      userSession = parseToken(token)
      if (userSession.isEmpty)
        return Response.status(Response.Status.FORBIDDEN).build()

      val uid = userSession.get().getUid
      cuAccess = ComputingUnitAccess.getComputingUnitAccess(cuidInt, uid)
      if (cuAccess == PrivilegeEnum.NONE)
        return Response.status(Response.Status.FORBIDDEN).build()
    } catch {
      case e: Exception =>
        logger.error(s"Failed parsing token $e")
        return Response.status(Response.Status.FORBIDDEN).build()
    }

    Response
      .ok()
      .header(HeaderField.UserComputingUnitAccess, cuAccess.toString)
      .header(HeaderField.UserId, userSession.get().getUid.toString)
      .header(HeaderField.UserName, userSession.get().getName)
      .header(HeaderField.UserEmail, userSession.get().getEmail)
      .build()
  }

  // Extracts a top-level "token" field from a JSON body
  private def extractTokenFromBody(body: String): Option[String] = {
    // 1) Try JSON
    val jsonToken: Option[String] =
      try {
        val node = mapper.readTree(body)
        if (node != null && node.has("token"))
          Option(node.get("token").asText()).map(_.trim).filter(_.nonEmpty)
        else None
      } catch {
        case _: Exception => None
      }

    // 2) Try application/x-www-form-urlencoded
    def extractTokenFromUrlEncoded(s: String): Option[String] = {
      // fast path: must contain '=' or '&'
      if (!s.contains("=")) return None
      val pairs = s.split("&").iterator
      var found: Option[String] = None
      while (pairs.hasNext && found.isEmpty) {
        val p = pairs.next()
        val idx = p.indexOf('=')
        val key = if (idx >= 0) p.substring(0, idx) else p
        if (key == "token") {
          val raw = if (idx >= 0) p.substring(idx + 1) else ""
          val decoded = URLDecoder.decode(raw, StandardCharsets.UTF_8.name())
          val v = decoded.trim
          if (v.nonEmpty) found = Some(v)
        }
      }
      found
    }

    // 3) Try multipart/form-data (best-effort; parses raw body text)
    def extractTokenFromMultipart(s: String): Option[String] = {
      // Look for the part with name="token" and capture its content until the next boundary
      val partWithBoundary = "(?s)name\\s*=\\s*\"token\"[^\\r\\n]*\\r?\\n\\r?\\n(.*?)\\r?\\n--".r
      val partToEnd = "(?s)name\\s*=\\s*\"token\"[^\\r\\n]*\\r?\\n\\r?\\n(.*)".r

      partWithBoundary
        .findFirstMatchIn(s)
        .map(_.group(1).trim)
        .filter(_.nonEmpty)
        .orElse(partToEnd.findFirstMatchIn(s).map(_.group(1).trim).filter(_.nonEmpty))
    }

    jsonToken
      .orElse(extractTokenFromUrlEncoded(body))
      .orElse(extractTokenFromMultipart(body))
  }
}
@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/auth")
class AccessControlResource extends LazyLogging {

  @GET
  @Path("/{path:.*}")
  def authorizeGet(
      @Context uriInfo: UriInfo,
      @Context headers: HttpHeaders
  ): Response = {
    AccessControlResource.authorize(uriInfo, headers)
  }

  @POST
  @Path("/{path:.*}")
  def authorizePost(
      @Context uriInfo: UriInfo,
      @Context headers: HttpHeaders,
      body: String
  ): Response = {
    logger.info("Request body: " + body)
    AccessControlResource.authorize(uriInfo, headers, Option(body).map(_.trim).filter(_.nonEmpty))
  }
}
