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

package edu.uci.ics.texera.service.resource

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.auth.JwtParser.parseToken
import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.auth.util.{ComputingUnitAccess, HeaderField}
import edu.uci.ics.texera.dao.jooq.generated.enums.PrivilegeEnum
import jakarta.ws.rs.core._
import jakarta.ws.rs.{GET, POST, Path, Produces}

import java.util.Optional
import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsScala}
import scala.util.matching.Regex

object AccessControlResource extends LazyLogging {

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
  def authorize(uriInfo: UriInfo, headers: HttpHeaders): Response = {
    val path = uriInfo.getPath
    logger.info(s"Authorizing request for path: $path")

    path match {
      case wsapiWorkflowWebsocket() | apiExecutionsStats() | apiExecutionsResultExport() =>
        checkComputingUnitAccess(uriInfo, headers)
      case _ =>
        logger.warn(s"No authorization logic for path: $path. Denying access.")
        Response.status(Response.Status.FORBIDDEN).build()
    }
  }

  private def checkComputingUnitAccess(uriInfo: UriInfo, headers: HttpHeaders): Response = {
    val queryParams: Map[String, String] = uriInfo
      .getQueryParameters()
      .asScala
      .view
      .mapValues(values => values.asScala.headOption.getOrElse(""))
      .toMap

    logger.info(s"Request URI: ${uriInfo.getRequestUri} and headers: ${headers.getRequestHeaders.asScala} and queryParams: $queryParams")

    val token = queryParams.getOrElse(
      "access-token",
      headers
        .getRequestHeader("Authorization")
        .asScala
        .headOption
        .getOrElse("")
        .replace("Bearer ", "")
    )
    val cuid = queryParams.getOrElse("cuid", "")
    val cuidInt = try {
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
                     @Context headers: HttpHeaders
                   ): Response = {
    AccessControlResource.authorize(uriInfo, headers)
  }
}
