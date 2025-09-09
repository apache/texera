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
  import jakarta.ws.rs.{GET, POST, Path, Produces}
  import jakarta.ws.rs.core.{Context, HttpHeaders, MediaType, Response, UriInfo}

  import java.util.Optional
  import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsScala}

  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/auth")
  class AccessControlResource extends LazyLogging {

    private val computingUnitAccess: ComputingUnitAccess = new ComputingUnitAccess()

    private def performAuth(
        uriInfo: UriInfo,
        headers: HttpHeaders
    ): Response = {
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
        cuAccess = computingUnitAccess.getComputingUnitAccess(cuidInt, uid)
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

    @GET
    @Path("/{path:.*}")
    def authorizeGet(
        @Context uriInfo: UriInfo,
        @Context headers: HttpHeaders
    ): Response = {
      performAuth(uriInfo, headers)
    }

    @POST
    @Path("/{path:.*}")
    def authorizePost(
        @Context uriInfo: UriInfo,
        @Context headers: HttpHeaders
    ): Response = {
      performAuth(uriInfo, headers)
    }
  }
