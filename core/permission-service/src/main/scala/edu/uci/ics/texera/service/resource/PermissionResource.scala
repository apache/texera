package edu.uci.ics.texera.service.resource

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.auth.JwtParser.parseToken
import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.jooq.generated.enums.PrivilegeEnum
import edu.uci.ics.texera.service.util.ComputingUnit
import jakarta.ws.rs.{GET, POST, Path, PathParam, Produces}
import jakarta.ws.rs.core.{Context, HttpHeaders, MediaType, Response, UriInfo}

import java.util.Optional
import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsScala}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/authorize")
class PermissionResource extends LazyLogging {

  private val computingUnit: ComputingUnit = new ComputingUnit()

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
      cuAccess = computingUnit.getComputingUnitAccess(cuidInt, uid)
      if (cuAccess == PrivilegeEnum.NONE)
        return Response.status(Response.Status.FORBIDDEN).build()
    } catch {
      case e: Exception =>
        return Response.status(Response.Status.FORBIDDEN).build()
    }

    Response
      .ok()
      .header("x-user-cu-access", cuAccess.toString)
      .header("x-user-id", userSession.get().getUid.toString)
      .header("x-user-name", userSession.get().getName)
      .header("x-user-email", userSession.get().getEmail)
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
