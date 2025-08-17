package edu.uci.ics.texera.web.resource.auth

import edu.uci.ics.texera.auth.{JwtAuth, SessionUser}
import edu.uci.ics.texera.web.auth.DownloadTokenClaims
import io.dropwizard.auth.Auth

import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}

@Path("/auth/download")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
@RolesAllowed(Array("REGULAR", "ADMIN"))
class DownloadTokenResource {

  @POST
  @Path("/token")
  def generateDownloadToken(
      request: DownloadTokenClaims,
      @Auth user: SessionUser
  ): Response = {
    val claims = JwtAuth.jwtDownloadClaims(
      request.exportType,
      request.workflowId,
      request.workflowName,
      request.rowIndex,
      request.columnIndex,
      request.filename,
      request.computingUnitId,
      request.destination,
      JwtAuth.DOWNLOAD_TOKEN_EXPIRE_TIME_IN_SECONDS
    )
    val token = JwtAuth.jwtToken(claims)

    Response.ok(s"""{"token": "$token"}""", MediaType.APPLICATION_JSON).build()
  }

}
