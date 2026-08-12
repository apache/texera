package org.apache.texera.web.resource.auth

import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.common.config.UserSystemConfig.orcidBaseUrl
import org.apache.texera.web.model.http.response.TokenIssueResponse
import org.apache.texera.web.resource.auth.OrcidAuthResource.clientId

import javax.ws.rs.core.MediaType
import javax.ws.rs.{Consumes, GET, POST, Path, Produces}

object OrcidAuthResource {
  final private lazy val clientId = UserSystemConfig.orcidClientId
}

@Path("/auth/orcid")
class OrcidAuthResource {
  @GET
  @Path("/config")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getConfig: Map[String, String] = Map(
    "clientId" -> clientId,
    "authorizeUrl" -> s"$orcidBaseUrl/oauth/authorize"
  )

  @POST
  @Consumes(Array(MediaType.TEXT_PLAIN))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/login")
  def login(code: String): TokenIssueResponse = {
    print("this works!")
    throw new NotImplementedError("you haven't actually done sign in")
  }
}
