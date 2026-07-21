package org.apache.texera.web.resource

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.texera.auth.JwtAuth.{TOKEN_EXPIRE_TIME_IN_MINUTES, jwtClaims, jwtToken}
import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.dao.jooq.generated.enums.ProviderTypeEnum
import org.apache.texera.web.model.http.response.TokenIssueResponse
import org.apache.texera.web.resource.auth.{ExternalAuthProvisioner, ExternalProfile}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import javax.ws.rs.core.{MediaType, UriBuilder}
import javax.ws.rs._

@Path("/auth/facebook")
class FacebookAuthResource {

  final private lazy val clientId = UserSystemConfig.facebookClientId

  @GET
  @Path("/clientid")
  def getClientId: String = clientId

  private val http = HttpClient.newHttpClient()
  private val mapper = new ObjectMapper()

  private def getJson(uri: URI) = {
    val resp = http.send(
      HttpRequest.newBuilder(uri).GET().build(),
      HttpResponse.BodyHandlers.ofString()
    )
    if (resp.statusCode() != 200)
      throw new NotAuthorizedException(s"Facebook API error: ${resp.statusCode()}")
    mapper.readTree(resp.body())
  }

  private def verifyFacebookToken(accessToken: String): (String, Option[String], Option[String]) = {
    val facebookUrl = "https://graph.facebook.com"
    val appId     = UserSystemConfig.facebookClientId
    val appSecret = UserSystemConfig.facebookAppSecret
    val appToken  = s"$appId|$appSecret"

    val verifyRequest = getJson(
      UriBuilder
        .fromUri(s"$facebookUrl/debug_token")
        .queryParam("input_token", accessToken)
        .queryParam("access_token", appToken)
        .build()
    ).path("data")

    if (!verifyRequest.path("is_valid").asBoolean(false) || verifyRequest.path("app_id").asText() != appId)
      throw new NotAuthorizedException("Invalid Facebook token")

    val me = getJson(
      UriBuilder
        .fromUri(s"$facebookUrl/me")
        .queryParam("fields", "id,name,email")
        .queryParam("access_token", accessToken)
        .build()
    )

    val id    = me.path("id").asText()
    val name  = Option(me.path("name").asText(null))
    val email = Option(me.path("email").asText(null))
    (id, name, email)
  }

  @POST
  @Consumes(Array(MediaType.TEXT_PLAIN))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/login")
  def login(credential: String): TokenIssueResponse = {
    val (facebookId, nameOpt, emailOpt) = verifyFacebookToken(credential)

    if (facebookId.isEmpty) throw new NotAuthorizedException("Login credentials are incorrect.")

    val facebookEmail = emailOpt.filter(_.nonEmpty).getOrElse(s"$facebookId@facebook.local")
    val facebookName = nameOpt.filter(_.nonEmpty).getOrElse(facebookEmail)

    val user = ExternalAuthProvisioner.loginOrProvision(
      ExternalProfile(ProviderTypeEnum.FACEBOOK, facebookId, facebookName, facebookEmail)
    )

    TokenIssueResponse(jwtToken(jwtClaims(user, TOKEN_EXPIRE_TIME_IN_MINUTES)))
  }

}
