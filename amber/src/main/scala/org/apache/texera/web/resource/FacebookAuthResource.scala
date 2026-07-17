package org.apache.texera.web.resource

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.web.model.http.response.TokenIssueResponse

import java.net.{URI, URLEncoder}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets

import javax.ws.rs.core.MediaType
import javax.ws.rs._

@Path("/auth/facebook")
class FacebookAuthResource {
  final private lazy val clientId = UserSystemConfig.facebookClientId

  @GET
  @Path("/clientid")
  def getClientId: String = clientId

  private val http = HttpClient.newHttpClient()
  private val mapper = new ObjectMapper()

  private def getJson(url: String) = {
    val resp = http.send(
      HttpRequest.newBuilder(URI.create(url)).GET().build(),
      HttpResponse.BodyHandlers.ofString()
    )
    if (resp.statusCode() != 200)
      throw new NotAuthorizedException(s"Facebook API error: ${resp.statusCode()}")
    mapper.readTree(resp.body())
  }
  private def enc(s: String) = URLEncoder.encode(s, StandardCharsets.UTF_8)

  private def verifyFacebookToken(accessToken: String): (String, String, String) = {
    val appId     = UserSystemConfig.facebookClientId
    val appSecret = UserSystemConfig.facebookAppSecret
    val appToken  = s"$appId|$appSecret"

    // 1. validate the token belongs to *this* app and is live
    val debug = getJson(
      s"https://graph.facebook.com/debug_token?input_token=${enc(accessToken)}&access_token=${enc(appToken)}"
    ).path("data")
    if (!debug.path("is_valid").asBoolean(false) || debug.path("app_id").asText() != appId)
      throw new NotAuthorizedException("Invalid Facebook token")

    // 2. fetch the profile
    val me = getJson(
      s"https://graph.facebook.com/me?fields=id,name,email&access_token=${enc(accessToken)}"
    )
    val id    = me.path("id").asText()
    val name  = Option(me.path("name").asText(null)).filter(_.nonEmpty).getOrElse(id)
    val email = me.path("email").asText(null)  // may be null — decide policy
    (id, name, email)
  }

  @POST
  @Consumes(Array(MediaType.TEXT_PLAIN))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/login")
  def login(credential: String): TokenIssueResponse = {
    throw new NotImplementedError("Facebook Oauth isn't implemented yet!")


  }

}
