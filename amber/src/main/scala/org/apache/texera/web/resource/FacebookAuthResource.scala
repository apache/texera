package org.apache.texera.web.resource

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.texera.auth.JwtAuth.{TOKEN_EXPIRE_TIME_IN_MINUTES, jwtClaims, jwtToken}
import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.{AUTH_PROVIDER, USER}
import org.apache.texera.dao.jooq.generated.enums.{ProviderTypeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{AuthProviderDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{AuthProvider, User}
import org.apache.texera.web.model.http.response.TokenIssueResponse

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.OffsetDateTime
import javax.ws.rs.core.{MediaType, UriBuilder}
import javax.ws.rs._
import scala.util.chaining.scalaUtilChainingOps

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

    val user = SqlServer.withTransaction(SqlServer.getInstance().createDSLContext()) { ctx =>
      val txUserDao = new UserDao(ctx.configuration())
      val txAuthDao = new AuthProviderDao(ctx.configuration())

      Option(
        ctx
          .select()
          .from(USER)
          .join(AUTH_PROVIDER)
          .on(USER.UID.eq(AUTH_PROVIDER.UID))
          .where(AUTH_PROVIDER.PROVIDER_TYPE.eq(ProviderTypeEnum.FACEBOOK))
          .and(AUTH_PROVIDER.PROVIDER_ID.eq(facebookId))
          .fetchOne()
      ) match {
        case Some(record) =>
          val uid = record.get(USER.UID)
          txUserDao.fetchOneByUid(uid).tap { user =>
            // name/email are profile fields on "user"; refresh from Facebook if changed
            if (user.getName != facebookName || user.getEmail != facebookEmail) {
              user.setName(facebookName)
              user.setEmail(facebookEmail)
              txUserDao.update(user)
            }
          }
        case None =>
          val user = Option(txUserDao.fetchOneByEmail(facebookEmail)) match {
            case Some(user) =>
              user.tap { user =>
                if (user.getName != facebookName) {
                  user.setName(facebookName)
                  txUserDao.update(user)
                }
              }
            case None =>
              new User().tap { user =>
                user.setName(facebookName)
                user.setEmail(facebookEmail)
                user.setRole(UserRoleEnum.INACTIVE)
                txUserDao.insert(user)
              }
          }

          // an email-matched user may already have a FACEBOOK provider row, so
          // upsert rather than blindly insert (avoids a (uid, provider_type) PK collision)
          val hasFacebookProvider = ctx.fetchExists(
            ctx
              .selectFrom(AUTH_PROVIDER)
              .where(AUTH_PROVIDER.UID.eq(user.getUid))
              .and(AUTH_PROVIDER.PROVIDER_TYPE.eq(ProviderTypeEnum.FACEBOOK))
          )
          if (hasFacebookProvider) {
            ctx
              .update(AUTH_PROVIDER)
              .set(AUTH_PROVIDER.PROVIDER_ID, facebookId)
              .where(AUTH_PROVIDER.UID.eq(user.getUid))
              .and(AUTH_PROVIDER.PROVIDER_TYPE.eq(ProviderTypeEnum.FACEBOOK))
              .execute()
          } else {
            txAuthDao.insert(
              new AuthProvider().tap { auth =>
                auth.setUid(user.getUid)
                auth.setProviderType(ProviderTypeEnum.FACEBOOK)
                auth.setProviderId(facebookId)
                auth.setCreatedAt(OffsetDateTime.now())
              }
            )
          }
          user
      }
    }

    TokenIssueResponse(jwtToken(jwtClaims(user, TOKEN_EXPIRE_TIME_IN_MINUTES)))
  }

}
