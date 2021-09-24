package edu.uci.ics.texera.web.resource.auth

import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.typesafe.config.{Config, ConfigFactory}
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.request.auth.GoogleUserLoginRequest
import edu.uci.ics.texera.web.resource.auth.GoogleAuthResource.{GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, SESSION_USER}
import io.dropwizard.jersey.sessions.Session

import javax.servlet.http.HttpSession
import javax.ws.rs.core.{MediaType, Response}
import javax.ws.rs.{Consumes, POST, Path, Produces}
import scala.util.{Failure, Success, Try}
object GoogleAuthResource {
  val googleAPIConfig: Config = ConfigFactory.load("google_api")
  private val SESSION_USER = "texera-user"
  private val GOOGLE_CLIENT_ID: String = googleAPIConfig.getString("google.clientId")
  private val GOOGLE_CLIENT_SECRET: String = googleAPIConfig.getString("google.clientSecret")
}

@Path("/auth/google")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class GoogleAuthResource {
  final private val userDao = new UserDao(SqlServer.createDSLContext.configuration)

  private val TRANSPORT = new NetHttpTransport
  private val JSON_FACTORY = new JacksonFactory

  @POST
  @Path("/login")
  def googleLogin(@Session session: HttpSession, request: GoogleUserLoginRequest): Response = {

    retrieveUserByGoogleAuthCode(request.authCode) match {
      case Success(user) =>
        setUserSession(session, Some(user))
        Response.ok(user).build()
      case Failure(_) => Response.status(Response.Status.UNAUTHORIZED).build()
    }

  }

  /**
    * Set user into the current HTTPSession. It will remove sensitive information of the user.
    * @param session HttpSession, current session being retrieved.
    * @param userToSet Option[User], a user that might contain sensitive information like password.
    *             if None, the session will be cleared.
    */
  private def setUserSession(session: HttpSession, userToSet: Option[User]): Unit = {
    userToSet match {
      case Some(user) =>
        session.setAttribute(SESSION_USER, new User(user.getName, user.getUid, null, null))
      case None =>
        session.setAttribute(SESSION_USER, null)
    }

  }

  /**
    * Retrieve exactly one User from Google with the given googleAuthCode
    * It will update the database to sync with the information retrieved from Google.
    * @param googleAuthCode String, a Google authorization code, see
    *                       https://developers.google.com/identity/protocols/oauth2
    * @return Try[User]
    */
  private def retrieveUserByGoogleAuthCode(googleAuthCode: String): Try[User] = {
    Try({
      // use authorization code to get tokens
      val tokenResponse = new GoogleAuthorizationCodeTokenV4Request(
        TRANSPORT,
        JSON_FACTORY,
        GOOGLE_CLIENT_ID,
        GOOGLE_CLIENT_SECRET,
        googleAuthCode,
        "postmessage"
      ).execute()

      // get the payload of id token
      val payload: GoogleIdToken.Payload = tokenResponse.parseIdToken().getPayload
      // get the subject of the payload, use this value as a key to identify a user.
      val googleId = payload.getSubject
      // get the Google username of the user, will be used as Texera username
      val googleUsername = payload.get("name").asInstanceOf[String]

      // get access token and refresh token (used for accessing Google API Service)
      // val access_token = tokenResponse.getAccessToken
      // val refresh_token = tokenResponse.getRefreshToken

      // store Google user id in database if it does not exist
      Option(userDao.fetchOneByGoogleId(googleId)) match {
        case Some(user) =>
          // the user's Google username could have been updated (due to user's action)
          // we update the user name in such case to reflect the change.
          if (user.getName != googleUsername) {
            user.setName(googleUsername)
            userDao.update(user)
          }
          user
        case None =>
          // create a new user with googleId
          val user = new User
          user.setName(googleUsername)
          user.setGoogleId(googleId)
          userDao.insert(user)
          user
      }
    })
  }
}
