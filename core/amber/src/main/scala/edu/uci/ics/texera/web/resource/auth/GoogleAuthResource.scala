package edu.uci.ics.texera.web.resource.auth

import com.google.api.client.googleapis.auth.oauth2.{
  GoogleAuthorizationCodeTokenRequest,
  GoogleIdToken
}
import com.google.api.client.http.HttpTransport
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.JsonFactory
import com.google.api.client.json.jackson2.JacksonFactory
import com.typesafe.config.{Config, ConfigFactory}
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.JwtAuth.{
  TOKEN_EXPIRE_TIME_IN_DAYS,
  dayToMin,
  jwtClaims,
  jwtToken
}
import edu.uci.ics.texera.web.model.http.request.auth.GoogleUserLoginRequest
import edu.uci.ics.texera.web.model.http.response.TokenIssueResponse
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.resource.auth.GoogleAuthResource.retrieveUserByGoogleAuthCode

import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.util.{Failure, Success, Try}
object GoogleAuthResource {
  final private val userDao = new UserDao(SqlServer.createDSLContext.configuration)
  val googleAPIConfig: Config = ConfigFactory.load("google_api")
  private val GOOGLE_CLIENT_ID: String = googleAPIConfig.getString("google.clientId")
  private val GOOGLE_CLIENT_SECRET: String = googleAPIConfig.getString("google.clientSecret")
  private val transport = new NetHttpTransport
  private val jsonFactory = new JacksonFactory

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
        transport,
        jsonFactory,
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

  /**
    * referenced from https://stackoverflow.com/questions/36496308/get-user-profile-from-googleidtoken
    * The TOKEN_SERVER_URL of GoogleAuthorizationCodeTokenRequest is "https://oauth2.googleapis.com/token",
    * which will not return user's information other than user id, email and email verified.
    */
  class GoogleAuthorizationCodeTokenV4Request(
      val transport: HttpTransport,
      val jsonFactory: JsonFactory,
      val clientId: String,
      val clientSecret: String,
      val code: String,
      val redirectUri: String
  ) extends GoogleAuthorizationCodeTokenRequest(
        transport,
        jsonFactory,
        "https://www.googleapis.com/oauth2/v4/token",
        clientId,
        clientSecret,
        code,
        redirectUri
      ) {}
}

@Path("/auth/google")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class GoogleAuthResource {

  @POST
  @Path("/login")
  def login(request: GoogleUserLoginRequest): TokenIssueResponse = {
    retrieveUserByGoogleAuthCode(request.authCode) match {
      case Success(user) =>
        TokenIssueResponse(jwtToken(jwtClaims(user, dayToMin(TOKEN_EXPIRE_TIME_IN_DAYS))))
      case Failure(_) => throw new NotAuthorizedException("Login credentials are incorrect.")
    }
  }

}
