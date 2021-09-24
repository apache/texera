package edu.uci.ics.texera.web.resource.auth

import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.typesafe.config.{Config, ConfigFactory}
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.JwtAuth.{jwtConsumer, jwtTokenSecret}
import edu.uci.ics.texera.web.model.jooq.generated.Tables.USER
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.request.auth.{
  GoogleUserLoginRequest,
  RefreshTokenRequest,
  UserLoginRequest,
  UserRegistrationRequest
}
import edu.uci.ics.texera.web.resource.auth.AuthResource._
import io.dropwizard.jersey.sessions.Session
import org.apache.commons.lang3.tuple.Pair
import org.jose4j.jws.AlgorithmIdentifiers.HMAC_SHA256
import org.jose4j.jws.JsonWebSignature
import org.jose4j.jwt.JwtClaims
import org.jose4j.keys.HmacKey

import javax.annotation.security.PermitAll
import javax.servlet.http.HttpSession
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import scala.util.{Failure, Success, Try}

object AuthResource {
  final private val TOKEN_EXPIRE_TIME_IN_DAYS =
    AmberUtils.amberConfig.getString("user-sys.jwt.exp-in-days").toInt
  val googleAPIConfig: Config = ConfigFactory.load("google_api")
  private val SESSION_USER = "texera-user"
  private val GOOGLE_CLIENT_ID: String = googleAPIConfig.getString("google.clientId")
  private val GOOGLE_CLIENT_SECRET: String = googleAPIConfig.getString("google.clientSecret")

  /**
    * Retrieve exactly one User from databases with the given username and password
    * @param name String
    * @param password String, plain text password
    * @return
    */
  def retrieveUserByUsernameAndPassword(name: String, password: String): Option[User] = {
    Option(
      SqlServer.createDSLContext
        .select()
        .from(USER)
        .where(USER.NAME.eq(name).and(USER.GOOGLE_ID.isNull))
        .fetchOneInto(classOf[User])
    ).filter(user => PasswordEncryption.checkPassword(user.getPassword, password))
  }

  def generateNewJwtToken(claims: JwtClaims): String = {
    val jws = new JsonWebSignature()
    jws.setPayload(claims.toJson)
    jws.setAlgorithmHeaderValue(HMAC_SHA256)
    jws.setKey(new HmacKey(jwtTokenSecret.getBytes))
    jws.getCompactSerialization
  }

  def generateNewJwtClaims(user: User): JwtClaims = {
    val claims = new JwtClaims
    claims.setSubject(user.getName)
    claims.setClaim("userId", user.getUid)
    claims.setExpirationTimeMinutesInTheFuture(TOKEN_EXPIRE_TIME_IN_DAYS * 24 * 60)
    claims
  }

  // TODO: rewrite this
  private def validateUsername(userName: String): Pair[Boolean, String] =
    if (userName == null) Pair.of(false, "username cannot be null")
    else if (userName.trim.isEmpty) Pair.of(false, "username cannot be empty")
    else Pair.of(true, "username validation success")

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

}

@Path("/auth/")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class AuthResource {

  final private val userDao = new UserDao(SqlServer.createDSLContext.configuration)

  private val TRANSPORT = new NetHttpTransport
  private val JSON_FACTORY = new JacksonFactory

  @POST
  @Path("/login")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def login(request: UserLoginRequest): Response = {
    retrieveUserByUsernameAndPassword(request.userName, request.password) match {
      case Some(user) =>
        val claims = generateNewJwtClaims(user)
        Response.ok.entity(Map("accessToken" -> generateNewJwtToken(claims))).build()

      case None => Response.status(Response.Status.UNAUTHORIZED).build()
    }

  }

  @PermitAll
  @POST
  @Path("/refresh")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def refreshToken(request: RefreshTokenRequest): Response = {
    val claims = jwtConsumer.process(request.accessToken).getJwtClaims
    claims.setExpirationTimeMinutesInTheFuture(TOKEN_EXPIRE_TIME_IN_DAYS * 24 * 60)
    Response.ok.entity(Map("accessToken" -> generateNewJwtToken(claims))).build()
  }

  @POST
  @Path("/google-login")
  def googleLogin(@Session session: HttpSession, request: GoogleUserLoginRequest): Response = {

    retrieveUserByGoogleAuthCode(request.authCode) match {
      case Success(user) =>
        setUserSession(session, Some(user))
        Response.ok(user).build()
      case Failure(_) => Response.status(Response.Status.UNAUTHORIZED).build()
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

  @POST
  @Path("/register")
  def register(request: UserRegistrationRequest): Response = {
    val userName = request.userName
    val password = request.password
    val validationResult = validateUsername(userName)
    if (!validationResult.getLeft)
      // Using BAD_REQUEST as no other status code is suitable. Better to use 422.
      return Response.status(Response.Status.BAD_REQUEST).build()

    retrieveUserByUsernameAndPassword(userName, password) match {
      case Some(_) =>
        // the username is existing already
        // Using BAD_REQUEST as no other status code is suitable. Better to use 422.
        Response.status(Response.Status.BAD_REQUEST).build()
      case None =>
        val user = new User
        user.setName(userName)
        // hash the plain text password
        user.setPassword(PasswordEncryption.encrypt(password))
        userDao.insert(user)
        val claims = generateNewJwtClaims(user)
        Response.ok.entity(Map("accessToken" -> generateNewJwtToken(claims))).build()
    }

  }

}
