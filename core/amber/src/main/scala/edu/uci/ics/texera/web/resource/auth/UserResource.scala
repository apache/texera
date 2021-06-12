package edu.uci.ics.texera.web.resource.auth

import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.typesafe.config.Config
import edu.uci.ics.texera.web.{SqlServer, WebUtils}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.jooq.generated.Tables.USER
import edu.uci.ics.texera.web.model.request.auth.{
  GoogleUserLoginRequest,
  UserLoginRequest,
  UserRegistrationRequest
}
import edu.uci.ics.texera.web.resource.auth.UserResource.{getUser, setUserSession, validateUsername}
import io.dropwizard.jersey.sessions.Session
import org.apache.commons.lang3.tuple.Pair

import javax.servlet.http.HttpSession
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object UserResource {

  private val SESSION_USER = "texera-user"

  // TODO: rewrite this
  def getUser(session: HttpSession): Option[User] =
    Option.apply(session.getAttribute(SESSION_USER)).map(u => u.asInstanceOf[User])

  // TODO: rewrite this
  private def validateUsername(userName: String): Pair[Boolean, String] =
    if (userName == null) Pair.of(false, "username cannot be null")
    else if (userName.trim.isEmpty) Pair.of(false, "username cannot be empty")
    else Pair.of(true, "username validation success")

  private def setUserSession(session: HttpSession, user: User): Unit = {
    session.setAttribute(SESSION_USER, user)
  }

}

@Path("/users/")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class UserResource {

  final private val userDao = new UserDao(SqlServer.createDSLContext.configuration)
  val config: Config = WebUtils.config
  private val GOOGLE_CLIENT_ID: String = config.getString("google.clientId")
  private val GOOGLE_CLIENT_SECRET: String = config.getString("google.clientSecret")
  private val TRANSPORT = new NetHttpTransport
  private val JSON_FACTORY = new JacksonFactory

  @GET
  @Path("/auth/status")
  def authStatus(@Session session: HttpSession): Option[User] = {
    getUser(session)
  }

  @POST
  @Path("/login")
  def login(@Session session: HttpSession, request: UserLoginRequest): Response = {

    retrieveUserByUsernameAndPassword(request.userName, request.password) match {
      case Some(user) =>
        setUserSession(
          session,
          new User(user.getName, user.getUid, null, null)
        )
        Response.ok().build()
      case None => Response.status(Response.Status.UNAUTHORIZED).build()
    }
  }

  @POST
  @Path("/google-login")
  def googleLogin(@Session session: HttpSession, request: GoogleUserLoginRequest): Response = {

    retrieveUserByGoogleAuthCode(request.authoCode) match {
      case Success(user) =>
        // set session
        setUserSession(session, user)
        Response.ok().build()
      case Failure(_) => Response.status(Response.Status.UNAUTHORIZED).build()
    }

  }

  @POST
  @Path("/register")
  def register(@Session session: HttpSession, request: UserRegistrationRequest): Response = {
    val userName = request.userName
    var password = request.password
    val validationResult = validateUsername(userName)
    if (!validationResult.getLeft)
      // Using BAD_REQUEST as no other status code is suitable. Better to use 422.
      return Response.status(Response.Status.BAD_REQUEST).build()

    // hash the plain text password
    password = PasswordEncryption.encrypt(password)

    // the username is existing already
    if (this.userDao.fetchByName(userName).asScala.toList.exists(_.getGoogleId == null)) {
      // Using BAD_REQUEST as no other status code is suitable. Better to use 422.
      Response.status(Response.Status.BAD_REQUEST).build()
    } else {
      val user = new User
      user.setName(userName)
      user.setPassword(password)
      this.userDao.insert(user)
      user.setPassword(null)
      setUserSession(session, user)
      Response.ok().build()
    }

  }

  @GET
  @Path("/logout")
  def logOut(@Session session: HttpSession): Response = {
    setUserSession(session, null)
    Response.ok().build()
  }

  private def retrieveUserByGoogleAuthCode(googleAuthCode: String): Try[User] = {

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
    val userName = payload.get("name").asInstanceOf[String]

    // get access token and refresh token (used for accessing Google API Service)
    // val access_token = tokenResponse.getAccessToken
    // val refresh_token = tokenResponse.getRefreshToken

    // store Google user id in database if it does not exist
    Option(userDao.fetchOneByGoogleId(googleId)) match {
      case Some(user) =>
        // the user's Google username could have been updated (due to user's action)
        // we update the user name in such case to reflect the change.
        if (user.getName != userName) {
          user.setName(userName)
          userDao.update(user)
        }
        Success(user)
      case None =>
        // create a new user with googleId
        val user = new User
        user.setName(userName)
        user.setGoogleId(googleId)
        userDao.insert(user)
        Success(user)
    }
  }

  private def retrieveUserByUsernameAndPassword(name: String, password: String): Option[User] = {
    Option(
      SqlServer.createDSLContext
        .select()
        .from(USER)
        .where(USER.NAME.eq(name).and(USER.GOOGLE_ID.isNull))
        .fetchOneInto(classOf[User])
    ).filter(user => PasswordEncryption.checkPassword(user.getPassword, password))
  }

}
