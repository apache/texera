package edu.uci.ics.texera.web.resource.auth

import edu.uci.ics.texera.web.{SqlServer, WebUtils}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.GoogleUserDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.GoogleUser
import edu.uci.ics.texera.web.model.request.auth.{
  GoogleUserLoginRequest,
  UserLoginRequest,
  UserRegistrationRequest
}
import edu.uci.ics.texera.web.resource.auth.UserResource.{
  getGoogleUser,
  getUser,
  isGoogleUser,
  setGoogleUserSession,
  setUserSession,
  validateUsername
}
import io.dropwizard.jersey.sessions.Session
import org.apache.commons.lang3.tuple.Pair
import org.jooq.exception.DataAccessException
import javax.servlet.http.HttpSession
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken
import com.typesafe.config.Config

object UserResource {

  private val SESSION_USER = "texera-user"
  private val SESSION_GOOGLE_USER = "texera-google-user"
  // TODO: rewrite this
  def getUser(session: HttpSession): Option[User] =
    Option.apply(session.getAttribute(SESSION_USER)).map(u => u.asInstanceOf[User])

  def getGoogleUser(session: HttpSession): Option[GoogleUser] =
    Option.apply(session.getAttribute(SESSION_GOOGLE_USER)).map(u => u.asInstanceOf[GoogleUser])

  def isGoogleUser(session: HttpSession): Boolean = {
    session.getAttribute(SESSION_GOOGLE_USER) != null
  }

  // TODO: rewrite this
  private def validateUsername(userName: String): Pair[Boolean, String] =
    if (userName == null) Pair.of(false, "username cannot be null")
    else if (userName.trim.isEmpty) Pair.of(false, "username cannot be empty")
    else Pair.of(true, "username validation success")

  private def setUserSession(session: HttpSession, user: User): Unit = {
    session.setAttribute(SESSION_USER, user)
  }

  private def setGoogleUserSession(session: HttpSession, Googleuser: GoogleUser): Unit = {
    session.setAttribute(SESSION_GOOGLE_USER, Googleuser)
  }

}

@Path("/users/")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class UserResource {

  val config: Config = WebUtils.config

  private val GOOGLE_CLIENT_ID = config.getString("google.clientId")
  private val GOOGLE_CLIENT_SECRET = config.getString("google.clientSecret")

  final private val userDao = new UserDao(SqlServer.createDSLContext.configuration)
  final private val googleUserDao = new GoogleUserDao(SqlServer.createDSLContext.configuration)

  private val TRANSPORT = new NetHttpTransport
  private val JSON_FACTORY = new JacksonFactory

  @GET
  @Path("/auth/status")
  def authStatus(@Session session: HttpSession) = {
    if (isGoogleUser(session)) {
      getGoogleUser(session)
    } else {
      getUser(session)
    }
  }

  @POST
  @Path("/login")
  def login(@Session session: HttpSession, request: UserLoginRequest): Response = {

    // try to fetch the password given the username
    val userPassword = this.userDao.fetchOneByName(request.userName).getPassword

    // not found or password incorrect
    if (userPassword == null || !PasswordEncryption.checkPassword(userPassword, request.password)) {
      return Response.status(Response.Status.UNAUTHORIZED).build()
    }

    setUserSession(
      session,
      new User(request.userName, this.userDao.fetchOneByName(request.userName).getUid, null)
    )
    Response.ok().build()
  }

  @POST
  @Path("/google-login")
  def googleLogin(@Session session: HttpSession, request: GoogleUserLoginRequest): Response = {
    // get authorization code from request
    var code = request.authoCode

    // use authorization code to get tokens
    try {
      val tokenResponse = new GoogleAuthorizationCodeTokenV4Request(
        TRANSPORT,
        JSON_FACTORY,
        CLIENT_ID,
        CLIENT_SECRET,
        code,
        "postmessage"
      ).execute();
      // get the id token
      val idToken: GoogleIdToken = tokenResponse.parseIdToken()
      // get the payload of id token
      val payload = idToken.getPayload
      // get the subject of the payload, use this value as a key to identify a user.
      val userId = payload.getSubject
      // get the name of the user
      val userName = payload.get("name").asInstanceOf[String]

      // store Google user id in database if it does not exist
      if (this.googleUserDao.fetchOneByUid(userId) == null) {
        val googleUser = new GoogleUser(userId, userName)
        this.googleUserDao.insert(googleUser)
      } else if (this.googleUserDao.fetchOneByUid(userId).getName != userName) {
        this.googleUserDao.fetchOneByUid(userId).setName(userName)
      }

      // get access token and refresh token (used foe access Google API Service)
      val access_token = tokenResponse.getAccessToken
      val refresh_token = tokenResponse.getRefreshToken

      // set session
      setGoogleUserSession(
        session,
        new GoogleUser(userId, userName)
      )
    }
    Response.ok().build()
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
    password = PasswordEncryption.encrypt(password);

    // try to insert a new record
    try {
      val user = new User
      user.setName(userName)
      user.setPassword(password)
      this.userDao.insert(user)
      user.setPassword(null)
      setUserSession(session, user)
      Response.ok().build()
    } catch {
      // the username is existing already
      case _: DataAccessException =>
        // Using BAD_REQUEST as no other status code is suitable. Better to use 422.
        Response.status(Response.Status.BAD_REQUEST).build()
    }

  }

  @GET
  @Path("/logout")
  def logOut(@Session session: HttpSession): Response = {
    if (isGoogleUser(session)) {
      setGoogleUserSession(session, null)
    } else {
      setUserSession(session, null)
    }
    Response.ok().build()
  }

}
