package edu.uci.ics.texera.web.resource.auth

import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeTokenRequest
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.request.auth.{GoogleUserLoginRequest, UserLoginRequest, UserRegistrationRequest}
import edu.uci.ics.texera.web.resource.auth.UserResource.{getUser, setUserSession, validateUsername}
import io.dropwizard.jersey.sessions.Session
import org.apache.commons.lang3.tuple.Pair
import org.jooq.exception.DataAccessException
import javax.servlet.http.HttpSession
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import com.google.api.client.http.HttpTransport
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken
import com.google.api.client.googleapis.auth.oauth2._
import org.jooq.types.UInteger


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

  private val TRANSPORT = new NetHttpTransport
  private val JSON_FACTORY = new JacksonFactory
  private val CLIENT_ID = "256268030075-jl765kbkpbu2j4am3cjbtlrr973kqgdp.apps.googleusercontent.com"
  private val CLIENT_SECRET = "vSJ3DjZ9_Bf4WsM0-MxpZgfV"

  @GET
  @Path("/auth/status")
  def authStatus(@Session session: HttpSession): Option[User] = {
    getUser(session)
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
    try
      {
        val tokenResponse = new GoogleAuthorizationCodeTokenRequest(TRANSPORT, JSON_FACTORY, CLIENT_ID, CLIENT_SECRET, code, "postmessage").execute();
        // get id token
        var idToken: GoogleIdToken = tokenResponse.parseIdToken()
        // get the payload of id token
        val payload = idToken.getPayload()
        // get the subject of the payload, use this value as a key to identify a user.
        val userId = payload.getSubject().toInt
        // get the name of the user
        val userName = payload.get("name").asInstanceOf[String]
        setUserSession(
          session,
          new User(userName, userId.asInstanceOf[UInteger], null)
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
    setUserSession(session, null)
    Response.ok().build()
  }

}
