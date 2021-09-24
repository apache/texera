package edu.uci.ics.texera.web.resource.auth

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.JwtAuth._
import edu.uci.ics.texera.web.model.http.request.auth.{
  RefreshTokenRequest,
  UserLoginRequest,
  UserRegistrationRequest
}
import edu.uci.ics.texera.web.model.http.response.TokenIssueResponse
import edu.uci.ics.texera.web.model.jooq.generated.Tables.USER
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.resource.auth.AuthResource._
import org.jasypt.util.password.StrongPasswordEncryptor

import javax.annotation.security.PermitAll
import javax.ws.rs._
import javax.ws.rs.core.MediaType
object AuthResource {

  final private val userDao = new UserDao(SqlServer.createDSLContext.configuration)

  /**
    * Retrieve exactly one User from databases with the given username and password.
    *  The password is used to validate against the hashed password stored in the db.
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
    ).filter(user => new StrongPasswordEncryptor().checkPassword(password, user.getPassword))
  }

  @throws[NotAcceptableException]
  def validateUsername(username: String): Unit = {
    if (username == null) throw new NotAcceptableException("Username cannot be null.")
    if (username.trim.isEmpty) throw new NotAcceptableException("Username cannot be empty.")
  }

}

@Path("/auth/")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class AuthResource {

  @POST
  @Path("/login")
  def login(request: UserLoginRequest): TokenIssueResponse = {
    retrieveUserByUsernameAndPassword(request.username, request.password) match {
      case Some(user) =>
        TokenIssueResponse(jwtToken(jwtClaims(user, dayToMin(TOKEN_EXPIRE_TIME_IN_DAYS))))
      case None => throw new NotAuthorizedException("Login credentials are incorrect.")
    }
  }

  @PermitAll
  @POST
  @Path("/refresh")
  def refresh(request: RefreshTokenRequest): TokenIssueResponse = {
    val claims = jwtConsumer.process(request.accessToken).getJwtClaims
    claims.setExpirationTimeMinutesInTheFuture(dayToMin(TOKEN_EXPIRE_TIME_IN_DAYS))
    TokenIssueResponse(jwtToken(claims))
  }

  @POST
  @Path("/register")
  def register(request: UserRegistrationRequest): TokenIssueResponse = {
    val username = request.username
    validateUsername(username)

    userDao.fetchByName(username).size() match {
      case 0 =>
        val user = new User
        user.setName(username)
        // hash the plain text password
        user.setPassword(new StrongPasswordEncryptor().encryptPassword(request.password))
        userDao.insert(user)
        TokenIssueResponse(jwtToken(jwtClaims(user, dayToMin(TOKEN_EXPIRE_TIME_IN_DAYS))))
      case _ =>
        // the username exists already
        throw new NotAcceptableException("Username exists already.")
    }
  }

}
