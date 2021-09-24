package edu.uci.ics.texera.web.resource.auth

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.JwtAuth.{
  TOKEN_EXPIRE_TIME_IN_DAYS,
  generateNewJwtClaims,
  generateNewJwtToken,
  jwtConsumer
}
import edu.uci.ics.texera.web.auth.PasswordEncryption
import edu.uci.ics.texera.web.model.jooq.generated.Tables.USER
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.request.auth.{
  RefreshTokenRequest,
  UserLoginRequest,
  UserRegistrationRequest
}
import edu.uci.ics.texera.web.resource.auth.AuthResource._
import org.apache.commons.lang3.tuple.Pair

import javax.annotation.security.PermitAll
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}

object AuthResource {

  final private val userDao = new UserDao(SqlServer.createDSLContext.configuration)

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

  // TODO: rewrite this
  private def validateUsername(userName: String): Pair[Boolean, String] =
    if (userName == null) Pair.of(false, "username cannot be null")
    else if (userName.trim.isEmpty) Pair.of(false, "username cannot be empty")
    else Pair.of(true, "username validation success")

}

@Path("/auth/")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class AuthResource {

  @POST
  @Path("/login")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def login(request: UserLoginRequest): Response = {
    retrieveUserByUsernameAndPassword(request.userName, request.password) match {
      case Some(user) =>
        val claims = generateNewJwtClaims(user, TOKEN_EXPIRE_TIME_IN_DAYS)
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
        val claims = generateNewJwtClaims(user, TOKEN_EXPIRE_TIME_IN_DAYS)
        Response.ok.entity(Map("accessToken" -> generateNewJwtToken(claims))).build()
    }

  }

}
