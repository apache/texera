package edu.uci.ics.texera.web.resource.auth

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.request.auth.{UserLoginRequest, UserRegistrationRequest}
import edu.uci.ics.texera.web.resource.auth.UserResource.{getUser, setUserSession, validateUsername}
import io.dropwizard.jersey.sessions.Session
import org.apache.commons.lang3.tuple.Pair
import org.jooq.exception.DataAccessException

import javax.servlet.http.HttpSession
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}

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

  @GET
  @Path("/auth/status")
  def authStatus(@Session session: HttpSession): Option[User] = {
    getUser(session)
  }

  @POST
  @Path("/login")
  def login(@Session session: HttpSession, request: UserLoginRequest): Response = {

    // try to fetch the record
    val user = this.userDao.fetchOneByName(request.userName)
    if (user == null) { // not found
      return Response.status(Response.Status.UNAUTHORIZED).build()
    }
    setUserSession(session, new User(request.userName, user.getUid))
    Response.ok().build()
  }

  @POST
  @Path("/register")
  def register(@Session session: HttpSession, request: UserRegistrationRequest): Response = {
    val userName = request.userName
    val validationResult = validateUsername(userName)
    if (!validationResult.getLeft)
      // Using BAD_REQUEST as no other status code is suitable. Better to use 422.
      return Response.status(Response.Status.BAD_REQUEST).build()

    // try to insert a new record
    try {
      val user = new User
      user.setName(userName)
      this.userDao.insert(user)
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
