package edu.uci.ics.texera.web.resource.auth

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.Tables.USER
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.request.auth.{UserLoginRequest, UserRegistrationRequest}
import edu.uci.ics.texera.web.model.response.GenericWebResponse
import io.dropwizard.jersey.sessions.Session
import javax.servlet.http.HttpSession
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import org.apache.commons.lang3.tuple.Pair
import org.jooq.Condition
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL.defaultValue
import org.jooq.types.UInteger

@Path("/users/")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class UserResource {

  private val SESSION_USER = "texera-user"

  @GET
  @Path("/auth/status")
  def authStatus(@Session session: HttpSession): User = {
    getUser(session)

  }

  @POST
  @Path("/login")
  def login(@Session session: HttpSession, request: UserLoginRequest): Response = {

    // try to fetch the record
    val user = SqlServer.createDSLContext
      .select()
      .from(USER)
      .where(USER.NAME.equal(request.userName))
      .fetchOne

    if (user == null) { // not found
      return Response.status(Response.Status.UNAUTHORIZED).build()
    }
    setUserSession(session, new User(request.userName, user.get("uid").asInstanceOf[UInteger]))
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
      val returnID = SqlServer.createDSLContext
        .insertInto(USER)
        .set(USER.NAME, userName)
        .set(USER.UID, defaultValue(USER.UID))
        .returning(USER.UID)
        .fetchOne
      val user = new User(userName, returnID.get(USER.UID))
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

  // TODO: rewrite this
  def getUser(session: HttpSession): User =
    session.getAttribute(SESSION_USER).asInstanceOf[User]

  // TODO: rewrite this
  private def validateUsername(userName: String) =
    if (userName == null) Pair.of(false, "username cannot be null")
    else if (userName.trim.isEmpty) Pair.of(false, "username cannot be empty")
    else Pair.of(true, "username validation success")

  private def setUserSession(session: HttpSession, user: User): Unit = {
    session.setAttribute(SESSION_USER, user)

  }

}
