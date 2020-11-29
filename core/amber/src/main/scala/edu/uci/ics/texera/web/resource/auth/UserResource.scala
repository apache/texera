package edu.uci.ics.texera.web.resource.auth

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.common.auth.User
import edu.uci.ics.texera.web.model.request.auth.{UserLoginRequest, UserRegistrationRequest}
import edu.uci.ics.texera.web.model.response.GenericWebResponse
import edu.uci.ics.texera.web.model.response.auth.UserWebResponse
import edu.uci.ics.texera.workflow.jooq.generated.Tables.USER
import io.dropwizard.jersey.sessions.Session
import javax.servlet.http.HttpSession
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import org.apache.commons.lang3.tuple.Pair
import org.jooq.Condition
import org.jooq.impl.DSL.defaultValue

@Path("/users/")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON)) object UserResource {
  private val SESSION_USER = "texera-user"

  def getUser(session: HttpSession): User =
    session.getAttribute(SESSION_USER).asInstanceOf[User]

  private def setUser(session: HttpSession, user: User): Unit = {
    session.setAttribute(SESSION_USER, user)
  }

}

@Path("/users/")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON)) class UserResource {
  @GET
  @Path("/auth/status") def authStatus(
                                          @Session session: HttpSession
                                      ): UserWebResponse = {
    val user = UserResource.getUser(session)
    if (user == null) UserWebResponse.generateErrorResponse("")
    else UserWebResponse.generateSuccessResponse(user)
  }

  @POST
  @Path("/login") def login(
                               @Session session: HttpSession,
                               request: UserLoginRequest
                           ): UserWebResponse = {
    val userName = request.userName
    val loginCondition = USER.NAME.equal(userName)
    val result = getUserID(loginCondition)
    if (result == null) { // not found
      return UserWebResponse.generateErrorResponse("username/password is incorrect")
    }
    val user = new User(userName, result.get(USER.UID))
    setUserSession(session, user)
    UserWebResponse.generateSuccessResponse(user)
  }

  @POST
  @Path("/register") def register(
                                     @Session session: HttpSession,
                                     request: UserRegistrationRequest
                                 ): UserWebResponse = {
    val userName = request.userName
    val validationResult = validateUsername(userName)
    if (!validationResult.getLeft)
      return UserWebResponse.generateErrorResponse(validationResult.getRight)
    val registerCondition = USER.NAME.equal(userName)
    val result = getUserID(registerCondition)
    if (result != null)
      return UserWebResponse.generateErrorResponse("Username already exists")
    val returnID = insertUserAccount(userName)
    val user = new User(userName, returnID.get(USER.UID))
    setUserSession(session, user)
    UserWebResponse.generateSuccessResponse(user)
  }

  private def getUserID(condition: Condition) =
    SqlServer.createDSLContext.select(USER.UID).from(USER).where(condition).fetchOne

  // TODO: extract this out
  private def insertUserAccount(userName: String) =
    SqlServer.createDSLContext
        .insertInto(USER)
        .set(USER.NAME, userName)
        .set(USER.UID, defaultValue(USER.UID))
        .returning(USER.UID)
        .fetchOne

  private def validateUsername(userName: String) =
    if (userName == null) Pair.of(false, "username cannot be null")
    else if (userName.trim.isEmpty) Pair.of(false, "username cannot be empty")
    else Pair.of(true, "username validation success")

  private def setUserSession(session: HttpSession, user: User): Unit = {
    session.setAttribute(UserResource.SESSION_USER, user)
  }

  @GET
  @Path("/logout") def logOut(@Session session: HttpSession): GenericWebResponse = {
    setUserSession(session, null)
    GenericWebResponse.generateSuccessResponse
  }
}
