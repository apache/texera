package edu.uci.ics.texera.web.resource.dashboard.user

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables.USER_FILE_ACCESS
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{File, User}
import edu.uci.ics.texera.web.resource.dashboard.file.UserFileResource.context
import AdminUserResource.{context, userDao}
import io.dropwizard.auth.Auth
import org.jooq.types.UInteger

import java.util
import javax.annotation.security.PermitAll
import javax.ws.rs._
import javax.ws.rs.core.MediaType


object AdminUserResource {
  final private val context = SqlServer.createDSLContext()
  final private val userDao = new UserDao(context.configuration)
}

@Path("/admin/user")
@PermitAll
@Produces(Array(MediaType.APPLICATION_JSON))
class AdminUserResource {

  /**
   * This method returns the specified user.
   *
   * @param uid user id
   * @return user specified by the user id
   */
  @GET
  @Path("/{uid}")
  def getUser(@PathParam("uid") uid: UInteger): User = {
    userDao.fetchOneByUid(uid)
  }

  /**
   * This method returns the list of users
   *
   * @return a list of users
   */
  @GET
  @Path("/list")
  def listUsers(): util.List[User] = {
    userDao.fetchRangeOfUid(UInteger.MIN,UInteger.MAX)
  }

  @POST
  @Path("/update")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  def changeUserFileDescription(user: User): Unit = {
    val updatedUser = userDao.fetchOneByUid(user.getUid)
    updatedUser.setRole(user.getRole)
    userDao.update(updatedUser)
  }
}