package edu.uci.ics.texera.web.resource.dashboard.user
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.resource.dashboard.user.UserResource.{context, userDao}
import org.jooq.types.UInteger
import java.util
import javax.annotation.security.PermitAll
import javax.ws.rs._
import javax.ws.rs.core.MediaType


object UserResource {
  final private val context = SqlServer.createDSLContext()
  final private val userDao = new UserDao(context.configuration)
}

@Path("/admin/user")
@PermitAll
@Produces(Array(MediaType.APPLICATION_JSON))
class UserResource {

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
}
