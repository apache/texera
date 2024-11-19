package edu.uci.ics.texera.web.resource.dashboard.admin.user
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.enums.UserRole
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.resource.dashboard.admin.user.AdminUserResource.userDao
import org.jasypt.util.password.StrongPasswordEncryptor
import org.jooq.types.UInteger
import java.util
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import edu.uci.ics.texera.web.resource.dashboard.user.quota.UserQuotaResource.{
  Workflow,
  getUserCreatedWorkflow,
  getUserAccessedWorkflow,
  getUserMongoDBSize,
  deleteMongoCollection,
  MongoStorage
}
import javax.ws.rs.core.Response
import javax.ws.rs.WebApplicationException

object AdminUserResource {
  final private lazy val context = SqlServer.createDSLContext()
  final private lazy val userDao = new UserDao(context.configuration)
}

@Path("/admin/user")
@RolesAllowed(Array("ADMIN"))
class AdminUserResource {

  /**
    * This method returns the list of users
    *
    * @return a list of users
    */
  @GET
  @Path("/list")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def listUser(): util.List[User] = {
    userDao.fetchRangeOfUid(UInteger.MIN, UInteger.MAX)
  }

  @PUT
  @Path("/update")
  def updateUser(user: User): Unit = {
    val existingUser = userDao.fetchOneByEmail(user.getEmail)
    if (existingUser != null && existingUser.getUid != user.getUid) {
      throw new WebApplicationException("Email already exists", Response.Status.CONFLICT)
    }
    val updatedUser = userDao.fetchOneByUid(user.getUid)
    updatedUser.setName(user.getName)
    updatedUser.setEmail(user.getEmail)
    updatedUser.setRole(user.getRole)
    userDao.update(updatedUser)
  }

  @POST
  @Path("/add")
  def addUser(): Unit = {
    val random = System.currentTimeMillis().toString
    val newUser = new User
    newUser.setName("User" + random)
    newUser.setPassword(new StrongPasswordEncryptor().encryptPassword(random))
    newUser.setRole(UserRole.INACTIVE)
    userDao.insert(newUser)
  }

  @GET
  @Path("/created_workflows")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getCreatedWorkflow(@QueryParam("user_id") user_id: UInteger): List[Workflow] = {
    getUserCreatedWorkflow(user_id)
  }

  @GET
  @Path("/access_workflows")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getAccessedWorkflow(@QueryParam("user_id") user_id: UInteger): util.List[UInteger] = {
    getUserAccessedWorkflow(user_id)
  }

  @GET
  @Path("/mongodb_size")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def mongoDBSize(@QueryParam("user_id") user_id: UInteger): Array[MongoStorage] = {
    getUserMongoDBSize(user_id)
  }

  @DELETE
  @Path("/deleteCollection/{collectionName}")
  def deleteCollection(@PathParam("collectionName") collectionName: String): Unit = {
    deleteMongoCollection(collectionName)
  }

}
