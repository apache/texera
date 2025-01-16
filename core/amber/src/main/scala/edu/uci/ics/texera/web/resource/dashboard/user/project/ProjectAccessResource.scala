package edu.uci.ics.texera.web.resource.dashboard.user.project

import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.web.model.common.AccessEntry
import edu.uci.ics.texera.dao.jooq.generated.Tables.{PROJECT_USER_ACCESS, USER}
import edu.uci.ics.texera.dao.jooq.generated.enums.ProjectUserAccessPrivilege
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{ProjectDao, ProjectUserAccessDao, UserDao}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.ProjectUserAccess
import org.jooq.DSLContext
import org.jooq.types.UInteger

import java.util
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType

@Produces(Array(MediaType.APPLICATION_JSON))
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/access/project")
class ProjectAccessResource() {
  final private val context: DSLContext = SqlServer
    .getInstance(StorageConfig.jdbcUrl, StorageConfig.jdbcUsername, StorageConfig.jdbcPassword)
    .createDSLContext()
  final private val userDao = new UserDao(context.configuration())
  final private val projectDao = new ProjectDao(context.configuration)
  final private val projectUserAccessDao = new ProjectUserAccessDao(context.configuration)

  /**
    * This method returns the owner of a project
    *
    * @param pid ,  project id
    * @return ownerEmail,  the owner's email
    */
  @GET
  @Path("/owner/{pid}")
  def getOwner(@PathParam("pid") pid: UInteger): String = {
    userDao.fetchOneByUid(projectDao.fetchOneByPid(pid).getOwnerId).getEmail
  }

  /**
    * Returns information about all current shared access of the given project
    *
    * @param pid project id
    * @return a List of email/permission pair
    */
  @GET
  @Path("/list/{pid}")
  def getAccessList(
      @PathParam("pid") pid: UInteger
  ): util.List[AccessEntry] = {
    context
      .select(
        USER.EMAIL,
        USER.NAME,
        PROJECT_USER_ACCESS.PRIVILEGE
      )
      .from(PROJECT_USER_ACCESS)
      .join(USER)
      .on(USER.UID.eq(PROJECT_USER_ACCESS.UID))
      .where(
        PROJECT_USER_ACCESS.PID
          .eq(pid)
          .and(PROJECT_USER_ACCESS.UID.notEqual(projectDao.fetchOneByPid(pid).getOwnerId))
      )
      .fetchInto(classOf[AccessEntry])
  }

  /**
    * This method shares a project to a user with a specific access type
    *
    * @param pid       the given project
    * @param email     the email which the access is given to
    * @param privilege the type of Access given to the target user
    * @return rejection if user not permitted to share the project or Success Message
    */
  @PUT
  @Path("/grant/{pid}/{email}/{privilege}")
  def grantAccess(
      @PathParam("pid") pid: UInteger,
      @PathParam("email") email: String,
      @PathParam("privilege") privilege: String
  ): Unit = {
    projectUserAccessDao.merge(
      new ProjectUserAccess(
        userDao.fetchOneByEmail(email).getUid,
        pid,
        ProjectUserAccessPrivilege.valueOf(privilege)
      )
    )
  }

  /**
    * Revoke a user's access to a file
    *
    * @param pid   the id of the file
    * @param email the email of target user whose access is about to be revoked
    * @return A successful resp if granted, failed resp otherwise
    */
  @DELETE
  @Path("/revoke/{pid}/{email}")
  def revokeAccess(
      @PathParam("pid") pid: UInteger,
      @PathParam("email") email: String
  ): Unit = {
    context
      .delete(PROJECT_USER_ACCESS)
      .where(
        PROJECT_USER_ACCESS.UID
          .eq(userDao.fetchOneByEmail(email).getUid)
          .and(PROJECT_USER_ACCESS.PID.eq(pid))
      )
      .execute()
  }
}
