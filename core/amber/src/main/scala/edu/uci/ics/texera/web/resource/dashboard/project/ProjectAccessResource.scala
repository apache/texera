package edu.uci.ics.texera.web.resource.dashboard.project

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.common.AccessEntry2
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{USER, WORKFLOW_OF_USER, PROJECT_USER_ACCESS}
import edu.uci.ics.texera.web.model.jooq.generated.enums.ProjectUserAccessPrivilege
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{UserDao, ProjectDao, ProjectUserAccessDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.ProjectUserAccess
import edu.uci.ics.texera.web.resource.dashboard.project.ProjectAccessResource.context
import io.dropwizard.auth.Auth
import org.jooq.DSLContext
import org.jooq.types.UInteger

import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object ProjectAccessResource {
  final private val context: DSLContext = SqlServer.createDSLContext

  /**
    * @param wid workflow id
    * @param uid user id, works with workflow id as primary keys in database
    * @return WorkflowUserAccessPrivilege value indicating NONE/READ/WRITE
    */
  def getPrivilege(wid: UInteger, uid: UInteger): WorkflowUserAccessPrivilege = {
    val access = context
      .select()
      .from(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(wid).and(WORKFLOW_USER_ACCESS.UID.eq(uid)))
      .fetchOneInto(classOf[WorkflowUserAccess])
    if (access == null) {
      WorkflowUserAccessPrivilege.NONE
    } else {
      access.getPrivilege
    }
  }

  /**
    * Identifies whether the given user has read-only access over the given workflow
    *
    * @param wid workflow id
    * @param uid user id, works with workflow id as primary keys in database
    * @return boolean value indicating yes/no
    */
  def hasReadAccess(wid: UInteger, uid: UInteger): Boolean = {
    getPrivilege(wid, uid).eq(WorkflowUserAccessPrivilege.READ)
  }

  /**
    * Identifies whether the given user has write access over the given workflow
    *
    * @param wid workflow id
    * @param uid user id, works with workflow id as primary keys in database
    * @return boolean value indicating yes/no
    */
  def hasWriteAccess(wid: UInteger, uid: UInteger): Boolean = {
    getPrivilege(wid, uid).eq(WorkflowUserAccessPrivilege.WRITE)
  }

  /**
    * Identifies whether the given user has no access over the given workflow
    * @param wid     workflow id
    * @param uid     user id, works with workflow id as primary keys in database
    * @return boolean value indicating yes/no
    */
  def hasAccess(wid: UInteger, uid: UInteger): Boolean = {
    hasReadAccess(wid, uid) || hasWriteAccess(wid, uid)
  }
}

@Produces(Array(MediaType.APPLICATION_JSON))
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/access/project")
class ProjectAccessResource() {
  final private val userDao = new UserDao(context.configuration())
  final private val projectDao = new ProjectDao(context.configuration)
  final private val projectUserAccessDao = new ProjectUserAccessDao(context.configuration)

  /**
    * This method returns the owner of a project
    * @param pid,  project id
    * @return ownerEmail,  the owner's email
    */
  @GET
  @Path("/owner/{pid}")
  def getOwner(@PathParam("pid") pid: UInteger): String = {
    userDao.fetchOneByUid(projectDao.fetchOneByPid(pid).getOwnerId).getEmail
  }

  /**
    * Returns information about all current shared access of the given project
    * @param pid project id
    * @return a List of email/permission pair
    */


/**
  @GET
  @Path("/list/{pid}")
  def getAccessList(
      @PathParam("pid") pid: UInteger,
      @Auth sessionUser: SessionUser
  ): List[AccessEntry2] = {

    val user = sessionUser.getUser
    if (
      workflowOfUserDao.existsById(
        context
          .newRecord(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
          .values(user.getUid, wid)
      )
    ) {
      context
        .select(
          USER.EMAIL,
          USER.NAME,
          WORKFLOW_USER_ACCESS.PRIVILEGE
        )
        .from(WORKFLOW_USER_ACCESS)
        .join(USER)
        .on(USER.UID.eq(WORKFLOW_USER_ACCESS.UID))
        .where(WORKFLOW_USER_ACCESS.WID.eq(wid).and(WORKFLOW_USER_ACCESS.UID.notEqual(user.getUid)))
        .fetch()
        .map(access => { access.into(classOf[AccessEntry2]) })
        .toList
    } else {
      throw new ForbiddenException("You are not the owner of the workflow.")
    }
  }
*/


  /**
    * This method identifies the user access level of the given project
    *
    * @param pid      the given project
    * @param email the email of the use whose access is about to be removed
    * @return message indicating a success message
    */
  @DELETE
  @Path("/revoke/{pid}/{email}")
  def revokeAccess(
      @PathParam("pid") wid: UInteger,
      @PathParam("email") email: String,
  ): Unit = {
      context
        .delete(PROJECT_USER_ACCESS)
        .where(
          PROJECT_USER_ACCESS.UID
            .eq(userDao.fetchOneByEmail(email).getUid)
            .and(PROJECT_USER_ACCESS.PID.eq(wid))
        )
        .execute()
    }
  }

  /**
    * This method shares a project to a user with a specific access type
    * @param pid      the given project
    * @param email    the email which the access is given to
    * @param privilege the type of Access given to the target user
    * @return rejection if user not permitted to share the project or Success Message
    */
  @PUT
  @Path("/grant/{pid}/{email}/{privilege}")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def grantAccess(
      @PathParam("pid") pid: UInteger,
      @PathParam("email") email: String,
      @PathParam("privilege") privilege: String,
  ): Unit = {
    projectUserAccessDao.merge(
      new ProjectUserAccess(userDao.fetchOneByEmail(email).getUid, pid, ProjectUserAccessPrivilege.valueOf(privilege))
    )
  }
}
