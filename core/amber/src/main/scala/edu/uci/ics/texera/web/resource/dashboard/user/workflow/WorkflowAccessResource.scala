package edu.uci.ics.texera.web.resource.dashboard.user.workflow

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.common.AccessEntry
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{
  PROJECT_USER_ACCESS,
  USER,
  WORKFLOW_OF_PROJECT,
  WORKFLOW_USER_ACCESS
}
import edu.uci.ics.texera.web.model.jooq.generated.enums.WorkflowUserAccessPrivilege
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{
  UserDao,
  WorkflowOfUserDao,
  WorkflowUserAccessDao
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.WorkflowUserAccess
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowAccessResource.context
import io.dropwizard.auth.Auth
import org.jooq.DSLContext
import org.jooq.types.UInteger

import java.util
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType

object WorkflowAccessResource {
  final private val context: DSLContext = SqlServer.createDSLContext

  /**
    * Identifies whether the given user has read-only access over the given workflow
    * @param wid workflow id
    * @param uid user id, works with workflow id as primary keys in database
    * @return boolean value indicating yes/no
    */
  def hasReadAccess(wid: UInteger, uid: UInteger): Boolean = {
    getPrivilege(wid, uid).eq(WorkflowUserAccessPrivilege.READ) || hasWriteAccess(wid, uid)
  }

  /**
    * Identifies whether the given user has write access over the given workflow
    * @param wid workflow id
    * @param uid user id, works with workflow id as primary keys in database
    * @return boolean value indicating yes/no
    */
  def hasWriteAccess(wid: UInteger, uid: UInteger): Boolean = {
    getPrivilege(wid, uid).eq(WorkflowUserAccessPrivilege.WRITE)
  }

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
      val projectAccess = context
        .select()
        .from(PROJECT_USER_ACCESS)
        .join(WORKFLOW_OF_PROJECT)
        .on(WORKFLOW_OF_PROJECT.PID.eq(PROJECT_USER_ACCESS.PID))
        .where(WORKFLOW_OF_PROJECT.WID.eq(wid).and(PROJECT_USER_ACCESS.UID.eq(uid)))
        .fetchOneInto(classOf[WorkflowUserAccess])
      if (projectAccess == null) {
        WorkflowUserAccessPrivilege.NONE
      } else {
        projectAccess.getPrivilege
      }
    } else {
      access.getPrivilege
    }
  }
}

@Produces(Array(MediaType.APPLICATION_JSON))
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/access/workflow")
class WorkflowAccessResource() {
  final private val userDao = new UserDao(context.configuration())
  final private val workflowOfUserDao = new WorkflowOfUserDao(context.configuration)
  final private val workflowUserAccessDao = new WorkflowUserAccessDao(context.configuration)

  /**
    * This method returns the owner of a workflow
    * @param wid,  workflow id
    * @return ownerEmail,  the owner's email
    */
  @GET
  @Path("/owner/{wid}")
  def getOwner(@PathParam("wid") wid: UInteger): String = {
    userDao.fetchOneByUid(workflowOfUserDao.fetchByWid(wid).get(0).getUid).getEmail
  }

  /**
    * Returns information about all current shared access of the given workflow
    *
    * @param wid workflow id
    * @return a List of email/name/permission
    */
  @GET
  @Path("/list/{wid}")
  def getAccessList(
      @PathParam("wid") wid: UInteger
  ): util.List[AccessEntry] = {
    context
      .select(
        USER.EMAIL,
        USER.NAME,
        WORKFLOW_USER_ACCESS.PRIVILEGE
      )
      .from(WORKFLOW_USER_ACCESS)
      .join(USER)
      .on(USER.UID.eq(WORKFLOW_USER_ACCESS.UID))
      .where(
        WORKFLOW_USER_ACCESS.WID
          .eq(wid)
          .and(WORKFLOW_USER_ACCESS.UID.notEqual(workflowOfUserDao.fetchByWid(wid).get(0).getUid))
      )
      .fetchInto(classOf[AccessEntry])
  }

  /**
    * This method shares a workflow to a user with a specific access type
    *
    * @param wid       the given workflow
    * @param email     the email which the access is given to
    * @param privilege the type of Access given to the target user
    * @return rejection if user not permitted to share the workflow or Success Message
    */
  @PUT
  @Path("/grant/{wid}/{email}/{privilege}")
  def grantAccess(
      @PathParam("wid") wid: UInteger,
      @PathParam("email") email: String,
      @PathParam("privilege") privilege: String,
      @Auth user: SessionUser
  ): Unit = {
    if (email.equals(user.getEmail)) {
      throw new BadRequestException("You cannot grant access to yourself!")
    }
    try {
      workflowUserAccessDao.merge(
        new WorkflowUserAccess(
          userDao.fetchOneByEmail(email).getUid,
          wid,
          WorkflowUserAccessPrivilege.valueOf(privilege)
        )
      )
    } catch {
      case _: NullPointerException =>
        throw new BadRequestException("User Not Found!")
    }

  }

  /**
    * This method identifies the user access level of the given workflow
    *
    * @param wid   the given workflow
    * @param email the email of the use whose access is about to be removed
    * @return message indicating a success message
    */
  @DELETE
  @Path("/revoke/{wid}/{email}")
  def revokeAccess(
      @PathParam("wid") wid: UInteger,
      @PathParam("email") email: String
  ): Unit = {
    context
      .delete(WORKFLOW_USER_ACCESS)
      .where(
        WORKFLOW_USER_ACCESS.UID
          .eq(userDao.fetchOneByEmail(email).getUid)
          .and(WORKFLOW_USER_ACCESS.WID.eq(wid))
      )
      .execute()
  }
}
