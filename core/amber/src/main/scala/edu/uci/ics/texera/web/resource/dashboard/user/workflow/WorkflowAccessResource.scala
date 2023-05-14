package edu.uci.ics.texera.web.resource.dashboard.user.workflow

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.common.AccessEntry2
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{
  USER,
  WORKFLOW_OF_USER,
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

import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object WorkflowAccessResource {
  final private val context: DSLContext = SqlServer.createDSLContext

  /**
    * Identifies whether the given user has no access over the given workflow
    * @param wid     workflow id
    * @param uid     user id, works with workflow id as primary keys in database
    * @return boolean value indicating yes/no
    */
  def hasAccess(wid: UInteger, uid: UInteger): Boolean = {
    hasReadAccess(wid, uid) || hasWriteAccess(wid, uid)
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
    val uid = workflowOfUserDao.fetchByWid(wid).get(0).getUid
    userDao.fetchOneByUid(uid).getEmail
  }

  /**
    * Returns information about all current shared access of the given workflow
    * @param wid workflow id
    * @return a List of email/permission pair
    */
  @GET
  @Path("/list/{wid}")
  def getAccessList(
      @PathParam("wid") wid: UInteger,
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

  /**
    * This method identifies the user access level of the given workflow
    *
    * @param wid      the given workflow
    * @param email the email of the use whose access is about to be removed
    * @return message indicating a success message
    */
  @DELETE
  @Path("/revoke/{wid}/{email}")
  def revokeAccess(
      @PathParam("wid") wid: UInteger,
      @PathParam("email") email: String,
      @Auth sessionUser: SessionUser
  ): Unit = {
    if (
      !workflowOfUserDao.existsById(
        context
          .newRecord(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
          .values(sessionUser.getUser.getUid, wid)
      )
    ) {
      throw new ForbiddenException("No sufficient access privilege.")
    } else {
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

  /**
    * This method shares a workflow to a user with a specific access type
    * @param wid         the given workflow
    * @param email    the email which the access is given to
    * @param privilege the type of Access given to the target user
    * @return rejection if user not permitted to share the workflow or Success Message
    */
  @PUT
  @Path("/grant/{wid}/{email}/{privilege}")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def grantAccess(
      @PathParam("wid") wid: UInteger,
      @PathParam("email") email: String,
      @PathParam("privilege") privilege: String,
      @Auth sessionUser: SessionUser
  ): Unit = {
    val uid: UInteger =
      try {
        userDao.fetchOneByEmail(email).getUid
      } catch {
        case _: IndexOutOfBoundsException =>
          throw new BadRequestException("Target user does not exist.")
      }
    if (
      !workflowOfUserDao.existsById(
        context
          .newRecord(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
          .values(sessionUser.getUser.getUid, wid)
      )
    ) {
      throw new ForbiddenException("No sufficient access privilege.")
    } else {
      workflowUserAccessDao.merge(
        new WorkflowUserAccess(uid, wid, WorkflowUserAccessPrivilege.valueOf(privilege))
      )
    }
  }
}
