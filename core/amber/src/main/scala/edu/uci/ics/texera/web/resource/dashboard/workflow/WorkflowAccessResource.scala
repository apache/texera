package edu.uci.ics.texera.web.resource.dashboard.workflow

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.common.AccessEntry
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{WORKFLOW_OF_USER, WORKFLOW_USER_ACCESS}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{
  UserDao,
  WorkflowOfUserDao,
  WorkflowUserAccessDao
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.WorkflowUserAccess
import edu.uci.ics.texera.web.resource.dashboard.workflow.AccessLevel.{NONE, READ, WRITE}
import edu.uci.ics.texera.web.resource.dashboard.workflow.WorkflowAccessResource.{
  context,
  getGrantedWorkflowAccessList,
  toWorkflowUserAccessRecord,
  userDao
}
import io.dropwizard.auth.Auth
import org.jooq.DSLContext
import org.jooq.types.UInteger

import javax.annotation.security.PermitAll
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.collection.JavaConverters._

/**
  * An enum class identifying the specific workflow access level
  *
  * READ indicates read-only
  * WRITE indicates write access (which dominates)
  * NONE indicates having an access record, but either read or write access is granted
  * NO_RECORD indicates having no record in the table
  */

/**
  * Helper functions for retrieving access level based on given information
  */
object WorkflowAccessResource {

  private var context: DSLContext = SqlServer.createDSLContext
  private lazy val userDao = new UserDao(context.configuration())
  private lazy val workflowOwnerDao = new WorkflowOfUserDao(context.configuration())

  val GUEST_USER = "guest"
  val GENERAL_USER = "general"

  def getWorkflowOwnerUid(wid: UInteger): UInteger = {
    workflowOwnerDao.fetchByWid(wid).get(0).getUid
  }

  def getUserRole(uid: UInteger): String = {
    userDao.fetchOneByUid(uid).getRoleName
  }

  /**
    * Identifies whether the given user has read-only access over the given workflow
    */
  def hasReadAccess(wid: UInteger, uid: UInteger): Boolean = {
    getWorkflowOwnerUid(wid) == uid || getAccessRecord(wid, uid).getReadPrivilege
  }

  /**
    * Identifies whether the given user has write access over the given workflow
    */
  def hasWriteAccess(wid: UInteger, uid: UInteger): Boolean = {
    getWorkflowOwnerUid(wid) == uid || getAccessRecord(wid, uid).getWritePrivilege
  }

  /**
    * Identifies whether the given user has no access over the given workflow
    * @param wid     workflow id
    * @param uid     user id, works with workflow id as primary keys in database
    * @return boolean value indicating yes/no
    */
  def hasNoWorkflowAccess(wid: UInteger, uid: UInteger): Boolean = {
    toAccessLevel(getAccessRecord(wid, uid)) == AccessLevel.NONE
  }

  def canCreateNewWorkflow(uid: UInteger): Boolean = {
    val role = getUserRole(uid)
    role != null && role != GUEST_USER
  }

  /**
    * Returns an Access Object based on given wid and uid
    * Searches in database for the given uid-wid pair, and returns Access Object based on search result
    *
    * @param wid     workflow id
    * @param uid     user id, works with workflow id as primary keys in database
    * @return Access Object indicating the access level
    */
  def getAccessRecord(wid: UInteger, uid: UInteger): WorkflowUserAccess = {
    context
      .select(WORKFLOW_USER_ACCESS.READ_PRIVILEGE, WORKFLOW_USER_ACCESS.WRITE_PRIVILEGE)
      .from(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(wid).and(WORKFLOW_USER_ACCESS.UID.eq(uid)))
      .fetchOneInto(classOf[WorkflowUserAccess])
  }

  /**
    * Converts a record in the workflow_user_access table into the AccessLevel abstraction
    */
  def toAccessLevel(workflowUserAccess: WorkflowUserAccess): AccessLevel.Value = {
    if (workflowUserAccess == null) {
      return AccessLevel.NONE
    }
    if (workflowUserAccess.getWritePrivilege == true) {
      AccessLevel.WRITE
    } else if (workflowUserAccess.getReadPrivilege == true) {
      AccessLevel.READ
    } else {
      AccessLevel.NONE
    }
  }

  /**
    * Converts the AccessLevel abstraction to a record in the workflow_user_access table
    */
  def toWorkflowUserAccessRecord(
      wid: UInteger,
      uid: UInteger,
      accessLevel: AccessLevel.Value
  ): WorkflowUserAccess = {
    accessLevel match {
      case WRITE   => new WorkflowUserAccess(wid, uid, true, true)
      case READ    => new WorkflowUserAccess(wid, uid, true, false)
      case NONE    => new WorkflowUserAccess(wid, uid, false, false)
    }
  }

  /**
    * Returns information about all current shared access of the given workflow
    *
    * @param wid     workflow id
    * @param uid     user id of current user, used to identify ownership
    * @return a List with corresponding information Ex: [{"Jim": "Read"}]
    */
  def getGrantedWorkflowAccessList(wid: UInteger, uid: UInteger): List[AccessEntry] = {
    val shares = context
      .select(
        WORKFLOW_USER_ACCESS.UID,
        WORKFLOW_USER_ACCESS.READ_PRIVILEGE,
        WORKFLOW_USER_ACCESS.WRITE_PRIVILEGE
      )
      .from(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(wid).and(WORKFLOW_USER_ACCESS.UID.notEqual(uid)))
      .fetch()

    shares
      .getValues(0)
      .asScala
      .toList
      .zipWithIndex
      .map({
        case (id, index) =>
          val userName = userDao.fetchOneByUid(id.asInstanceOf[UInteger]).getName
          if (shares.getValue(index, 2) == true) {
            AccessEntry(userName, "Write")
          } else {
            AccessEntry(userName, "Read")
          }
      })
  }

}

object AccessLevel extends Enumeration {
  type Access = Value
  val READ: AccessLevel.Value = Value("read")
  val WRITE: AccessLevel.Value = Value("write")
  val NONE: AccessLevel.Value = Value("none")
}

/**
  * Provides endpoints for operations related to Workflow Access.
  */
@PermitAll
@Path("/workflow/access")
@Produces(Array(MediaType.APPLICATION_JSON))
class WorkflowAccessResource() {

  private val workflowOfUserDao = new WorkflowOfUserDao(
    context.configuration
  )
  private val workflowUserAccessDao = new WorkflowUserAccessDao(
    context.configuration
  )

  def this(dslContext: DSLContext) {
    this()
    context = dslContext
  }

  /**
    * This method returns the owner of a workflow
    *
    * @param wid,     the given workflow
    * @return ownerName,  the owner's name
    */
  @GET
  @Path("/owner/{wid}")
  def getWorkflowOwner(@PathParam("wid") wid: UInteger): String = {
    val uid = workflowOfUserDao.fetchByWid(wid).get(0).getUid
    val ownerName = userDao.fetchOneByUid(uid).getName
    ownerName
  }
//
//  /**
//    * This method identifies the user access level of the given workflow
//    *
//    * @param wid     the given workflow
//    * @return json object indicating uid, wid and access level, ex: {"level": "Write", "uid": 1, "wid": 15}
//    */
//  @GET
//  @Path("/workflow/{wid}/level")
//  def retrieveUserAccessLevel(
//      @PathParam("wid") wid: UInteger,
//      @Auth sessionUser: SessionUser
//  ): AccessResponse = {
//    val user = sessionUser.getUser
//    val uid = user.getUid
//    val workflowAccessLevel = getAccessLevel(wid, uid).toString
//    AccessResponse(uid, wid, workflowAccessLevel)
//  }

  /**
    * Returns all current shared accesses of the given workflow
    *
    * @param wid     the given workflow
    * @return json object indicating user with access and access type, ex: [{"Jim": "Write"}]
    */
  @GET
  @Path("/list/{wid}")
  def retrieveGrantedWorkflowAccessList(
      @PathParam("wid") wid: UInteger,
      @Auth sessionUser: SessionUser
  ): List[AccessEntry] = {
    val user = sessionUser.getUser
    val workflowOfUserId = context
      .newRecord(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
      .values(user.getUid, wid)
    if (workflowOfUserDao.existsById(workflowOfUserId)) {
      getGrantedWorkflowAccessList(wid, user.getUid)
    } else {
      throw new ForbiddenException("You are not the owner of the workflow.")
    }
  }

//  /**
//    * This method identifies the user access level of the given workflow
//    *
//    * @param wid     the given workflow
//    * @param username the username of the use whose access is about to be removed
//    * @return message indicating a success message
//    */
//  @DELETE
//  @Path("/revoke/{wid}/{username}")
//  def revokeWorkflowAccess(
//      @PathParam("wid") wid: UInteger,
//      @PathParam("username") username: String,
//      @Auth sessionUser: SessionUser
//  ): Unit = {
//    val user = sessionUser.getUser
//    val uid: UInteger =
//      try {
//        userDao.fetchByName(username).get(0).getUid
//      } catch {
//        case _: NullPointerException =>
//          throw new BadRequestException("Target user does not exist.")
//      }
//    val workflowOfUserId = context
//      .newRecord(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
//      .values(user.getUid, wid)
//    if (!workflowOfUserDao.existsById(workflowOfUserId)) {
//      throw new ForbiddenException("No sufficient access privilege.")
//    }
//    context
//      .delete(WORKFLOW_USER_ACCESS)
//      .where(WORKFLOW_USER_ACCESS.UID.eq(uid).and(WORKFLOW_USER_ACCESS.WID.eq(wid)))
//      .execute()
//  }

  /**
    * This method shares a workflow to a user with a specific access type
    *
    * @param wid     the given workflow
    * @param username    the user name which the access is given to
    * @param accessLevel the type of Access given to the target user
    * @return rejection if user not permitted to share the workflow or Success Message
    */
  @POST
  @Path("/grant/{wid}/{username}/{accessLevel}")
  def setWorkflowAccessLevel(
      @PathParam("wid") wid: UInteger,
      @PathParam("username") username: String,
      @PathParam("accessLevel") accessLevel: AccessLevel.Value,
      @Auth sessionUser: SessionUser
  ): Unit = {
    val user = sessionUser.getUser
    val uid: UInteger =
      try {
        userDao.fetchByName(username).get(0).getUid
      } catch {
        case _: IndexOutOfBoundsException =>
          throw new BadRequestException("Target user does not exist.")
      }

    val workflowOfUserId = context
      .newRecord(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
      .values(user.getUid, wid)
    if (!workflowOfUserDao.existsById(workflowOfUserId)) {
      throw new ForbiddenException("No sufficient access privilege.")
    }
    workflowUserAccessDao.update(toWorkflowUserAccessRecord(wid, uid, accessLevel))
  }
}
