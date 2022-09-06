package edu.uci.ics.texera.web.resource.dashboard.workflow

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.common.AccessEntry
import edu.uci.ics.texera.web.model.http.response.OwnershipResponse
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{WORKFLOW, WORKFLOW_USER_ACCESS}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{UserDao, WorkflowDao, WorkflowUserAccessDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.WorkflowUserAccess
import edu.uci.ics.texera.web.model.jooq.generated.tables.records.WorkflowUserAccessRecord
import edu.uci.ics.texera.web.resource.dashboard.workflow.AccessLevel.{NONE, READ, WRITE}
import edu.uci.ics.texera.web.resource.dashboard.workflow.WorkflowAccessResource.{context, getAccessRecord, getGrantedWorkflowAccessList, getWorkflowOwnerUid, toAccessLevel, toWorkflowUserAccessRecord, userDao}
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

  def getWorkflowOwnerUid(wid: UInteger): UInteger = {
    context
      .select(WORKFLOW.OWNER_UID)
      .from(WORKFLOW)
      .where(WORKFLOW.WID.eq(wid))
      .fetchOne()
      .value1()
  }

  /**
    * Identifies whether the given user has read-only access over the given workflow
    */
  def hasReadAccess(wid: UInteger, uid: UInteger): Boolean = {
    getAccessRecord(wid, uid).getReadPrivilege
  }

  /**
    * Identifies whether the given user has write access over the given workflow
    */
  def hasWriteAccess(wid: UInteger, uid: UInteger): Boolean = {
    getAccessRecord(wid, uid).getWritePrivilege
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
  def toAccessLevel(workflowUserAccess: WorkflowUserAccess): AccessLevel = {
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
      accessLevel: AccessLevel
  ): WorkflowUserAccessRecord = {
    accessLevel match {
      case WRITE => new WorkflowUserAccessRecord(uid, wid, true, true)
      case READ  => new WorkflowUserAccessRecord(uid, wid, true, false)
      case NONE  => new WorkflowUserAccessRecord(uid, wid, false, false)
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

/**
  * Provides endpoints for operations related to Workflow Access.
  */
@PermitAll
@Path("/workflow/access")
@Produces(Array(MediaType.APPLICATION_JSON))
class WorkflowAccessResource() {

  private val workflowDao = new WorkflowDao(context.configuration)
  private val workflowUserAccessDao = new WorkflowUserAccessDao(context.configuration)

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
  def getWorkflowOwnerName(@PathParam("wid") wid: UInteger): OwnershipResponse = {
    val uid = workflowDao.fetchByWid(wid).get(0).getOwnerUid
    val ownerName = userDao.fetchOneByUid(uid).getName
    OwnershipResponse(ownerName)
  }

  /**
    * Returns the the access level of the given workflow of the session user
    *
    * @param wid     the given workflow
    * @return access level
    */
  @GET
  @Path("/workflow/{wid}/level")
  def retrieveUserAccessLevel(
      @PathParam("wid") wid: UInteger,
      @Auth sessionUser: SessionUser
  ): AccessLevel = {
    toAccessLevel(getAccessRecord(wid, sessionUser.getUser.getUid))
  }

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
    // workflow must exist
    if (!workflowDao.existsById(wid)) {
      throw new BadRequestException(s"workflow ${wid} does not exist.")
    }
    getGrantedWorkflowAccessList(wid, sessionUser.getUser.getUid)
  }

  /**
    * Revokes the user access level of the given workflow
    */
  @DELETE
  @Path("/revoke/{wid}/{username}")
  def revokeWorkflowAccess(
      @PathParam("wid") wid: UInteger,
      @PathParam("username") username: String,
      @Auth sessionUser: SessionUser
  ): Unit = {
    setWorkflowAccessLevel(wid, username, AccessLevel.NONE, sessionUser)
  }

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
      @PathParam("accessLevel") accessLevel: AccessLevel,
      @Auth sessionUser: SessionUser
  ): Unit = {
    // target user must exist
    val targetUid: UInteger =
      try {
        userDao.fetchByName(username).get(0).getUid
      } catch {
        case _: NullPointerException =>
          throw new BadRequestException(s"Target user ${username} does not exist.")
      }
    // workflow must exist
    if (!workflowDao.existsById(wid)) {
      throw new BadRequestException(s"workflow ${wid} does not exist.")
    }
    // only owner can change access level
    // session user must be the owner of the workflow
    val ownerUid = getWorkflowOwnerUid(wid)
    if (ownerUid != sessionUser.getUser.getUid) {
      throw new BadRequestException(s"only workflow owner can change access level.")
    }
    // cannot change the access level of the owner: always full access
    if (targetUid == ownerUid) {
      throw new BadRequestException(s"cannot change access level of the workflow owner.")
    }

    if (accessLevel == AccessLevel.NONE) {
      // NONE access level: directly delete the record
      context
        .delete(WORKFLOW_USER_ACCESS)
        .where(WORKFLOW_USER_ACCESS.UID.eq(targetUid).and(WORKFLOW_USER_ACCESS.WID.eq(wid)))
        .execute()
    } else {
      val access = toWorkflowUserAccessRecord(wid, targetUid, accessLevel)
      context.insertInto(WORKFLOW_USER_ACCESS)
        .set(access)
        .onDuplicateKeyUpdate()
        .set(access)
        .execute()
    }
  }
}
