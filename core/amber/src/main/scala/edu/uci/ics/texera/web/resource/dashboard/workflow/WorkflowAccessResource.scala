package edu.uci.ics.texera.web.resource.dashboard.workflow

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.common.AccessEntry
import edu.uci.ics.texera.web.model.http.response.{AccessResponse, OwnershipResponse}
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{WORKFLOW_OF_USER, WORKFLOW_USER_ACCESS}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{
  UserDao,
  WorkflowOfUserDao,
  WorkflowUserAccessDao
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.WorkflowUserAccess
import edu.uci.ics.texera.web.resource.dashboard.workflow.WorkflowAccessResource.{
  checkAccessLevel,
  context,
  getGrantedWorkflowAccessList,
  hasNoWorkflowAccessRecord,
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

  private lazy val userDao = new UserDao(context.configuration())
  private var context: DSLContext = SqlServer.createDSLContext

  /**
    * Identifies whether the given user has read-only access over the given workflow
    *
    * @param wid     workflow id
    * @param uid     user id, works with workflow id as primary keys in database
    * @return boolean value indicating yes/no
    */
  def hasReadAccess(wid: UInteger, uid: UInteger): Boolean = {
    checkAccessLevel(wid, uid).eq(WorkflowAccess.READ)
  }

  /**
    * Identifies whether the given user has write access over the given workflow
    * @param wid     workflow id
    * @param uid     user id, works with workflow id as primary keys in database
    * @return boolean value indicating yes/no
    */
  def hasWriteAccess(wid: UInteger, uid: UInteger): Boolean = {
    checkAccessLevel(wid, uid).eq(WorkflowAccess.WRITE)
  }

  /**
    * Identifies whether the given user has no access over the given workflow
    * @param wid     workflow id
    * @param uid     user id, works with workflow id as primary keys in database
    * @return boolean value indicating yes/no
    */
  def hasNoWorkflowAccess(wid: UInteger, uid: UInteger): Boolean = {
    checkAccessLevel(wid, uid).eq(WorkflowAccess.NONE)
  }

  /**
    * Returns an Access Object based on given wid and uid
    * Searches in database for the given uid-wid pair, and returns Access Object based on search result
    *
    * @param wid     workflow id
    * @param uid     user id, works with workflow id as primary keys in database
    * @return Access Object indicating the access level
    */
  def checkAccessLevel(wid: UInteger, uid: UInteger): WorkflowAccess.Value = {
    val workflowUserAccess = context
      .select(WORKFLOW_USER_ACCESS.READ_PRIVILEGE, WORKFLOW_USER_ACCESS.WRITE_PRIVILEGE)
      .from(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(wid).and(WORKFLOW_USER_ACCESS.UID.eq(uid)))
      .fetchOneInto(classOf[WorkflowUserAccess])
    toAccessLevel(workflowUserAccess)
  }

  def toAccessLevel(workflowUserAccess: WorkflowUserAccess): WorkflowAccess.Value = {
    if (workflowUserAccess == null) return WorkflowAccess.NO_RECORD
    if (workflowUserAccess.getWritePrivilege == true) {
      WorkflowAccess.WRITE
    } else if (workflowUserAccess.getReadPrivilege == true) {
      WorkflowAccess.READ
    } else {
      WorkflowAccess.NONE
    }
  }

  /**
    * Identifies whether the given user has no access record over the given workflow
    * @param wid     workflow id
    * @param uid     user id, works with workflow id as primary keys in database
    * @return boolean value indicating yes/no
    */
  def hasNoWorkflowAccessRecord(wid: UInteger, uid: UInteger): Boolean = {
    checkAccessLevel(wid, uid).eq(WorkflowAccess.NO_RECORD)
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

  object WorkflowAccess extends Enumeration {
    type Access = Value
    val READ: WorkflowAccess.Value = Value("Read")
    val WRITE: WorkflowAccess.Value = Value("Write")
    val NONE: WorkflowAccess.Value = Value("None")
    val NO_RECORD: WorkflowAccess.Value = Value("NoRecord")
  }
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
  def getWorkflowOwner(@PathParam("wid") wid: UInteger): OwnershipResponse = {
    val uid = workflowOfUserDao.fetchByWid(wid).get(0).getUid
    val ownerName = userDao.fetchOneByUid(uid).getName
    OwnershipResponse(ownerName)
  }

  /**
    * This method identifies the user access level of the given workflow
    *
    * @param wid     the given workflow
    * @return json object indicating uid, wid and access level, ex: {"level": "Write", "uid": 1, "wid": 15}
    */
  @GET
  @Path("/workflow/{wid}/level")
  def retrieveUserAccessLevel(
      @PathParam("wid") wid: UInteger,
      @Auth sessionUser: SessionUser
  ): AccessResponse = {
    val user = sessionUser.getUser
    val uid = user.getUid
    val workflowAccessLevel = checkAccessLevel(wid, uid).toString
    AccessResponse(uid, wid, workflowAccessLevel)
  }

  /**
    * This method returns all current shared accesses of the given workflow
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
    if (
      workflowOfUserDao.existsById(
        context
          .newRecord(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
          .values(user.getUid, wid)
      )
    ) {
      getGrantedWorkflowAccessList(wid, user.getUid)
    } else {
      throw new ForbiddenException("You are not the owner of the workflow.")
    }
  }

  /**
    * This method identifies the user access level of the given workflow
    *
    * @param wid     the given workflow
    * @param username the username of the use whose access is about to be removed
    * @return message indicating a success message
    */
  @DELETE
  @Path("/revoke/{wid}/{username}")
  def revokeWorkflowAccess(
      @PathParam("wid") wid: UInteger,
      @PathParam("username") username: String,
      @Auth sessionUser: SessionUser
  ): Unit = {
    val user = sessionUser.getUser
    val uid: UInteger =
      try {
        userDao.fetchByName(username).get(0).getUid
      } catch {
        case _: NullPointerException =>
          throw new BadRequestException("Target user does not exist.")
      }
    if (
      !workflowOfUserDao.existsById(
        context
          .newRecord(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
          .values(user.getUid, wid)
      )
    ) {
      throw new ForbiddenException("No sufficient access privilege.")
    } else {
      context
        .delete(WORKFLOW_USER_ACCESS)
        .where(WORKFLOW_USER_ACCESS.UID.eq(uid).and(WORKFLOW_USER_ACCESS.WID.eq(wid)))
        .execute()
    }
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
  def grantWorkflowAccess(
      @PathParam("wid") wid: UInteger,
      @PathParam("username") username: String,
      @PathParam("accessLevel") accessLevel: String,
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

    if (
      !workflowOfUserDao.existsById(
        context
          .newRecord(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
          .values(user.getUid, wid)
      )
    ) {
      throw new ForbiddenException("No sufficient access privilege.")
    } else {
      if (hasNoWorkflowAccessRecord(wid, uid)) {
        accessLevel match {
          case "read" =>
            workflowUserAccessDao.insert(
              new WorkflowUserAccess(
                uid,
                wid,
                true, // readPrivilege
                false // writePrivilege
              )
            )
          case "write" =>
            workflowUserAccessDao.insert(
              new WorkflowUserAccess(
                uid,
                wid,
                true, // readPrivilege
                true // writePrivilege
              )
            )
          case _ =>
            throw new ForbiddenException("No sufficient access privilege.")
        }
      } else {
        accessLevel match {
          case "read" =>
            workflowUserAccessDao.update(
              new WorkflowUserAccess(
                uid,
                wid,
                true, // readPrivilege
                false // writePrivilege
              )
            )
          case "write" =>
            workflowUserAccessDao.update(
              new WorkflowUserAccess(
                uid,
                wid,
                true, // readPrivilege
                true // writePrivilege
              )
            )
          case _ => throw new ForbiddenException("No sufficient access privilege.")
        }
      }
    }
  }
}
