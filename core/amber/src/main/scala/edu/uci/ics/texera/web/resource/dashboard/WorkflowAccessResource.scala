package edu.uci.ics.texera.web.resource.dashboard
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{WORKFLOW_OF_USER, WORKFLOW_USER_ACCESS}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{
  UserDao,
  WorkflowOfUserDao,
  WorkflowUserAccessDao
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.WorkflowUserAccess
import edu.uci.ics.texera.web.resource.auth.UserResource
import edu.uci.ics.texera.web.resource.dashboard.WorkflowAccessResource.{
  UserWorkflowAccess,
  checkAccessLevel,
  context,
  hasNoWorkflowAccessRecord
}
import io.dropwizard.jersey.sessions.Session
import org.jooq.DSLContext
import org.jooq.types.UInteger

import javax.servlet.http.HttpSession
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import scala.collection.JavaConverters._
import scala.collection.mutable

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
    * Identifies whether the given user has no access record over the given workflow
    * @param wid     workflow id
    * @param uid     user id, works with workflow id as primary keys in database
    * @return boolean value indicating yes/no
    */
  def hasNoWorkflowAccessRecord(wid: UInteger, uid: UInteger): Boolean = {
    checkAccessLevel(wid, uid).eq(WorkflowAccess.NO_RECORD)
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
    val accessDetail = context
      .select(WORKFLOW_USER_ACCESS.READ_PRIVILEGE, WORKFLOW_USER_ACCESS.WRITE_PRIVILEGE)
      .from(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(wid).and(WORKFLOW_USER_ACCESS.UID.eq(uid)))
      .fetch()
    if (accessDetail.isEmpty) return WorkflowAccess.NO_RECORD
    if (accessDetail.getValue(0, 1) == true) {
      WorkflowAccess.WRITE
    } else if (accessDetail.getValue(0, 0) == true) {
      WorkflowAccess.READ
    } else {
      WorkflowAccess.NONE
    }
  }

  case class UserWorkflowAccess(userName: String, accessLevel: String) {}

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
@Path("/workflow-access")
@Produces(Array(MediaType.APPLICATION_JSON))
class WorkflowAccessResource() {

  private val userDao = new UserDao(context.configuration())
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
    * This method identifies the user access level of the given workflow
    *
    * @param wid     the given workflow
    * @param session the session indicating current User
    * @return json object indicating uid, wid and access level, ex: {"level": "Write", "uid": 1, "wid": 15}
    */
  @GET
  @Path("/workflow/{wid}/level")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def retrieveUserAccessLevel(
      @PathParam("wid") wid: UInteger,
      @Session session: HttpSession
  ): Response = {
    UserResource.getUser(session) match {
      case Some(user) =>
        val uid = user.getUid
        val workflowAccessLevel = checkAccessLevel(wid, uid).toString
        val resultData = mutable.HashMap("uid" -> uid, "wid" -> wid, "level" -> workflowAccessLevel)
        Response.ok(resultData).build()
      case None =>
        Response.status(Response.Status.UNAUTHORIZED).build()
    }
  }

  /**
    * This method returns all current shared accesses of the given workflow
    *
    * @param wid     the given workflow
    * @param session the session indicating current User
    * @return json object indicating user with access and access type, ex: [{"Jim": "Write"}]
    */
  @GET
  @Path("/list/{wid}")
  def retrieveGrantedList(
      @PathParam("wid") wid: UInteger,
      @Session session: HttpSession
  ): Response = {
    UserResource.getUser(session) match {
      case Some(user) =>
        if (
          workflowOfUserDao.existsById(
            context
              .newRecord(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
              .values(user.getUid, wid)
          )
        ) {
          Response.ok(getGrantedList(wid, user.getUid)).build()
        } else {
          Response
            .status(Response.Status.UNAUTHORIZED)
            .entity("You are not the owner of the workflow.")
            .build()
        }
      case None =>
        Response.status(Response.Status.UNAUTHORIZED).entity("Please Login.").build()
    }
  }

  /**
    * Returns information about all current shared access of the given workflow
    *
    * @param wid     workflow id
    * @param uid     user id of current user, used to identify ownership
    * @return a List with corresponding information Ex: [{"Jim": "Read"}]
    */
  def getGrantedList(wid: UInteger, uid: UInteger): List[UserWorkflowAccess] = {
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
            UserWorkflowAccess(userName, "Write")
          } else {
            UserWorkflowAccess(userName, "Read")
          }
      })

  }

  /**
    * This method identifies the user access level of the given workflow
    *
    * @param wid     the given workflow
    * @param username the username of the use whose access is about to be removed
    * @param session the session indicating current User
    * @return message indicating a success message
    */
  @POST
  @Path("/revoke/{wid}/{username}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def revokeAccess(
      @PathParam("wid") wid: UInteger,
      @PathParam("username") username: String,
      @Session session: HttpSession
  ): Response = {
    UserResource.getUser(session) match {
      case Some(user) =>
        val uid: UInteger =
          try {
            userDao.fetchByName(username).get(0).getUid
          } catch {
            case _: NullPointerException =>
              return Response
                .status(Response.Status.BAD_REQUEST)
                .entity("Target user does not exist!")
                .build()
          }
        if (
          !workflowOfUserDao.existsById(
            context
              .newRecord(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
              .values(user.getUid, wid)
          )
        ) {
          Response.status(Response.Status.UNAUTHORIZED).build()
        } else {
          context
            .delete(WORKFLOW_USER_ACCESS)
            .where(WORKFLOW_USER_ACCESS.UID.eq(uid).and(WORKFLOW_USER_ACCESS.WID.eq(wid)))
            .execute()
        }
        Response.ok().build()

      case None =>
        Response.status(Response.Status.UNAUTHORIZED).build()
    }
  }

  /**
    * This method shares a workflow to a user with a specific access type
    *
    * @param wid     the given workflow
    * @param username    the user name which the access is given to
    * @param session the session indicating current User
    * @param accessLevel the type of Access given to the target user
    * @return rejection if user not permitted to share the workflow or Success Message
    */
  @POST
  @Path("/grant/{wid}/{username}/{accessLevel}")
  def grantAccess(
      @PathParam("wid") wid: UInteger,
      @PathParam("username") username: String,
      @PathParam("accessLevel") accessLevel: String,
      @Session session: HttpSession
  ): Response = {
    UserResource.getUser(session) match {
      case Some(user) =>
        val uid: UInteger =
          try {
            userDao.fetchByName(username).get(0).getUid
          } catch {
            case _: IndexOutOfBoundsException =>
              return Response
                .status(Response.Status.BAD_REQUEST)
                .entity("Target user does not exist.")
                .build()
          }

        if (
          !workflowOfUserDao.existsById(
            context
              .newRecord(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
              .values(user.getUid, wid)
          )
        ) {
          Response
            .status(Response.Status.UNAUTHORIZED)
            .entity("Do not have ownership to grant access.")
            .build()
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
                Response
                  .status(Response.Status.BAD_REQUEST)
                  .entity("Does not have sufficient access level.")
                  .build()
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
              case _ => Response.status(Response.Status.BAD_REQUEST).build()
            }
          }
          Response.ok().build()
        }
      case None => Response.status(Response.Status.UNAUTHORIZED).build()
    }
  }

}
