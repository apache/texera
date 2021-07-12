package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{WORKFLOW_OF_USER, WORKFLOW_USER_ACCESS}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{
  WorkflowDao,
  WorkflowOfUserDao,
  WorkflowUserAccessDao
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.WorkflowUserAccess
import edu.uci.ics.texera.web.resource.auth.UserResource
import edu.uci.ics.texera.web.resource.dashboard
import io.dropwizard.jersey.sessions.Session
import org.jooq.types.UInteger

import javax.servlet.http.HttpSession
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import scala.collection.immutable.HashMap

/**
  * An enum class identifying the specific workflow access level
  *
  * READ indicates read-only
  * WRITE indicates write access (which dominates)
  * NONE indicates having an access record, but either read or write access is granted
  * NO_RECORD indicates having no record in the table
  */
object WorkflowAccess extends Enumeration {
  type Access = Value
  val READ: dashboard.WorkflowAccess.Value = Value("read")
  val WRITE: dashboard.WorkflowAccess.Value = Value("write")
  val NONE: dashboard.WorkflowAccess.Value = Value("none")
  val NO_RECORD: dashboard.WorkflowAccess.Value = Value("No Record")
}

class WorkflowAccessResponse(uid: UInteger, wid: UInteger, level: String)

/**
  * Helper functions for retrieving access level based on given information
  */
object WorkflowAccessResource {

  /**
    * Returns a short workflow access description in string
    *
    * @param wid     workflow id
    * @param uid     user id, works with workflow id as primary keys in database
    * @return String indicating the access level
    */
  def getWorkflowAccessDesc(wid: UInteger, uid: UInteger): String = {
    WorkflowAccessResource.checkAccessLevel(wid, uid) match {
      case WorkflowAccess.READ      => "Read"
      case WorkflowAccess.WRITE     => "Write"
      case WorkflowAccess.NONE      => "None"
      case WorkflowAccess.NO_RECORD => "NoRecord"
    }
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
    val accessDetail = SqlServer.createDSLContext
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

  /**
    * Identifies whether the given user has read-only access over the given workflow
    *
    * @param wid     workflow id
    * @param uid     user id, works with workflow id as primary keys in database
    * @return boolean value indicating yes/no
    */
  def hasReadAccess(wid: UInteger, uid: UInteger): Boolean = {
    WorkflowAccessResource.checkAccessLevel(wid, uid).eq(WorkflowAccess.READ)
  }

  /**
    * Identifies whether the given user has write access over the given workflow
    * @param wid     workflow id
    * @param uid     user id, works with workflow id as primary keys in database
    * @return boolean value indicating yes/no
    */
  def hasWriteAccess(wid: UInteger, uid: UInteger): Boolean = {
    WorkflowAccessResource.checkAccessLevel(wid, uid).eq(WorkflowAccess.WRITE)
  }

  /**
    * Identifies whether the given user has no access over the given workflow
    * @param wid     workflow id
    * @param uid     user id, works with workflow id as primary keys in database
    * @return boolean value indicating yes/no
    */
  def hasNoWorkflowAccess(wid: UInteger, uid: UInteger): Boolean = {
    WorkflowAccessResource.checkAccessLevel(wid, uid).eq(WorkflowAccess.NONE)
  }

  /**
    * Identifies whether the given user has no access record over the given workflow
    * @param wid     workflow id
    * @param uid     user id, works with workflow id as primary keys in database
    * @return boolean value indicating yes/no
    */
  def hasNoWorkflowAccessRecord(wid: UInteger, uid: UInteger): Boolean = {
    WorkflowAccessResource.checkAccessLevel(wid, uid).eq(WorkflowAccess.NO_RECORD)
  }
}

/**
  * Provides endpoints for operations related to Workflow Access.
  */
@Path("/workflowaccess")
@Produces(Array(MediaType.APPLICATION_JSON))
class WorkflowAccessResource {
  final private val workflowDao = new WorkflowDao(SqlServer.createDSLContext.configuration)
  final private val workflowOfUserDao = new WorkflowOfUserDao(
    SqlServer.createDSLContext.configuration
  )
  final private val workflowUserAccessDao = new WorkflowUserAccessDao(
    SqlServer.createDSLContext().configuration()
  )

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
        val workflowAccessLevel = WorkflowAccessResource.getWorkflowAccessDesc(wid, uid)
        val resultData = HashMap("uid" -> uid, "wid" -> wid, "level" -> workflowAccessLevel)
//        Response.ok(new WorkflowAccessResponse(uid, wid, workflowAccessLevel)).build()
        Response.ok(resultData).build()
      case None =>
        Response.status(Response.Status.UNAUTHORIZED).build()
    }
  }

  /**
    * This method shares a workflow to a user with a specific access type
    *
    * @param wid     the given workflow
    * @param uid     the user id which the access is given to
    * @param session the session indicating current User
    * @param accessType the type of Access given to the target user
    * @return rejection if user not permitted to share the workflow
    */
  @POST
  @Path("/share/{wid}/{uid}/{accessType}")
  def shareAccess(
      @PathParam("wid") wid: UInteger,
      @PathParam("uid") uid: UInteger,
      @PathParam("accessType") accessType: String,
      @Session session: HttpSession
  ): Response = {
    UserResource.getUser(session) match {
      case Some(user) =>
        if (
          !workflowOfUserDao.existsById(
            SqlServer.createDSLContext
              .newRecord(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
              .values(user.getUid, wid)
          )
        ) {
          Response.status(Response.Status.UNAUTHORIZED).build()
        } else {
          if (WorkflowAccessResource.hasNoWorkflowAccessRecord(wid, uid)) {
            accessType match {
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
              case _ => Response.status(Response.Status.BAD_REQUEST).build()
            }
          } else {
            accessType match {
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
