package edu.uci.ics.texera.web.resource.dashboard

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.flipkart.zjsonpatch.{JsonDiff, JsonPatch}
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{
  USER,
  WORKFLOW,
  WORKFLOW_OF_USER,
  WORKFLOW_USER_ACCESS,
  WORKFLOW_VERSION
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{
  WorkflowDao,
  WorkflowOfUserDao,
  WorkflowUserAccessDao,
  WorkflowVersionDao
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowOfUser,
  WorkflowUserAccess,
  WorkflowVersion
}
import edu.uci.ics.texera.web.resource.auth.UserResource
import edu.uci.ics.texera.web.resource.dashboard.WorkflowAccessResource.{
  WorkflowAccess,
  toAccessLevel
}
import edu.uci.ics.texera.web.resource.dashboard.WorkflowResource.context
import io.dropwizard.jersey.sessions.Session
import org.jooq.types.UInteger

import java.sql.Timestamp
import javax.servlet.http.HttpSession
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
  * This file handles various request related to saved-workflows.
  * It sends mysql queries to the MysqlDB regarding the UserWorkflow Table
  * The details of UserWorkflowTable can be found in /core/scripts/sql/texera_ddl.sql
  */

case class DashboardWorkflowEntry(
    isOwner: Boolean,
    accessLevel: String,
    ownerName: String,
    workflow: Workflow
)

case class VersionEntry(
    vId: UInteger,
    creationTime: Timestamp,
    content: String
)

object WorkflowResource {
  val context = SqlServer.createDSLContext()
}

@Path("/workflow")
@Produces(Array(MediaType.APPLICATION_JSON))
class WorkflowResource {

  final private val workflowDao = new WorkflowDao(context.configuration)
  final private val workflowVersionDao = new WorkflowVersionDao(context.configuration)
  final private val workflowOfUserDao = new WorkflowOfUserDao(
    context.configuration
  )
  final private val workflowUserAccessDao = new WorkflowUserAccessDao(
    context.configuration()
  )

  /**
    * This method returns the current in-session user's workflow list based on all workflows he/she has access to
    *
    * @param session HttpSession
    * @return Workflow[]
    */
  @GET
  @Path("/list")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def retrieveWorkflowsBySessionUser(
      @Session session: HttpSession
  ): List[DashboardWorkflowEntry] = {
    UserResource.getUser(session) match {
      case Some(user) =>
        val workflowEntries = context
          .select(
            WORKFLOW.WID,
            WORKFLOW.NAME,
            WORKFLOW.CREATION_TIME,
            WORKFLOW.LAST_MODIFIED_TIME,
            WORKFLOW_USER_ACCESS.READ_PRIVILEGE,
            WORKFLOW_USER_ACCESS.WRITE_PRIVILEGE,
            WORKFLOW_OF_USER.UID,
            USER.NAME
          )
          .from(WORKFLOW)
          .leftJoin(WORKFLOW_USER_ACCESS)
          .on(WORKFLOW_USER_ACCESS.WID.eq(WORKFLOW.WID))
          .leftJoin(WORKFLOW_OF_USER)
          .on(WORKFLOW_OF_USER.WID.eq(WORKFLOW.WID))
          .leftJoin(USER)
          .on(USER.UID.eq(WORKFLOW_OF_USER.UID))
          .where(WORKFLOW_USER_ACCESS.UID.eq(user.getUid))
          .fetch()
        workflowEntries
          .map(workflowRecord =>
            DashboardWorkflowEntry(
              workflowRecord.into(WORKFLOW_OF_USER).getUid.eq(user.getUid),
              toAccessLevel(
                workflowRecord.into(WORKFLOW_USER_ACCESS).into(classOf[WorkflowUserAccess])
              ).toString,
              workflowRecord.into(USER).getName,
              workflowRecord.into(WORKFLOW).into(classOf[Workflow])
            )
          )
          .toList
      case None => List()
    }
  }

  /**
    * This method handles the client request to get a specific workflow to be displayed in canvas
    * at current design, it only takes the workflowID and searches within the database for the matching workflow
    * for future design, it should also take userID as an parameter.
    *
    * @param wid     workflow id, which serves as the primary key in the UserWorkflow database
    * @param session HttpSession
    * @return a json string representing an savedWorkflow
    */
  @GET
  @Path("/{wid}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def retrieveWorkflow(@PathParam("wid") wid: UInteger, @Session session: HttpSession): Response = {
    UserResource.getUser(session) match {
      case Some(user) =>
        if (
          WorkflowAccessResource.hasNoWorkflowAccess(wid, user.getUid) ||
          WorkflowAccessResource.hasNoWorkflowAccessRecord(wid, user.getUid)
        ) {
          Response.status(Response.Status.UNAUTHORIZED).build()
        } else {
          Response.ok(workflowDao.fetchOneByWid(wid)).build()
        }
      case None =>
        Response.status(Response.Status.UNAUTHORIZED).build()
    }
  }

  //TODO should add an entry to store diff given a wid and diff content

  /**
    * This method persists the workflow into database
    *
    * @param session  HttpSession
    * @param workflow , a workflow
    * @return Workflow, which contains the generated wid if not provided//
    *         TODO: divide into two endpoints -> one for new-workflow and one for updating existing workflow
    */
  @POST
  @Path("/persist")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  def persistWorkflow(@Session session: HttpSession, workflow: Workflow): Response = {
    UserResource.getUser(session) match {
      case Some(user) =>
        if (workflowOfUserExists(workflow.getWid, user.getUid)) {
          // todo retrieve current workflow from DB
          val currentWorkflow = workflowDao.fetchOneByWid(workflow.getWid)
          // todo compute diff
          val mapper = new ObjectMapper()
          val patch = JsonDiff.asJson(
            mapper.readTree(workflow.getContent),
            mapper.readTree(currentWorkflow.getContent)
          )
          // todo if they are different
          if (!patch.isEmpty) {
            // todo write into DB both diff and updated version
            val workflowVersion = new WorkflowVersion()
            workflowVersion.setContent(patch.toString)
            workflowVersion.setWid(workflow.getWid)
            workflowVersionDao.insert(workflowVersion)
          }
          // current user reading
          workflowDao.update(workflow)
        } else {
          if (WorkflowAccessResource.hasNoWorkflowAccessRecord(workflow.getWid, user.getUid)) {
            // not owner and not access record --> new record
            insertWorkflow(workflow, user)

          } else if (WorkflowAccessResource.hasWriteAccess(workflow.getWid, user.getUid)) {
            // not owner but has write access
            workflowDao.update(workflow)
          } else {
            // not owner and no write access -> rejected
            Response.status(Response.Status.UNAUTHORIZED).build()
          }
        }
        Response.ok(workflowDao.fetchOneByWid(workflow.getWid)).build()
      case None =>
        Response.status(Response.Status.UNAUTHORIZED).build()
    }
  }

  private def insertWorkflow(workflow: Workflow, user: User): Unit = {
    workflowDao.insert(workflow)
    workflowOfUserDao.insert(new WorkflowOfUser(user.getUid, workflow.getWid))
    workflowUserAccessDao.insert(
      new WorkflowUserAccess(
        user.getUid,
        workflow.getWid,
        true, // readPrivilege
        true // writePrivilege
      )
    )
  }

  /**
    * This method duplicates the target workflow, the new workflow name is appended with `_copy`
    *
    * @param session  HttpSession
    * @param workflow , a workflow to be duplicated
    * @return Workflow, which contains the generated wid if not provided
    */
  @POST
  @Path("/duplicate")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  def duplicateWorkflow(@Session session: HttpSession, workflow: Workflow): Response = {
    val wid = workflow.getWid
    UserResource.getUser(session) match {
      case Some(user) =>
        if (
          WorkflowAccessResource.hasNoWorkflowAccess(wid, user.getUid) ||
          WorkflowAccessResource.hasNoWorkflowAccessRecord(wid, user.getUid)
        ) {
          Response.status(Response.Status.UNAUTHORIZED).build()
        } else {
          val workflow: Workflow = workflowDao.fetchOneByWid(wid)
          workflow.getContent
          workflow.getName
          createWorkflow(
            session,
            new Workflow(workflow.getName + "_copy", null, workflow.getContent, null, null, 0)
          )

        }
      case None =>
        Response.status(Response.Status.UNAUTHORIZED).build()
    }
  }

  /**
    * This method creates and insert a new workflow to database
    *
    * @param session  HttpSession
    * @param workflow , a workflow to be created
    * @return Workflow, which contains the generated wid if not provided
    */
  @POST
  @Path("/create")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  def createWorkflow(@Session session: HttpSession, workflow: Workflow): Response = {
    UserResource.getUser(session) match {
      case Some(user) =>
        if (workflow.getWid != null) {
          Response.status(Response.Status.BAD_REQUEST).build()
        } else {
          insertWorkflow(workflow, user)
          val resp = DashboardWorkflowEntry(
            isOwner = true,
            WorkflowAccess.WRITE.toString,
            user.getName,
            workflowDao.fetchOneByWid(workflow.getWid)
          )
          Response.ok(resp).build()
        }
      case None =>
        Response.status(Response.Status.UNAUTHORIZED).build()
    }
  }

  /**
    * This method deletes the workflow from database
    *
    * @param session HttpSession
    * @return Response, deleted - 200, not deleted - 304 // TODO: change the error code
    */
  @DELETE
  @Path("/{wid}")
  def deleteWorkflow(@PathParam("wid") wid: UInteger, @Session session: HttpSession): Response = {
    UserResource.getUser(session) match {
      case Some(user) =>
        if (workflowOfUserExists(wid, user.getUid)) {
          workflowDao.deleteById(wid)
          Response.ok().build()
        } else {
          Response.notModified().build()
        }
      case None =>
        Response.status(Response.Status.UNAUTHORIZED).build()
    }
  }

  private def workflowOfUserExists(wid: UInteger, uid: UInteger): Boolean = {
    workflowOfUserDao.existsById(
      context
        .newRecord(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
        .values(uid, wid)
    )
  }

  /**
    * This method returns the versions of a workflow
    *
    * @param session HttpSession
    * @return versions[]
    */
  @GET
  @Path("/versions/{wid}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def retrieveVersionsOfWorkflow(
      @PathParam("wid") wid: UInteger,
      @Session session: HttpSession
  ): List[VersionEntry] = {
    UserResource.getUser(session) match {
      case Some(user) =>
        val versions = context
          .select(WORKFLOW_VERSION.VID, WORKFLOW_VERSION.CREATION_TIME, WORKFLOW_VERSION.CONTENT)
          .from(WORKFLOW_VERSION)
          .leftJoin(WORKFLOW)
          .on(WORKFLOW_VERSION.WID.eq(WORKFLOW.WID))
          .where(WORKFLOW_VERSION.WID.eq(wid))
          .fetch()
        versions
          .map(workflowRecord =>
            VersionEntry(
              workflowRecord.into(WORKFLOW_VERSION).getVid,
              workflowRecord.into(WORKFLOW_VERSION).getCreationTime,
              workflowRecord.into(WORKFLOW_VERSION).getContent
            )
          )
          .toList
      case None => List()
    }
  }

  /**
    * This method returns the versions of a workflow
    *
    * @param wid workflowID of the current workflow the user is working on
    * @param vid versionID of the checked-out version to be computed and returned
    * @param session HttpSession
    * @return workflow of a particular version
    */
  @POST
  @Path("/version")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  def retrieveWorkflowVersion(@Session session: HttpSession, workflow: Workflow): Response = {
    val wid = workflow.getWid
    val vid = workflow.getVid
    UserResource.getUser(session) match {
      case Some(user) =>
        if (
          WorkflowAccessResource.hasNoWorkflowAccess(wid, user.getUid) ||
          WorkflowAccessResource.hasNoWorkflowAccessRecord(wid, user.getUid)
        ) {
          Response.status(Response.Status.UNAUTHORIZED).build()
        } else {
          // todo fetch all versions preceeding this
          val versions = context
            .select(WORKFLOW_VERSION.VID, WORKFLOW_VERSION.CREATION_TIME, WORKFLOW_VERSION.CONTENT)
            .from(WORKFLOW_VERSION)
            .leftJoin(WORKFLOW)
            .on(WORKFLOW_VERSION.WID.eq(WORKFLOW.WID))
            .where(WORKFLOW_VERSION.WID.eq(wid))
            .fetch()
          // todo apply patch
          val currentWorkflow = workflowDao.fetchOneByWid(wid)
          val versionEntries = versions
            .map(workflowRecord =>
              VersionEntry(
                workflowRecord.into(WORKFLOW_VERSION).getVid,
                workflowRecord.into(WORKFLOW_VERSION).getCreationTime,
                workflowRecord.into(WORKFLOW_VERSION).getContent
              )
            )
            .toList
          //todo return result
          val res = applyPatch(versionEntries.reverse, currentWorkflow)
          Response
            .ok(res)
            .build()
        }
      case None =>
        Response.status(Response.Status.UNAUTHORIZED).build()
    }
  }
  private def applyPatch(versions: List[VersionEntry], workflow: Workflow): Workflow = {
    // todo loop all versions and apply the patch
    val mapper = new ObjectMapper()
    for (patch <- versions) {
      workflow.setContent(
        JsonPatch
          .apply(mapper.readTree(patch.content), mapper.readTree(workflow.getContent))
          .toString
      )
      workflow.setVid(patch.vId.intValue())
      workflow.setLastModifiedTime(patch.creationTime)
    }
    //todo need to persist the chosen version to disk
    return workflow
  }
}
