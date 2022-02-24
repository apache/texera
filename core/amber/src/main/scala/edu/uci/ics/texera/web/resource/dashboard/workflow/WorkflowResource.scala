package edu.uci.ics.texera.web.resource.dashboard.workflow

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{
  USER,
  WORKFLOW,
  WORKFLOW_OF_USER,
  WORKFLOW_USER_ACCESS
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{
  WorkflowDao,
  WorkflowOfUserDao,
  WorkflowUserAccessDao
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos._
import edu.uci.ics.texera.web.resource.dashboard.workflow.WorkflowAccessResource.{
  WorkflowAccess,
  toAccessLevel
}
import edu.uci.ics.texera.web.resource.dashboard.workflow.WorkflowResource.{
  DashboardWorkflowEntry,
  context,
  insertWorkflow,
  workflowDao,
  workflowOfUserExists
}
import io.dropwizard.auth.Auth
import org.jooq.types.UInteger

import javax.annotation.security.PermitAll
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
  * This file handles various request related to saved-workflows.
  * It sends mysql queries to the MysqlDB regarding the UserWorkflow Table
  * The details of UserWorkflowTable can be found in /core/scripts/sql/texera_ddl.sql
  */

object WorkflowResource {
  final private lazy val context = SqlServer.createDSLContext()
  final private val workflowDao = new WorkflowDao(context.configuration)
  final private val workflowOfUserDao = new WorkflowOfUserDao(
    context.configuration
  )
  final private val workflowUserAccessDao = new WorkflowUserAccessDao(
    context.configuration()
  )

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

  private def workflowOfUserExists(wid: UInteger, uid: UInteger): Boolean = {
    workflowOfUserDao.existsById(
      context
        .newRecord(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
        .values(uid, wid)
    )
  }

  case class DashboardWorkflowEntry(
      isOwner: Boolean,
      accessLevel: String,
      ownerName: String,
      workflow: Workflow
  )

}

@PermitAll
@Path("/workflow")
@Produces(Array(MediaType.APPLICATION_JSON))
class WorkflowResource {

  /**
    * This method returns the current in-session user's workflow list based on all workflows he/she has access to
    *
    * @return Workflow[]
    */

  @GET
  @Path("/list")
  def retrieveWorkflowsBySessionUser(
      @Auth sessionUser: SessionUser
  ): List[DashboardWorkflowEntry] = {
    val user = sessionUser.getUser
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

  }

  /**
    * This method handles the client request to get a specific workflow to be displayed in canvas
    * at current design, it only takes the workflowID and searches within the database for the matching workflow
    * for future design, it should also take userID as an parameter.
    *
    * @param wid     workflow id, which serves as the primary key in the UserWorkflow database
    * @return a json string representing an savedWorkflow
    */
  @GET
  @Path("/{wid}")
  def retrieveWorkflow(
      @PathParam("wid") wid: UInteger,
      @Auth sessionUser: SessionUser
  ): Workflow = {
    val user = sessionUser.getUser
    if (
      WorkflowAccessResource.hasNoWorkflowAccess(wid, user.getUid) ||
      WorkflowAccessResource.hasNoWorkflowAccessRecord(wid, user.getUid)
    ) {
      throw new ForbiddenException("No sufficient access privilege.")
    } else {
      workflowDao.fetchOneByWid(wid)
    }
  }

  /**
    * This method persists the workflow into database
    *
    * @param workflow , a workflow
    * @return Workflow, which contains the generated wid if not provided//
    *         TODO: divide into two endpoints -> one for new-workflow and one for updating existing workflow
    *         TODO: if the persist is triggered in parallel, the none atomic actions currently might cause an issue.
    *             Should consider making the operations atomic
    */
  @POST
  @Path("/persist")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def persistWorkflow(workflow: Workflow, @Auth sessionUser: SessionUser): Workflow = {
    val user = sessionUser.getUser

    if (workflowOfUserExists(workflow.getWid, user.getUid)) {
      WorkflowVersionResource.insertVersion(workflow, false)
      // current user reading
      workflowDao.update(workflow)
    } else {
      if (WorkflowAccessResource.hasNoWorkflowAccessRecord(workflow.getWid, user.getUid)) {
        // not owner and not access record --> new record
        insertWorkflow(workflow, user)
        WorkflowVersionResource.insertVersion(workflow, true)
      } else if (WorkflowAccessResource.hasWriteAccess(workflow.getWid, user.getUid)) {
        WorkflowVersionResource.insertVersion(workflow, false)
        // not owner but has write access
        workflowDao.update(workflow)
      } else {
        // not owner and no write access -> rejected
        throw new ForbiddenException("No sufficient access privilege.")
      }
    }
    workflowDao.fetchOneByWid(workflow.getWid)

  }

  /**
    * This method duplicates the target workflow, the new workflow name is appended with `_copy`
    *
    * @param workflow , a workflow to be duplicated
    * @return Workflow, which contains the generated wid if not provided
    */
  @POST
  @Path("/duplicate")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def duplicateWorkflow(
      workflow: Workflow,
      @Auth sessionUser: SessionUser
  ): DashboardWorkflowEntry = {
    val wid = workflow.getWid
    val user = sessionUser.getUser
    if (
      WorkflowAccessResource.hasNoWorkflowAccess(wid, user.getUid) ||
      WorkflowAccessResource.hasNoWorkflowAccessRecord(wid, user.getUid)
    ) {
      throw new ForbiddenException("No sufficient access privilege.")
    } else {
      val workflow: Workflow = workflowDao.fetchOneByWid(wid)
      workflow.getContent
      workflow.getName
      createWorkflow(
        new Workflow(workflow.getName + "_copy", null, workflow.getContent, null, null),
        sessionUser
      )

    }

  }

  /**
    * This method creates and insert a new workflow to database
    *
    * @param workflow , a workflow to be created
    * @return Workflow, which contains the generated wid if not provided
    */
  @POST
  @Path("/create")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  def createWorkflow(workflow: Workflow, @Auth sessionUser: SessionUser): DashboardWorkflowEntry = {
    val user = sessionUser.getUser
    if (workflow.getWid != null) {
      throw new BadRequestException("Cannot create a new workflow with a provided id.")
    } else {
      insertWorkflow(workflow, user)
      WorkflowVersionResource.insertVersion(workflow, true)
      DashboardWorkflowEntry(
        isOwner = true,
        WorkflowAccess.WRITE.toString,
        user.getName,
        workflowDao.fetchOneByWid(workflow.getWid)
      )
    }

  }

  /**
    * This method deletes the workflow from database
    *
    * @return Response, deleted - 200, not exists - 400
    */
  @DELETE
  @Path("/{wid}")
  def deleteWorkflow(@PathParam("wid") wid: UInteger, @Auth sessionUser: SessionUser): Unit = {
    val user = sessionUser.getUser
    if (workflowOfUserExists(wid, user.getUid)) {
      workflowDao.deleteById(wid)
    } else {
      throw new BadRequestException("The workflow does not exist.")
    }
  }

  /**
    * This method updates the name of a given workflow
    *
    * @return Response
    */
  @POST
  @Path("/update/name/{wid}/{workflowName}")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  def updateWorkflowName(
      @PathParam("wid") wid: UInteger,
      @PathParam("workflowName") workflowName: String,
      @Auth sessionUser: SessionUser
  ): Unit = {
    val user = sessionUser.getUser
    if (!WorkflowAccessResource.hasWriteAccess(wid, user.getUid)) {
      throw new ForbiddenException("No sufficient access privilege.")
    } else if (!workflowOfUserExists(wid, user.getUid)) {
      throw new BadRequestException("The workflow does not exist.")
    } else {
      val userWorkflow = workflowDao.fetchOneByWid(wid)
      userWorkflow.setName(workflowName)
      workflowDao.update(userWorkflow)
    }
  }

}
