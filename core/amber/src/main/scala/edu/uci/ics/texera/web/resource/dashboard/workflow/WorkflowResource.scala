package edu.uci.ics.texera.web.resource.dashboard.workflow

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{USER, WORKFLOW, WORKFLOW_OF_PROJECT, WORKFLOW_OF_USER, WORKFLOW_USER_ACCESS}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{WorkflowDao, WorkflowOfUserDao, WorkflowUserAccessDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos._
import edu.uci.ics.texera.web.resource.dashboard.workflow.WorkflowAccessResource.toAccessLevel
import edu.uci.ics.texera.web.resource.dashboard.workflow.WorkflowResource.{DashboardWorkflowEntry, WorkflowMetadata, context, fetchWorkflowMetadata, insertWorkflow, workflowDao}
import io.dropwizard.auth.Auth
import org.jooq.Condition
import org.jooq.impl.DSL.{groupConcat, noCondition}
import org.jooq.types.UInteger

import java.sql.Timestamp
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
    require(workflow.getWid == null, "a new workflow must not have an id")
    // insert workflow
    workflowDao.insert(workflow)
    // insert workflow owner
    workflowOfUserDao.insert(new WorkflowOfUser(user.getUid, workflow.getWid))
    // insert workflow access
    workflowUserAccessDao.insert(
      new WorkflowUserAccess(
        user.getUid,
        workflow.getWid,
        true, // readPrivilege
        true, // writePrivilege
      )
    )
  }

  def fetchWorkflowMetadata(wid: UInteger): WorkflowMetadata = {
    context
      .select(WORKFLOW.WID, WORKFLOW.NAME, WORKFLOW.CREATION_TIME, WORKFLOW.LAST_MODIFIED_TIME)
      .from(WORKFLOW)
      .where(WORKFLOW.WID.eq(wid))
      .fetchOneInto(classOf[WorkflowMetadata])
  }

  def fetchDashboardWorkflowEntry(wid: UInteger): DashboardWorkflowEntry = {
    val workflowEntry = context
      .select(WORKFLOW.WID, WORKFLOW.NAME,
        WORKFLOW.CREATION_TIME, WORKFLOW.LAST_MODIFIED_TIME, USER.NAME)
      .from(WORKFLOW)
      .join(USER)
      .on(WORKFLOW.OWNER_UID.eq(USER.UID))
      .where(WORKFLOW.WID.eq(wid))
      .fetchOne()
//      .fetchOne(record => {
////        val workflowMetadata = record.into
//      })
    null
  }

  case class WorkflowMetadata(
      wid: UInteger,
      name: String,
      ownerUid: UInteger,
      creationTime: Timestamp,
      lastModifiedTime: Timestamp
  )

  case class DashboardWorkflowEntry(
      workflowMetadata: WorkflowMetadata,
      isOwner: Boolean,
      accessLevel: AccessLevel.Value,
      ownerName: String,
      projectIDs: List[UInteger]
  )
}

@PermitAll
@Path("/workflow")
@Produces(Array(MediaType.APPLICATION_JSON))
class WorkflowResource {

  /**
    * This method returns all workflow IDs that the user has access to
    *
    * @return WorkflowID[]
    */
  @GET
  @Path("/workflow-ids")
  def retrieveIDs(@Auth sessionUser: SessionUser): List[String] = {
    val user = sessionUser.getUser
    val workflowEntries = context
      .select(WORKFLOW_USER_ACCESS.WID)
      .from(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.UID.eq(user.getUid))
      .fetch()

    workflowEntries
      .map(workflowRecord => workflowRecord.into(WORKFLOW_OF_USER).getWid().intValue().toString())
      .toList
  }

  /**
    * This method returns all owner user names of the workflows that the user has access to
    *
    * @return OwnerName[]
    */
  @GET
  @Path("/owners")
  def retrieveOwners(@Auth sessionUser: SessionUser): List[String] = {
    val user = sessionUser.getUser
    val workflowEntries = context
      .select(USER.NAME)
      .from(WORKFLOW_USER_ACCESS)
      .join(WORKFLOW_OF_USER)
      .on(WORKFLOW_USER_ACCESS.WID.eq(WORKFLOW_OF_USER.WID))
      .join(USER)
      .on(WORKFLOW_OF_USER.UID.eq(USER.UID))
      .where(WORKFLOW_USER_ACCESS.UID.eq(user.getUid))
      .groupBy(USER.UID)
      .fetch()

    workflowEntries
      .map(workflowRecord => workflowRecord.into(USER).getName())
      .toList
  }

  /**
    * This method returns workflow IDs, that contain the selected operators, as strings
    *
    * @return WorkflowID[]
    */
  @GET
  @Path("/search-by-operators")
  def searchWorkflowByOperator(
      @QueryParam("operator") operator: String,
      @Auth sessionUser: SessionUser
  ): List[String] = {
    // Example GET url: localhost:8080/workflow/searchOperators?operator=Regex,CSVFileScan
    val user = sessionUser.getUser
    val quotes = "\""
    val operatorArray =
      operator.replaceAllLiterally(" ", "").stripPrefix("[").stripSuffix("]").split(',')
    var orCondition: Condition = noCondition()
    for (i <- operatorArray.indices) {
      val operatorName = operatorArray(i)
      orCondition = orCondition.or(
        WORKFLOW.CONTENT
          .likeIgnoreCase(
            "%" + quotes + "operatorType" + quotes + ":" + quotes + s"$operatorName" + quotes + "%"
            //gives error when I try to combine escape character with formatted string
            //may be due to old scala version bug
          )
      )

    }

    val workflowEntries =
      context
        .select(
          WORKFLOW.WID
        )
        .from(WORKFLOW)
        .join(WORKFLOW_USER_ACCESS)
        .on(WORKFLOW_USER_ACCESS.WID.eq(WORKFLOW.WID))
        .where(
          orCondition
            .and(WORKFLOW_USER_ACCESS.UID.eq(user.getUid))
        )
        .fetch()

    workflowEntries
      .map(workflowRecord => {
        workflowRecord.into(WORKFLOW).getWid().intValue().toString()
      })
      .toList
  }

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
        WORKFLOW.OWNER_UID,
        USER.NAME,
        groupConcat(WORKFLOW_OF_PROJECT.PID).as("projects")
      )
      .from(WORKFLOW)
      .leftJoin(WORKFLOW_USER_ACCESS)
      .on(USER.UID.eq(WORKFLOW.OWNER_UID))
      .leftJoin(WORKFLOW_OF_PROJECT)
      .on(WORKFLOW.WID.eq(WORKFLOW_OF_PROJECT.WID))
      .where(WORKFLOW_USER_ACCESS.UID.eq(user.getUid))
      .groupBy(WORKFLOW.WID, WORKFLOW_OF_USER.UID)
      .fetch()
    workflowEntries
      .map(workflowRecord => {
        DashboardWorkflowEntry(
          workflowRecord.into(classOf[WorkflowMetadata]),
          workflowRecord.into(USER).getName,
          toAccessLevel(
            workflowRecord.into(WORKFLOW_USER_ACCESS).into(classOf[WorkflowUserAccess])
          ),
          workflowRecord.into(WORKFLOW).into(classOf[Workflow]),
          if (workflowRecord.component9() == null) List[UInteger]()
          else workflowRecord.component9().split(',').map(number => UInteger.valueOf(number)).toList
        )
      }
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
    if (! WorkflowAccessResource.hasReadAccess(wid, sessionUser.getUser.getUid)) {
      throw new ForbiddenException("No sufficient access privilege.")
    }
    workflowDao.fetchOneByWid(wid)
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
  def persistWorkflow(workflow: Workflow, @Auth sessionUser: SessionUser): Unit = {
    val user = sessionUser.getUser

    // workflow wid must be not null: must be an existing workflow
    if (workflow.getWid == null) {
      throw new BadRequestException("workflow id is not present")
    }
    // if workflow does not exist, reject the persist request
    if (! workflowDao.existsById(workflow.getWid)) {
      throw new BadRequestException(s"workflow ${workflow.getWid} does not exist")
    }
    // the user must have write access to this workflow
    if (! WorkflowAccessResource.hasWriteAccess(workflow.getWid, user.getUid)) {
      throw new BadRequestException(
        s"no write access to workflow ${workflow.getWid}: ${workflow.getName}")
    }

    workflowDao.update(workflow)
    WorkflowVersionResource.insertVersion(workflow, false)
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
      wid: UInteger,
      @Auth sessionUser: SessionUser
  ): WorkflowMetadata = {
    val uid = sessionUser.getUser.getUid

    // the user must have the privilege to read the workflow to be duplicated
    if (! WorkflowAccessResource.hasReadAccess(wid, uid)) {
      throw new BadRequestException(s"no read access to workflow ${wid}")
    }

    val workflow = workflowDao.fetchOneByWid(wid)
    val newWorkflow = new Workflow(workflow.getName + "_copy", null, workflow.getContent, null, null)
    createWorkflow(newWorkflow, sessionUser)
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
  def createWorkflow(workflow: Workflow, @Auth sessionUser: SessionUser): WorkflowMetadata = {
    val user = sessionUser.getUser
    if (workflow.getWid != null) {
      throw new BadRequestException("Cannot create a new workflow with a provided workflow id.")
    }

    // the user must have the privilege to create a new
    if (! WorkflowAccessResource.canCreateNewWorkflow(user.getUid)) {
      throw new BadRequestException(s"no privilege to create a new workflow")
    }

    insertWorkflow(workflow, user)
    WorkflowVersionResource.insertVersion(workflow, true)
    fetchWorkflowMetadata(workflow.getWid)
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
    }
    if (!workflowDao.existsById(wid)) {
      throw new BadRequestException("The workflow does not exist.")
    }
    val userWorkflow = workflowDao.fetchOneByWid(wid)
    userWorkflow.setName(workflowName)
    workflowDao.update(userWorkflow)
  }

}
