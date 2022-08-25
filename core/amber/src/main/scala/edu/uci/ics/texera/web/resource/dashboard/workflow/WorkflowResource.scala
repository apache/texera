package edu.uci.ics.texera.web.resource.dashboard.workflow

import com.codahale.metrics.annotation.Timed
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{USER, WORKFLOW, WORKFLOW_OF_PROJECT, WORKFLOW_USER_ACCESS}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{UserDao, WorkflowDao, WorkflowUserAccessDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos._
import edu.uci.ics.texera.web.resource.dashboard.file.UserFileResource.DashboardFileEntry
import edu.uci.ics.texera.web.resource.dashboard.workflow.WorkflowAccessResource.{getAccessRecord, toAccessLevel}
import edu.uci.ics.texera.web.resource.dashboard.workflow.WorkflowResource.{DashboardWorkflowEntry, WorkflowMetadata, context, fetchDashboardWorkflows, fetchOneDashboardWorkflow, insertWorkflow, workflowDao}
import io.dropwizard.auth.Auth
import org.jooq.Condition
import org.jooq.impl.DSL
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
  final private val userDao = new UserDao(context.configuration)
  final private val workflowDao = new WorkflowDao(context.configuration)
  final private val workflowUserAccessDao = new WorkflowUserAccessDao(
    context.configuration()
  )

  def main(args: Array[String]): Unit = {
    val accessLevel = AccessLevel.WRITE

    val entry = DashboardFileEntry("owner", accessLevel, true, null, List())
    val a = Utils.objectMapper.writeValueAsString(accessLevel)
    val b = Utils.objectMapper.writeValueAsString(entry)
    println(a)
    println(b)
  }

  private def insertWorkflow(workflow: Workflow, user: User): Unit = {
    require(workflow.getWid == null, "a new workflow must not have an id")

    workflow.setOwnerUid(user.getUid)
    // insert workflow
    workflowDao.insert(workflow)
    // insert workflow access
    val access = new WorkflowUserAccess(user.getUid, workflow.getWid, true, true)
    workflowUserAccessDao.insert(access)
  }

//  def fetchWorkflowMetadata(wid: UInteger): WorkflowMetadata = {
//    context
//      .select(WORKFLOW.WID, WORKFLOW.NAME, WORKFLOW.CREATION_TIME, WORKFLOW.LAST_MODIFIED_TIME)
//      .from(WORKFLOW)
//      .where(WORKFLOW.WID.eq(wid))
//      .fetchOneInto(classOf[WorkflowMetadata])
//  }

  def fetchOneDashboardWorkflow(wid: UInteger, uid: UInteger): Option[DashboardWorkflowEntry] = {
    fetchDashboardWorkflows(uid, Some(wid)).headOption
  }

  def fetchDashboardWorkflows(uid: UInteger, wid: Option[UInteger] = None, pid: Option[UInteger] = None): List[DashboardWorkflowEntry] = {
    val buildStart = System.currentTimeMillis()

    val sql = context
      .select(
        WORKFLOW.WID,
        WORKFLOW.NAME,
        WORKFLOW.OWNER_UID,
        WORKFLOW.CREATION_TIME,
        WORKFLOW.LAST_MODIFIED_TIME,
        WORKFLOW_USER_ACCESS.READ_PRIVILEGE,
        WORKFLOW_USER_ACCESS.WRITE_PRIVILEGE,
        USER.NAME,
        groupConcat(WORKFLOW_OF_PROJECT.PID).as("projects")
      )
      .from(WORKFLOW)
      .join(USER)
      .on(WORKFLOW.OWNER_UID.eq(USER.UID))
      .join(WORKFLOW_USER_ACCESS)
      .on(WORKFLOW_USER_ACCESS.WID.eq(WORKFLOW.WID))
      .leftOuterJoin(WORKFLOW_OF_PROJECT)
      .on(WORKFLOW.WID.eq(WORKFLOW_OF_PROJECT.WID))
      .where(WORKFLOW_USER_ACCESS.UID.eq(uid)
        .and(if(wid.nonEmpty) WORKFLOW.WID.eq(wid.get) else DSL.noCondition())
        .and(if(pid.nonEmpty) WORKFLOW_OF_PROJECT.PID.eq(pid.get) else DSL.noCondition())
      )
      .groupBy(WORKFLOW.WID)

    println(sql.getSQL)

    val execStart = System.currentTimeMillis()
    val workflowEntries = sql.fetch().toList

    val mapStart = System.currentTimeMillis()
    val ret = workflowEntries
      .map(record =>
        DashboardWorkflowEntry(
          WorkflowMetadata(record.value1(), record.value2(), record.value4(), record.value4()),
//          workflowRecord.into(classOf[WorkflowMetadata]),
          record.get(WORKFLOW.OWNER_UID) == uid,
          toAccessLevel(
            record.into(WORKFLOW_USER_ACCESS).into(classOf[WorkflowUserAccess])
          ),
          record.into(USER).getName,
          if (record.value9() == null) List[UInteger]()
          else record.value9().split(',').map(number => UInteger.valueOf(number)).toList
        )
      )
      .toList

    println(s"dashboard list: building sql ${execStart - buildStart} ms")
    println(s"dashboard list: exec sql ${mapStart - execStart} ms")
    println(s"dashboard list: map results ${System.currentTimeMillis() - mapStart} ms")

    ret
  }

  case class WorkflowMetadata(
      wid: UInteger,
      name: String,
      creationTime: Timestamp,
      lastModifiedTime: Timestamp
  )

  case class DashboardWorkflowEntry(
      workflow: WorkflowMetadata,
      isOwner: Boolean,
      accessLevel: AccessLevel,
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
    context
      .select(WORKFLOW_USER_ACCESS.WID)
      .from(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.UID.eq(sessionUser.getUser.getUid))
      .fetch()
      .map(record => record.value1().toString)
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
    context
      .selectDistinct(USER.NAME)
      .from(WORKFLOW_USER_ACCESS)
      .join(WORKFLOW)
      .on(WORKFLOW.WID.eq(WORKFLOW_USER_ACCESS.WID))
      .join(USER)
      .on(WORKFLOW.OWNER_UID.eq(USER.UID))
      .where(WORKFLOW_USER_ACCESS.UID.eq(sessionUser.getUser.getUid))
      .fetch()
      .map(record => record.value1())
      .toList
  }

  /**
    * Returns workflow IDs that contain the selected operators, as strings
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
    val quote = "\""
    val operatorArray =
      operator.replaceAllLiterally(" ", "").stripPrefix("[").stripSuffix("]").split(',')
    var orCondition: Condition = noCondition()
    for (i <- operatorArray.indices) {
      val operatorName = operatorArray(i)
      orCondition = orCondition.or(
        WORKFLOW.CONTENT.likeIgnoreCase(
          s"%${quote}operatorType${quote}:${quote}${operatorName}${quote}%"
        )
      )
    }

    val workflowEntries = context
      .select(WORKFLOW.WID)
      .from(WORKFLOW)
      .join(WORKFLOW_USER_ACCESS)
      .on(WORKFLOW_USER_ACCESS.WID.eq(WORKFLOW.WID))
      .where(orCondition.and(WORKFLOW_USER_ACCESS.UID.eq(user.getUid)))
      .fetch()

    workflowEntries
      .map(workflowRecord => {
        workflowRecord.into(WORKFLOW).getWid().intValue().toString()
      })
      .toList
  }

  /**
    * Returns the current in-session user's workflow list that he/she has access to
    *
    * @return DashboardWorkflowEntry[]
    */
  @GET
  @Path("/list")
//  @Timed(name="/workflow/list")
  def retrieveWorkflowsBySessionUser(
      @Auth sessionUser: SessionUser
  ): List[DashboardWorkflowEntry] = {
    val user = sessionUser.getUser
    val start = System.currentTimeMillis()
    try {
      fetchDashboardWorkflows(user.getUid)
    } finally {
      println("/list took " + (System.currentTimeMillis() - start) + "ms")
    }
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
    if (!WorkflowAccessResource.hasReadAccess(wid, sessionUser.getUser.getUid)) {
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
    // the user must have write access to this workflow
    if (!WorkflowAccessResource.hasWriteAccess(workflow.getWid, user.getUid)) {
      throw new BadRequestException(
        s"no write access to workflow ${workflow.getWid}: ${workflow.getName}"
      )
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
  ): DashboardWorkflowEntry = {
    val uid = sessionUser.getUser.getUid

    // the user must have the privilege to read the workflow to be duplicated
    if (!WorkflowAccessResource.hasReadAccess(wid, uid)) {
      throw new BadRequestException(s"no read access to workflow ${wid}")
    }

    val workflow = workflowDao.fetchOneByWid(wid)
    val newWorkflow =
      new Workflow(workflow.getName + "_copy", null, workflow.getContent, null, null, null)
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
  def createWorkflow(workflow: Workflow, @Auth sessionUser: SessionUser): DashboardWorkflowEntry = {
    val user = sessionUser.getUser
    if (workflow.getWid != null) {
      throw new BadRequestException("Cannot create a new workflow with a provided workflow id.")
    }
    if (workflow.getName == null || workflow.getName.isEmpty) {
      throw new BadRequestException("Workflow should have a name")
    }
    insertWorkflow(workflow, user)
    WorkflowVersionResource.insertVersion(workflow, true)
    fetchOneDashboardWorkflow(workflow.getWid, user.getUid)
      .getOrElse(throw new WebApplicationException(
        s"workflow ${workflow.getWid} ${workflow.getName} not created"))
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
    if (!WorkflowAccessResource.hasWriteAccess(wid, user.getUid)) {
      throw new ForbiddenException("No sufficient access privilege.")
    }
    if (!workflowDao.existsById(wid)) {
      throw new BadRequestException("The workflow does not exist.")
    }
    workflowDao.deleteById(wid)
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
