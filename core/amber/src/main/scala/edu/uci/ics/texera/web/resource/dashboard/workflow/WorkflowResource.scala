package edu.uci.ics.texera.web.resource.dashboard.workflow

import com.github.nscala_time.time.Imports.LocalDate
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{
  PROJECT,
  USER,
  WORKFLOW,
  WORKFLOW_OF_PROJECT,
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
import org.jooq.Condition
import org.jooq.impl.DSL
import org.jooq.impl.DSL.{groupConcat, noCondition}
import org.jooq.types.UInteger

import javax.ws.rs.DefaultValue
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Optional
import java.util.concurrent.TimeUnit
import javax.annotation.security.RolesAllowed
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
  final private lazy val workflowDao = new WorkflowDao(context.configuration)
  final private lazy val workflowOfUserDao = new WorkflowOfUserDao(
    context.configuration
  )
  final private lazy val workflowUserAccessDao = new WorkflowUserAccessDao(
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
      workflow: Workflow,
      projectIDs: List[UInteger]
  )
}
@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/workflow")
class WorkflowResource {

  /**
    * This method returns all workflow IDs that the user has access to
    *
    * @return WorkflowID[]
    */
  @GET
  @Path("/workflow-ids")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
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
  @RolesAllowed(Array("REGULAR", "ADMIN"))
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
  @RolesAllowed(Array("REGULAR", "ADMIN"))
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
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def retrieveWorkflowsBySessionUser(
      @Auth sessionUser: SessionUser
  ): List[DashboardWorkflowEntry] = {
    val user = sessionUser.getUser
    val workflowEntries = context
      .select(
        WORKFLOW.WID,
        WORKFLOW.NAME,
        WORKFLOW.DESCRIPTION,
        WORKFLOW.CREATION_TIME,
        WORKFLOW.LAST_MODIFIED_TIME,
        WORKFLOW_USER_ACCESS.READ_PRIVILEGE,
        WORKFLOW_USER_ACCESS.WRITE_PRIVILEGE,
        WORKFLOW_OF_USER.UID,
        USER.NAME,
        groupConcat(WORKFLOW_OF_PROJECT.PID).as("projects")
      )
      .from(WORKFLOW)
      .leftJoin(WORKFLOW_USER_ACCESS)
      .on(WORKFLOW_USER_ACCESS.WID.eq(WORKFLOW.WID))
      .leftJoin(WORKFLOW_OF_USER)
      .on(WORKFLOW_OF_USER.WID.eq(WORKFLOW.WID))
      .leftJoin(USER)
      .on(USER.UID.eq(WORKFLOW_OF_USER.UID))
      .leftJoin(WORKFLOW_OF_PROJECT)
      .on(WORKFLOW.WID.eq(WORKFLOW_OF_PROJECT.WID))
      .where(WORKFLOW_USER_ACCESS.UID.eq(user.getUid))
      .groupBy(WORKFLOW.WID, WORKFLOW_OF_USER.UID)
      .fetch()
    workflowEntries
      .map(workflowRecord =>
        DashboardWorkflowEntry(
          workflowRecord.into(WORKFLOW_OF_USER).getUid.eq(user.getUid),
          toAccessLevel(
            workflowRecord.into(WORKFLOW_USER_ACCESS).into(classOf[WorkflowUserAccess])
          ).toString,
          workflowRecord.into(USER).getName,
          workflowRecord.into(WORKFLOW).into(classOf[Workflow]),
          if (workflowRecord.component10() == null) List[UInteger]()
          else
            workflowRecord.component10().split(',').map(number => UInteger.valueOf(number)).toList
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
  @RolesAllowed(Array("REGULAR", "ADMIN"))
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
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Path("/persist")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
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
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Path("/duplicate")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
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
        new Workflow(
          workflow.getName + "_copy",
          workflow.getDescription,
          null,
          workflow.getContent,
          null,
          null
        ),
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
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/create")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
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
        workflowDao.fetchOneByWid(workflow.getWid),
        List[UInteger]()
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
  @RolesAllowed(Array("REGULAR", "ADMIN"))
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
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/update/name")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def updateWorkflowName(
      workflow: Workflow,
      @Auth sessionUser: SessionUser
  ): Unit = {
    val wid = workflow.getWid
    val name = workflow.getName
    val user = sessionUser.getUser
    if (!WorkflowAccessResource.hasWriteAccess(wid, user.getUid)) {
      throw new ForbiddenException("No sufficient access privilege.")
    } else if (!workflowOfUserExists(wid, user.getUid)) {
      throw new BadRequestException("The workflow does not exist.")
    } else {
      val userWorkflow = workflowDao.fetchOneByWid(wid)
      userWorkflow.setName(name)
      workflowDao.update(userWorkflow)
    }
  }

  /**
    * This method updates the description of a given workflow
    *
    * @return Response
    */
  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/update/description")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def updateWorkflowDescription(
      workflow: Workflow,
      @Auth sessionUser: SessionUser
  ): Unit = {
    val wid = workflow.getWid
    val description = workflow.getDescription
    val user = sessionUser.getUser
    if (!WorkflowAccessResource.hasWriteAccess(wid, user.getUid)) {
      throw new ForbiddenException("No sufficient access privilege.")
    } else {
      val userWorkflow = workflowDao.fetchOneByWid(wid)
      userWorkflow.setDescription(description)
      workflowDao.update(userWorkflow)
    }
  }

  /**
    * This method performs a full-text search in the content column of the
    * workflow table for workflows that match the specified keywords.
    *
    * This method utilizes MySQL Boolean Full-Text Searches
    * reference: https://dev.mysql.com/doc/refman/8.0/en/fulltext-boolean.html
    * @param sessionUser The authenticated user.
    * @param keywords    The search keywords.
    * @return A list of workflows that match the search term.
    */
  @GET
  @Path("/search")
  def searchWorkflows(
      @Auth sessionUser: SessionUser,
      @QueryParam("query") keywords: java.util.List[String],
//      @QueryParam("page") page: Int = 1,
//      @QueryParam("pageSize") pageSize: Int = 20,
      @QueryParam("createDateStart") @DefaultValue("") creationStartDate: String = "",
      @QueryParam("createDateEnd") @DefaultValue("") creationEndDate: String = "",
      @QueryParam("modifiedDateStart") @DefaultValue("") modifiedStartDate: String = "",
      @QueryParam("modifiedDateEnd") @DefaultValue("") modifiedEndDate: String = "",
      @QueryParam("owner") owners: java.util.List[String] = new java.util.ArrayList[String](),
      @QueryParam("id") workflowIDs: java.util.List[UInteger] = new java.util.ArrayList[UInteger](),
      @QueryParam("operator") operators: java.util.List[String] =
        new java.util.ArrayList[String](),
      @QueryParam("projectId") projectIds: java.util.List[UInteger] =
        new java.util.ArrayList[UInteger]()
  ): List[DashboardWorkflowEntry] = {
    val user = sessionUser.getUser
//    if (keywords.size() == 0) {
//      return List.empty[DashboardWorkflowEntry]
//    }
    // make sure keywords don't contain "+-()<>~*\"", these are reserved for SQL full-text boolean operator
    val splitKeywords = keywords.flatMap(word => word.split("[+\\-()<>~*@\"]+"))
    var matchQuery: Condition = noCondition()
    for (key: String <- splitKeywords) {
      if (key != "") {
        val words = key.split("\\s+")

        def getSearchQuery(subStringSearchEnabled: Boolean): String =
          "(MATCH(texera_db.workflow.name, texera_db.workflow.description, texera_db.workflow.content) AGAINST(+{0}" +
            (if (subStringSearchEnabled) "'*'" else "") + " IN BOOLEAN mode) OR " +
            "MATCH(texera_db.user.name) AGAINST (+{0}" +
            (if (subStringSearchEnabled) "'*'" else "") + " IN BOOLEAN mode) " +
            "OR MATCH(texera_db.project.name, texera_db.project.description) AGAINST (+{0}" +
            (if (subStringSearchEnabled) "'*'" else "") + " IN BOOLEAN mode))"

        if (words.length == 1) {
          // Use "*" to enable sub-string search.
          matchQuery = matchQuery.and(getSearchQuery(true), key)
        } else {
          // When the search query contains multiple words, sub-string search is not supported by MySQL.
          matchQuery = matchQuery.and(getSearchQuery(false), '"' + key + '"')
        }
      }
    }

    // Apply creation_time date filter
    val creationDateFilter = getCreationDateFilter(creationStartDate, creationEndDate)
    // Apply lastModified_time date filter
    val modifiedDateFilter = getModifiedDateFilter(modifiedStartDate, modifiedEndDate)
    // Apply workflowID filter
    val workflowIdFilter = getWorkflowIdFilter(workflowIDs)
    // Apply owner filter
    val ownerFilter = getOwnerFilter(owners)
    // Apply operators filter
    val operatorsFilter = getOperatorsFilter(operators)
    // Apply projectId filter
    val projectIdFilter = getProjectFilter(projectIds)

    // combine all filters with AND
    var optionalFilters: Condition = noCondition()
    optionalFilters = optionalFilters
      .and(creationDateFilter)
      .and(modifiedDateFilter)
      .and(ownerFilter)
      .and(workflowIdFilter)
      .and(operatorsFilter)
      .and(projectIdFilter)

    // When input contains only reserved keywords like "+-()<>~*\""
    // the api should return empty list
//    if (matchQuery == DSL.noCondition()) {
//      return List.empty[DashboardWorkflowEntry]
//    }
    try {
      // Add offset calculation for pagination
//      val offset = (page - 1) * pageSize

      val workflowEntries = context
        .select(
          WORKFLOW.WID,
          WORKFLOW.NAME,
          WORKFLOW.DESCRIPTION,
          WORKFLOW.CREATION_TIME,
          WORKFLOW.LAST_MODIFIED_TIME,
          WORKFLOW_USER_ACCESS.READ_PRIVILEGE,
          WORKFLOW_USER_ACCESS.WRITE_PRIVILEGE,
          WORKFLOW_OF_USER.UID,
          USER.NAME,
          groupConcat(PROJECT.PID).as("projects")
        )
        .from(WORKFLOW)
        .leftJoin(WORKFLOW_USER_ACCESS)
        .on(WORKFLOW_USER_ACCESS.WID.eq(WORKFLOW.WID))
        .leftJoin(WORKFLOW_OF_USER)
        .on(WORKFLOW_OF_USER.WID.eq(WORKFLOW.WID))
        .join(USER)
        .on(USER.UID.eq(WORKFLOW_OF_USER.UID))
        .leftJoin(WORKFLOW_OF_PROJECT)
        .on(WORKFLOW.WID.eq(WORKFLOW_OF_PROJECT.WID))
        .leftJoin(PROJECT)
        .on(PROJECT.PID.eq(WORKFLOW_OF_PROJECT.PID))
        .where(matchQuery.and(optionalFilters))
        .and(WORKFLOW_USER_ACCESS.UID.eq(user.getUid))
        .groupBy(
          WORKFLOW.WID,
          WORKFLOW.NAME,
          WORKFLOW.DESCRIPTION,
          WORKFLOW.CREATION_TIME,
          WORKFLOW.LAST_MODIFIED_TIME,
          WORKFLOW_USER_ACCESS.READ_PRIVILEGE,
          WORKFLOW_USER_ACCESS.WRITE_PRIVILEGE,
          WORKFLOW_OF_USER.UID,
          USER.NAME
        )
//        .limit(pageSize)
//        .offset(offset)
        .fetch()

      workflowEntries
        .map(workflowRecord =>
          DashboardWorkflowEntry(
            workflowRecord.into(WORKFLOW_OF_USER).getUid.eq(user.getUid),
            toAccessLevel(
              workflowRecord.into(WORKFLOW_USER_ACCESS).into(classOf[WorkflowUserAccess])
            ).toString,
            workflowRecord.into(USER).getName,
            workflowRecord.into(WORKFLOW).into(classOf[Workflow]),
            if (workflowRecord.component10() == null) List[UInteger]()
            else
              workflowRecord.component10().split(',').map(number => UInteger.valueOf(number)).toList
          )
        )
        .toList

    } catch {
      case e: Exception =>
        println(
          "Exception: Fulltext index is missing, have you run the script at core/scripts/sql/update/fulltext_indexes.sql?"
        )
        // return a empty list
        List[DashboardWorkflowEntry]()
    }
  }

  /**
    * Helper function to retrieve the owner filter.
    * Applies a filter based on the specified owner names.
    *
    * @param owners The list of owner names to filter by.
    * @return The owner filter.
    */
  def getOwnerFilter(owners: java.util.List[String]): Condition = {
    var ownerFilter: Condition = noCondition()
    if (owners != null && !owners.isEmpty) {
      for (owner <- owners) {
        ownerFilter = ownerFilter.or(USER.NAME.eq(owner))
      }
    }
    ownerFilter
  }

  /**
    * Helper function to retrieve the project filter.
    * Applies a filter based on the specified project IDs.
    *
    * @param projectIds The list of owner names to filter by.
    * @return The projectId filter.
    */
  def getProjectFilter(projectIds: java.util.List[UInteger]): Condition = {
    var projectIdFilter: Condition = noCondition()
    if (projectIds.nonEmpty) {
      for (projectId <- projectIds) {
        projectIdFilter = projectIdFilter.or(WORKFLOW_OF_PROJECT.PID.eq(projectId))
      }
    }
    projectIdFilter
  }

  /**
    * Helper function to retrieve the workflowID filter.
    * Applies a filter based on the specified workflow IDs.
    * @param workflowIDs The list of workflow IDs to filter by.
    * @return The workflowID filter.
    */
  def getWorkflowIdFilter(workflowIDs: java.util.List[UInteger]): Condition = {
    var workflowIdFilter: Condition = noCondition()
    if (workflowIDs != null && !workflowIDs.isEmpty) {
      for (workflowID <- workflowIDs) {
        workflowIdFilter = workflowIdFilter.or(WORKFLOW.WID.eq(workflowID))
      }
    }
    workflowIdFilter
  }

  /**
    * Helper function to retrieve the creation date filter.
    * Applies a filter based on the specified creation start and end dates.
    *
    * @param creationStartDate The start date to filter by.
    * @param creationEndDate   The end date to filter by.
    * @return The creation date filter.
    */
  def getCreationDateFilter(
      creationStartDate: String,
      creationEndDate: String
  ): Condition = {
    var creationDateFilter: Condition = noCondition()
    if (creationStartDate.nonEmpty || creationEndDate.nonEmpty) {
      val start = if (creationStartDate.nonEmpty) creationStartDate else "1970-01-01"
      val end = if (creationEndDate.nonEmpty) creationEndDate else "9999-12-31"
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val startTimestamp = new Timestamp(dateFormat.parse(start).getTime)
      val endTimestamp = new Timestamp(
        dateFormat.parse(end).getTime + TimeUnit.DAYS.toMillis(1) - 1
      )
      creationDateFilter = WORKFLOW.CREATION_TIME.between(startTimestamp, endTimestamp)
    }
    creationDateFilter
  }

  /**
    * Helper function to retrieve the modified date filter.
    * Applies a filter based on the specified modified start and end dates.
    * @param modifiedStartDate The modified start date to filter by.
    * @param modifiedEndDate   The modified end date to filter by.
    * @return The modified date filter.
    */
  def getModifiedDateFilter(modifiedStartDate: String, modifiedEndDate: String): Condition = {
    var modifiedDateFilter: Condition = noCondition()
    if (modifiedStartDate.nonEmpty || modifiedEndDate.nonEmpty) {
      val start = if (modifiedStartDate.nonEmpty) modifiedStartDate else "1970-01-01"
      val end = if (modifiedEndDate.nonEmpty) modifiedEndDate else "9999-12-31"
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val startTimestamp = new Timestamp(dateFormat.parse(start).getTime)
      val endTimestamp = new Timestamp(
        dateFormat.parse(end).getTime + TimeUnit.DAYS.toMillis(1) - 1
      )
      modifiedDateFilter = WORKFLOW.LAST_MODIFIED_TIME.between(startTimestamp, endTimestamp)
    }
    modifiedDateFilter
  }

  /**
    * Helper function to retrieve the operators filter.
    * Applies a filter based on the specified operators.
    *
    * @param operators The list of operators to filter by.
    * @return The operators filter.
    */
  def getOperatorsFilter(operators: java.util.List[String]): Condition = {
    var operatorsFilter: Condition = noCondition()
    if (operators.nonEmpty) {
      for (operator <- operators) {
        val quotes = "\""
        val operatorArray =
          operator.replaceAllLiterally(" ", "").stripPrefix("[").stripSuffix("]").split(',')
        for (i <- operatorArray.indices) {
          val operatorName = operatorArray(i)
          operatorsFilter = operatorsFilter.or(
            WORKFLOW.CONTENT
              .likeIgnoreCase(
                "%" + quotes + "operatorType" + quotes + ":" + quotes + s"$operatorName" + quotes + "%"
                //gives error when I try to combine escape character with formatted string
                //may be due to old scala version bug
              )
          )
        }
      }
    }
    operatorsFilter
  }

}
