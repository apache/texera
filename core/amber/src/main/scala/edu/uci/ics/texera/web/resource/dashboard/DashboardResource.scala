package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables._
import edu.uci.ics.texera.web.model.jooq.generated.enums.UserFileAccessPrivilege
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{FileDao, ProjectDao, UserDao, WorkflowDao, WorkflowOfProjectDao, WorkflowOfUserDao, WorkflowUserAccessDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos._
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource._
import edu.uci.ics.texera.web.resource.dashboard.user.file.UserFileResource.DashboardFile
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource.DashboardWorkflow
import io.dropwizard.auth.Auth
import org.jooq.impl.DSL
import org.jooq.impl.DSL._
import org.jooq.impl.Internal.fields

import javax.ws.rs._
import javax.ws.rs.core.MediaType
import org.jooq.{Field, Record, Record1, SelectConditionStep, SelectHavingStep, SelectSeekStepN}
import org.jooq.types.UInteger

import java.sql.Timestamp
import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters._

object DashboardResource {
  final private lazy val context = SqlServer.createDSLContext()
  final private lazy val workflowDao = new WorkflowDao(context.configuration)
  final private lazy val workflowOfProjectDao = new WorkflowOfProjectDao(context.configuration)
  final private lazy val workflowOfUserDao = new WorkflowOfUserDao(context.configuration)
  final private lazy val workflowUserAccessDao = new WorkflowUserAccessDao(context.configuration)
  final private lazy val userDao = new UserDao(context.configuration)
  final private lazy val fileDao = new FileDao(context.configuration)
  final private lazy val projectDao = new ProjectDao(context.configuration)
  case class DashboardClickableFileEntry(
      resourceType: String,
      workflow: Option[DashboardWorkflow],
      project: Option[Project],
      file: Option[DashboardFile]
  )

  val FILE_RESOURCE_TYPE = "file"
  val WORKFLOW_RESOURCE_TYPE = "workflow"
  val PROJECT_RESOURCE_TYPE = "project"
  val ALL_RESOURCE_TYPE = ""

  case class DashboardSearchResult(results: List[DashboardClickableFileEntry], more: Boolean)

  case class SearchQueryParams(
      @QueryParam("query") keywords: java.util.List[String] = new util.ArrayList[String](),
      @QueryParam("resourceType") @DefaultValue("") resourceType: String = ALL_RESOURCE_TYPE,
      @QueryParam("createDateStart") @DefaultValue("") creationStartDate: String = "",
      @QueryParam("createDateEnd") @DefaultValue("") creationEndDate: String = "",
      @QueryParam("modifiedDateStart") @DefaultValue("") modifiedStartDate: String = "",
      @QueryParam("modifiedDateEnd") @DefaultValue("") modifiedEndDate: String = "",
      @QueryParam("owner") owners: java.util.List[String] = new util.ArrayList(),
      @QueryParam("id") workflowIDs: java.util.List[UInteger] = new util.ArrayList(),
      @QueryParam("operator") operators: java.util.List[String] = new util.ArrayList(),
      @QueryParam("projectId") projectIds: java.util.List[UInteger] = new util.ArrayList(),
      @QueryParam("start") @DefaultValue("0") offset: Int = 0,
      @QueryParam("count") @DefaultValue("20") count: Int = 20,
      @QueryParam("orderBy") @DefaultValue("EditTimeDesc") orderBy: String = "EditTimeDesc"
  )

  case class SearchFieldMapping(
      specificResourceType: String,
      baseQuery: SelectConditionStep[Record],
      fieldsForKeywords: List[Field[String]],
      fieldsForCreationDate: List[Field[Timestamp]],
      fieldsForModifiedDate: List[Field[Timestamp]],
      fieldsForOwner: List[Field[String]],
      fieldsForWorkflowIds: List[Field[UInteger]],
      fieldsForOperators: List[Field[String]],
      fieldsForProjectIds: List[Field[UInteger]]
  )

  private def retrieveResultAsDashboardEntry[T](
                                  iterator: Iterator[
                                    Record1[T]
                                  ], // Assuming T is the type of the ID (e.g., UInteger for WID, FID, PID)
                                  resultState: SearchResultState,
                                  toEntry: T => DashboardClickableFileEntry // Conversion function from ID to entry
                                ): Unit = {

    // Skip items based on the remainingOffset
    while (resultState.remainingOffset > 0 && iterator.hasNext) {
      iterator.next()
      resultState.remainingOffset -= 1
    }

    // Check if we have exhausted the iterator while skipping for offset
    if (!iterator.hasNext) {
      resultState.hasMore = false
      return
    }

    // Process items based on the remainingCount
    while (resultState.remainingCount > 0 && iterator.hasNext) {
      resultState.result.append(toEntry(iterator.next().value1()))
      resultState.remainingCount -= 1
    }
    resultState.hasMore = iterator.hasNext
  }

  // Construct query for workflows

  def getFieldMappingsForWorkflowSearch(user: SessionUser): SearchFieldMapping = {
    SearchFieldMapping(
      WORKFLOW_RESOURCE_TYPE,
      queryAccessibleWorkflowsOfUser(user),
      List(WORKFLOW.NAME, WORKFLOW.CONTENT, WORKFLOW.DESCRIPTION),
      List(WORKFLOW.CREATION_TIME),
      List(WORKFLOW.LAST_MODIFIED_TIME),
      List(USER.EMAIL), // accessible from the base query
      List(WORKFLOW.WID),
      List(WORKFLOW.CONTENT),
      List(WORKFLOW_OF_PROJECT.PID) // accessible from the base query
    )
  }

  def getFieldMappingsForFileSearch(user: SessionUser): SearchFieldMapping = {
    SearchFieldMapping(
      FILE_RESOURCE_TYPE,
      queryAccessibleFilesOfUser(user),
      List(FILE.NAME, FILE.DESCRIPTION),
      List(FILE.UPLOAD_TIME),
      List(FILE.UPLOAD_TIME),
      List(USER.EMAIL), // accessible from the base query
      List(),
      List(WORKFLOW.CONTENT),
      List(WORKFLOW_OF_PROJECT.PID) // accessible from the base query
    )
  }

  def getFieldMappingsForProjectSearch(user: SessionUser): SearchFieldMapping = {
    SearchFieldMapping(
      PROJECT_RESOURCE_TYPE,
      queryAccessibleProjectsOfUser(user),
      List(PROJECT.NAME, PROJECT.DESCRIPTION),
      List(PROJECT.CREATION_TIME),
      List(),
      List(USER.EMAIL), // accessible from the base query
      List(),
      List(),
      List(PROJECT.PID)
    )
  }

  private def queryAccessibleWorkflowsOfUser(user: SessionUser): SelectHavingStep[Record] = {
    context
      .selectDistinct(getWorkflowPartSchema():_*, DSL.inline("workflow").as("resourceType"))
      .from(WORKFLOW)
      .leftJoin(WORKFLOW_USER_ACCESS)
      .on(WORKFLOW_USER_ACCESS.WID.eq(WORKFLOW.WID))
      .leftJoin(WORKFLOW_OF_USER)
      .on(WORKFLOW_OF_USER.WID.eq(WORKFLOW.WID))
      .leftJoin(USER)
      .on(USER.UID.eq(WORKFLOW_OF_USER.UID))
      .leftJoin(WORKFLOW_OF_PROJECT)
      .on(WORKFLOW_OF_PROJECT.WID.eq(WORKFLOW.WID))
      .leftJoin(PROJECT_USER_ACCESS)
      .on(PROJECT_USER_ACCESS.PID.eq(WORKFLOW_OF_PROJECT.PID))
      .where(
        WORKFLOW_USER_ACCESS.UID.eq(user.getUid).or(PROJECT_USER_ACCESS.UID.eq(user.getUid))
      )
      .groupBy(WORKFLOW.WID) // for group concat of project IDs.
  }

  private def queryAccessibleFilesOfUser(user: SessionUser): SelectConditionStep[Record] = {
    context
      .selectDistinct(getFilePartSchema():_*, DSL.inline("file").as("resourceType"))
      .from(USER_FILE_ACCESS)
      .join(FILE)
      .on(USER_FILE_ACCESS.FID.eq(FILE.FID))
      .join(USER)
      .on(FILE.OWNER_UID.eq(USER.UID))
      .where(USER_FILE_ACCESS.UID.eq(user.getUid))
  }

  private def queryAccessibleProjectsOfUser(user: SessionUser): SelectConditionStep[Record] = {
    context
      .selectDistinct(getProjectPartSchema():_*, DSL.inline("project").as("resourceType"))
      .from(PROJECT)
      .leftJoin(PROJECT_USER_ACCESS)
      .on(PROJECT_USER_ACCESS.PID.eq(PROJECT.PID))
      .where(PROJECT_USER_ACCESS.UID.eq(user.getUid))
  }

  def getWorkflowPartSchema(): Seq[Field[_]] = {
    Seq(WORKFLOW.fields(): _*,
      USER.UID,
      USER.NAME,
      WORKFLOW_USER_ACCESS.PRIVILEGE,
      groupConcat(WORKFLOW_OF_PROJECT.PID)
        .as("projects")
      )
  }

  def getFilePartSchema(): Seq[Field[_]] = {
    Seq(FILE.fields():_*, USER.UID, USER_FILE_ACCESS.PRIVILEGE)
  }

  def getProjectPartSchema(): Seq[Field[_]] = PROJECT.fields()


  def getUnifiedSchema(): Seq[Field[_]] = {
    Seq(field("resourceType", classOf[String])) ++
      getWorkflowPartSchema() ++
      getFilePartSchema() ++
      getProjectPartSchema()
  }



  private class SearchResultState(
      var remainingCount: Int,
      var remainingOffset: Int,
      var hasMore: Boolean,
      val result: mutable.ArrayBuffer[DashboardClickableFileEntry] = mutable.ArrayBuffer()
  )

  def searchAllResources(
                          @Auth user: SessionUser,
                          @BeanParam params: SearchQueryParams
                        ): DashboardSearchResult = {
    val filesFieldMapping = getFieldMappingsForFileSearch(user)
    val searchQuery =
      FulltextSearchQueryUtils.addSearchConditions(params, filesFieldMapping)
    val filesFieldMapping1 = getFieldMappingsForFileSearch(user)
    val searchQuery2 = {
      FulltextSearchQueryUtils.addSearchConditions(params, filesFieldMapping1)
    }
    val filesFieldMapping2 = getFieldMappingsForProjectSearch(user)
    val searchQuery3 = {
      FulltextSearchQueryUtils.addSearchConditions(params, filesFieldMapping2)
    }
    searchQuery
      .union(searchQuery2)
      .union(searchQuery3)
      .offset(params.offset)
      .limit(params.count)
      .fetch()
  }


  def toWorkflowEntry(uid: UInteger, wid: UInteger): DashboardClickableFileEntry = {
    val workflow = workflowDao.fetchOneByWid(wid)
    val projects = workflowOfProjectDao.fetchByWid(wid)
    // assume we only have one uid per wid.
    val userId = workflowOfUserDao.fetchByWid(wid).asScala.map(_.getUid).head
    // assume we only have one access level per wid.
    val access = workflowUserAccessDao.fetchByWid(wid).asScala.head
    val user = userDao.fetchOneByUid(userId)
    val dashboardWorkflow = DashboardWorkflow(
      userId == uid,
      access.getPrivilege.toString,
      user.getName,
      workflow,
      projects.asScala.map(p => UInteger.valueOf(p.getPid.intValue())).toList
    )
    DashboardClickableFileEntry(WORKFLOW_RESOURCE_TYPE, Some(dashboardWorkflow), None, None)
  }

  def toFileEntry(uid: UInteger, fid: UInteger): DashboardClickableFileEntry = {
    val file = fileDao.fetchOneByFid(fid)
    val user = userDao.fetchOneByUid(uid)
    val access = context
      .selectFrom(USER_FILE_ACCESS)
      .where(USER_FILE_ACCESS.UID.eq(uid).and(USER_FILE_ACCESS.FID.eq(fid)))
      .fetchOne()
    val dashboardFile = DashboardFile(user.getEmail, access.getPrivilege.toString, file)
    DashboardClickableFileEntry(FILE_RESOURCE_TYPE, None, None, Some(dashboardFile))
  }

  def toProjectEntry(pid: UInteger): DashboardClickableFileEntry = {
    DashboardClickableFileEntry(PROJECT_RESOURCE_TYPE, None, Some(projectDao.fetchOneByPid(pid)), None)
  }


}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/dashboard")
class DashboardResource {

  // Refactored searchAllResources method to call specific methods for each resource type
  @GET
  @Path("/search")
  def searchAllResourcesCall(
      @Auth user: SessionUser,
      @BeanParam params: SearchQueryParams
  ): DashboardSearchResult = {
    DashboardResource.searchAllResources(user, params)
  }
}
