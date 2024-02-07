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

import javax.ws.rs._
import javax.ws.rs.core.MediaType
import org.jooq.{Condition, Field, OrderField, Record, Select, SelectConditionStep, SelectLimitStep, SelectOrderByStep}
import org.jooq.types.UInteger

import java.sql.Timestamp
import java.util
import scala.jdk.CollectionConverters._

object DashboardResource {
  final private lazy val context = SqlServer.createDSLContext()
  case class DashboardClickableFileEntry(
      resourceType: String,
      workflow: Option[DashboardWorkflow] = None,
      project: Option[Project] = None,
      file: Option[DashboardFile] = None
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
      fieldsForKeywords: List[Field[String]],
      fieldsForCreationDate: List[Field[Timestamp]],
      fieldsForModifiedDate: List[Field[Timestamp]],
      fieldsForOwner: List[Field[String]],
      fieldsForWorkflowIds: List[Field[UInteger]],
      fieldsForOperators: List[Field[String]],
      fieldsForProjectIds: List[Field[UInteger]]
  )

  // Construct query for workflows

  def getFieldMappingsForWorkflowSearch(user: SessionUser): SearchFieldMapping = {
    SearchFieldMapping(
      WORKFLOW_RESOURCE_TYPE,
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
      List(PROJECT.NAME, PROJECT.DESCRIPTION),
      List(PROJECT.CREATION_TIME),
      List(),
      List(USER.EMAIL), // accessible from the base query
      List(),
      List(),
      List(PROJECT.PID)
    )
  }

  private def queryAccessibleWorkflowsOfUser(user: SessionUser, condition:Condition, orderByFields:List[OrderField[_]]): SelectLimitStep[Record] = {
    val selectedFields = getWorkflowPartSchema ++ Seq(DSL.inline("workflow").as("resourceType"))
    context
      .selectDistinct(getUnifiedSchemaFor(selectedFields):_*)
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
      .and(condition)
      .groupBy(WORKFLOW.WID) // for group concat of project IDs.
      .orderBy(orderByFields:_*)
  }

  private def queryAccessibleFilesOfUser(user: SessionUser, condition: Condition, orderByFields:List[OrderField[_]]): SelectLimitStep[Record] = {
    val selectedFields = getFilePartSchema ++ Seq(DSL.inline("file").as("resourceType"))
    context
      .selectDistinct(getUnifiedSchemaFor(selectedFields):_*)
      .from(USER_FILE_ACCESS)
      .join(FILE)
      .on(USER_FILE_ACCESS.FID.eq(FILE.FID))
      .join(USER)
      .on(FILE.OWNER_UID.eq(USER.UID))
      .where(USER_FILE_ACCESS.UID.eq(user.getUid))
      .and(condition)
      .orderBy(orderByFields:_*)
  }

  private def queryAccessibleProjectsOfUser(user: SessionUser, condition:Condition, orderByFields:List[OrderField[_]]): SelectLimitStep[Record] = {
    val selectedFields = getProjectPartSchema ++ Seq(DSL.inline("project").as("resourceType"))
    context
      .selectDistinct(getUnifiedSchemaFor(selectedFields):_*)
      .from(PROJECT)
      .leftJoin(PROJECT_USER_ACCESS)
      .on(PROJECT_USER_ACCESS.PID.eq(PROJECT.PID))
      .where(PROJECT_USER_ACCESS.UID.eq(user.getUid))
      .and(condition)
      .orderBy(orderByFields:_*)
  }

  val getWorkflowPartSchema: Seq[Field[_]] = {
    WORKFLOW.fields() ++ Seq(
      USER.UID,
      USER.NAME,
      WORKFLOW_USER_ACCESS.PRIVILEGE,
      groupConcat(WORKFLOW_OF_PROJECT.PID)
        .as("projects")
      )
  }

  def getUnifiedSchemaFor(keepFields:Seq[Field[_]]): Seq[Field[_]] = {
    getUnifiedSchema.map(field =>{
      keepFields.find(f => f.getQualifiedName == field.getQualifiedName) match {
        case Some(value) => value
        case None => field // TODO: fix
      }
    })
  }

  val getFilePartSchema: Seq[Field[_]] = {
    FILE.fields() ++ Seq(USER.UID, USER_FILE_ACCESS.PRIVILEGE)
  }

  val getProjectPartSchema: Seq[Field[_]] = PROJECT.fields()


  val getUnifiedSchema: Seq[Field[_]] = {
    (Set(field("resourceType", classOf[String])) ++
      getWorkflowPartSchema ++
      getFilePartSchema ++
      getProjectPartSchema).toSeq
  }


  def searchAllResources(
                          @Auth user: SessionUser,
                          @BeanParam params: SearchQueryParams
                        ): DashboardSearchResult = {

    def buildQuery(resourceType: String): SelectLimitStep[Record] = {
      val fieldMapping = resourceType match {
        case WORKFLOW_RESOURCE_TYPE => getFieldMappingsForWorkflowSearch(user)
        case FILE_RESOURCE_TYPE => getFieldMappingsForFileSearch(user)
        case PROJECT_RESOURCE_TYPE => getFieldMappingsForProjectSearch(user)
      }

      val condition = FulltextSearchQueryUtils.getSearchConditions(params, fieldMapping)
      val orderByFields = FulltextSearchQueryUtils.getOrderFields(fieldMapping, params)

      resourceType match {
        case WORKFLOW_RESOURCE_TYPE => queryAccessibleWorkflowsOfUser(user, condition, orderByFields)
        case FILE_RESOURCE_TYPE => queryAccessibleFilesOfUser(user, condition, orderByFields)
        case PROJECT_RESOURCE_TYPE => queryAccessibleProjectsOfUser(user, condition, orderByFields)
      }
    }

    val query = params.resourceType match {
      case ALL_RESOURCE_TYPE =>
        val query1 = buildQuery(WORKFLOW_RESOURCE_TYPE)
        val query2 = buildQuery(FILE_RESOURCE_TYPE)
        val query3 = buildQuery(PROJECT_RESOURCE_TYPE)
        query1.unionAll(query2).unionAll(query3)
      case _ => buildQuery(params.resourceType)
    }

    val queryResult = query.offset(params.offset).limit(params.count + 1).fetch()

    val entries = queryResult.asScala.toList.map(record => {
      val resourceType = record.get("resourceType", classOf[String])
      resourceType match {
        case WORKFLOW_RESOURCE_TYPE => DashboardClickableFileEntry(resourceType, workflow = Some(toWorkflowEntry(user, record)))
        case FILE_RESOURCE_TYPE => DashboardClickableFileEntry(resourceType, file = Some(toFileEntry(record)))
        case PROJECT_RESOURCE_TYPE => DashboardClickableFileEntry(resourceType, project = Some(record.into(PROJECT).into(classOf[Project])))
      }
    })

    DashboardSearchResult(results = entries, more = queryResult.size() > params.count)
  }


  def toWorkflowEntry(user:SessionUser, record: Record): DashboardWorkflow = {
    DashboardWorkflow(
      record.into(WORKFLOW_OF_USER).getUid.eq(user.getUid),
      record
        .into(WORKFLOW_USER_ACCESS)
        .into(classOf[WorkflowUserAccess])
        .getPrivilege
        .toString,
      record.into(USER).getName,
      record.into(WORKFLOW).into(classOf[Workflow]),
      if (record.get("projects") == null) {
        List[UInteger]()
      } else {
        record
          .get("projects")
          .asInstanceOf[String]
          .split(',')
          .map(number => UInteger.valueOf(number))
          .toList
      }
    )
  }

  def toFileEntry(record: Record): DashboardFile = {
    DashboardFile(
      record.into(USER).getEmail,
      record
        .get(
          USER_FILE_ACCESS.PRIVILEGE,
          classOf[UserFileAccessPrivilege]
        )
        .toString,
      record.into(FILE).into(classOf[File])
    )
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
