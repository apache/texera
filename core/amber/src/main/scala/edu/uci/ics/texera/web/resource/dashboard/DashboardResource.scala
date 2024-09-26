package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables._
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos._
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource._
import edu.uci.ics.texera.web.resource.dashboard.SearchQueryBuilder.{ALL_RESOURCE_TYPE, context}
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetResource.DashboardDataset
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource.DashboardWorkflow
import io.dropwizard.auth.Auth
import org.jooq.{Field, OrderField}

import javax.ws.rs._
import javax.ws.rs.core.MediaType
import org.jooq.types.UInteger

import java.util
import scala.jdk.CollectionConverters._
object DashboardResource {
  case class DashboardClickableFileEntry(
      resourceType: String,
      workflow: Option[DashboardWorkflow] = None,
      project: Option[Project] = None,
      dataset: Option[DashboardDataset] = None
  )

  case class UserInfo(userId: UInteger, userName: String, googleAvatar: Option[String])

  case class DashboardSearchResult(results: List[DashboardClickableFileEntry], more: Boolean)

  /*
   The following class describe the available params from the frontend for full text search.
   * @param user       The authenticated user performing the search.
   * @param keywords          A list of search keywords. The API will return resources that match any of these keywords.
   * @param resourceType      The type of the resources to include in the search results. Acceptable values are "workflow", "project", "file" and "" (for all types).
   * @param creationStartDate The start of the date range for the creation time filter. It should be provided in 'yyyy-MM-dd' format.
   * @param creationEndDate   The end of the date range for the creation time filter. It should be provided in 'yyyy-MM-dd' format.
   * @param modifiedStartDate The start of the date range for the modification time filter. It should be provided in 'yyyy-MM-dd' format.
   * @param modifiedEndDate   The end of the date range for the modification time filter. It should be provided in 'yyyy-MM-dd' format.
   * @param owners            A list of owner names to include in the search results.
   * @param workflowIDs       A list of workflow IDs to include in the search results.
   * @param operators         A list of operators to include in the search results.
   * @param projectIds        A list of project IDs to include in the search results.
   * @param offset            The number of initial results to skip. This is useful for implementing pagination.
   * @param count             The maximum number of results to return.
   * @param orderBy           The order in which to sort the results. Acceptable values are 'NameAsc', 'NameDesc', 'CreateTimeDesc', and 'EditTimeDesc'.
   */
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
      @QueryParam("datasetId") datasetIds: java.util.List[UInteger] = new util.ArrayList(),
      @QueryParam("start") @DefaultValue("0") offset: Int = 0,
      @QueryParam("count") @DefaultValue("20") count: Int = 20,
      @QueryParam("orderBy") @DefaultValue("EditTimeDesc") orderBy: String = "EditTimeDesc"
  )

  // Construct query for workflows

  def searchAllResources(
      @Auth user: SessionUser,
      @BeanParam params: SearchQueryParams,
      includePublic: Boolean = false
  ): DashboardSearchResult = {
    val uid = user.getUid
    val query = params.resourceType match {
      case SearchQueryBuilder.WORKFLOW_RESOURCE_TYPE =>
        WorkflowSearchQueryBuilder.constructQuery(uid, params, includePublic)
      case SearchQueryBuilder.PROJECT_RESOURCE_TYPE =>
        ProjectSearchQueryBuilder.constructQuery(uid, params, includePublic)
      case SearchQueryBuilder.DATASET_RESOURCE_TYPE =>
        DatasetSearchQueryBuilder.constructQuery(uid, params, includePublic)
      case SearchQueryBuilder.ALL_RESOURCE_TYPE =>
        val q1 = WorkflowSearchQueryBuilder.constructQuery(uid, params, includePublic)
        val q3 = ProjectSearchQueryBuilder.constructQuery(uid, params, includePublic)
        val q4 = DatasetSearchQueryBuilder.constructQuery(uid, params, includePublic)
        q1.unionAll(q3).unionAll(q4)
      case _ => throw new IllegalArgumentException(s"Unknown resource type: ${params.resourceType}")
    }

    val finalQuery =
      query.orderBy(getOrderFields(params): _*).offset(params.offset).limit(params.count + 1)
    val queryResult = finalQuery.fetch()

    val entries = queryResult.asScala.toList
      .take(params.count)
      .map(record => {
        val resourceType = record.get("resourceType", classOf[String])
        resourceType match {
          case SearchQueryBuilder.WORKFLOW_RESOURCE_TYPE =>
            WorkflowSearchQueryBuilder.toEntry(uid, record)
          case SearchQueryBuilder.PROJECT_RESOURCE_TYPE =>
            ProjectSearchQueryBuilder.toEntry(uid, record)
          case SearchQueryBuilder.DATASET_RESOURCE_TYPE =>
            DatasetSearchQueryBuilder.toEntry(uid, record)
        }
      })

    DashboardSearchResult(results = entries, more = queryResult.size() > params.count)
  }

  def getOrderFields(
      searchQueryParams: SearchQueryParams
  ): List[OrderField[_]] = {
    // Regex pattern to extract column name and order direction
    val pattern = "(Name|CreateTime|EditTime)(Asc|Desc)".r

    searchQueryParams.orderBy match {
      case pattern(column, order) =>
        val field = getColumnField(column)
        field match {
          case Some(value) =>
            List(order match {
              case "Asc"  => value.asc()
              case "Desc" => value.desc()
            })
          case None => List()
        }
      case _ => List() // Default case if the orderBy string doesn't match the pattern
    }
  }

  // Helper method to map column names to actual database fields based on resource type
  private def getColumnField(columnName: String): Option[Field[_]] = {
    Option(columnName match {
      case "Name"       => UnifiedResourceSchema.resourceNameField
      case "CreateTime" => UnifiedResourceSchema.resourceCreationTimeField
      case "EditTime"   => UnifiedResourceSchema.resourceLastModifiedTimeField
      case _            => null // Default case for unmatched resource types or column names
    })
  }

}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/dashboard")
class DashboardResource {

  /**
    * This method performs a full-text search across all resources - workflows, projects, and files -
    * that match the specified keywords.
    * It supports advanced filters such as resource type, creation and modification dates, owner,
    * workflow IDs, operators, project IDs and allows to specify the number of results and their ordering.
    *
    * This method utilizes MySQL Boolean Full-Text Searches
    * reference: https://dev.mysql.com/doc/refman/8.0/en/fulltext-boolean.html
    *
    * @return A DashboardSearchResult object containing a list of DashboardClickableFileEntry objects that match the search criteria, and a boolean indicating whether more results are available.
    */
  @GET
  @Path("/search")
  def searchAllResourcesCall(
      @Auth user: SessionUser,
      @BeanParam params: SearchQueryParams,
      @QueryParam("includePublic") includePublic: Boolean = false
  ): DashboardSearchResult = {
    DashboardResource.searchAllResources(user, params, includePublic = includePublic)
  }

  @GET
  @Path("/publicSearch")
  def searchAllPublicResourceCall(
      @BeanParam params: SearchQueryParams,
      @QueryParam("includePublic ") includePublic: Boolean = true
  ): DashboardSearchResult = {
    DashboardResource.searchAllResources(
      new SessionUser(new User()),
      params,
      includePublic = includePublic
    )
  }

  @GET
  @Path("/resultsOwnersInfo")
  def resultsOwnersInfo(
      @QueryParam("userIds") userIds: util.List[UInteger]
  ): util.Map[UInteger, UserInfo] = {
    val scalaUserIds: Set[UInteger] = userIds.asScala.toSet

    val records = context
      .select(USER.UID, USER.NAME, USER.GOOGLE_AVATAR)
      .from(USER)
      .where(USER.UID.in(scalaUserIds.asJava))
      .fetch()

    val userIdToInfoMap = records.asScala
      .map { record =>
        val userId = record.get(USER.UID)
        val userName = record.get(USER.NAME)
        val googleAvatar = Option(record.get(USER.GOOGLE_AVATAR))
        userId -> UserInfo(userId, userName, googleAvatar)
      }
      .toMap
      .asJava

    userIdToInfoMap
  }

  @GET
  @Path("/workflowUserAccess")
  def workflowUserAccess(
      @QueryParam("wid") wid: UInteger
  ): util.List[UInteger] = {
    val records = context
      .select(WORKFLOW_USER_ACCESS.UID)
      .from(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(wid))
      .fetch()

    records.getValues(WORKFLOW_USER_ACCESS.UID)
  }
}
