package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos._
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource._
import edu.uci.ics.texera.web.resource.dashboard.SearchQueryBuilder.ALL_RESOURCE_TYPE
import edu.uci.ics.texera.web.resource.dashboard.user.file.UserFileResource.DashboardFile
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource.DashboardWorkflow
import io.dropwizard.auth.Auth

import javax.ws.rs._
import javax.ws.rs.core.MediaType
import org.jooq.types.UInteger

import java.util
import scala.jdk.CollectionConverters.CollectionHasAsScala

object DashboardResource {
  case class DashboardClickableFileEntry(
      resourceType: String,
      workflow: Option[DashboardWorkflow] = None,
      project: Option[Project] = None,
      file: Option[DashboardFile] = None
  )

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

  // Construct query for workflows

  def searchAllResources(
      @Auth user: SessionUser,
      @BeanParam params: SearchQueryParams
  ): DashboardSearchResult = {
    val query = params.resourceType match {
      case SearchQueryBuilder.WORKFLOW_RESOURCE_TYPE =>
        WorkflowSearchQueryBuilder.constructQuery(user, params)
      case SearchQueryBuilder.FILE_RESOURCE_TYPE => FileSearchQueryBuilder.constructQuery(user, params)
      case SearchQueryBuilder.PROJECT_RESOURCE_TYPE =>
        ProjectSearchQueryBuilder.constructQuery(user, params)
      case SearchQueryBuilder.ALL_RESOURCE_TYPE =>
        val q1 = WorkflowSearchQueryBuilder.constructQuery(user, params)
        val q2 = FileSearchQueryBuilder.constructQuery(user, params)
        val q3 = ProjectSearchQueryBuilder.constructQuery(user, params)
        q1.unionAll(q2).unionAll(q3)
      case _ => throw new IllegalArgumentException(s"Unknown resource type: ${params.resourceType}")
    }

    val finalQuery = query.offset(params.offset).limit(params.count + 1)
    val queryResult = finalQuery.fetch()

    val entries = queryResult.asScala.toList
      .take(params.count)
      .map(record => {
        val resourceType = record.get("resourceType", classOf[String])
        resourceType match {
          case SearchQueryBuilder.WORKFLOW_RESOURCE_TYPE =>
            WorkflowSearchQueryBuilder.toEntry(user, record)
          case SearchQueryBuilder.FILE_RESOURCE_TYPE => FileSearchQueryBuilder.toEntry(user, record)
          case SearchQueryBuilder.PROJECT_RESOURCE_TYPE =>
            ProjectSearchQueryBuilder.toEntry(user, record)
        }
      })

    DashboardSearchResult(results = entries, more = queryResult.size() > params.count)
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
