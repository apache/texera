package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos._
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource._
import edu.uci.ics.texera.web.resource.dashboard.SearchQueryBuilder.ALL_RESOURCE_TYPE
import edu.uci.ics.texera.web.resource.dashboard.user.file.UserFileResource.DashboardFile
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource.DashboardWorkflow
import io.dropwizard.auth.Auth
import org.jooq.{Field, OrderField}

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
    val uid = user.getUid
    val query = params.resourceType match {
      case SearchQueryBuilder.WORKFLOW_RESOURCE_TYPE =>
        WorkflowSearchQueryBuilder.constructQuery(uid, params)
      case SearchQueryBuilder.FILE_RESOURCE_TYPE =>
        FileSearchQueryBuilder.constructQuery(uid, params)
      case SearchQueryBuilder.PROJECT_RESOURCE_TYPE =>
        ProjectSearchQueryBuilder.constructQuery(uid, params)
      case SearchQueryBuilder.ALL_RESOURCE_TYPE =>
        val q1 = WorkflowSearchQueryBuilder.constructQuery(uid, params)
        val q2 = FileSearchQueryBuilder.constructQuery(uid, params)
        val q3 = ProjectSearchQueryBuilder.constructQuery(uid, params)
        q1.unionAll(q2).unionAll(q3)
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
          case SearchQueryBuilder.FILE_RESOURCE_TYPE =>
            FileSearchQueryBuilder.toEntry(uid, record)
          case SearchQueryBuilder.PROJECT_RESOURCE_TYPE =>
            ProjectSearchQueryBuilder.toEntry(uid, record)
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
