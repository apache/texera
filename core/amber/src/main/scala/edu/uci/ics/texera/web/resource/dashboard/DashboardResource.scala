package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables._
import edu.uci.ics.texera.web.model.jooq.generated.enums.{UserFileAccessPrivilege, WorkflowUserAccessPrivilege}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos._
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource._
import edu.uci.ics.texera.web.resource.dashboard.FulltextSearchQueryBuilder.buildQueries
import edu.uci.ics.texera.web.resource.dashboard.user.file.UserFileResource.DashboardFile
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource.DashboardWorkflow
import io.dropwizard.auth.Auth

import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.beans.BeanProperty
import org.jooq.{Condition, Record}
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.SelectConditionStep
import org.jooq.types.UInteger

import java.sql.Timestamp
import scala.jdk.CollectionConverters._

object DashboardResource {
  final private lazy val context = SqlServer.createDSLContext()

  case class DashboardClickableFileEntry(resourceType: String, workflow: Option[DashboardWorkflow], project: Option[Project], file: Option[DashboardFile])
  case class DashboardSearchResult(results: List[DashboardClickableFileEntry], more: Boolean)
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/dashboard")
class DashboardResource {

  @GET
  @Path("/search")
  def searchAllResources(@Auth user: SessionUser, @BeanParam searchParams: SearchParams): DashboardSearchResult = {
    val (workflowQuery, projectQuery, fileQuery, sharedWorkflowFileQuery) = buildQueries(user, searchParams)
    val combinedQuery = combineQueriesBasedOnResourceType(context, searchParams.resourceType, workflowQuery, projectQuery, fileQuery, sharedWorkflowFileQuery)
    val orderedQuery = applyOrdering(combinedQuery, searchParams.orderBy)
    val results = executeQuery(orderedQuery, searchParams.start, searchParams.count + 1)

    DashboardSearchResult(
      results = mapResultsToEntries(results, searchParams.count),
      more = results.size > searchParams.count
    )
  }

  private def combineQueriesBasedOnResourceType(
                                                 dsl: DSLContext,
                                                 resourceType: String,
                                                 workflowQuery: Condition,
                                                 projectQuery: Condition,
                                                 fileQuery: Condition,
                                                 sharedWorkflowFileQuery: Condition
                                               ): Condition = resourceType match {
    case "workflow" => workflowQuery
    case "project" => projectQuery
    case "file" => fileQuery.union(sharedWorkflowFileQuery)
    case _ =>
      // Combine all types of queries
      val combinedQuery = workflowQuery
        .unionAll(projectQuery)
        .unionAll(fileQuery)
        .unionAll(sharedWorkflowFileQuery)
      dsl.select().from(combinedQuery.asTable("combined_resources"))
  }



  private def applyOrdering(query: Condition, orderBy: String): Condition = {
    // Apply ordering to the SQL query based on the orderBy parameter
    val sharedWorkflowFileQuery = context
      .select(
        // common attributes: 4 columns
        DSL.inline("file").as("resourceType"),
        FILE.NAME,
        FILE.DESCRIPTION,
        DSL.inline(FILE.UPLOAD_TIME, classOf[Timestamp]).as("creation_time"),
        // workflow attributes: 6 columns
        DSL.inline(null, classOf[UInteger]).as("wid"),
        DSL.inline(FILE.UPLOAD_TIME, classOf[Timestamp]).as("last_modified_time"),
        DSL.inline(null, classOf[WorkflowUserAccessPrivilege]).as("privilege"),
        DSL.inline(null, classOf[UInteger]).as("uid"),
        DSL.inline(null, classOf[String]).as("userName"),
        DSL.inline(null, classOf[String]).as("projects"),
        // project attributes: 3 columns
        DSL.inline(null, classOf[UInteger]).as("pid"),
        DSL.inline(null, classOf[UInteger]).as("owner_id"),
        DSL.inline(null, classOf[String]).as("color"),
        // file attributes 7 columns
        FILE.OWNER_UID,
        FILE.FID,
        FILE.UPLOAD_TIME,
        FILE.PATH,
        FILE.SIZE,
        USER.EMAIL,
        DSL.inline(null, classOf[UserFileAccessPrivilege])
      )
      .from(FILE_OF_WORKFLOW)
      .join(FILE)
      .on(FILE_OF_WORKFLOW.FID.eq(FILE.FID))
      .join(USER)
      .on(FILE.OWNER_UID.eq(USER.UID))
      .join(WORKFLOW_USER_ACCESS)
      .on(FILE_OF_WORKFLOW.WID.eq(WORKFLOW_USER_ACCESS.WID))
      .where("1==1")
  }

  private def executeQuery(query: Condition, start: Int, limit: Int): Seq[Record] = {
    // Execute the query with the specified start (offset) and limit, returning a sequence of records
    ???
  }


  private def mapResultsToEntries(records: Seq[Record], count: Int): List[DashboardClickableFileEntry] = {
    // Map the query results to DashboardClickableFileEntry instances
    ???
  }
}

case class SearchParams(
                         @BeanProperty @QueryParam("query") query: java.util.List[String] = new java.util.ArrayList[String](),
                         @BeanProperty @QueryParam("resourceType") resourceType: String = "",
                         @BeanProperty @QueryParam("createDateStart") createDateStart: String = "",
                         @BeanProperty @QueryParam("createDateEnd") createDateEnd: String = "",
                         @BeanProperty @QueryParam("modifiedDateStart") modifiedDateStart: String = "",
                         @BeanProperty @QueryParam("modifiedDateEnd") modifiedEndDate: String = "",
                         @BeanProperty @QueryParam("owner") owner: java.util.List[String] = new java.util.ArrayList[String](),
                         @BeanProperty @QueryParam("workflowId") workflowId: java.util.List[UInteger] = new java.util.ArrayList[UInteger](),
                         @BeanProperty @QueryParam("operator") operator: java.util.List[String] = new java.util.ArrayList[String](),
                         @BeanProperty @QueryParam("projectId") projectId: java.util.List[UInteger] = new java.util.ArrayList[UInteger](),
                         @BeanProperty @QueryParam("start") start: Int = 0,
                         @BeanProperty @QueryParam("count") count: Int = 20,
                         @BeanProperty @QueryParam("orderBy") orderBy: String = "EditTimeDesc"
                       )

