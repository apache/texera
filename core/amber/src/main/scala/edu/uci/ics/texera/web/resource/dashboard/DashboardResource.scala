package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables._
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos._
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource._
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource._
import io.dropwizard.auth.Auth
import org.jooq.Condition
import org.jooq.impl.{DSL, SQLDataType}
import org.jooq.impl.DSL.{groupConcat, noCondition}
import org.jooq.types.UInteger

import java.sql.Timestamp
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
  * This file handles various requests that need to interact with multiple tables.
  */
object DashboardResource {
  final private lazy val context = SqlServer.createDSLContext()
  // Add any additional DAOs or objects you need

  case class DashboardClickableFileEntry(
      resourceType: String, // workflow, project, or file
      id: UInteger, // wid for workflow, pid for project, fid for file
      name: String, // name for the ClickableFile
      description: String, // description for the ClickableFile
      creation_time: Timestamp, //creation_time for workflow and project, upload_time for file
      file_path: String, // path of file, set to NULL for workflow and project
      file_size: UInteger // size of file, set to 0 for workflow and project
  )
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/dashboard")
class DashboardResource {

  /**
    * This method performs a full-text search in all resources(workflow, project, file)
    * that match the specified keywords.
    *
    * This method utilizes MySQL Boolean Full-Text Searches
    * reference: https://dev.mysql.com/doc/refman/8.0/en/fulltext-boolean.html
    *
    * @param sessionUser The authenticated user.
    * @param keywords    The search keywords.
    * @return A list of DashboardClickableFileEntry that match the search term.
    */
  @GET
  @Path("/search")
  def searchAllResources(
      @Auth sessionUser: SessionUser,
      @QueryParam("query") keywords: java.util.List[String],
      @QueryParam("resourceType") @DefaultValue("") resourceType: String = ""
  ): List[DashboardClickableFileEntry] = {
    val user = sessionUser.getUser

    // make sure keywords don't contain "+-()<>~*\"", these are reserved for SQL full-text boolean operator
    val splitKeywords = keywords.flatMap(word => word.split("[+\\-()<>~*@\"]+"))
    var workflowMatchQuery: Condition = noCondition()
    var projectMatchQuery: Condition = noCondition()
    var fileMatchQuery: Condition = noCondition()
    for (key: String <- splitKeywords) {
      if (key != "") {
        val words = key.split("\\s+")

        def getSearchQuery(subStringSearchEnabled: Boolean, resourceType: String): String = {
          resourceType match {
            case "workflow" =>
              "MATCH(texera_db.workflow.name, texera_db.workflow.description, texera_db.workflow.content) AGAINST(+{0}" +
                (if (subStringSearchEnabled) "'*'" else "") + " IN BOOLEAN mode)"
            case "project" =>
              "MATCH(texera_db.project.name, texera_db.project.description) AGAINST (+{0}" +
                (if (subStringSearchEnabled) "'*'" else "") + " IN BOOLEAN mode)"
            case "file" =>
              "MATCH(texera_db.file.name, texera_db.file.description) AGAINST (+{0}" +
                (if (subStringSearchEnabled) "'*'" else "") + " IN BOOLEAN mode) "
          }
        }

        if (words.length == 1) {
          // Use "*" to enable sub-string search.
          workflowMatchQuery = workflowMatchQuery.and(
            getSearchQuery(true, "workflow"),
            key
          )
          projectMatchQuery = projectMatchQuery.and(
            getSearchQuery(true, "project"),
            key
          )
          fileMatchQuery = fileMatchQuery.and(
            getSearchQuery(true, "file"),
            key
          )
        } else {
          // When the search query contains multiple words, sub-string search is not supported by MySQL.
          workflowMatchQuery = workflowMatchQuery.and(
            getSearchQuery(false, "workflow"),
            key
          )
          projectMatchQuery = projectMatchQuery.and(
            getSearchQuery(false, "project"),
            key
          )
          fileMatchQuery = fileMatchQuery.and(
            getSearchQuery(false, "file"),
            key
          )
        }
      }
    }

    // Retrieve workflow resource
    val workflowQuery =
      context
        .select(
          DSL.inline("workflow").as("resourceType"),
          WORKFLOW.WID,
          WORKFLOW.NAME,
          WORKFLOW.DESCRIPTION,
          WORKFLOW.CREATION_TIME,
          DSL.inline("null").as("file_path"),
          DSL.inline(UInteger.valueOf(0)).as("file_size")
        )
        .from(WORKFLOW)
        .leftJoin(WORKFLOW_USER_ACCESS)
        .on(WORKFLOW_USER_ACCESS.WID.eq(WORKFLOW.WID))
        .where(WORKFLOW_USER_ACCESS.UID.eq(user.getUid))
        .and(
          workflowMatchQuery
        )

    // Retrieve project resource
    val projectQuery = context
      .select(
        DSL.inline("project").as("resourceType"),
        PROJECT.PID,
        PROJECT.NAME,
        PROJECT.DESCRIPTION,
        PROJECT.CREATION_TIME,
        DSL.inline("null").as("file_path"),
        DSL.inline(UInteger.valueOf(0)).as("file_size")
      )
      .from(PROJECT)
      .where(PROJECT.OWNER_ID.eq(user.getUid()))
      .and(
        projectMatchQuery
      )

    // Retrieve file resource
    val fileQuery = context
      .select(
        DSL.inline("file").as("resourceType"),
        FILE.FID,
        FILE.NAME,
        FILE.DESCRIPTION,
        FILE.UPLOAD_TIME.as("creation_time"),
        FILE.PATH.as("file_path"),
        FILE.SIZE.as("file_size")
      )
      .from(USER_FILE_ACCESS)
      .join(FILE)
      .on(USER_FILE_ACCESS.FID.eq(FILE.FID))
      .join(USER)
      .on(FILE.OWNER_UID.eq(USER.UID))
      .where(USER_FILE_ACCESS.UID.eq(user.getUid()))
      .and(
        fileMatchQuery
      )
    // Retrieve files to which all shared workflows have access
    val sharedWorkflowFileQuery = context
      .select(
        DSL.inline("file").as("resourceType"),
        FILE.FID,
        FILE.NAME,
        FILE.DESCRIPTION,
        FILE.UPLOAD_TIME.as("creation_time"),
        FILE.PATH.as("file_path"),
        FILE.SIZE.as("file_size")
      )
      .from(FILE_OF_WORKFLOW)
      .join(FILE)
      .on(FILE_OF_WORKFLOW.FID.eq(FILE.FID))
      .join(USER)
      .on(FILE.OWNER_UID.eq(USER.UID))
      .join(WORKFLOW_USER_ACCESS)
      .on(FILE_OF_WORKFLOW.WID.eq(WORKFLOW_USER_ACCESS.WID))
      .where(WORKFLOW_USER_ACCESS.UID.eq(user.getUid()))
      .and(
        fileMatchQuery
      )

    // Combine all queries using union and fetch results
    val clickableFileEntry =
      resourceType match {
        case "workflow" => workflowQuery.fetch()
        case "project"  => projectQuery.fetch()
        case "file"     => fileQuery.union(sharedWorkflowFileQuery).fetch()
        case "" =>
          workflowQuery
            .union(projectQuery)
            .union(fileQuery)
            .union(sharedWorkflowFileQuery)
            .fetch()
        case _ =>
          throw new BadRequestException(
            "Unknown resourceType. Only 'workflow', 'project', and 'file' are allowed"
          )
      }

    clickableFileEntry
      .map(record =>
        DashboardClickableFileEntry(
          record.value1(),
          record.value2(),
          record.value3(),
          record.value4(),
          record.value5(),
          record.value6(),
          record.value7()
        )
      )
      .toList

  }

}
