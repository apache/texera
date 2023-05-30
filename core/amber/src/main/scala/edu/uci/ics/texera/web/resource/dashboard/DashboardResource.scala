package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{USER, _}
import edu.uci.ics.texera.web.model.jooq.generated.enums.{
  UserFileAccessPrivilege,
  WorkflowUserAccessPrivilege
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos._
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource._
import edu.uci.ics.texera.web.resource.dashboard.user.file.UserFileResource.DashboardFileEntry
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

  case class DashboardClickableFileEntry(
      // common attributes: 4 columns
      resourceType: String,
//      name: String,
//      description: String,
//      creationTime: Timestamp,
//      // workflow attributes: 5 columns
//      wid: UInteger,
//      lastModifiedTime: Timestamp,
//      privilege: WorkflowUserAccessPrivilege, //
//      uid: UInteger,
//      userName: String, //
//      // project attributes: 3 columns
//      pid: UInteger,
//      owner_id: UInteger,
//      color: String,
//      // file attributes 7 columns
//      owner_uid: UInteger,
//      fid: UInteger,
//      uploadTime: Timestamp,
//      path: String,
//      size: UInteger,
//      email: String,
//      userFileAccessPrivilege: UserFileAccessPrivilege,
      workflow: DashboardWorkflowEntry,
      project: Project,
      file: DashboardFileEntry
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
          //common attributes: 4 columns
          DSL.inline("workflow").as("resourceType"),
          WORKFLOW.NAME,
          WORKFLOW.DESCRIPTION,
          WORKFLOW.CREATION_TIME,
          // workflow attributes: 5 columns
          WORKFLOW.WID,
          WORKFLOW.LAST_MODIFIED_TIME,
          WORKFLOW_USER_ACCESS.PRIVILEGE,
          WORKFLOW_OF_USER.UID,
          USER.NAME.as("userName"),
          // project attributes: 3 columns
          DSL.inline(null, classOf[UInteger]).as("pid"),
          DSL.inline(null, classOf[UInteger]).as("owner_id"),
          DSL.inline(null, classOf[String]).as("color"),
          // file attributes 6 columns
          DSL.inline(null, classOf[UInteger]).as("owner_uid"),
          DSL.inline(null, classOf[UInteger]).as("fid"),
          DSL.inline(null, classOf[Timestamp]).as("upload_time"),
          DSL.inline(null, classOf[String]).as("path"),
          DSL.inline(null, classOf[UInteger]).as("size"),
          DSL.inline(null, classOf[String]).as("email"),
          DSL.inline(null, classOf[UserFileAccessPrivilege]).as("user_file_access")
        )
        .from(WORKFLOW)
        .leftJoin(WORKFLOW_USER_ACCESS)
        .on(WORKFLOW_USER_ACCESS.WID.eq(WORKFLOW.WID))
        .leftJoin(WORKFLOW_OF_USER)
        .on(WORKFLOW_OF_USER.WID.eq(WORKFLOW.WID))
        .leftJoin(USER)
        .on(USER.UID.eq(WORKFLOW_OF_USER.UID))
        .leftJoin(WORKFLOW_OF_PROJECT)
        .on(WORKFLOW_OF_PROJECT.WID.eq(WORKFLOW.WID))
        .where(WORKFLOW_USER_ACCESS.UID.eq(user.getUid()))
        .and(
          workflowMatchQuery
        )

    // Retrieve project resource
    val projectQuery = context
      .select(
        //common attributes: 4 columns
        DSL.inline("project").as("resourceType"),
        PROJECT.NAME,
        PROJECT.DESCRIPTION,
        PROJECT.CREATION_TIME,
        // workflow attributes: 5 columns
        DSL.inline(null, classOf[UInteger]),
        DSL.inline(null, classOf[Timestamp]),
        DSL.inline(null, classOf[WorkflowUserAccessPrivilege]),
        DSL.inline(null, classOf[UInteger]),
        DSL.inline(null, classOf[String]),
        // project attributes: 3 columns
        PROJECT.PID,
        PROJECT.OWNER_ID,
        PROJECT.COLOR,
        // file attributes 6 columns
        DSL.inline(null, classOf[UInteger]).as("owner_uid"),
        DSL.inline(null, classOf[UInteger]).as("fid"),
        DSL.inline(null, classOf[Timestamp]).as("upload_time"),
        DSL.inline(null, classOf[String]).as("path"),
        DSL.inline(null, classOf[UInteger]).as("size"),
        DSL.inline(null, classOf[String]).as("email"),
        DSL.inline(null, classOf[UserFileAccessPrivilege])
      )
      .from(PROJECT)
      .where(PROJECT.OWNER_ID.eq(user.getUid()))
      .and(
        projectMatchQuery
      )

    // Retrieve file resource
    val fileQuery = context
      .select(
        // common attributes: 4 columns
        DSL.inline("file").as("resourceType"),
        FILE.NAME,
        FILE.DESCRIPTION,
        DSL.inline(null, classOf[Timestamp]),
        // workflow attributes: 5 columns
        DSL.inline(null, classOf[UInteger]),
        DSL.inline(null, classOf[Timestamp]),
        DSL.inline(null, classOf[WorkflowUserAccessPrivilege]),
        DSL.inline(null, classOf[UInteger]),
        DSL.inline(null, classOf[String]),
        // project attributes: 3 columns
        DSL.inline(null, classOf[UInteger]),
        DSL.inline(null, classOf[UInteger]),
        DSL.inline(null, classOf[String]),
        // file attributes 5 columns
        FILE.OWNER_UID,
        FILE.FID,
        FILE.UPLOAD_TIME,
        FILE.PATH,
        FILE.SIZE,
        USER.EMAIL,
        USER_FILE_ACCESS.PRIVILEGE.as("user_file_access")
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
        // common attributes: 4 columns
        DSL.inline("file").as("resourceType"),
        FILE.NAME,
        FILE.DESCRIPTION,
        DSL.inline(null, classOf[Timestamp]),
        // workflow attributes: 5 columns
        DSL.inline(null, classOf[UInteger]),
        DSL.inline(null, classOf[Timestamp]),
        DSL.inline(null, classOf[WorkflowUserAccessPrivilege]),
        DSL.inline(null, classOf[UInteger]),
        DSL.inline(null, classOf[String]),
        // project attributes: 3 columns
        DSL.inline(null, classOf[UInteger]),
        DSL.inline(null, classOf[UInteger]),
        DSL.inline(null, classOf[String]),
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
      .map(record => {
        val resourceType = record.get("resourceType", classOf[String])
        DashboardClickableFileEntry(
          resourceType,
//          record.get("name", classOf[String]),
//          record.get("description", classOf[String]),
//          record.get("creation_time", classOf[Timestamp]),
//          record.get("wid", classOf[UInteger]),
//          record.get("last_modified_time", classOf[Timestamp]),
//          record.get("privilege", classOf[WorkflowUserAccessPrivilege]),
//          record.get("uid", classOf[UInteger]),
//          record.get("userName", classOf[String]), // Specify the alias of the user name column
//          record.get("pid", classOf[UInteger]),
//          record.get("owner_id", classOf[UInteger]),
//          record.get("color", classOf[String]),
//          record.get("owner_uid", classOf[UInteger]),
//          record.get("fid", classOf[UInteger]),
//          record.get("upload_time", classOf[Timestamp]),
//          record.get("path", classOf[String]),
//          record.get("size", classOf[UInteger]),
//          record.get("email", classOf[String]),
//          record.get("user_file_access", classOf[UserFileAccessPrivilege]),
          if (resourceType == "workflow") {
            DashboardWorkflowEntry(
              record.into(WORKFLOW_OF_USER).getUid.eq(user.getUid),
              record
                .into(WORKFLOW_USER_ACCESS)
                .into(classOf[WorkflowUserAccess])
                .getPrivilege
                .toString,
              record.into(USER).getName,
              record.into(WORKFLOW).into(classOf[Workflow]),
              List[UInteger]()
            )
          } else {
            null
          },
          if (resourceType == "project") {
            record.into(PROJECT).into(classOf[Project])
          } else {
            null
          },
          if (resourceType == "file") {
            DashboardFileEntry(
              record.into(USER).getEmail,
              record.get(
                "user_file_access",
                classOf[UserFileAccessPrivilege]
              ) == UserFileAccessPrivilege.WRITE,
              record.into(FILE).into(classOf[File])
            )
          } else {
            null
          }
        )
      })
      .toList

  }

}
