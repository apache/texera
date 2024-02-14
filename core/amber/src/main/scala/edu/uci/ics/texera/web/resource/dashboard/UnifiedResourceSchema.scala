package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.model.jooq.generated.enums.{UserFileAccessPrivilege, WorkflowUserAccessPrivilege}
import org.jooq.impl.DSL
import org.jooq.types.UInteger
import org.jooq.{Field, Table}

import java.sql.Timestamp
import scala.collection.mutable


object UnifiedResourceSchema{
  def apply(resourceType: Field[String] = DSL.inline(""),
            name: Field[String] = DSL.inline(""),
            description: Field[String] = DSL.inline(""),
            creationTime: Field[Timestamp] = DSL.inline(null, classOf[Timestamp]),
            wid: Field[UInteger] = DSL.inline(null, classOf[UInteger]),
            workflowLastModifiedTime: Field[Timestamp] =
            DSL.inline(null, classOf[Timestamp]),
            workflowUserAccess: Field[WorkflowUserAccessPrivilege] =
            DSL.inline(null, classOf[WorkflowUserAccessPrivilege]),
            projectsOfWorkflow: Field[String] = DSL.inline(""),
            uid: Field[UInteger] = DSL.inline(null, classOf[UInteger]),
            userName: Field[String] = DSL.inline(""),
            userEmail: Field[String] = DSL.inline(""),
            pid: Field[UInteger] = DSL.inline(null, classOf[UInteger]),
            projectOwnerId: Field[UInteger] = DSL.inline(null, classOf[UInteger]),
            projectColor: Field[String] = DSL.inline(""),
            fid: Field[UInteger] = DSL.inline(null, classOf[UInteger]),
            fileOwnerId: Field[UInteger] = DSL.inline(null, classOf[UInteger]),
            fileUploadTime: Field[Timestamp] = DSL.inline(null, classOf[Timestamp]),
            filePath: Field[String] = DSL.inline(""),
            fileSize: Field[UInteger] = DSL.inline(null, classOf[UInteger]),
            fileUserAccess: Field[UserFileAccessPrivilege] =
            DSL.inline(null, classOf[UserFileAccessPrivilege])
           ):UnifiedResourceSchema = {
    new UnifiedResourceSchema(Map(
      resourceType -> resourceType.as("resourceType"),
      name -> name.as("resource_name"),
      description -> description.as("resource_description"),
      creationTime -> creationTime.as("creation_time"),
      wid -> wid.as("wid"),
      workflowLastModifiedTime -> workflowLastModifiedTime.as("last_modified_time"),
      workflowUserAccess -> workflowUserAccess.as("workflow_privilege"),
      projectsOfWorkflow -> projectsOfWorkflow.as("projects"),
      uid -> uid.as("uid"),
      userName -> userName.as("userName"),
      userEmail -> userEmail.as("email"),
      pid -> pid.as("pid"),
      projectOwnerId -> projectOwnerId.as("owner_uid"),
      projectColor -> projectColor.as("color"),
      fid -> fid.as("fid"),
      fileOwnerId -> fileOwnerId.as("owner_id"),
      fileUploadTime -> fileUploadTime.as("upload_time"),
      filePath -> filePath.as("path"),
      fileSize -> fileSize.as("size"),
      fileUserAccess -> fileUserAccess.as("user_file_access"))
  }
}


/**
  * Refer to texera/core/scripts/sql/texera_ddl.sql to understand what each attribute is
  *
  * Common Attributes (4 columns): All types of resources have these 4 attributes
  * 1. `resourceType`: Represents the type of resource (`String`). Allowed value: project, workflow, file
  * 2. `name`: Specifies the name of the resource (`String`).
  * 3. `description`: Provides a description of the resource (`String`).
  * 4. `creation_time`: Indicates the timestamp of when the resource was created (`Timestamp`). It represents upload_time if the resourceType is `file`
  *
  * Workflow Attributes (6 columns): Only workflow will have these 6 attributes.
  * 5. `WID`: Represents the Workflow ID (`UInteger`).
  * 6. `lastModifiedTime`: Indicates the timestamp of the last modification made to the workflow (`Timestamp`).
  * 7. `privilege`: Specifies the privilege associated with the workflow (`Privilege`).
  * 8. `UID`: Represents the User ID associated with the workflow (`UInteger`).
  * 9. `userName`: Provides the name of the user associated with the workflow (`String`).
  * 10. `projects`: The project IDs for the workflow, concatenated as a string (`String`).
  *
  * Project Attributes (3 columns): Only project will have these 3 attributes.
  * 11. `pid`: Represents the Project ID (`UInteger`).
  * 12. `ownerId`: Indicates the ID of the project owner (`UInteger`).
  * 13. `color`: Specifies the color associated with the project (`String`).
  *
  * File Attributes (7 columns): Only files will have these 7 attributes.
  * 14. `ownerUID`: Represents the User ID of the file owner (`UInteger`).
  * 15. `fid`: Indicates the File ID (`UInteger`).
  * 16. `uploadTime`: Indicates the timestamp when the file was uploaded (`Timestamp`).
  * 17. `path`: Specifies the path of the file (`String`).
  * 18. `size`: Represents the size of the file (`UInteger`).
  * 19. `email`: Represents the email associated with the file owner (`String`).
  * 20. `userFileAccess`: Specifies the user file access privilege (`UserFileAccessPrivilege`).
  */
class UnifiedResourceSchema private(
   fieldMappings:Map[Field[_],Field[_]]
) {

  private val translatedTables = new mutable.HashMap[Table[_], Seq[Field[_]]]()

  val allFields: Iterable[Field[_]] = fieldMappings.keys

  def translate[T](field:Field[T]):Field[T] = {
    fieldMappings.getOrElse(field, field).asInstanceOf[Field[T]]
  }

  def translate(table:Table[_]):Seq[Field[_]] = {
    if(!translatedTables.contains(table)){
      translatedTables(table) = table.fields().map {
        field =>
          translate(field)
      }
    }
    translatedTables(table)
  }

}
