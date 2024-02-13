package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.model.jooq.generated.enums.{
  UserFileAccessPrivilege,
  WorkflowUserAccessPrivilege
}
import org.jooq.impl.DSL
import org.jooq.types.UInteger
import org.jooq.Field

import java.sql.Timestamp

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
case class UnifiedResourceSchema(
    resourceType: Field[String] = DSL.inline("").as("resourceType"),
    name: Field[String] = DSL.inline("").as("name"),
    description: Field[String] = DSL.inline("").as("description"),
    creationTime: Field[Timestamp] = DSL.inline(null, classOf[Timestamp]).as("creation_time"),
    wid: Field[UInteger] = DSL.inline(null, classOf[UInteger]).as("wid"),
    workflowLastModifiedTime: Field[Timestamp] =
      DSL.inline(null, classOf[Timestamp]).as("last_modified_time"),
    workflowUserAccess: Field[WorkflowUserAccessPrivilege] =
      DSL.inline(null, classOf[WorkflowUserAccessPrivilege]).as("privilege"),
    projectsOfWorkflow: Field[String] = DSL.inline("").as("projects"),
    uid: Field[UInteger] = DSL.inline(null, classOf[UInteger]).as("uid"),
    userName: Field[String] = DSL.inline("").as("userName"),
    userEmail: Field[String] = DSL.inline("").as("email"),
    pid: Field[UInteger] = DSL.inline(null, classOf[UInteger]).as("pid"),
    projectOwnerId: Field[UInteger] = DSL.inline(null, classOf[UInteger]).as("owner_uid"),
    projectColor: Field[String] = DSL.inline("").as("color"),
    fid: Field[UInteger] = DSL.inline(null, classOf[UInteger]).as("fid"),
    fileOwnerId: Field[UInteger] = DSL.inline(null, classOf[UInteger]).as("owner_id"),
    fileUploadTime: Field[Timestamp] = DSL.inline(null, classOf[Timestamp]).as("upload_time"),
    filePath: Field[String] = DSL.inline("").as("path"),
    fileSize: Field[UInteger] = DSL.inline(null, classOf[UInteger]).as("size"),
    fileUserAccess: Field[UserFileAccessPrivilege] =
      DSL.inline(null, classOf[UserFileAccessPrivilege]).as("user_file_access")
) {

  def getAllFields: Seq[Field[_]] = {
    Seq(
      resourceType,
      name,
      description,
      creationTime,
      wid,
      workflowLastModifiedTime,
      workflowUserAccess,
      projectsOfWorkflow,
      uid,
      userName,
      userEmail,
      pid,
      projectOwnerId,
      projectColor,
      fid,
      fileOwnerId,
      fileUploadTime,
      filePath,
      fileSize,
      fileUserAccess
    )
  }

}
