package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.jooq.generated.enums.{
  DatasetUserAccessPrivilege,
  WorkflowUserAccessPrivilege
}
import edu.uci.ics.texera.web.resource.dashboard.UnifiedResourceSchema.context
import org.jooq.impl.DSL
import org.jooq.types.UInteger
import org.jooq.{Field, Record}

import java.sql.Timestamp
import scala.collection.mutable

object UnifiedResourceSchema {

  // Define alias strings
  private val resourceTypeAlias = "resourceType"
  private val resourceNameAlias = "resourceName"
  private val resourceDescriptionAlias = "resourceDescription"
  private val resourceCreationTimeAlias = "resourceCreationTime"
  private val resourceOwnerIdAlias = "resourceOwnerId"
  private val resourceLastModifiedTimeAlias = "resourceLastModifiedTime"

  // Use the alias variables to create fields
  val resourceTypeField: Field[_] = DSL.field(DSL.name(resourceTypeAlias))
  val resourceNameField: Field[_] = DSL.field(DSL.name(resourceNameAlias))
  val resourceDescriptionField: Field[_] = DSL.field(DSL.name(resourceDescriptionAlias))
  val resourceCreationTimeField: Field[_] = DSL.field(DSL.name(resourceCreationTimeAlias))
  val resourceOwnerIdField: Field[_] = DSL.field(DSL.name(resourceOwnerIdAlias))
  val resourceLastModifiedTimeField: Field[_] = DSL.field(DSL.name(resourceLastModifiedTimeAlias))

  final lazy val context = SqlServer
    .getInstance(StorageConfig.jdbcUrl, StorageConfig.jdbcUsername, StorageConfig.jdbcPassword)
    .createDSLContext()

  def apply(
      resourceType: Field[String] = DSL.inline(""),
      name: Field[String] = DSL.inline(""),
      description: Field[String] = DSL.inline(""),
      creationTime: Field[Timestamp] = DSL.inline(null, classOf[Timestamp]),
      lastModifiedTime: Field[Timestamp] = DSL.inline(null, classOf[Timestamp]),
      ownerId: Field[UInteger] = DSL.inline(null, classOf[UInteger]),
      wid: Field[UInteger] = DSL.inline(null, classOf[UInteger]),
      workflowUserAccess: Field[WorkflowUserAccessPrivilege] =
        DSL.inline(null, classOf[WorkflowUserAccessPrivilege]),
      projectsOfWorkflow: Field[String] = DSL.inline(""),
      uid: Field[UInteger] = DSL.inline(null, classOf[UInteger]),
      userName: Field[String] = DSL.inline(""),
      userEmail: Field[String] = DSL.inline(""),
      pid: Field[UInteger] = DSL.inline(null, classOf[UInteger]),
      projectOwnerId: Field[UInteger] = DSL.inline(null, classOf[UInteger]),
      projectColor: Field[String] = DSL.inline(""),
      did: Field[UInteger] = DSL.inline(null, classOf[UInteger]),
      datasetStoragePath: Field[String] = DSL.inline(null, classOf[String]),
      isDatasetPublic: Field[java.lang.Byte] = DSL.inline(null, classOf[java.lang.Byte]),
      datasetUserAccess: Field[DatasetUserAccessPrivilege] =
        DSL.inline(null, classOf[DatasetUserAccessPrivilege])
  ): UnifiedResourceSchema = {
    new UnifiedResourceSchema(
      Seq(
        resourceType -> resourceType.as(resourceTypeAlias),
        name -> name.as(resourceNameAlias),
        description -> description.as(resourceDescriptionAlias),
        creationTime -> creationTime.as(resourceCreationTimeAlias),
        lastModifiedTime -> lastModifiedTime.as(resourceLastModifiedTimeAlias),
        ownerId -> ownerId.as(resourceOwnerIdAlias),
        wid -> wid.as("wid"),
        workflowUserAccess -> workflowUserAccess.as("workflow_privilege"),
        projectsOfWorkflow -> projectsOfWorkflow.as("projects"),
        uid -> uid.as("uid"),
        userName -> userName.as("userName"),
        userEmail -> userEmail.as("email"),
        pid -> pid.as("pid"),
        projectOwnerId -> projectOwnerId.as("owner_uid"),
        projectColor -> projectColor.as("color"),
        did -> did.as("did"),
        datasetStoragePath -> datasetStoragePath.as("dataset_storage_path"),
        datasetUserAccess -> datasetUserAccess.as("user_dataset_access"),
        isDatasetPublic -> isDatasetPublic.as("is_dataset_public")
      )
    )
  }
}

/**
  * Refer to texera/core/scripts/sql/texera_ddl.sql to understand what each attribute is
  *
  * Attributes common across all resource types:
  * - `resourceType`: The type of the resource (e.g., project, workflow, file) as a `String`.
  * - `name`: The name of the resource as a `String`.
  * - `description`: A textual description of the resource as a `String`.
  * - `creationTime`: The timestamp when the resource was created, as a `Timestamp`.
  * - `lastModifiedTime`: The timestamp of the last modification to the resource, as a `Timestamp` (applicable to workflows).
  * - `ownerId`: The identifier of the resource's owner, as a `UInteger`.
  *
  * Attributes specific to workflows:
  * - `wid`: Workflow ID, as a `UInteger`.
  * - `workflowUserAccess`: Access privileges associated with the workflow, as a `WorkflowUserAccessPrivilege`.
  * - `projectsOfWorkflow`: IDs of projects associated with the workflow, concatenated as a `String`.
  * - `uid`: User ID associated with the workflow, as a `UInteger`.
  * - `userName`: Name of the user associated with the workflow, as a `String`.
  * - `userEmail`: Email of the user associated with the workflow, as a `String`.
  *
  * Attributes specific to projects:
  * - `pid`: Project ID, as a `UInteger`.
  * - `projectOwnerId`: ID of the project owner, as a `UInteger`.
  * - `projectColor`: Color associated with the project, as a `String`.
  *
  * Attributes specific to files:
  * - `fid`: File ID, as a `UInteger`.
  * - `fileUploadTime`: Timestamp when the file was uploaded, as a `Timestamp`.
  * - `filePath`: Path of the file, as a `String`.
  * - `fileSize`: Size of the file, as a `UInteger`.
  * - `fileUserAccess`: Access privileges for the file, as a `UserFileAccessPrivilege`.
  *
  * Attributes specific to datasets:
  * - `did`: Dataset ID, as a `UInteger`.
  * - `datasetUserAccess`: Access privileges for the dataset, as a `DatasetUserAccessPrivilege`
  */
class UnifiedResourceSchema private (
    fieldMappingSeq: Seq[(Field[_], Field[_])]
) {
  val allFields: Seq[Field[_]] = fieldMappingSeq.map(_._2)

  private val translatedFieldSet: Seq[(Field[_], Field[_])] = {
    val addedFields = new mutable.HashSet[Field[_]]()
    val output = new mutable.ArrayBuffer[(Field[_], Field[_])]()
    fieldMappingSeq.foreach {
      case (original, translated) =>
        if (!addedFields.contains(original)) {
          addedFields.add(original)
          output.addOne((original, translated))
        }
    }
    output.toSeq
  }

  def translateRecord(record: Record): Record = {
    val ret = context.newRecord(translatedFieldSet.map(_._1): _*)
    translatedFieldSet.foreach {
      case (original, translated) =>
        ret.set(original.asInstanceOf[org.jooq.Field[Any]], record.get(translated))
    }
    ret
  }

}
