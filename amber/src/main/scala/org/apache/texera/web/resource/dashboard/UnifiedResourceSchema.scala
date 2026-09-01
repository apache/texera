/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.web.resource.dashboard

import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.enums.{DefaultViewEnum, PrivilegeEnum}
import org.apache.texera.web.resource.dashboard.UnifiedResourceSchema.context
import org.jooq.impl.DSL
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
  private val resourceExecutionTimeAlias = "resourceExecutionTime"

  // Use the alias variables to create fields
  val resourceTypeField: Field[_] = DSL.field(DSL.name(resourceTypeAlias))
  val resourceNameField: Field[_] = DSL.field(DSL.name(resourceNameAlias))
  val resourceDescriptionField: Field[_] = DSL.field(DSL.name(resourceDescriptionAlias))
  val resourceCreationTimeField: Field[_] = DSL.field(DSL.name(resourceCreationTimeAlias))
  val resourceOwnerIdField: Field[_] = DSL.field(DSL.name(resourceOwnerIdAlias))
  val resourceLastModifiedTimeField: Field[_] = DSL.field(DSL.name(resourceLastModifiedTimeAlias))
  val resourceExecutionTimeField: Field[_] = DSL.field(DSL.name(resourceExecutionTimeAlias))

  def context =
    SqlServer
      .getInstance()
      .createDSLContext()

  def apply(
      resourceType: Field[String] = DSL.inline(""),
      name: Field[String] = DSL.inline(""),
      description: Field[String] = DSL.inline(""),
      creationTime: Field[Timestamp] = DSL.cast(null, classOf[Timestamp]),
      lastModifiedTime: Field[Timestamp] = DSL.cast(null, classOf[Timestamp]),
      executionTime: Field[Timestamp] = DSL.cast(null, classOf[Timestamp]),
      ownerId: Field[Integer] = DSL.cast(null, classOf[Integer]),
      wid: Field[Integer] = DSL.cast(null, classOf[Integer]),
      workflowUserAccess: Field[PrivilegeEnum] = DSL.castNull(classOf[PrivilegeEnum]),
      uid: Field[Integer] = DSL.cast(null, classOf[Integer]),
      userName: Field[String] = DSL.inline(""),
      userEmail: Field[String] = DSL.inline(""),
      versionedResourceId: Field[Integer] = DSL.cast(null, classOf[Integer]),
      datasetStoragePath: Field[String] = DSL.cast(null, classOf[String]),
      repositoryName: Field[String] = DSL.inline(""),
      isVersionedResourcePublic: Field[java.lang.Boolean] =
        DSL.cast(null, classOf[java.lang.Boolean]),
      isVersionedResourceDownloadable: Field[java.lang.Boolean] =
        DSL.cast(null, classOf[java.lang.Boolean]),
      versionedResourceUserAccess: Field[PrivilegeEnum] = DSL.castNull(classOf[PrivilegeEnum]),
      versionedResourceCoverImage: Field[String] = DSL.cast(null, classOf[String]),
      workflowCoverImage: Field[String] = DSL.cast(null, classOf[String]),
      // Workflow-only: which view the workflow opens in by default, so the listing can
      // mark the row and route it accordingly.
      workflowDefaultView: Field[DefaultViewEnum] = DSL.cast(null, classOf[DefaultViewEnum])
  ): UnifiedResourceSchema = {
    new UnifiedResourceSchema(
      Seq(
        resourceType -> resourceType.as(resourceTypeAlias),
        name -> name.as(resourceNameAlias),
        description -> description.as(resourceDescriptionAlias),
        creationTime -> creationTime.as(resourceCreationTimeAlias),
        lastModifiedTime -> lastModifiedTime.as(resourceLastModifiedTimeAlias),
        executionTime -> executionTime.as(resourceExecutionTimeAlias),
        ownerId -> ownerId.as(resourceOwnerIdAlias),
        wid -> wid.as("wid"),
        workflowUserAccess -> workflowUserAccess.as("workflow_privilege"),
        uid -> uid.as("uid"),
        userName -> userName.as("userName"),
        userEmail -> userEmail.as("email"),
        versionedResourceId -> versionedResourceId.as("versioned_resource_id"),
        datasetStoragePath -> datasetStoragePath.as("dataset_storage_path"),
        repositoryName -> repositoryName.as("repository_name"),
        isVersionedResourcePublic -> isVersionedResourcePublic
          .as("is_versioned_resource_public"),
        isVersionedResourceDownloadable -> isVersionedResourceDownloadable
          .as("is_versioned_resource_downloadable"),
        versionedResourceUserAccess -> versionedResourceUserAccess
          .as("user_versioned_resource_access"),
        versionedResourceCoverImage -> versionedResourceCoverImage
          .as("versioned_resource_cover_image"),
        workflowCoverImage -> workflowCoverImage.as("workflow_cover_image"),
        workflowDefaultView -> workflowDefaultView.as("workflow_default_view")
      )
    )
  }
}

/**
  * Refer to /sql/texera_ddl.sql to understand what each attribute is
  *
  * Attributes common across all resource types:
  * - `resourceType`: The type of the resource (e.g., workflow, dataset) as a `String`.
  * - `name`: The name of the resource as a `String`.
  * - `description`: A textual description of the resource as a `String`.
  * - `creationTime`: The timestamp when the resource was created, as a `Timestamp`.
  * - `lastModifiedTime`: The timestamp of the last modification to the resource, as a `Timestamp` (applicable to workflows).
  * - `ownerId`: The identifier of the resource's owner, as an `Integer`.
  *
  * Attributes specific to workflows:
  * - `wid`: Workflow ID, as an `Integer`.
  * - `workflowUserAccess`: Access privileges associated with the workflow, as a `PrivilegeEnum`.
  * - `uid`: User ID associated with the workflow, as an `Integer`.
  * - `userName`: Name of the user associated with the workflow, as a `String`.
  * - `userEmail`: Email of the user associated with the workflow, as a `String`.
  *
  * Attributes shared by the LakeFS-backed resources (datasets, models). One row is one
  * resource type, so each builder feeds these slots its own columns instead of duplicating
  * them per resource; the aliases only line up positionally across the UNION ALL.
  * - `versionedResourceId`: Dataset ID / model ID, as an `Integer`.
  * - `datasetStoragePath`: The storage path of the dataset, as a `String`.
  * - `repositoryName`: The name of the repository where the resource is stored, as a `String`.
  * - `isVersionedResourcePublic`: Indicates if the resource is public, as a `Boolean`.
  * - `isVersionedResourceDownloadable`: Indicates if the resource is downloadable, as a `Boolean`.
  * - `versionedResourceUserAccess`: Access privileges for the resource, as a `PrivilegeEnum`
  * - `versionedResourceCoverImage`: Cover image path of the resource, as a `String`.
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
