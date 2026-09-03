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

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.dao.jooq.generated.Tables.{DATASET, MODEL}
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.User.USER
import org.apache.texera.dao.jooq.generated.tables.pojos.{Dataset, Model, User}
import org.apache.texera.dao.jooq.generated.tables.records.{DatasetRecord, ModelRecord}
import org.apache.texera.web.resource.dashboard.DashboardResource.{
  DashboardClickableFileEntry,
  SearchQueryParams
}
import org.apache.texera.web.resource.dashboard.hub.EntityTables.{AccessTable, BaseEntityTable}
import org.apache.texera.web.resource.dashboard.user.dataset.DatasetResource.DashboardDataset
import org.apache.texera.web.resource.dashboard.user.model.ModelResource.DashboardModel
import org.jooq._

import java.sql.Timestamp

/**
  * Names the columns of one LakeFS-backed resource (dataset, model) so unified search and the
  * hub are written once. A new resource type is one descriptor plus one `EntityTables` entry.
  */
trait VersionedResourceTables[Rec <: Record, P] extends BaseEntityTable with LazyLogging {
  override type R = Rec
  override val table: Table[Rec]
  override val idColumn: TableField[Rec, Integer]
  override val isPublicColumn: TableField[Rec, java.lang.Boolean]

  val resourceType: String
  val nameColumn: TableField[Rec, String]
  val descriptionColumn: TableField[Rec, String]
  val creationTimeColumn: TableField[Rec, Timestamp]
  val ownerUidColumn: TableField[Rec, Integer]
  val access: AccessTable

  def searchIds(params: SearchQueryParams): java.util.List[Integer]

  def pojo(record: Record): P
  def idOf(resource: P): Integer
  def ownerUidOf(resource: P): Integer
  def repositoryNameOf(resource: P): String

  def entry(
      resource: P,
      ownerEmail: String,
      accessPrivilege: PrivilegeEnum,
      isOwner: Boolean,
      size: Long
  ): DashboardClickableFileEntry

  /** Owner-only; `joinWithAccessAndOwner` would fan out a row per grant. */
  final override def joinWithOwner: Table[_ <: Record] =
    table.join(USER).on(USER.UID.eq(ownerUidColumn))

  /** `accessCondition` narrows the access join: search to the caller, the hub to nothing. */
  final def joinWithAccessAndOwner(accessCondition: Option[Condition]): Table[Record] = {
    val accessJoin = table
      .leftJoin(access.table)
      .on(access.idColumn.eq(idColumn))
    val filtered = accessCondition.map(condition => accessJoin.and(condition)).getOrElse(accessJoin)
    filtered
      .leftJoin(USER)
      .on(USER.UID.eq(ownerUidColumn))
  }

  /** `None` when LakeFS cannot size the resource; callers drop it and report a mismatch. */
  final def hydrate(
      record: Record,
      uid: Integer
  ): Option[(Integer, DashboardClickableFileEntry)] = {
    val resource = pojo(record)
    repositorySize(resource).map { size =>
      idOf(resource) -> entry(
        resource,
        record.into(USER).into(classOf[User]).getEmail,
        Option(record.get(access.privilegeColumn, classOf[PrivilegeEnum]))
          .getOrElse(PrivilegeEnum.NONE),
        ownerUidOf(resource) == uid,
        size
      )
    }
  }

  final def repositorySize(resource: P): Option[Long] = {
    val repositoryName = repositoryNameOf(resource)
    try {
      Some(LakeFSStorageClient.retrieveRepositorySize(repositoryName))
    } catch {
      case e: io.lakefs.clients.sdk.ApiException =>
        logger.error(
          s"LakeFS ApiException for $resourceType repository '$repositoryName': ${e.getMessage}",
          e
        )
        None
    }
  }
}

object VersionedResourceTables {

  case object DatasetTables extends VersionedResourceTables[DatasetRecord, Dataset] {
    override val resourceType: String = SearchQueryBuilder.DATASET_RESOURCE_TYPE
    override val table: Table[DatasetRecord] = DATASET
    override val idColumn: TableField[DatasetRecord, Integer] = DATASET.DID
    override val isPublicColumn: TableField[DatasetRecord, java.lang.Boolean] = DATASET.IS_PUBLIC
    override val nameColumn: TableField[DatasetRecord, String] = DATASET.NAME
    override val descriptionColumn: TableField[DatasetRecord, String] = DATASET.DESCRIPTION
    override val creationTimeColumn: TableField[DatasetRecord, Timestamp] = DATASET.CREATION_TIME
    override val ownerUidColumn: TableField[DatasetRecord, Integer] = DATASET.OWNER_UID
    override val access: AccessTable = AccessTable.DatasetAccessTable

    override def searchIds(params: SearchQueryParams): java.util.List[Integer] = params.datasetIds

    override def pojo(record: Record): Dataset = record.into(DATASET).into(classOf[Dataset])
    override def idOf(resource: Dataset): Integer = resource.getDid
    override def ownerUidOf(resource: Dataset): Integer = resource.getOwnerUid
    override def repositoryNameOf(resource: Dataset): String = resource.getRepositoryName

    override def entry(
        resource: Dataset,
        ownerEmail: String,
        accessPrivilege: PrivilegeEnum,
        isOwner: Boolean,
        size: Long
    ): DashboardClickableFileEntry =
      DashboardClickableFileEntry(
        resourceType = resourceType,
        dataset = Some(DashboardDataset(resource, ownerEmail, accessPrivilege, isOwner, size))
      )
  }

  case object ModelTables extends VersionedResourceTables[ModelRecord, Model] {
    override val resourceType: String = SearchQueryBuilder.MODEL_RESOURCE_TYPE
    override val table: Table[ModelRecord] = MODEL
    override val idColumn: TableField[ModelRecord, Integer] = MODEL.MID
    override val isPublicColumn: TableField[ModelRecord, java.lang.Boolean] = MODEL.IS_PUBLIC
    override val nameColumn: TableField[ModelRecord, String] = MODEL.NAME
    override val descriptionColumn: TableField[ModelRecord, String] = MODEL.DESCRIPTION
    override val creationTimeColumn: TableField[ModelRecord, Timestamp] = MODEL.CREATION_TIME
    override val ownerUidColumn: TableField[ModelRecord, Integer] = MODEL.OWNER_UID
    override val access: AccessTable = AccessTable.ModelAccessTable

    override def searchIds(params: SearchQueryParams): java.util.List[Integer] = params.modelIds

    override def pojo(record: Record): Model = record.into(MODEL).into(classOf[Model])
    override def idOf(resource: Model): Integer = resource.getMid
    override def ownerUidOf(resource: Model): Integer = resource.getOwnerUid
    override def repositoryNameOf(resource: Model): String = resource.getRepositoryName

    override def entry(
        resource: Model,
        ownerEmail: String,
        accessPrivilege: PrivilegeEnum,
        isOwner: Boolean,
        size: Long
    ): DashboardClickableFileEntry =
      DashboardClickableFileEntry(
        resourceType = resourceType,
        model = Some(DashboardModel(resource, ownerEmail, accessPrivilege, isOwner, size))
      )
  }
}
