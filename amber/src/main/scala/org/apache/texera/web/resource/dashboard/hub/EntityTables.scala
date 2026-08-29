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

package org.apache.texera.web.resource.dashboard.hub

import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.records._
import org.apache.texera.web.resource.dashboard.VersionedResourceTables
import org.jooq._

object EntityTables {

  // ==================== THE REGISTRY ====================

  /** Every table one hub entity type owns. A new entity type is one set plus one line in `apply`. */
  sealed trait EntityTableSet {
    val base: BaseEntityTable
    val like: LikeTable
    val viewCount: ViewCountTable
    val access: AccessTable

    /**
      * Empty for cloneless entities (datasets, models): callers that can skip cloning read
      * this and report zero, instead of `CloneTable.apply` throwing. Not named `clone`
      * because `Object.clone` owns that name.
      */
    val cloneTable: Option[CloneTable]

    /** Set for LakeFS-backed resources, so the hub can hydrate without a per-type branch. */
    val versionedResource: Option[VersionedResourceTables[_ <: Record, _]]
  }

  case object WorkflowTableSet extends EntityTableSet {
    override val base: BaseEntityTable = BaseEntityTable.WorkflowTable
    override val like: LikeTable = LikeTable.WorkflowLikeTable
    override val viewCount: ViewCountTable = ViewCountTable.WorkflowViewCountTable
    override val access: AccessTable = AccessTable.WorkflowAccessTable
    override val cloneTable: Option[CloneTable] = Some(CloneTable.WorkflowCloneTable)
    override val versionedResource: Option[VersionedResourceTables[_ <: Record, _]] = None
  }

  case object DatasetTableSet extends EntityTableSet {
    override val base: BaseEntityTable = VersionedResourceTables.DatasetTables
    override val like: LikeTable = LikeTable.DatasetLikeTable
    override val viewCount: ViewCountTable = ViewCountTable.DatasetViewCountTable
    override val access: AccessTable = AccessTable.DatasetAccessTable
    override val cloneTable: Option[CloneTable] = None
    override val versionedResource: Option[VersionedResourceTables[_ <: Record, _]] = Some(
      VersionedResourceTables.DatasetTables
    )
  }

  def apply(entityType: EntityType): EntityTableSet =
    entityType match {
      case EntityType.Workflow => WorkflowTableSet
      case EntityType.Dataset  => DatasetTableSet
    }

  // ==================== BASE TABLE ====================
  // Not sealed: VersionedResourceTables implements it, so id/is_public are named once.
  trait BaseEntityTable {
    type R <: Record
    val table: Table[R]
    val isPublicColumn: TableField[R, java.lang.Boolean]
    val idColumn: TableField[R, Integer]
  }

  object BaseEntityTable {
    case object WorkflowTable extends BaseEntityTable {
      override type R = WorkflowRecord
      override val table: Table[WorkflowRecord] = WORKFLOW
      override val isPublicColumn: TableField[WorkflowRecord, java.lang.Boolean] =
        WORKFLOW.IS_PUBLIC
      override val idColumn: TableField[WorkflowRecord, Integer] = WORKFLOW.WID
    }

    def apply(entityType: EntityType): BaseEntityTable = EntityTables(entityType).base
  }

  // ==================== BASE LC (like & clone) TABLE ====================
  sealed trait BaseLCTable {
    type R <: Record
    val table: Table[R]
    val uidColumn: TableField[R, Integer]
    val idColumn: TableField[R, Integer]
  }

  // ==================== LIKE TABLE ====================
  sealed trait LikeTable extends BaseLCTable

  object LikeTable {
    case object WorkflowLikeTable extends LikeTable {
      override type R = WorkflowUserLikesRecord
      override val table: Table[WorkflowUserLikesRecord] = WORKFLOW_USER_LIKES
      override val uidColumn: TableField[WorkflowUserLikesRecord, Integer] =
        WORKFLOW_USER_LIKES.UID
      override val idColumn: TableField[WorkflowUserLikesRecord, Integer] = WORKFLOW_USER_LIKES.WID
    }

    case object DatasetLikeTable extends LikeTable {
      override type R = DatasetUserLikesRecord
      override val table: Table[DatasetUserLikesRecord] = DATASET_USER_LIKES
      override val uidColumn: TableField[DatasetUserLikesRecord, Integer] =
        DATASET_USER_LIKES.UID
      override val idColumn: TableField[DatasetUserLikesRecord, Integer] = DATASET_USER_LIKES.DID
    }

    def apply(entityType: EntityType): LikeTable = EntityTables(entityType).like
  }

  // ==================== CLONE TABLE ====================
  sealed trait CloneTable extends BaseLCTable

  object CloneTable {
    case object WorkflowCloneTable extends CloneTable {
      override type R = WorkflowUserClonesRecord
      override val table: Table[WorkflowUserClonesRecord] = WORKFLOW_USER_CLONES
      override val uidColumn: TableField[WorkflowUserClonesRecord, Integer] =
        WORKFLOW_USER_CLONES.UID
      override val idColumn: TableField[WorkflowUserClonesRecord, Integer] =
        WORKFLOW_USER_CLONES.WID
    }

    /** For callers that cannot proceed without one; see `EntityTableSet.cloneTable`. */
    def apply(entityType: EntityType): CloneTable =
      EntityTables(entityType).cloneTable.getOrElse(
        throw new IllegalArgumentException(s"Unsupported entity type: $entityType for clone")
      )
  }

  // ==================== VIEW COUNT TABLE ====================
  sealed trait ViewCountTable {
    type R <: Record
    val table: Table[R]
    val idColumn: TableField[R, Integer]
    val viewCountColumn: TableField[R, Integer]
  }

  object ViewCountTable {
    case object WorkflowViewCountTable extends ViewCountTable {
      override type R = WorkflowViewCountRecord
      override val table: Table[WorkflowViewCountRecord] = WORKFLOW_VIEW_COUNT
      override val idColumn: TableField[WorkflowViewCountRecord, Integer] = WORKFLOW_VIEW_COUNT.WID
      override val viewCountColumn: TableField[WorkflowViewCountRecord, Integer] =
        WORKFLOW_VIEW_COUNT.VIEW_COUNT
    }

    case object DatasetViewCountTable extends ViewCountTable {
      override type R = DatasetViewCountRecord
      override val table: Table[DatasetViewCountRecord] = DATASET_VIEW_COUNT
      override val idColumn: TableField[DatasetViewCountRecord, Integer] = DATASET_VIEW_COUNT.DID
      override val viewCountColumn: TableField[DatasetViewCountRecord, Integer] =
        DATASET_VIEW_COUNT.VIEW_COUNT
    }

    def apply(entityType: EntityType): ViewCountTable = EntityTables(entityType).viewCount
  }

  // ==================== ACCESS TABLE ====================
  /** The `<entity>_user_access` sibling table, replacing the inline match in HubResource. */
  sealed trait AccessTable {
    type R <: Record
    val table: Table[R]
    val idColumn: TableField[R, Integer]
    val uidColumn: TableField[R, Integer]
    val privilegeColumn: TableField[R, PrivilegeEnum]
  }

  object AccessTable {
    case object WorkflowAccessTable extends AccessTable {
      override type R = WorkflowUserAccessRecord
      override val table: Table[WorkflowUserAccessRecord] = WORKFLOW_USER_ACCESS
      override val idColumn: TableField[WorkflowUserAccessRecord, Integer] =
        WORKFLOW_USER_ACCESS.WID
      override val uidColumn: TableField[WorkflowUserAccessRecord, Integer] =
        WORKFLOW_USER_ACCESS.UID
      override val privilegeColumn: TableField[WorkflowUserAccessRecord, PrivilegeEnum] =
        WORKFLOW_USER_ACCESS.PRIVILEGE
    }

    case object DatasetAccessTable extends AccessTable {
      override type R = DatasetUserAccessRecord
      override val table: Table[DatasetUserAccessRecord] = DATASET_USER_ACCESS
      override val idColumn: TableField[DatasetUserAccessRecord, Integer] = DATASET_USER_ACCESS.DID
      override val uidColumn: TableField[DatasetUserAccessRecord, Integer] = DATASET_USER_ACCESS.UID
      override val privilegeColumn: TableField[DatasetUserAccessRecord, PrivilegeEnum] =
        DATASET_USER_ACCESS.PRIVILEGE
    }

    def apply(entityType: EntityType): AccessTable = EntityTables(entityType).access
  }
}
