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

package edu.uci.ics.texera.web.resource.dashboard.hub

import edu.uci.ics.texera.dao.jooq.generated.Tables._
import edu.uci.ics.texera.dao.jooq.generated.tables.records._
import org.jooq._

object EntityTables {
  // ==================== BASE TABLE ====================
  sealed trait BaseEntityTable {
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

    case object DatasetTable extends BaseEntityTable {
      override type R = DatasetRecord
      override val table: Table[DatasetRecord] = DATASET
      override val isPublicColumn: TableField[DatasetRecord, java.lang.Boolean] = DATASET.IS_PUBLIC
      override val idColumn: TableField[DatasetRecord, Integer] = DATASET.DID
    }

    def apply(entityType: String): BaseEntityTable = {
      entityType match {
        case "workflow" => WorkflowTable
        case "dataset"  => DatasetTable
        case _ =>
          throw new IllegalArgumentException(
            s"Unsupported entity type: $entityType for base entity"
          )
      }
    }
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

    def apply(entityType: String): LikeTable =
      entityType match {
        case "workflow" => WorkflowLikeTable
        case "dataset"  => DatasetLikeTable
        case _ =>
          throw new IllegalArgumentException(s"Unsupported entity type: $entityType for like")
      }
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

    def apply(entityType: String): CloneTable =
      entityType match {
        case "workflow" => WorkflowCloneTable
        case _ =>
          throw new IllegalArgumentException(s"Unsupported entity type: $entityType for clone")
      }
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

    def apply(entityType: String): ViewCountTable =
      entityType match {
        case "workflow" => WorkflowViewCountTable
        case "dataset"  => DatasetViewCountTable
        case _ =>
          throw new IllegalArgumentException(s"Unsupported entity type: $entityType for view count")
      }
  }
}
