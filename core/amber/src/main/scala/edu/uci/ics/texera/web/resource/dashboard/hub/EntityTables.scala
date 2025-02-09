package edu.uci.ics.texera.web.resource.dashboard.hub

import edu.uci.ics.texera.dao.jooq.generated.tables.records.{
  DatasetRecord,
  WorkflowRecord,
  WorkflowUserClonesRecord,
  WorkflowUserLikesRecord,
  WorkflowViewCountRecord
}
import org.jooq._
import org.jooq.types.UInteger
import edu.uci.ics.texera.dao.jooq.generated.Tables._

object EntityTables {
  // ==================== BASE TABLE ====================
  sealed trait BaseEntityTable {
    type R <: Record
    val table: Table[R]
    val isPublicColumn: TableField[R, java.lang.Byte]
  }

  object BaseEntityTable {
    case object WorkflowTable extends BaseEntityTable {
      override type R = WorkflowRecord
      override val table: Table[WorkflowRecord] = WORKFLOW
      override val isPublicColumn: TableField[WorkflowRecord, java.lang.Byte] = WORKFLOW.IS_PUBLIC
    }

    case object DatasetTable extends BaseEntityTable {
      override type R = DatasetRecord
      override val table: Table[DatasetRecord] = DATASET
      override val isPublicColumn: TableField[DatasetRecord, java.lang.Byte] = DATASET.IS_PUBLIC
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
    val uidColumn: TableField[R, UInteger]
    val idColumn: TableField[R, UInteger]
  }

  // ==================== LIKE TABLE ====================
  sealed trait LikeTable extends BaseLCTable

  object LikeTable {
    case object WorkflowLikeTable extends LikeTable {
      override type R = WorkflowUserLikesRecord
      override val table: Table[WorkflowUserLikesRecord] = WORKFLOW_USER_LIKES
      override val uidColumn: TableField[WorkflowUserLikesRecord, UInteger] =
        WORKFLOW_USER_LIKES.UID
      override val idColumn: TableField[WorkflowUserLikesRecord, UInteger] = WORKFLOW_USER_LIKES.WID
    }

    def apply(entityType: String): LikeTable =
      entityType match {
        case "workflow" => WorkflowLikeTable
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
      override val uidColumn: TableField[WorkflowUserClonesRecord, UInteger] =
        WORKFLOW_USER_CLONES.UID
      override val idColumn: TableField[WorkflowUserClonesRecord, UInteger] =
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
    val idColumn: TableField[R, UInteger]
    val viewCountColumn: TableField[R, UInteger]
  }

  object ViewCountTable {
    case object WorkflowViewCountTable extends ViewCountTable {
      override type R = WorkflowViewCountRecord
      override val table: Table[WorkflowViewCountRecord] = WORKFLOW_VIEW_COUNT
      override val idColumn: TableField[WorkflowViewCountRecord, UInteger] = WORKFLOW_VIEW_COUNT.WID
      override val viewCountColumn: TableField[WorkflowViewCountRecord, UInteger] =
        WORKFLOW_VIEW_COUNT.VIEW_COUNT
    }

    def apply(entityType: String): ViewCountTable =
      entityType match {
        case "workflow" => WorkflowViewCountTable
        case _ =>
          throw new IllegalArgumentException(s"Unsupported entity type: $entityType for view count")
      }
  }
}
