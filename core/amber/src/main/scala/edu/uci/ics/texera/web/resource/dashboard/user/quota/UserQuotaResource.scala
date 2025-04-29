package edu.uci.ics.texera.web.resource.dashboard.user.quota

import edu.uci.ics.amber.core.storage.result.iceberg.OnIceberg
import edu.uci.ics.amber.core.storage.{DocumentFactory, IcebergCatalogInstance}
import edu.uci.ics.amber.core.storage.util.mongo.MongoDatabaseManager
import edu.uci.ics.amber.core.storage.util.mongo.MongoDatabaseManager.database
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.dao.jooq.generated.Tables._
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{
  OperatorExecutions,
  OperatorPortExecutions,
  WorkflowExecutions
}
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.utils.DatasetStatisticsUtils.getUserCreatedDatasets
import edu.uci.ics.texera.web.resource.dashboard.user.quota.UserQuotaResource._
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource
import io.dropwizard.auth.Auth
import org.apache.iceberg.{BaseTable, Table}
import edu.uci.ics.amber.util.IcebergUtil
import edu.uci.ics.texera.web.service.WorkflowService
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}
import org.apache.iceberg.exceptions.NoSuchTableException
import org.bson.Document

import java.lang.Integer
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import java.net.URI
import java.util
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.jdk.CollectionConverters.IterableHasAsScala

object UserQuotaResource {
  final private lazy val context = SqlServer
    .getInstance()
    .createDSLContext()

  case class Workflow(
      userId: Integer,
      workflowId: Integer,
      workflowName: String,
      creationTime: Long,
      lastModifiedTime: Long
  )

  case class DatasetQuota(
      did: Integer,
      name: String,
      creationTime: Long,
      size: Long
  )

  case class MongoStorage(
      workflowName: String,
      size: Double,
      pointer: String,
      eid: Integer
  )

  case class QuotaStorage(
      eid: Integer,
      workflowId: Integer,
      workflowName: String,
      resultBytes: Long,
      runTimeStatsBytes: Long,
      logBytes: Long
  )

  def getDatabaseSize(collectionNames: Array[MongoStorage]): Array[MongoStorage] = {
    var count = 0

    for (collection <- collectionNames) {
      val stats: Document = database.runCommand(new Document("collStats", collection.pointer))
      collectionNames(count) = MongoStorage(
        collection.workflowName,
        stats.getInteger("totalSize").toDouble,
        collection.pointer,
        collection.eid
      )
      count += 1
    }

    collectionNames
  }

  def getCollectionName(result: String): String = {

    /**
      * Get the Collection Name from
      * {"results":["1_TextInput-operator-6c3be22b-b2e2-4896-891c-cfa849638e5c"]}
      * to
      * 1_TextInput-operator-6c3be22b-b2e2-4896-891c-cfa849638e5c
      */

    var quoteCount = 0
    var name = ""
    for (chr <- result) {
      if (chr == '\"') {
        quoteCount += 1
      } else if (quoteCount == 3) { // collection name starts from the third quote and ends at the fourth quote.
        name += chr
      }
    }

    name
  }

  def getUserCreatedWorkflow(uid: Integer): List[Workflow] = {
    val userWorkflowEntries = context
      .select(
        WORKFLOW_OF_USER.UID,
        WORKFLOW_OF_USER.WID,
        WORKFLOW.NAME,
        WORKFLOW.CREATION_TIME,
        WORKFLOW.LAST_MODIFIED_TIME
      )
      .from(
        WORKFLOW_OF_USER
      )
      .leftJoin(
        WORKFLOW
      )
      .on(
        WORKFLOW.WID.eq(WORKFLOW_OF_USER.WID)
      )
      .where(
        WORKFLOW_OF_USER.UID.eq(uid)
      )
      .fetch()

    userWorkflowEntries
      .map(workflowRecord => {
        Workflow(
          workflowRecord.get(WORKFLOW_OF_USER.UID),
          workflowRecord.get(WORKFLOW_OF_USER.WID),
          workflowRecord.get(WORKFLOW.NAME),
          workflowRecord.get(WORKFLOW.CREATION_TIME).getTime,
          workflowRecord.get(WORKFLOW.LAST_MODIFIED_TIME).getTime
        )
      })
      .asScala
      .toList
  }

  def getUserAccessedWorkflow(uid: Integer): util.List[Integer] = {
    val availableWorkflowIds = context
      .select(
        WORKFLOW_USER_ACCESS.WID
      )
      .from(
        WORKFLOW_USER_ACCESS
      )
      .where(
        WORKFLOW_USER_ACCESS.UID.eq(uid)
      )
      .fetchInto(classOf[Integer])

    availableWorkflowIds
  }

  def getWorkflowExecutions(uid: Integer): util.List[WorkflowExecutions] = {
    val workflowExecutions = context
      .select(
        WORKFLOW_EXECUTIONS.EID,
        WORKFLOW_EXECUTIONS.VID,
        WORKFLOW_EXECUTIONS.UID,
        WORKFLOW_EXECUTIONS.STATUS,
        WORKFLOW_EXECUTIONS.RESULT,
        WORKFLOW_EXECUTIONS.EID,
        WORKFLOW_EXECUTIONS.LAST_UPDATE_TIME,
        WORKFLOW_EXECUTIONS.BOOKMARKED,
        WORKFLOW_EXECUTIONS.NAME,
        WORKFLOW_EXECUTIONS.ENVIRONMENT_VERSION,
        WORKFLOW_EXECUTIONS.LOG_LOCATION,
        WORKFLOW_EXECUTIONS.RUNTIME_STATS_URI
      )
      .from(WORKFLOW_EXECUTIONS)
      .where(WORKFLOW_EXECUTIONS.UID.eq(uid))
      .fetchInto(classOf[WorkflowExecutions])

    workflowExecutions
  }

  def getOperatorExecutions(eid: Integer): util.List[OperatorExecutions] = {
    val operatorExecutions = context
      .select(
        OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID,
        OPERATOR_EXECUTIONS.OPERATOR_ID,
        OPERATOR_EXECUTIONS.CONSOLE_MESSAGES_URI
      )
      .from(OPERATOR_EXECUTIONS)
      .where(OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid))
      .fetchInto(classOf[OperatorExecutions])

    operatorExecutions
  }

  def getOperatorPortExecutions(eid: Integer): util.List[OperatorPortExecutions] = {
    val operatorPortExecutions = context
      .select(
        OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID,
        OPERATOR_PORT_EXECUTIONS.RESULT_URI
      )
      .from(OPERATOR_PORT_EXECUTIONS)
      .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid))
      .fetchInto(classOf[OperatorPortExecutions])

    operatorPortExecutions
  }

  def getWorkflowNameByExecutionId(eid: Integer): String = {
    context
      .select(WORKFLOW.NAME)
      .from(WORKFLOW_EXECUTIONS)
      .join(WORKFLOW_VERSION)
      .on(WORKFLOW_EXECUTIONS.VID.eq(WORKFLOW_VERSION.VID))
      .join(WORKFLOW)
      .on(WORKFLOW_VERSION.WID.eq(WORKFLOW.WID))
      .where(WORKFLOW_EXECUTIONS.EID.eq(eid))
      .fetchOneInto(classOf[String])
  }

  def getRuntimeStatsSizesByUserId(uid: Integer): List[Integer] = {
    context
      .select(WORKFLOW_EXECUTIONS.RUNTIME_STATS_SIZE)
      .from(WORKFLOW_EXECUTIONS)
      .where(WORKFLOW_EXECUTIONS.UID.eq(uid))
      .fetch()
      .asScala
      .map(r => r.get(WORKFLOW_EXECUTIONS.RUNTIME_STATS_SIZE))
      .toList
  }

  def getUserQuotaSize(uid: Integer): Array[QuotaStorage] = {
    val executions = context
      .select(
        WORKFLOW_EXECUTIONS.EID,
        WORKFLOW_EXECUTIONS.RUNTIME_STATS_SIZE,
        WORKFLOW.WID,
        WORKFLOW.NAME
      )
      .from(WORKFLOW_EXECUTIONS)
      .leftJoin(WORKFLOW_VERSION)
      .on(WORKFLOW_EXECUTIONS.VID.eq(WORKFLOW_VERSION.VID))
      .leftJoin(WORKFLOW)
      .on(WORKFLOW_VERSION.WID.eq(WORKFLOW.WID))
      .where(WORKFLOW_EXECUTIONS.UID.eq(uid))
      .orderBy(WORKFLOW_EXECUTIONS.EID.desc)
      .fetch()

    if (executions == null || executions.isEmpty) {
      return Array.empty
    }

    executions.asScala.map { record =>
      val eid = record.get(WORKFLOW_EXECUTIONS.EID)
      val wid = record.get(WORKFLOW.WID)
      val workflowName = record.get(WORKFLOW.NAME)
      val runTimeStatsSize =
        Option(record.get(WORKFLOW_EXECUTIONS.RUNTIME_STATS_SIZE)).map(_.toLong).getOrElse(0L)

      val resultSize = context
        .select(OPERATOR_PORT_EXECUTIONS.RESULT_SIZE)
        .from(OPERATOR_PORT_EXECUTIONS)
        .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid))
        .fetch()
        .asScala
        .map(r =>
          Option(r.get(OPERATOR_PORT_EXECUTIONS.RESULT_SIZE)).getOrElse(0).asInstanceOf[Integer]
        )
        .map(_.toLong)
        .sum

      val logSize = context
        .select(OPERATOR_EXECUTIONS.CONSOLE_MESSAGES_SIZE)
        .from(OPERATOR_EXECUTIONS)
        .where(OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid))
        .fetch()
        .asScala
        .map(r =>
          Option(r.get(OPERATOR_EXECUTIONS.CONSOLE_MESSAGES_SIZE))
            .getOrElse(0)
            .asInstanceOf[Integer]
        )
        .map(_.toLong)
        .sum

      QuotaStorage(
        eid,
        wid,
        workflowName,
        resultSize,
        runTimeStatsSize,
        logSize
      )
    }.toArray
  }

  def deleteWorkflowCollection(eid: Integer): Unit = {
    // === 1. 清理文件内容 ===
    // 假设你有 workflowId 和 eid
    val wid = context
      .select(WORKFLOW_VERSION.WID)
      .from(WORKFLOW_EXECUTIONS)
      .join(WORKFLOW_VERSION)
      .on(WORKFLOW_EXECUTIONS.VID.eq(WORKFLOW_VERSION.VID))
      .where(WORKFLOW_EXECUTIONS.EID.eq(eid))
      .fetchOne(0, classOf[Integer])
    val workflowId = WorkflowIdentity(wid.toLong)
    val executionId = ExecutionIdentity(eid.toLong)
    println("clear wid + eid: " + wid + " " + eid)

    // 拿到 WorkflowService，然后清除
    WorkflowService.getOrCreate(workflowId).clearExecutionResources(executionId)
    WorkflowExecutionsResource.removeRuntimeStats(Array(eid))
    WorkflowExecutionsResource.clearUris(executionId)
    WorkflowExecutionsResource.removeExecution(executionId)
  }
}

@Path("/quota")
class UserQuotaResource {

  @GET
  @Path("/created_datasets")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getCreatedDatasets(@Auth current_user: SessionUser): List[DatasetQuota] = {
    getUserCreatedDatasets(current_user.getUid)
  }

  @GET
  @Path("/created_workflows")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getCreatedWorkflow(@Auth current_user: SessionUser): List[Workflow] = {
    getUserCreatedWorkflow(current_user.getUid)
  }

  @GET
  @Path("/access_workflows")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getAccessedWorkflow(@Auth current_user: SessionUser): util.List[Integer] = {
    getUserAccessedWorkflow(current_user.getUid)
  }

  @GET
  @Path("/user_quota_size")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getUserQuota(@Auth current_user: SessionUser): Array[QuotaStorage] = {
    getUserQuotaSize(current_user.getUid)
  }

  @DELETE
  @Path("/deleteCollection/{eid}")
  def deleteCollection(@PathParam("eid") eid: Integer): Unit = {
    deleteWorkflowCollection(eid)
  }
}
