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

package org.apache.texera.web.resource.dashboard.user.quota

import io.dropwizard.auth.Auth
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.web.resource.dashboard.user.dataset.utils.DatasetStatisticsUtils.getUserCreatedDatasets
import org.apache.texera.web.resource.dashboard.user.quota.UserQuotaResource._
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource

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

  case class QuotaStorage(
      eid: Integer,
      workflowId: Integer,
      workflowName: String,
      resultBytes: Long,
      runTimeStatsBytes: Long,
      logBytes: Long
  )

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

  def deleteExecutionCollection(eid: Integer): Unit = {
    WorkflowExecutionsResource.removeAllExecutionFiles(Array(eid))
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
    deleteExecutionCollection(eid)
  }
}
