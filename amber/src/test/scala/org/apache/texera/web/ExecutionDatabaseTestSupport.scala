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

package org.apache.texera.web

import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.enums.{UserRoleEnum, WorkflowComputingUnitTypeEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowComputingUnitDao,
  WorkflowDao,
  WorkflowExecutionsDao,
  WorkflowVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowComputingUnit,
  WorkflowExecutions,
  WorkflowVersion
}

import java.sql.Timestamp

/**
  * Shared `MockTexeraDB` row builders and reverse-FK cleanup helpers for execution-backed suites.
  */
trait ExecutionDatabaseTestSupport { this: MockTexeraDB =>

  protected def workflowExecutionsDao: WorkflowExecutionsDao =
    new WorkflowExecutionsDao(getDSLContext.configuration())

  protected def insertUser(
      uid: Int,
      name: String,
      email: String,
      role: Option[UserRoleEnum] = None
  ): User = {
    val user = new User
    user.setUid(uid)
    user.setName(name)
    user.setEmail(email)
    role.foreach(user.setRole)
    new UserDao(getDSLContext.configuration()).insert(user)
    user
  }

  protected def insertWorkflow(
      wid: Int,
      name: String,
      content: String = "{}",
      description: String = "",
      withTimestamps: Boolean = true
  ): Workflow = {
    val workflow = new Workflow
    workflow.setWid(wid)
    workflow.setName(name)
    workflow.setContent(content)
    workflow.setDescription(description)
    if (withTimestamps) {
      val now = new Timestamp(System.currentTimeMillis())
      workflow.setCreationTime(now)
      workflow.setLastModifiedTime(now)
    }
    new WorkflowDao(getDSLContext.configuration()).insert(workflow)
    workflow
  }

  protected def insertWorkflowVersion(
      wid: Int,
      vidOpt: Option[Int] = None,
      content: String = "{}",
      withTimestamp: Boolean = true
  ): WorkflowVersion = {
    val version = new WorkflowVersion
    version.setWid(wid)
    vidOpt.foreach(v => version.setVid(v))
    version.setContent(content)
    if (withTimestamp) version.setCreationTime(new Timestamp(System.currentTimeMillis()))
    new WorkflowVersionDao(getDSLContext.configuration()).insert(version)
    version
  }

  protected def insertComputingUnit(
      uid: Int,
      name: String,
      cuidOpt: Option[Int] = None,
      unitType: Option[WorkflowComputingUnitTypeEnum] = Some(WorkflowComputingUnitTypeEnum.local),
      uri: Option[String] = Some("local://test"),
      resource: Option[String] = Some("{}")
  ): WorkflowComputingUnit = {
    val cu = new WorkflowComputingUnit
    cuidOpt.foreach(id => cu.setCuid(id))
    cu.setUid(uid)
    cu.setName(name)
    if (unitType.isDefined) cu.setCreationTime(new Timestamp(System.currentTimeMillis()))
    unitType.foreach(cu.setType)
    uri.foreach(cu.setUri)
    resource.foreach(cu.setResource)
    new WorkflowComputingUnitDao(getDSLContext.configuration()).insert(cu)
    cu
  }

  protected def insertWorkflowExecution(
      vid: Int,
      uid: Int,
      eidOpt: Option[Int] = None,
      cuidOpt: Option[Int] = None,
      status: Byte = 0.toByte,
      name: String = "execution",
      envVersion: String = "test-env",
      result: Option[String] = Some(""),
      logLocation: Option[String] = None,
      runtimeStatsUri: Option[String] = None,
      bookmarked: Option[Boolean] = Some(false),
      startOffsetMillis: Option[Long] = Some(0L),
      durationMillis: Option[Long] = None
  ): WorkflowExecutions = {
    val execution = new WorkflowExecutions
    eidOpt.foreach(id => execution.setEid(id))
    execution.setVid(vid)
    execution.setUid(uid)
    cuidOpt.foreach(id => execution.setCuid(id))
    execution.setStatus(status)
    result.foreach(execution.setResult)
    logLocation.foreach(execution.setLogLocation)
    runtimeStatsUri.foreach(execution.setRuntimeStatsUri)
    val now = System.currentTimeMillis()
    startOffsetMillis.foreach(offset => execution.setStartingTime(new Timestamp(now - offset)))
    for (offset <- startOffsetMillis; dur <- durationMillis) {
      execution.setLastUpdateTime(new Timestamp(now - offset + dur))
    }
    bookmarked.foreach(b => execution.setBookmarked(b))
    if (name.nonEmpty) execution.setName(name)
    execution.setEnvironmentVersion(envVersion)
    workflowExecutionsDao.insert(execution)
    execution
  }

  protected def cleanupExecutionsForVersion(
      vid: Int,
      includePortExecutions: Boolean = false
  ): Unit = {
    val vids = org.jooq.impl.DSL
      .select(WORKFLOW_VERSION.VID)
      .from(WORKFLOW_VERSION)
      .where(WORKFLOW_VERSION.VID.eq(vid))
    deleteExecutionsForVersions(vids, includePortExecutions)
  }

  private def deleteExecutionsForVersions(
      vids: org.jooq.SelectConditionStep[org.jooq.Record1[Integer]],
      includePortExecutions: Boolean
  ): Unit = {
    if (includePortExecutions) {
      val eids = getDSLContext
        .select(WORKFLOW_EXECUTIONS.EID)
        .from(WORKFLOW_EXECUTIONS)
        .where(WORKFLOW_EXECUTIONS.VID.in(vids))
      getDSLContext
        .deleteFrom(OPERATOR_PORT_EXECUTIONS)
        .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.in(eids))
        .execute()
    }
    getDSLContext.deleteFrom(WORKFLOW_EXECUTIONS).where(WORKFLOW_EXECUTIONS.VID.in(vids)).execute()
  }

  protected def cleanupWorkflowRecords(
      wids: Seq[Int],
      uids: Seq[Int] = Seq.empty,
      includePortExecutions: Boolean = false,
      includeUserAccess: Boolean = false,
      includeComputingUnits: Boolean = false
  ): Unit = {
    val boxedWids = wids.map(Integer.valueOf)
    val vids = getDSLContext
      .select(WORKFLOW_VERSION.VID)
      .from(WORKFLOW_VERSION)
      .where(WORKFLOW_VERSION.WID.in(boxedWids: _*))
    deleteExecutionsForVersions(vids, includePortExecutions)
    if (includeUserAccess) {
      getDSLContext
        .deleteFrom(WORKFLOW_USER_ACCESS)
        .where(WORKFLOW_USER_ACCESS.WID.in(boxedWids: _*))
        .execute()
    }
    getDSLContext
      .deleteFrom(WORKFLOW_VERSION)
      .where(WORKFLOW_VERSION.WID.in(boxedWids: _*))
      .execute()
    getDSLContext.deleteFrom(WORKFLOW).where(WORKFLOW.WID.in(boxedWids: _*)).execute()
    if (uids.nonEmpty) {
      val boxedUids = uids.map(Integer.valueOf)
      if (includeComputingUnits) {
        getDSLContext
          .deleteFrom(WORKFLOW_COMPUTING_UNIT)
          .where(WORKFLOW_COMPUTING_UNIT.UID.in(boxedUids: _*))
          .execute()
      }
      getDSLContext.deleteFrom(USER).where(USER.UID.in(boxedUids: _*)).execute()
    }
  }
}
