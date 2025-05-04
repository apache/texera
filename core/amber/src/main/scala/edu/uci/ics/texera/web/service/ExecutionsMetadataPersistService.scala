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

package edu.uci.ics.texera.web.service

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.workflow.WorkflowContext.DEFAULT_EXECUTION_ID
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.WorkflowExecutionsDao
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.WorkflowExecutions
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowVersionResource._

import java.sql.Timestamp

/**
  * This global object handles inserting a new entry to the DB to store metadata information about every workflow execution
  * It also updates the entry if an execution status is updated
  */
object ExecutionsMetadataPersistService extends LazyLogging {
  final private lazy val context = SqlServer
    .getInstance()
    .createDSLContext()
  private lazy val workflowExecutionsDao = new WorkflowExecutionsDao(
    context.configuration
  )

  /**
    * This method inserts a new entry of a workflow execution in the database and returns the generated eId
    *
    * @param workflowId the given workflow
    * @param uid        user id that initiated the execution
    * @return generated execution ID
    */

  def insertNewExecution(
      workflowId: WorkflowIdentity,
      uid: Option[Integer],
      executionName: String,
      environmentVersion: String
  ): ExecutionIdentity = {
    if (!AmberConfig.isUserSystemEnabled) return DEFAULT_EXECUTION_ID
    // first retrieve the latest version of this workflow
    val vid = getLatestVersion(workflowId.id.toInt)
    val newExecution = new WorkflowExecutions()
    if (executionName != "") {
      newExecution.setName(executionName)
    }
    newExecution.setVid(vid)
    newExecution.setUid(uid.orNull)
    newExecution.setStartingTime(new Timestamp(System.currentTimeMillis()))
    newExecution.setEnvironmentVersion(environmentVersion)
    workflowExecutionsDao.insert(newExecution)
    ExecutionIdentity(newExecution.getEid.longValue())
  }

  def tryGetExistingExecution(executionId: ExecutionIdentity): Option[WorkflowExecutions] = {
    if (!AmberConfig.isUserSystemEnabled) return None
    try {
      Some(workflowExecutionsDao.fetchOneByEid(executionId.id.toInt))
    } catch {
      case t: Throwable =>
        logger.info("Unable to get execution. Error = " + t.getMessage)
        None
    }
  }

  def tryUpdateExistingExecution(
      executionId: ExecutionIdentity
  )(updateFunc: WorkflowExecutions => Unit): Unit = {
    if (!AmberConfig.isUserSystemEnabled) return
    try {
      val execution = workflowExecutionsDao.fetchOneByEid(executionId.id.toInt)
      updateFunc(execution)
      workflowExecutionsDao.update(execution)
    } catch {
      case t: Throwable =>
        logger.info("Unable to update execution. Error = " + t.getMessage)
    }
  }
}
