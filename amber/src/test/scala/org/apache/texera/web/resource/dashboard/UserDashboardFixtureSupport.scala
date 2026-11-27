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

import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.tables.daos._
import org.apache.texera.dao.jooq.generated.tables.pojos._
import org.apache.texera.web.ExecutionDatabaseTestSupport
import org.jooq.DSLContext

import java.sql.Timestamp
import java.util.UUID

private[dashboard] trait DashboardWorkflowFixtureSupport extends ExecutionDatabaseTestSupport {
  self: MockTexeraDB =>
  protected lazy val workflowDao = new WorkflowDao(getDSLContext.configuration())

  protected def uniqueSuffix: String = UUID.randomUUID().toString.substring(0, 8)
  protected def now: Timestamp = new Timestamp(System.currentTimeMillis())

  protected def fixtureUser(uid: Int, name: String): User = {
    val user = new User
    user.setUid(uid)
    user.setName(name)
    user.setEmail(s"$name@test.com")
    user
  }

  protected def fixtureWorkflow(
      wid: Int,
      name: String,
      description: String,
      isPublic: java.lang.Boolean
  ): Workflow = {
    val workflow = new Workflow
    workflow.setWid(wid)
    workflow.setName(name)
    workflow.setContent("{}")
    workflow.setDescription(description)
    workflow.setIsPublic(isPublic)
    workflow.setCreationTime(now)
    workflow.setLastModifiedTime(now)
    workflow
  }
}

private[dashboard] trait UserDashboardFixtureSupport extends DashboardWorkflowFixtureSupport {
  self: MockTexeraDB =>
  protected lazy val userDao = new UserDao(getDSLContext.configuration())
  private lazy val datasetDao = new DatasetDao(getDSLContext.configuration())

  protected def seedDataset(uid: Int, namePrefix: String): Dataset = {
    val dataset = new Dataset
    dataset.setOwnerUid(uid)
    dataset.setName(namePrefix + uniqueSuffix)
    dataset.setRepositoryName("repo-" + uniqueSuffix)
    dataset.setIsPublic(false)
    dataset.setIsDownloadable(true)
    dataset.setDescription("")
    dataset.setCreationTime(now)
    datasetDao.insert(dataset)
    dataset
  }
}

/** Creates the accounts used by warehouse endpoint and workflow selection tests. */
private[web] object WarehouseAccountFixture {
  def seed(context: DSLContext, name: String, avatar: Option[String] = None): User = {
    val user = new User
    user.setName(name)
    user.setEmail(s"user_${UUID.randomUUID()}@example.com")
    avatar.foreach(user.setAvatar)
    new UserDao(context.configuration()).insert(user)
    user
  }
}
