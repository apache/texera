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

package org.apache.texera.web.resource.dashboard.user.workflow

import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import org.apache.texera.amber.core.workflow.{GlobalPortIdentity, PortIdentity}
import org.apache.texera.amber.util.serde.GlobalPortIdentitySerde.SerdeOps
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowDao,
  WorkflowExecutionsDao,
  WorkflowVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowExecutions,
  WorkflowVersion
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, PrivateMethodTester}

import java.net.URI
import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer

class WorkflowExecutionsResourceSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB
    with PrivateMethodTester {

  private val testWorkflowWid = 3000 + scala.util.Random.nextInt(1000)
  private val testUserId = 1000 + scala.util.Random.nextInt(1000)

  private var testWorkflow: Workflow = _
  private var testVersion: WorkflowVersion = _
  private var testUser: User = _
  private var userDao: UserDao = _
  private var workflowDao: WorkflowDao = _
  private var workflowVersionDao: WorkflowVersionDao = _
  private var workflowExecutionsDao: WorkflowExecutionsDao = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def beforeEach(): Unit = {
    testUser = new User
    testUser.setUid(testUserId)
    testUser.setName("test_user")
    testUser.setEmail("test@example.com")
    testUser.setPassword("password")
    testUser.setGoogleAvatar("avatar_url")

    testWorkflow = new Workflow
    testWorkflow.setWid(testWorkflowWid)
    testWorkflow.setName("test_workflow_" + UUID.randomUUID().toString.substring(0, 8))
    testWorkflow.setContent("{}")
    testWorkflow.setDescription("test description")
    testWorkflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    testWorkflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))

    testVersion = new WorkflowVersion
    testVersion.setWid(testWorkflowWid)
    testVersion.setContent("{}")
    testVersion.setCreationTime(new Timestamp(System.currentTimeMillis()))

    workflowDao = new WorkflowDao(getDSLContext.configuration())
    workflowVersionDao = new WorkflowVersionDao(getDSLContext.configuration())
    userDao = new UserDao(getDSLContext.configuration())
    workflowExecutionsDao = new WorkflowExecutionsDao(getDSLContext.configuration())

    cleanupTestData()

    userDao.insert(testUser)
    workflowDao.insert(testWorkflow)
    workflowVersionDao.insert(testVersion)
  }

  override protected def afterEach(): Unit = {
    cleanupTestData()
  }

  private def cleanupTestData(): Unit = {
    getDSLContext
      .deleteFrom(WORKFLOW_EXECUTIONS)
      .where(
        WORKFLOW_EXECUTIONS.VID.in(
          getDSLContext
            .select(WORKFLOW_VERSION.VID)
            .from(WORKFLOW_VERSION)
            .where(WORKFLOW_VERSION.WID.eq(testWorkflowWid))
        )
      )
      .execute()

    getDSLContext
      .deleteFrom(WORKFLOW_VERSION)
      .where(WORKFLOW_VERSION.WID.eq(testWorkflowWid))
      .execute()

    getDSLContext
      .deleteFrom(WORKFLOW)
      .where(WORKFLOW.WID.eq(testWorkflowWid))
      .execute()

    getDSLContext
      .deleteFrom(USER)
      .where(USER.UID.eq(testUserId))
      .execute()
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  "WorkflowExecutionsResource.getWorkflowExecutions" should "return executions with EIDs in descending order" in {
    val numExecutions = 10
    val executionIds = ArrayBuffer.empty[Integer]

    for (i <- 1 to numExecutions) {
      val execution = new WorkflowExecutions
      execution.setVid(testVersion.getVid)
      execution.setUid(testUser.getUid)
      execution.setStatus(0.toByte)
      execution.setResult("")
      execution.setStartingTime(
        new Timestamp(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(numExecutions - i))
      )
      execution.setBookmarked(false)
      execution.setName(s"Execution ${i}")
      execution.setEnvironmentVersion("test-env-1.0")

      workflowExecutionsDao.insert(execution)
      executionIds.append(execution.getEid)
    }

    val result = WorkflowExecutionsResource.getWorkflowExecutions(testWorkflowWid, getDSLContext)

    assert(result.nonEmpty, "Result should not be empty")
    assert(
      result.size == numExecutions,
      s"Expected $numExecutions executions, but got ${result.size}"
    )

    for (i <- 0 until result.size - 1) {
      assert(
        result(i).eId > result(i + 1).eId,
        s"Executions are not in descending order: ${result(i).eId} should be > ${result(i + 1).eId}"
      )
    }

    val returnedIds = result.map(_.eId).toSet
    assert(
      executionIds.toSet.subsetOf(returnedIds),
      "All inserted execution IDs should be returned"
    )
  }

  "WorkflowExecutionsResource.insertOperatorPortResultUri" should "insert a result URI row" in {
    val execution = new WorkflowExecutions
    execution.setVid(testVersion.getVid)
    execution.setUid(testUser.getUid)
    execution.setStatus(0.toByte)
    execution.setResult("")
    execution.setStartingTime(new Timestamp(System.currentTimeMillis()))
    execution.setBookmarked(false)
    execution.setName("Execution with duplicate result URI insert")
    execution.setEnvironmentVersion("test-env-1.0")
    workflowExecutionsDao.insert(execution)

    val executionId = ExecutionIdentity(execution.getEid.longValue())
    val globalPortId = GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity("operator-1"), "main"),
      PortIdentity(),
      input = false
    )
    val uri = URI.create("vfs:///test-result")

    WorkflowExecutionsResource.insertOperatorPortResultUri(executionId, globalPortId, uri)

    val rows = getDSLContext
      .selectFrom(OPERATOR_PORT_EXECUTIONS)
      .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(execution.getEid))
      .and(OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID.eq(globalPortId.serializeAsString))
      .fetch()

    assert(rows.size() == 1)
    assert(rows.get(0).getResultUri == uri.toString)
  }

  // --- compareOperatorPortStructure ---------------------------------------

  private def insertExecutionForTestWorkflow(name: String): WorkflowExecutions = {
    val execution = new WorkflowExecutions
    execution.setVid(testVersion.getVid)
    execution.setUid(testUser.getUid)
    execution.setStatus(0.toByte)
    execution.setResult("")
    execution.setStartingTime(new Timestamp(System.currentTimeMillis()))
    execution.setBookmarked(false)
    execution.setName(name)
    execution.setEnvironmentVersion("test-env-1.0")
    workflowExecutionsDao.insert(execution)
    execution
  }

  private def insertPortRow(
      eid: Integer,
      logicalOpId: String,
      portId: Int,
      internal: Boolean = false,
      isInput: Boolean = false
  ): GlobalPortIdentity = {
    val gpi = GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity(logicalOpId), "main"),
      PortIdentity(id = portId, internal = internal),
      input = isInput
    )
    WorkflowExecutionsResource.insertOperatorPortResultUri(
      ExecutionIdentity(eid.longValue()),
      gpi,
      URI.create(s"vfs:///wid/${testWorkflowWid}/eid/${eid}/op/${logicalOpId}/port/${portId}")
    )
    gpi
  }

  "WorkflowExecutionsResource.compareOperatorPortStructure" should
    "classify ports as shared, onlyInA, or onlyInB by logical op id and external port id" in {
    val execA = insertExecutionForTestWorkflow("execA")
    val execB = insertExecutionForTestWorkflow("execB")

    // shared operator with two external output ports
    insertPortRow(execA.getEid, "op-shared", 0)
    insertPortRow(execA.getEid, "op-shared", 1)
    insertPortRow(execB.getEid, "op-shared", 0)
    insertPortRow(execB.getEid, "op-shared", 1)

    // only on A
    insertPortRow(execA.getEid, "op-only-a", 0)

    // only on B
    insertPortRow(execB.getEid, "op-only-b", 0)

    val entries = WorkflowExecutionsResource.compareOperatorPortStructure(
      ExecutionIdentity(execA.getEid.longValue()),
      ExecutionIdentity(execB.getEid.longValue())
    )

    // Granularity is (operator, external output port)
    val keyed = entries.map(e => (e.operatorId, e.portId) -> e.status).toMap
    assert(keyed.size == 4, s"expected 4 entries, got ${keyed.size}: $entries")
    assert(keyed(("op-shared", 0)) == "shared")
    assert(keyed(("op-shared", 1)) == "shared")
    assert(keyed(("op-only-a", 0)) == "onlyInA")
    assert(keyed(("op-only-b", 0)) == "onlyInB")

    // shared entries carry both URIs; one-sided entries carry only their own
    val sharedP0 = entries.find(e => e.operatorId == "op-shared" && e.portId == 0).get
    assert(sharedP0.resultUriA.isDefined && sharedP0.resultUriB.isDefined)

    val onlyA = entries.find(_.operatorId == "op-only-a").get
    assert(onlyA.resultUriA.isDefined && onlyA.resultUriB.isEmpty)

    val onlyB = entries.find(_.operatorId == "op-only-b").get
    assert(onlyB.resultUriA.isEmpty && onlyB.resultUriB.isDefined)
  }

  it should "filter out internal ports and input ports" in {
    val execA = insertExecutionForTestWorkflow("execA-filter")
    val execB = insertExecutionForTestWorkflow("execB-filter")

    // valid external output that should appear
    insertPortRow(execA.getEid, "op-x", 0)
    insertPortRow(execB.getEid, "op-x", 0)

    // internal ports - should be ignored
    insertPortRow(execA.getEid, "op-x", 5, internal = true)
    insertPortRow(execB.getEid, "op-x", 5, internal = true)

    // input ports - should be ignored
    insertPortRow(execA.getEid, "op-x", 6, isInput = true)
    insertPortRow(execB.getEid, "op-x", 6, isInput = true)

    val entries = WorkflowExecutionsResource.compareOperatorPortStructure(
      ExecutionIdentity(execA.getEid.longValue()),
      ExecutionIdentity(execB.getEid.longValue())
    )

    assert(entries.size == 1, s"only the external output port should survive, got: $entries")
    assert(entries.head.operatorId == "op-x" && entries.head.portId == 0)
    assert(entries.head.status == "shared")
  }

  it should "return an empty list when neither execution has any port entries" in {
    val execA = insertExecutionForTestWorkflow("execA-empty")
    val execB = insertExecutionForTestWorkflow("execB-empty")

    val entries = WorkflowExecutionsResource.compareOperatorPortStructure(
      ExecutionIdentity(execA.getEid.longValue()),
      ExecutionIdentity(execB.getEid.longValue())
    )

    assert(entries.isEmpty)
  }

}
