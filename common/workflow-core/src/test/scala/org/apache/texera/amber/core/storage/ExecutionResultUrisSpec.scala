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

package org.apache.texera.amber.core.storage

import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
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
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.net.URI
import java.sql.Timestamp
import java.util.UUID

class ExecutionResultUrisSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val testWorkflowWid = 3000 + scala.util.Random.nextInt(1000)
  private val testUserId = 1000 + scala.util.Random.nextInt(1000)

  private var testVersion: WorkflowVersion = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def beforeEach(): Unit = {
    cleanupTestData()

    val testUser = new User
    testUser.setUid(testUserId)
    testUser.setName("test_user")
    testUser.setEmail("test@example.com")
    new UserDao(getDSLContext.configuration()).insert(testUser)

    val testWorkflow = new Workflow
    testWorkflow.setWid(testWorkflowWid)
    testWorkflow.setName("test_workflow_" + UUID.randomUUID().toString.substring(0, 8))
    testWorkflow.setContent("{}")
    testWorkflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    testWorkflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    new WorkflowDao(getDSLContext.configuration()).insert(testWorkflow)

    testVersion = new WorkflowVersion
    testVersion.setWid(testWorkflowWid)
    testVersion.setContent("{}")
    testVersion.setCreationTime(new Timestamp(System.currentTimeMillis()))
    new WorkflowVersionDao(getDSLContext.configuration()).insert(testVersion)
  }

  override protected def afterEach(): Unit = {
    cleanupTestData()
  }

  override protected def afterAll(): Unit = {
    closeConnectionPool()
  }

  // Child rows before parents: port results, executions, versions, the workflow, the user.
  private def cleanupTestData(): Unit = {
    val vids = getDSLContext
      .select(WORKFLOW_VERSION.VID)
      .from(WORKFLOW_VERSION)
      .where(WORKFLOW_VERSION.WID.eq(testWorkflowWid))
    getDSLContext
      .deleteFrom(OPERATOR_PORT_EXECUTIONS)
      .where(
        OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.in(
          getDSLContext
            .select(WORKFLOW_EXECUTIONS.EID)
            .from(WORKFLOW_EXECUTIONS)
            .where(WORKFLOW_EXECUTIONS.VID.in(vids))
        )
      )
      .execute()
    getDSLContext.deleteFrom(WORKFLOW_EXECUTIONS).where(WORKFLOW_EXECUTIONS.VID.in(vids)).execute()
    getDSLContext
      .deleteFrom(WORKFLOW_VERSION)
      .where(WORKFLOW_VERSION.WID.eq(testWorkflowWid))
      .execute()
    getDSLContext.deleteFrom(WORKFLOW).where(WORKFLOW.WID.eq(testWorkflowWid)).execute()
    getDSLContext.deleteFrom(USER).where(USER.UID.eq(testUserId)).execute()
  }

  private def insertExecution(): WorkflowExecutions = {
    val execution = new WorkflowExecutions
    execution.setVid(testVersion.getVid)
    execution.setUid(testUserId)
    execution.setEnvironmentVersion("test-env-1.0")
    new WorkflowExecutionsDao(getDSLContext.configuration()).insert(execution)
    execution
  }

  private def insertOperatorPortResult(
      eid: ExecutionIdentity,
      globalPortId: GlobalPortIdentity,
      uri: URI
  ): Unit =
    getDSLContext
      .insertInto(OPERATOR_PORT_EXECUTIONS)
      .columns(
        OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID,
        OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID,
        OPERATOR_PORT_EXECUTIONS.RESULT_URI
      )
      .values(Integer.valueOf(eid.id.toInt), globalPortId.serializeAsString, uri.toString)
      .execute()

  "getResultUrisByExecutionId" should "return inserted URIs and filter out null/empty entries" in {
    val execution = insertExecution()
    val eid = ExecutionIdentity(execution.getEid.longValue())
    val opA = GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity("opA"), "main"),
      PortIdentity(),
      input = false
    )
    val opB = GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity("opB"), "main"),
      PortIdentity(),
      input = false
    )
    val opC = GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity("opC"), "main"),
      PortIdentity(),
      input = false
    )

    insertOperatorPortResult(eid, opA, URI.create("vfs:///A"))
    insertOperatorPortResult(eid, opB, URI.create("vfs:///B"))
    // Empty-string URI row — the helper should drop it from the returned list.
    getDSLContext
      .insertInto(OPERATOR_PORT_EXECUTIONS)
      .columns(
        OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID,
        OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID,
        OPERATOR_PORT_EXECUTIONS.RESULT_URI
      )
      .values(execution.getEid, opC.serializeAsString, "")
      .execute()

    val uris = ExecutionResultUris.getResultUrisByExecutionId(eid)
    assert(uris.toSet == Set(URI.create("vfs:///A"), URI.create("vfs:///B")))
  }

  "getResultUriByLogicalPortId" should "match by logical operator id, port id, and resource type" in {
    val execution = insertExecution()
    val eid = ExecutionIdentity(execution.getEid.longValue())
    val wfId = WorkflowIdentity(testWorkflowWid.longValue())

    // Build a real VFS result URI that decodeURI can parse.
    val targetOpId = OperatorIdentity("target-op")
    val targetPortId = PortIdentity()
    val targetGlobalPort = GlobalPortIdentity(
      PhysicalOpIdentity(targetOpId, "main"),
      targetPortId,
      input = false
    )
    val targetUri = VFSURIFactory.resultURI(
      VFSURIFactory.createPortBaseURI(wfId, eid, targetGlobalPort)
    )
    insertOperatorPortResult(eid, targetGlobalPort, targetUri)

    // Distractor: same workflow, different op id.
    val otherGlobalPort = GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity("other-op"), "main"),
      PortIdentity(),
      input = false
    )
    val otherUri = VFSURIFactory.resultURI(
      VFSURIFactory.createPortBaseURI(wfId, eid, otherGlobalPort)
    )
    insertOperatorPortResult(eid, otherGlobalPort, otherUri)

    val found =
      ExecutionResultUris.getResultUriByLogicalPortId(eid, targetOpId, targetPortId)
    assert(found.contains(targetUri))

    // Sanity-check: the decoded URI is RESULT-typed and matches the target ids.
    val components = VFSURIFactory.decodeURI(found.get)
    assert(components.resourceType == VFSResourceType.RESULT)
    assert(
      components.globalPortId
        .exists(gp => gp.opId.logicalOpId == targetOpId && gp.portId == targetPortId)
    )
  }

  it should "return None when no URI matches the requested op/port" in {
    val execution = insertExecution()
    val eid = ExecutionIdentity(execution.getEid.longValue())
    val found =
      ExecutionResultUris.getResultUriByLogicalPortId(
        eid,
        OperatorIdentity("nope"),
        PortIdentity()
      )
    assert(found.isEmpty)
  }
}
