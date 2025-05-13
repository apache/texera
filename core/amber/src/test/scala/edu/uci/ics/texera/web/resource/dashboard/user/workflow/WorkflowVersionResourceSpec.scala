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

package edu.uci.ics.texera.web.resource.dashboard.user.workflow

import edu.uci.ics.amber.engine.common.Utils.objectMapper
import edu.uci.ics.texera.dao.MockTexeraDB
import edu.uci.ics.texera.dao.jooq.generated.Tables
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{WorkflowDao, WorkflowVersionDao}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{Workflow, WorkflowVersion}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.jooq.impl.DSL

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer

class WorkflowVersionResourceSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val testWorkflowWid = 2000 + scala.util.Random.nextInt(1000)

  private var testWorkflow: Workflow = _
  private var workflowDao: WorkflowDao = _
  private var workflowVersionDao: WorkflowVersionDao = _

  private val capturedVersions = ArrayBuffer.empty[Integer]

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def beforeEach(): Unit = {
    testWorkflow = new Workflow
    testWorkflow.setWid(Integer.valueOf(testWorkflowWid))
    testWorkflow.setName("test_workflow_" + UUID.randomUUID().toString.substring(0, 8))
    testWorkflow.setContent(createWorkflowContent("initial"))
    testWorkflow.setDescription("test description")

    workflowDao = new WorkflowDao(getDSLContext.configuration())
    workflowVersionDao = new WorkflowVersionDao(getDSLContext.configuration())

    cleanupTestData()
    workflowDao.insert(testWorkflow)
    capturedVersions.clear()
  }

  override protected def afterEach(): Unit = {
    cleanupTestData()
  }

  private def cleanupTestData(): Unit = {
    getDSLContext
      .deleteFrom(Tables.WORKFLOW_VERSION)
      .where(Tables.WORKFLOW_VERSION.WID.eq(testWorkflowWid))
      .execute()

    getDSLContext
      .deleteFrom(Tables.WORKFLOW)
      .where(Tables.WORKFLOW.WID.eq(testWorkflowWid))
      .execute()
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  private def createWorkflowContent(value: String): String = {
    val jsonNode = objectMapper.createObjectNode()
    jsonNode.put("value", value)
    jsonNode.toString
  }

  private def createVersionDiff(oldValue: String, newValue: String): String = {
    val oldJson = objectMapper.createObjectNode()
    oldJson.put("value", oldValue)

    val newJson = objectMapper.createObjectNode()
    newJson.put("value", newValue)

    val patch = com.flipkart.zjsonpatch.JsonDiff.asJson(
      oldJson,
      newJson
    )
    patch.toString
  }

  "WorkflowVersionResource" should "return versions in descending order from fetchSubsequentVersions and apply patches correctly" in {
    var currentContent = "initial"
    for (i <- 1 to 10) {
      val newContent = s"version_$i"
      val diffContent = createVersionDiff(currentContent, newContent)

      val version = new WorkflowVersion
      version.setWid(testWorkflow.getWid)
      version.setContent(diffContent)
      version.setCreationTime(
        new Timestamp(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(10 - i))
      )
      workflowVersionDao.insert(version)

      currentContent = newContent
    }

    testWorkflow.setContent(createWorkflowContent(currentContent))
    workflowDao.update(testWorkflow)

    val midVersionId = 5
    val versions = WorkflowVersionResource.fetchSubsequentVersions(
      testWorkflow.getWid,
      midVersionId,
      getDSLContext
    )

    assert(versions.nonEmpty, "No versions were returned")

    for (i <- 0 until versions.length - 1) {
      assert(
        versions(i).getVid > versions(i + 1).getVid,
        s"Versions not in descending order: ${versions(i).getVid} should be > ${versions(i + 1).getVid}"
      )
    }

    val highestVersionId = getDSLContext
      .select(DSL.max(Tables.WORKFLOW_VERSION.VID))
      .from(Tables.WORKFLOW_VERSION)
      .where(Tables.WORKFLOW_VERSION.WID.eq(testWorkflowWid))
      .fetchOneInto(classOf[Integer])

    assert(versions.head.getVid === highestVersionId, "First version should have the highest VID")

    capturedVersions.clear()
    versions.foreach(v => capturedVersions.append(v.getVid))

    val workflowFromDb = workflowDao.fetchOneByWid(testWorkflow.getWid)

    val workflowVersionDirect = WorkflowVersionResource.applyPatch(versions, workflowFromDb)
    val directVersionContent =
      objectMapper.readTree(workflowVersionDirect.getContent).get("value").asText()

    assert(
      directVersionContent === s"version_$midVersionId",
      s"Workflow content from direct applyPatch should be 'version_$midVersionId' but was '$directVersionContent'"
    )

    val combinedVersions = WorkflowVersionResource.fetchSubsequentVersions(
      testWorkflow.getWid,
      midVersionId,
      getDSLContext
    )
    val currentWorkflowForCombined = workflowDao.fetchOneByWid(testWorkflow.getWid)
    val workflowVersion =
      WorkflowVersionResource.applyPatch(combinedVersions, currentWorkflowForCombined)

    assert(capturedVersions.nonEmpty, "No versions were captured")
    assert(
      capturedVersions.length === versions.length,
      "Captured versions length doesn't match fetched versions"
    )

    for (i <- versions.indices) {
      assert(
        capturedVersions(i) === versions(i).getVid,
        s"Captured version ${capturedVersions(i)} doesn't match fetched version ${versions(i).getVid} at index $i"
      )
    }

    val midVersionContent = objectMapper.readTree(workflowVersion.getContent).get("value").asText()
    assert(
      midVersionContent === s"version_$midVersionId",
      s"Workflow content should be 'version_$midVersionId' but was '$midVersionContent'"
    )
  }
}
