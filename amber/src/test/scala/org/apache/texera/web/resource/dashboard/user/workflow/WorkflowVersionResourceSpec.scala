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

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables
import org.apache.texera.dao.jooq.generated.enums.{DefaultViewEnum, PrivilegeEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowDao,
  WorkflowOfUserDao,
  WorkflowUserAccessDao,
  WorkflowVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowOfUser,
  WorkflowUserAccess,
  WorkflowVersion
}
import org.jooq.impl.DSL
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, PrivateMethodTester}

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit
import javax.ws.rs.ForbiddenException
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class WorkflowVersionResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with PrivateMethodTester
    with MockTexeraDB {

  private val testWorkflowWid = 2000 + scala.util.Random.nextInt(1000)
  private val ownerUid = 4000 + scala.util.Random.nextInt(1000)
  private val strangerUid = 6000 + scala.util.Random.nextInt(1000)

  private var testWorkflow: Workflow = _
  private var owner: User = _
  private var stranger: User = _
  private var workflowDao: WorkflowDao = _
  private var workflowVersionDao: WorkflowVersionDao = _
  private var userDao: UserDao = _
  private var workflowOfUserDao: WorkflowOfUserDao = _
  private var workflowUserAccessDao: WorkflowUserAccessDao = _
  private var resource: WorkflowVersionResource = _

  private val capturedVersions = ArrayBuffer.empty[Integer]

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def beforeEach(): Unit = {
    workflowDao = new WorkflowDao(getDSLContext.configuration())
    workflowVersionDao = new WorkflowVersionDao(getDSLContext.configuration())
    userDao = new UserDao(getDSLContext.configuration())
    workflowOfUserDao = new WorkflowOfUserDao(getDSLContext.configuration())
    workflowUserAccessDao = new WorkflowUserAccessDao(getDSLContext.configuration())
    resource = new WorkflowVersionResource()

    testWorkflow = new Workflow
    testWorkflow.setWid(Integer.valueOf(testWorkflowWid))
    testWorkflow.setName("test_workflow_" + UUID.randomUUID().toString.substring(0, 8))
    testWorkflow.setContent(createWorkflowContent("initial"))
    testWorkflow.setDescription("test description")
    testWorkflow.setIsPublic(false)
    testWorkflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    testWorkflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))

    owner = makeUser(ownerUid, "wfv_owner")
    stranger = makeUser(strangerUid, "wfv_stranger")

    cleanupTestData()
    userDao.insert(owner)
    userDao.insert(stranger)
    workflowDao.insert(testWorkflow)

    val ownership = new WorkflowOfUser
    ownership.setUid(Integer.valueOf(ownerUid))
    ownership.setWid(Integer.valueOf(testWorkflowWid))
    workflowOfUserDao.insert(ownership)
    grantAccess(ownerUid, PrivilegeEnum.WRITE)

    capturedVersions.clear()
  }

  override protected def afterEach(): Unit = {
    cleanupTestData()
  }

  private def cleanupTestData(): Unit = {
    val ctx = getDSLContext
    // Purge the fixed test workflow plus anything our test users came to own
    // (cloneVersion creates a fresh workflow), children before parents so the
    // user rows can be removed without violating foreign keys.
    val wids = (ctx
      .select(Tables.WORKFLOW_OF_USER.WID)
      .from(Tables.WORKFLOW_OF_USER)
      .where(
        Tables.WORKFLOW_OF_USER.UID.in(Integer.valueOf(ownerUid), Integer.valueOf(strangerUid))
      )
      .fetchInto(classOf[Integer])
      .asScala
      .toSet + Integer.valueOf(testWorkflowWid)).toList

    ctx
      .deleteFrom(Tables.WORKFLOW_VERSION)
      .where(Tables.WORKFLOW_VERSION.WID.in(wids: _*))
      .execute()
    ctx
      .deleteFrom(Tables.WORKFLOW_USER_ACCESS)
      .where(Tables.WORKFLOW_USER_ACCESS.WID.in(wids: _*))
      .execute()
    ctx
      .deleteFrom(Tables.WORKFLOW_OF_USER)
      .where(Tables.WORKFLOW_OF_USER.WID.in(wids: _*))
      .execute()
    ctx.deleteFrom(Tables.WORKFLOW).where(Tables.WORKFLOW.WID.in(wids: _*)).execute()
    ctx
      .deleteFrom(Tables.USER)
      .where(Tables.USER.UID.in(Integer.valueOf(ownerUid), Integer.valueOf(strangerUid)))
      .execute()
  }

  override protected def afterAll(): Unit = {
    closeConnectionPool()
  }

  private def makeUser(uid: Int, name: String): User = {
    val user = new User
    user.setUid(Integer.valueOf(uid))
    user.setName(name)
    user.setEmail(s"$name@test.com")
    user
  }

  private def grantAccess(uid: Int, privilege: PrivilegeEnum): Unit = {
    val access = new WorkflowUserAccess
    access.setUid(Integer.valueOf(uid))
    access.setWid(Integer.valueOf(testWorkflowWid))
    access.setPrivilege(privilege)
    workflowUserAccessDao.insert(access)
  }

  private def session(user: User): SessionUser = new SessionUser(user)

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

  /** A patch whose only op is an `add`, i.e. a semantically important change. */
  private def createAddDiff(): String = {
    val before = objectMapper.createObjectNode()
    val after = objectMapper.createObjectNode()
    after.put("addedKey", "x")
    com.flipkart.zjsonpatch.JsonDiff.asJson(before, after).toString
  }

  private def seedVersion(content: String, minutesAgo: Int): Unit = {
    val version = new WorkflowVersion
    version.setWid(Integer.valueOf(testWorkflowWid))
    version.setContent(content)
    version.setCreationTime(
      new Timestamp(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(minutesAgo))
    )
    workflowVersionDao.insert(version)
  }

  /** The vids of the test workflow's versions, ascending (creation order). */
  private def versionVids(): List[Integer] = {
    getDSLContext
      .select(Tables.WORKFLOW_VERSION.VID)
      .from(Tables.WORKFLOW_VERSION)
      .where(Tables.WORKFLOW_VERSION.WID.eq(Integer.valueOf(testWorkflowWid)))
      .orderBy(Tables.WORKFLOW_VERSION.VID.asc())
      .fetchInto(classOf[Integer])
      .asScala
      .toList
  }

  private def versionCount(): Int = versionVids().size

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

  it should "reconstruct an older content by applying a stored delta (applyPatch, no DB)" in {
    val base = new Workflow
    base.setWid(Integer.valueOf(testWorkflowWid))
    base.setContent(createWorkflowContent("new"))

    val delta = new WorkflowVersion
    // a delta that turns "new" back into "old"
    delta.setContent(createVersionDiff("new", "old"))
    delta.setCreationTime(new Timestamp(System.currentTimeMillis()))

    val result = WorkflowVersionResource.applyPatch(List(delta), base)

    objectMapper.readTree(result.getContent).get("value").asText() shouldBe "old"
  }

  "isSnapshotInRangeUnimportant" should "return true when the bounds are equal" in {
    WorkflowVersionResource.isSnapshotInRangeUnimportant(7, 7, testWorkflowWid) shouldBe true
  }

  it should "return true when every delta in range is positional-only (replace)" in {
    seedVersion(createVersionDiff("a", "b"), minutesAgo = 2)
    seedVersion(createVersionDiff("b", "c"), minutesAgo = 1)
    val vids = versionVids()

    WorkflowVersionResource.isSnapshotInRangeUnimportant(
      vids.head,
      vids.last,
      testWorkflowWid
    ) shouldBe true
  }

  it should "return false when a delta in range is an important (non-replace) change" in {
    seedVersion(createVersionDiff("a", "b"), minutesAgo = 2)
    seedVersion(createAddDiff(), minutesAgo = 1)
    val vids = versionVids()

    WorkflowVersionResource.isSnapshotInRangeUnimportant(
      vids.head,
      vids.last,
      testWorkflowWid
    ) shouldBe false
  }

  "insertNewVersion" should "persist a retrievable version with the given content" in {
    val inserted = WorkflowVersionResource.insertNewVersion(testWorkflowWid, "[]")

    val fetched = workflowVersionDao.fetchOneByVid(inserted.getVid)
    fetched should not be null
    fetched.getWid shouldBe Integer.valueOf(testWorkflowWid)
    fetched.getContent shouldBe "[]"
  }

  "getLatestVersion" should "return the highest vid when versions exist" in {
    seedVersion(createVersionDiff("a", "b"), minutesAgo = 2)
    seedVersion(createVersionDiff("b", "c"), minutesAgo = 1)
    val vids = versionVids()

    WorkflowVersionResource.getLatestVersion(testWorkflowWid) shouldBe vids.max
  }

  it should "create and return a version when the workflow has none yet" in {
    versionCount() shouldBe 0

    val vid = WorkflowVersionResource.getLatestVersion(testWorkflowWid)

    versionCount() shouldBe 1
    workflowVersionDao.fetchOneByVid(vid) should not be null
  }

  "insertVersion" should "always create a version for a new workflow" in {
    WorkflowVersionResource.insertVersion(testWorkflow, insertingNewWorkflow = true)
    versionCount() shouldBe 1
  }

  it should "create a version only when the content actually changed" in {
    // unchanged: the passed content equals what is already stored -> empty patch -> no version
    WorkflowVersionResource.insertVersion(testWorkflow, insertingNewWorkflow = false)
    versionCount() shouldBe 0

    // changed: a different content produces a non-empty patch -> one version
    val edited = new Workflow
    edited.setWid(Integer.valueOf(testWorkflowWid))
    edited.setContent(createWorkflowContent("changed"))
    WorkflowVersionResource.insertVersion(edited, insertingNewWorkflow = false)
    versionCount() shouldBe 1
  }

  "retrieveVersionsOfWorkflow" should "return the importance-encoded versions for a user with access" in {
    seedVersion(createVersionDiff("a", "b"), minutesAgo = 2)
    seedVersion(createVersionDiff("b", "c"), minutesAgo = 1)

    val versions = resource.retrieveVersionsOfWorkflow(testWorkflowWid, session(owner))

    versions should have size 2
    // the latest version is always marked important
    versions.head.importance shouldBe true
  }

  it should "return an empty list for a user without read access" in {
    seedVersion(createVersionDiff("a", "b"), minutesAgo = 1)

    resource.retrieveVersionsOfWorkflow(testWorkflowWid, session(stranger)) shouldBe empty
  }

  "retrieveWorkflowVersion" should "reconstruct the workflow at a version for a user with access" in {
    // an empty delta leaves the content unchanged, so the reconstructed content
    // is exactly the workflow's current content
    val version = WorkflowVersionResource.insertNewVersion(testWorkflowWid, "[]")

    val result = resource.retrieveWorkflowVersion(testWorkflowWid, version.getVid, session(owner))

    result.getContent shouldBe testWorkflow.getContent
  }

  it should "throw ForbiddenException for a user without read access" in {
    val version = WorkflowVersionResource.insertNewVersion(testWorkflowWid, "[]")

    assertThrows[ForbiddenException] {
      resource.retrieveWorkflowVersion(testWorkflowWid, version.getVid, session(stranger))
    }
  }

  "cloneVersion" should "create a new workflow from an existing version" in {
    // cloneVersion re-ids operators, so the version must reconstruct to a real
    // workflow document; an empty delta keeps the workflow's own content.
    val workflowContent =
      """{"operators":[{"operatorID":"CSVFileScan-operator-a","operatorType":"CSVFileScan"}],"links":[]}"""
    testWorkflow.setContent(workflowContent)
    workflowDao.update(testWorkflow)
    val version = WorkflowVersionResource.insertNewVersion(testWorkflowWid, "[]")

    val newWid = resource.cloneVersion(
      version.getVid,
      session(owner),
      Map("displayedVersionId" -> 1).asJava
    )

    newWid should not be null
    newWid should not be Integer.valueOf(testWorkflowWid)
    val cloned = workflowDao.fetchOneByWid(newWid)
    cloned should not be null
    cloned.getName should include("_copy")
  }

  it should "inherit the source workflow's default view" in {
    val workflowContent =
      """{"operators":[{"operatorID":"CSVFileScan-operator-a","operatorType":"CSVFileScan"}],"links":[]}"""
    testWorkflow.setContent(workflowContent)
    testWorkflow.setDefaultView(DefaultViewEnum.FORM)
    workflowDao.update(testWorkflow)
    val version = WorkflowVersionResource.insertNewVersion(testWorkflowWid, "[]")

    val newWid = resource.cloneVersion(
      version.getVid,
      session(owner),
      Map("displayedVersionId" -> 1).asJava
    )

    workflowDao.fetchOneByWid(newWid).getDefaultView shouldBe DefaultViewEnum.FORM
  }

  it should "leave the clone defaulting to canvas when the source does" in {
    val workflowContent =
      """{"operators":[{"operatorID":"CSVFileScan-operator-a","operatorType":"CSVFileScan"}],"links":[]}"""
    testWorkflow.setContent(workflowContent)
    workflowDao.update(testWorkflow)
    val version = WorkflowVersionResource.insertNewVersion(testWorkflowWid, "[]")

    val newWid = resource.cloneVersion(
      version.getVid,
      session(owner),
      Map("displayedVersionId" -> 1).asJava
    )

    workflowDao.fetchOneByWid(newWid).getDefaultView shouldBe DefaultViewEnum.CANVAS
  }

  // ─── version-importance helpers (pure JSON/timestamp logic) ────────────────

  private val isSnapshotImportant = PrivateMethod[Boolean](Symbol("isSnapshotImportant"))
  private val isVersionImportant = PrivateMethod[Boolean](Symbol("isVersionImportant"))
  private val isWithinTimeLimit = PrivateMethod[Boolean](Symbol("isWithinTimeLimit"))
  private val encodeVersionImportance =
    PrivateMethod[List[WorkflowVersionResource.VersionEntry]](Symbol("encodeVersionImportance"))

  "isSnapshotImportant" should "treat a patch whose ops are all 'replace' as unimportant" in {
    val content = """[{"op":"replace","path":"/operatorPositions/x","value":1}]"""
    (WorkflowVersionResource invokePrivate isSnapshotImportant(content)) shouldBe false
  }

  it should "treat a patch containing any non-replace op as important" in {
    val content = """[{"op":"replace","path":"/a"},{"op":"add","path":"/operators/0"}]"""
    (WorkflowVersionResource invokePrivate isSnapshotImportant(content)) shouldBe true
  }

  it should "treat an empty patch as unimportant" in {
    (WorkflowVersionResource invokePrivate isSnapshotImportant("[]")) shouldBe false
  }

  "isVersionImportant" should "treat a patch touching only operator positions as unimportant" in {
    val content = """[{"op":"replace","path":"/operatorPositions/op-1/x","value":5}]"""
    (WorkflowVersionResource invokePrivate isVersionImportant(content)) shouldBe false
  }

  it should "treat a patch touching anything else as important" in {
    val content = """[{"op":"replace","path":"/operators/0/properties","value":{}}]"""
    (WorkflowVersionResource invokePrivate isVersionImportant(content)) shouldBe true
  }

  "isWithinTimeLimit" should "hold for timestamps closer together than the aggregate limit" in {
    val later = new Timestamp(1_700_000_000_000L)
    val earlier = new Timestamp(later.getTime - TimeUnit.SECONDS.toMillis(1))
    (WorkflowVersionResource invokePrivate isWithinTimeLimit(later, earlier)) shouldBe true
  }

  it should "not hold once the gap exceeds the aggregate limit" in {
    val later = new Timestamp(1_700_000_000_000L)
    val earlier = new Timestamp(later.getTime - TimeUnit.DAYS.toMillis(30))
    (WorkflowVersionResource invokePrivate isWithinTimeLimit(later, earlier)) shouldBe false
  }

  "encodeVersionImportance" should "always mark the latest version important and aggregate close ones" in {
    val base = 1_700_000_000_000L
    def version(vid: Int, offsetMillis: Long, content: String): WorkflowVersion = {
      val v = new WorkflowVersion
      v.setVid(vid)
      v.setWid(testWorkflowWid)
      v.setContent(content)
      v.setCreationTime(new Timestamp(base - offsetMillis))
      v
    }

    val positional = """[{"op":"replace","path":"/operatorPositions/op-1/x","value":5}]"""
    val meaningful = """[{"op":"replace","path":"/operators/0/properties","value":{}}]"""

    val encoded = WorkflowVersionResource invokePrivate encodeVersionImportance(
      List(
        version(3, 0L, positional), // latest — important regardless of content
        version(2, TimeUnit.SECONDS.toMillis(1), meaningful), // within the aggregate window
        version(1, TimeUnit.DAYS.toMillis(30), meaningful) // outside it, judged on content
      )
    )

    encoded.map(_.vId) shouldBe List(3, 2, 1)
    encoded.map(_.importance) shouldBe List(true, false, true)
  }
}
