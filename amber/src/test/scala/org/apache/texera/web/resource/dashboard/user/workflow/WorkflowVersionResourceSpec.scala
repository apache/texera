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
import org.apache.texera.dao.jooq.generated.tables.daos.{UserDao, WorkflowDao, WorkflowVersionDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{User, Workflow, WorkflowVersion}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, PrivateMethodTester}

import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import javax.ws.rs.ForbiddenException
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
  private val owner = makeUser(ownerUid, "wfv_owner")
  private val stranger = makeUser(strangerUid, "wfv_stranger")
  private lazy val workflowDao = new WorkflowDao(getDSLContext.configuration())
  private lazy val workflowVersionDao = new WorkflowVersionDao(getDSLContext.configuration())
  private lazy val resource = new WorkflowVersionResource()

  override protected def beforeAll(): Unit = initializeDBAndReplaceDSLContext()
  override protected def afterAll(): Unit = closeConnectionPool()

  override protected def beforeEach(): Unit = {
    val db = getDSLContext
    new UserDao(db.configuration()).insert(owner, stranger)
    val w = Tables.WORKFLOW
    testWorkflow = db
      .insertInto(w)
      .set(w.WID, Int.box(testWorkflowWid))
      .set(w.NAME, s"test_workflow_$testWorkflowWid")
      .set(w.DESCRIPTION, "test description")
      .set(w.CONTENT, createWorkflowContent("initial"))
      .returning()
      .fetchOneInto(classOf[Workflow])
    val ownership = Tables.WORKFLOW_OF_USER
    db.insertInto(ownership, ownership.UID, ownership.WID)
      .values(owner.getUid, testWorkflow.getWid)
      .execute()
    grantAccess(ownerUid, PrivilegeEnum.WRITE)
  }

  override protected def afterEach(): Unit = {
    // The fixture owns the source and any revision clones. Their access and version rows cascade.
    val db = getDSLContext
    val ownership = Tables.WORKFLOW_OF_USER
    val owned = db
      .select(ownership.WID)
      .from(ownership)
      .where(ownership.UID.in(owner.getUid, stranger.getUid))
    db.deleteFrom(Tables.WORKFLOW)
      .where(Tables.WORKFLOW.WID.eq(testWorkflowWid).or(Tables.WORKFLOW.WID.in(owned)))
      .execute()
    db.deleteFrom(Tables.USER).where(Tables.USER.UID.in(owner.getUid, stranger.getUid)).execute()
  }

  private def makeUser(uid: Int, name: String): User = {
    val user = new User
    user.setUid(Integer.valueOf(uid))
    user.setName(name)
    user.setEmail(s"$name@test.com")
    user
  }

  private def grantAccess(uid: Int, privilege: PrivilegeEnum): Unit = {
    val access = Tables.WORKFLOW_USER_ACCESS
    getDSLContext
      .insertInto(access, access.UID, access.WID, access.PRIVILEGE)
      .values(Int.box(uid), testWorkflow.getWid, privilege)
      .execute()
  }

  private def session(user: User): SessionUser = new SessionUser(user)

  private def versionValue(value: String) = objectMapper.createObjectNode().put("value", value)
  private def createWorkflowContent(value: String): String = versionValue(value).toString
  private def createVersionDiff(oldValue: String, newValue: String): String =
    com.flipkart.zjsonpatch.JsonDiff.asJson(versionValue(oldValue), versionValue(newValue)).toString

  private def createAddDiff(): String =
    com.flipkart.zjsonpatch.JsonDiff
      .asJson(objectMapper.createObjectNode(), versionValue("added"))
      .toString

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

  it should "pin the result of the selected save and protect the author's history" in {
    val workflows = new WorkflowResource
    val author = session(owner)
    val guest = session(stranger)
    intercept[javax.ws.rs.BadRequestException](
      resource.pinMacro(testWorkflowWid, "m", null, author)
    ).getMessage should include("not a macro")
    testWorkflow.setContent("""{"macroInterface":{"id":"m"},"operators":[]}""")
    workflowDao.update(testWorkflow)
    intercept[javax.ws.rs.NotFoundException](
      resource.pinMacro(testWorkflowWid, "m", null, author)
    ).getMessage should include("no saved version")
    def save(value: String): Unit = {
      testWorkflow.setContent(s"""{"macroInterface":{"id":"m"},"operators":[],"value":"$value"}""")
      workflows.persistWorkflow(testWorkflow, author)
    }
    save("first")
    save("second")
    val ids = versionVids()
    val summary = resource.macroSource(testWorkflowWid, "m", false, author)
    summary.revisions shouldBe ids
    summary.head shouldBe empty
    summary.metadata.readonly shouldBe false
    val older = resource.pinMacro(testWorkflowWid, "m", ids.head, author)
    older.content.path("value").asText() shouldBe "first"
    older.revision shouldBe 1
    val latest = resource.pinMacro(testWorkflowWid, "m", null, author)
    resource.macroSource(testWorkflowWid, "m", true, author).head shouldBe Some(latest)
    latest.version shouldBe ids.last
    latest.revision shouldBe 2
    latest.content.path("value").asText() shouldBe "second"
    intercept[javax.ws.rs.NotFoundException](resource.pinMacro(testWorkflowWid, "m", -1, author))
    intercept[javax.ws.rs.BadRequestException](
      resource.pinMacro(testWorkflowWid, "foreign", null, author)
    )
    testWorkflow.setIsPublic(true)
    testWorkflow.setPublishedContent(older.content.toString)
    testWorkflow.setPublishedName("Public title")
    testWorkflow.setPublishedDefaultView(DefaultViewEnum.CANVAS)
    workflowDao.update(testWorkflow)
    val visible = resource.macroSource(testWorkflowWid, "m", true, guest)
    visible.metadata.name shouldBe "Public title"
    visible.metadata.readonly shouldBe true
    visible.revisions shouldBe empty
    visible.head shouldBe empty
    intercept[ForbiddenException](resource.pinMacro(testWorkflowWid, "m", ids.last, guest))
    grantAccess(strangerUid, PrivilegeEnum.READ)
    resource.pinMacro(testWorkflowWid, "m", ids.head, guest) shouldBe older
    resource
      .macroSource(testWorkflowWid, "m", false, guest)
      .metadata
      .name shouldBe testWorkflow.getName
  }

  it should "list only granted workflows and their reachable pinned macro occurrences" in {
    val workflows = new WorkflowResource
    val author = session(owner)
    val guest = session(stranger)
    val inner =
      """{"name":"Inner","id":"i","version":3,"content":{"macroInterface":{"id":"i"},"operators":[]}}"""
    val child =
      """{"operatorID":"child","operatorType":"WorkflowMacro:i:3","customDisplayName":"Renamed"}"""
    val outer =
      s"""{"name":"Outer","id":"o","version":2,"content":{"macroInterface":{"id":"o"},"operators":[$child]}}"""
    val occurrence = """{"operatorID":"selected","operatorType":"WorkflowMacro:o:2"}"""
    val unused = inner.replace("\"i\"", "\"unused\"")
    testWorkflow.setContent(
      s"""{"macroInterface":{"id":"root"},"operators":[$occurrence,$child],"macroDependencies":[$inner,$outer,$unused]}"""
    )
    workflows.persistWorkflow(testWorkflow, author)
    val catalog = workflows.macroCatalog(author)
    catalog.errors shouldBe empty
    catalog.definitions.map(_.id) should contain only "root"
    catalog.usages.map(u => u.id -> u.path).toSet shouldBe Set(
      "o" -> List("selected"),
      "i" -> List("selected", "child"),
      "i" -> List("child")
    )
    catalog.usages
      .find(_.path.size == 2)
      .get
      .label shouldBe s"${testWorkflow.getName} / Outer / Renamed"
    workflows.macroCatalog(guest).usages shouldBe empty
    grantAccess(strangerUid, PrivilegeEnum.READ)
    workflows.macroCatalog(guest) shouldBe catalog
    testWorkflow.setContent(s"""{"operators":[$occurrence]}""")
    workflowDao.update(testWorkflow)
    workflows.macroCatalog(guest).errors.head should include("Missing referenced macro definition")
  }

  "WorkflowVersionResource" should "return versions in descending order from fetchSubsequentVersions and apply patches correctly" in {
    val values = "initial" +: (1 to 10).map(i => s"version_$i")
    values.sliding(2).zipWithIndex.foreach {
      case (pair, index) =>
        seedVersion(createVersionDiff(pair.head, pair.last), 10 - index)
    }
    testWorkflow.setContent(createWorkflowContent(values.last))
    workflowDao.update(testWorkflow)
    val ids = versionVids()
    val fifth = ids(4)
    val versions =
      WorkflowVersionResource.fetchSubsequentVersions(testWorkflowWid, fifth, getDSLContext)
    versions.map(_.getVid) shouldBe ids.drop(4).reverse
    val restored =
      WorkflowVersionResource.applyPatch(versions, workflowDao.fetchOneByWid(testWorkflowWid))
    objectMapper.readTree(restored.getContent).path("value").asText() shouldBe "version_5"
    resource
      .retrieveWorkflowVersion(testWorkflowWid, fifth, session(owner))
      .getContent shouldBe restored.getContent
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

  for (view <- List(DefaultViewEnum.CANVAS, DefaultViewEnum.FORM)) {
    "cloneVersion" should s"create an independent workflow with the source's $view default" in {
      testWorkflow.setContent(
        """{"operators":[{"operatorID":"source","operatorType":"CSVFileScan"}],"links":[]}"""
      )
      if (view == DefaultViewEnum.FORM) testWorkflow.setDefaultView(view)
      workflowDao.update(testWorkflow)
      val saved = WorkflowVersionResource.insertNewVersion(testWorkflowWid, "[]")
      val wid =
        resource.cloneVersion(saved.getVid, session(owner), Map("displayedVersionId" -> 1).asJava)
      wid should not be null
      wid should not be testWorkflow.getWid
      val cloned = workflowDao.fetchOneByWid(wid)
      cloned should not be null
      cloned.getName should include("_copy")
      cloned.getDefaultView shouldBe view
    }
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
