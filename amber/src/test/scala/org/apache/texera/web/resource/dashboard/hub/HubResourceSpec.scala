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

package org.apache.texera.web.resource.dashboard.hub

import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowDao,
  WorkflowOfUserDao,
  WorkflowUserAccessDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowOfUser,
  WorkflowUserAccess
}
import org.apache.texera.web.resource.dashboard.VersionedResourceTables
import org.apache.texera.web.resource.dashboard.hub.HubResource._
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.util.UUID
import javax.servlet.http.HttpServletRequest
import javax.ws.rs.BadRequestException
import scala.jdk.CollectionConverters._

class HubResourceSpec
    extends AnyFlatSpec
    with Matchers
    with MockFactory
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  // MockTexeraDB gives this suite its own UUID-named database, so fixed ids are
  // safe and keep failures reproducible.
  private val ownerUid = 9001
  private val likerUid = 9002
  private val wid = 7001
  private val Wf: EntityType = EntityType.Workflow

  private var userDao: UserDao = _
  private var workflowDao: WorkflowDao = _
  private var workflowOfUserDao: WorkflowOfUserDao = _
  private var workflowUserAccessDao: WorkflowUserAccessDao = _

  // A request whose remote address is a valid IPv4 so recordUserAction stores it.
  private def req: HttpServletRequest = {
    val r = stub[HttpServletRequest]
    (r.getRemoteAddr _).when().returns("127.0.0.1")
    r
  }

  private def session(uid: Int): SessionUser = {
    val u = new User
    u.setUid(Integer.valueOf(uid))
    new SessionUser(u)
  }

  private def makeUser(uid: Int, name: String): User = {
    val u = new User
    u.setUid(Integer.valueOf(uid))
    u.setName(name)
    u.setEmail(s"$name@test.com")
    u
  }

  private def ids(values: Int*): java.util.List[Integer] = values.map(Integer.valueOf).asJava
  private def types(values: EntityType*): java.util.List[EntityType] = values.toList.asJava
  private def actions(values: ActionType*): java.util.List[ActionType] = values.toList.asJava

  private def likeRows: Int =
    getDSLContext
      .selectCount()
      .from(WORKFLOW_USER_LIKES)
      .where(WORKFLOW_USER_LIKES.WID.eq(wid).and(WORKFLOW_USER_LIKES.UID.eq(likerUid)))
      .fetchOne(0, classOf[Integer])
      .intValue()

  private def cloneRows: Int =
    getDSLContext
      .selectCount()
      .from(WORKFLOW_USER_CLONES)
      .where(WORKFLOW_USER_CLONES.WID.eq(wid).and(WORKFLOW_USER_CLONES.UID.eq(likerUid)))
      .fetchOne(0, classOf[Integer])
      .intValue()

  private def userActionRows: Int =
    getDSLContext
      .selectCount()
      .from(USER_ACTION)
      .where(USER_ACTION.RESOURCE_ID.eq(wid))
      .fetchOne(0, classOf[Integer])
      .intValue()

  private def isLiked(uid: Int): Boolean =
    isLikedHelper(Integer.valueOf(uid), ids(wid), types(Wf)).asScala.head.isLiked

  // ---------------------------------------------------------------------------
  // Additional fixtures. These sit alongside the ones above and are used only by
  // the cases further down that need a second entity type, a second workflow, a
  // third liker or a non-IPv4 caller. Every id they introduce is deliberately far
  // away from wid 7001 / uid 9001 / uid 9002 so the fixture above is untouched.
  // ---------------------------------------------------------------------------

  private val thirdUid = 9003
  private val Ds: EntityType = EntityType.Dataset
  private val Md: EntityType = EntityType.Model

  private def hub: HubResource = new HubResource()

  // A request whose remote address is not IPv4-shaped, to exercise the ip guard.
  private def nonIpv4Req: HttpServletRequest = {
    val r = stub[HttpServletRequest]
    (r.getRemoteAddr _).when().returns("::1")
    r
  }

  /**
    * Inserts a workflow with an explicit wid (the serial sequence is left alone so
    * these ids never collide with anything jOOQ generates). `withOwnerRows` also
    * seeds the WORKFLOW_OF_USER / WORKFLOW_USER_ACCESS rows that
    * `mapWorkflowEntries` dereferences for ownerName, ownerId and accessLevel.
    */
  private def seedWorkflow(
      wfId: Int,
      name: String,
      isPublic: Boolean = true,
      owner: Int = ownerUid,
      withOwnerRows: Boolean = true
  ): Integer = {
    getDSLContext
      .insertInto(WORKFLOW)
      .set(WORKFLOW.WID, Integer.valueOf(wfId))
      .set(WORKFLOW.NAME, name)
      .set(WORKFLOW.DESCRIPTION, s"desc of $name")
      .set(WORKFLOW.CONTENT, "{}")
      .set(WORKFLOW.IS_PUBLIC, java.lang.Boolean.valueOf(isPublic))
      .execute()
    if (withOwnerRows) {
      getDSLContext
        .insertInto(WORKFLOW_OF_USER)
        .set(WORKFLOW_OF_USER.UID, Integer.valueOf(owner))
        .set(WORKFLOW_OF_USER.WID, Integer.valueOf(wfId))
        .execute()
      grantWorkflowAccess(wfId, owner, PrivilegeEnum.WRITE)
    }
    Integer.valueOf(wfId)
  }

  private def seedDataset(
      dsId: Int,
      name: String,
      isPublic: Boolean = true,
      owner: Int = ownerUid
  ): Integer = {
    getDSLContext
      .insertInto(DATASET)
      .set(DATASET.DID, Integer.valueOf(dsId))
      .set(DATASET.OWNER_UID, Integer.valueOf(owner))
      .set(DATASET.NAME, name)
      .set(DATASET.DESCRIPTION, s"desc of $name")
      .set(DATASET.IS_PUBLIC, java.lang.Boolean.valueOf(isPublic))
      .execute()
    Integer.valueOf(dsId)
  }

  private def seedModel(
      modelId: Int,
      name: String,
      isPublic: Boolean = true,
      owner: Int = ownerUid
  ): Integer = {
    getDSLContext
      .insertInto(MODEL)
      .set(MODEL.MID, Integer.valueOf(modelId))
      .set(MODEL.OWNER_UID, Integer.valueOf(owner))
      .set(MODEL.NAME, name)
      .set(MODEL.DESCRIPTION, s"desc of $name")
      .set(MODEL.IS_PUBLIC, java.lang.Boolean.valueOf(isPublic))
      .execute()
    Integer.valueOf(modelId)
  }

  private def grantWorkflowAccess(wfId: Int, uid: Int, privilege: PrivilegeEnum): Unit =
    getDSLContext
      .insertInto(WORKFLOW_USER_ACCESS)
      .set(WORKFLOW_USER_ACCESS.WID, Integer.valueOf(wfId))
      .set(WORKFLOW_USER_ACCESS.UID, Integer.valueOf(uid))
      .set(WORKFLOW_USER_ACCESS.PRIVILEGE, privilege)
      .execute()

  private def grantDatasetAccess(dsId: Int, uid: Int, privilege: PrivilegeEnum): Unit =
    getDSLContext
      .insertInto(DATASET_USER_ACCESS)
      .set(DATASET_USER_ACCESS.DID, Integer.valueOf(dsId))
      .set(DATASET_USER_ACCESS.UID, Integer.valueOf(uid))
      .set(DATASET_USER_ACCESS.PRIVILEGE, privilege)
      .execute()

  private def grantModelAccess(modelId: Int, uid: Int, privilege: PrivilegeEnum): Unit =
    getDSLContext
      .insertInto(MODEL_USER_ACCESS)
      .set(MODEL_USER_ACCESS.MID, Integer.valueOf(modelId))
      .set(MODEL_USER_ACCESS.UID, Integer.valueOf(uid))
      .set(MODEL_USER_ACCESS.PRIVILEGE, privilege)
      .execute()

  private def seedWorkflowLike(wfId: Int, uid: Int): Unit =
    getDSLContext
      .insertInto(WORKFLOW_USER_LIKES)
      .set(WORKFLOW_USER_LIKES.WID, Integer.valueOf(wfId))
      .set(WORKFLOW_USER_LIKES.UID, Integer.valueOf(uid))
      .execute()

  private def seedDatasetLike(dsId: Int, uid: Int): Unit =
    getDSLContext
      .insertInto(DATASET_USER_LIKES)
      .set(DATASET_USER_LIKES.DID, Integer.valueOf(dsId))
      .set(DATASET_USER_LIKES.UID, Integer.valueOf(uid))
      .execute()

  private def seedModelLike(modelId: Int, uid: Int): Unit =
    getDSLContext
      .insertInto(MODEL_USER_LIKES)
      .set(MODEL_USER_LIKES.MID, Integer.valueOf(modelId))
      .set(MODEL_USER_LIKES.UID, Integer.valueOf(uid))
      .execute()

  private def seedWorkflowClone(wfId: Int, uid: Int): Unit =
    getDSLContext
      .insertInto(WORKFLOW_USER_CLONES)
      .set(WORKFLOW_USER_CLONES.WID, Integer.valueOf(wfId))
      .set(WORKFLOW_USER_CLONES.UID, Integer.valueOf(uid))
      .execute()

  private def seedWorkflowViewCount(wfId: Int, count: Int): Unit =
    getDSLContext
      .insertInto(WORKFLOW_VIEW_COUNT)
      .set(WORKFLOW_VIEW_COUNT.WID, Integer.valueOf(wfId))
      .set(WORKFLOW_VIEW_COUNT.VIEW_COUNT, Integer.valueOf(count))
      .execute()

  private def seedDatasetViewCount(dsId: Int, count: Int): Unit =
    getDSLContext
      .insertInto(DATASET_VIEW_COUNT)
      .set(DATASET_VIEW_COUNT.DID, Integer.valueOf(dsId))
      .set(DATASET_VIEW_COUNT.VIEW_COUNT, Integer.valueOf(count))
      .execute()

  private def seedModelViewCount(modelId: Int, count: Int): Unit =
    getDSLContext
      .insertInto(MODEL_VIEW_COUNT)
      .set(MODEL_VIEW_COUNT.MID, Integer.valueOf(modelId))
      .set(MODEL_VIEW_COUNT.VIEW_COUNT, Integer.valueOf(count))
      .execute()

  private def workflowLikeUids(wfId: Int): Set[Int] =
    getDSLContext
      .select(WORKFLOW_USER_LIKES.UID)
      .from(WORKFLOW_USER_LIKES)
      .where(WORKFLOW_USER_LIKES.WID.eq(Integer.valueOf(wfId)))
      .fetchInto(classOf[Integer])
      .asScala
      .map(_.intValue())
      .toSet

  private def datasetLikeUids(dsId: Int): Set[Int] =
    getDSLContext
      .select(DATASET_USER_LIKES.UID)
      .from(DATASET_USER_LIKES)
      .where(DATASET_USER_LIKES.DID.eq(Integer.valueOf(dsId)))
      .fetchInto(classOf[Integer])
      .asScala
      .map(_.intValue())
      .toSet

  private def modelLikeUids(modelId: Int): Set[Int] =
    getDSLContext
      .select(MODEL_USER_LIKES.UID)
      .from(MODEL_USER_LIKES)
      .where(MODEL_USER_LIKES.MID.eq(Integer.valueOf(modelId)))
      .fetchInto(classOf[Integer])
      .asScala
      .map(_.intValue())
      .toSet

  private def workflowCloneUids(wfId: Int): Seq[Int] =
    getDSLContext
      .select(WORKFLOW_USER_CLONES.UID)
      .from(WORKFLOW_USER_CLONES)
      .where(WORKFLOW_USER_CLONES.WID.eq(Integer.valueOf(wfId)))
      .fetchInto(classOf[Integer])
      .asScala
      .map(_.intValue())
      .toSeq

  private def workflowViewCount(wfId: Int): Option[Int] =
    Option(
      getDSLContext
        .select(WORKFLOW_VIEW_COUNT.VIEW_COUNT)
        .from(WORKFLOW_VIEW_COUNT)
        .where(WORKFLOW_VIEW_COUNT.WID.eq(Integer.valueOf(wfId)))
        .fetchOne(WORKFLOW_VIEW_COUNT.VIEW_COUNT)
    ).map(_.intValue())

  private def datasetViewCount(dsId: Int): Option[Int] =
    Option(
      getDSLContext
        .select(DATASET_VIEW_COUNT.VIEW_COUNT)
        .from(DATASET_VIEW_COUNT)
        .where(DATASET_VIEW_COUNT.DID.eq(Integer.valueOf(dsId)))
        .fetchOne(DATASET_VIEW_COUNT.VIEW_COUNT)
    ).map(_.intValue())

  private def modelViewCount(modelId: Int): Option[Int] =
    Option(
      getDSLContext
        .select(MODEL_VIEW_COUNT.VIEW_COUNT)
        .from(MODEL_VIEW_COUNT)
        .where(MODEL_VIEW_COUNT.MID.eq(Integer.valueOf(modelId)))
        .fetchOne(MODEL_VIEW_COUNT.VIEW_COUNT)
    ).map(_.intValue())

  /** (uid, resourceType, resourceId, action literal, ip) for every audit row. */
  private def auditRows(): Seq[(Integer, String, Int, String, String)] =
    getDSLContext
      .selectFrom(USER_ACTION)
      .orderBy(USER_ACTION.USER_ACTION_ID.asc())
      .fetch()
      .asScala
      .map(r =>
        (
          r.getUid,
          r.getResourceType,
          r.getResourceId.intValue(),
          r.getAction.getLiteral,
          r.getIp
        )
      )
      .toSeq

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    userDao = new UserDao(getDSLContext.configuration())
    workflowDao = new WorkflowDao(getDSLContext.configuration())
    workflowOfUserDao = new WorkflowOfUserDao(getDSLContext.configuration())
    workflowUserAccessDao = new WorkflowUserAccessDao(getDSLContext.configuration())

    userDao.insert(makeUser(ownerUid, "hub_owner"))
    userDao.insert(makeUser(likerUid, "hub_liker"))
    // A third liker, needed by the ranking cases below to make "most liked" a
    // strict order (3 > 2 > 1) rather than a tie between two users' likes.
    userDao.insert(makeUser(thirdUid, "hub_third"))

    val wf = new Workflow
    wf.setWid(Integer.valueOf(wid))
    wf.setName("hub_wf_" + UUID.randomUUID().toString.substring(0, 8))
    wf.setContent("{}")
    wf.setDescription("hub test")
    wf.setIsPublic(true)
    wf.setCreationTime(new Timestamp(System.currentTimeMillis()))
    wf.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    workflowDao.insert(wf)

    val ownership = new WorkflowOfUser
    ownership.setUid(Integer.valueOf(ownerUid))
    ownership.setWid(Integer.valueOf(wid))
    workflowOfUserDao.insert(ownership)

    val access = new WorkflowUserAccess
    access.setUid(Integer.valueOf(ownerUid))
    access.setWid(Integer.valueOf(wid))
    access.setPrivilege(PrivilegeEnum.WRITE)
    workflowUserAccessDao.insert(access)
  }

  override protected def afterAll(): Unit = closeConnectionPool()

  // Reset the per-entity hub rows so every test starts from a clean slate.
  override protected def beforeEach(): Unit = {
    val ctx = getDSLContext
    ctx.deleteFrom(WORKFLOW_USER_LIKES).where(WORKFLOW_USER_LIKES.WID.eq(wid)).execute()
    ctx.deleteFrom(WORKFLOW_USER_CLONES).where(WORKFLOW_USER_CLONES.WID.eq(wid)).execute()
    ctx.deleteFrom(WORKFLOW_VIEW_COUNT).where(WORKFLOW_VIEW_COUNT.WID.eq(wid)).execute()
    ctx.deleteFrom(USER_ACTION).where(USER_ACTION.RESOURCE_ID.eq(wid)).execute()
    // The cases added below seed workflows, datasets and models of their own (ids in the
    // 81xxxx / 82xxxx / 83xxxx / 84xxxx ranges) plus audit rows pointing at them. MockTexeraDB
    // never truncates, so those have to go too or a later test would observe an
    // earlier one's rows. Deleting the parents cascades to their like, clone,
    // view-count, ownership and access children; user_action has no such FK and
    // needs its own delete. The beforeAll fixture (wid 7001 and its ownership /
    // access rows) is deliberately spared.
    ctx.deleteFrom(USER_ACTION).where(USER_ACTION.RESOURCE_ID.ne(wid)).execute()
    ctx.deleteFrom(WORKFLOW).where(WORKFLOW.WID.ne(wid)).execute()
    ctx.deleteFrom(DATASET).execute()
    ctx.deleteFrom(MODEL).execute()
  }

  "recordUserAction" should "insert a user_action row for the entity" in {
    recordUserAction(req, Integer.valueOf(likerUid), Integer.valueOf(wid), Wf, ActionType.Like)
    userActionRows shouldBe 1
  }

  it should "persist uid, resource type, resource id, action and an IPv4 address" in {
    val id = seedWorkflow(810101, "wf_audit")
    recordUserAction(req, Integer.valueOf(likerUid), id, Wf, ActionType.View)

    auditRows() shouldBe Seq(
      (Integer.valueOf(likerUid), "workflow", 810101, "view", "127.0.0.1")
    )
  }

  it should "leave the ip column null when the remote address is not IPv4-shaped" in {
    val id = seedWorkflow(810102, "wf_audit_v6")
    recordUserAction(nonIpv4Req, Integer.valueOf(ownerUid), id, Wf, ActionType.Clone)

    // Everything but the address is still recorded; only the ip column is skipped.
    auditRows() shouldBe Seq((Integer.valueOf(ownerUid), "workflow", 810102, "clone", null))
  }

  it should "tag each row with its own entity type, keeping equal ids apart" in {
    // Same numeric id on purpose: only resource_type separates the two rows.
    seedWorkflow(830001, "wf_shared_id")
    seedDataset(830001, "ds_shared_id")
    val shared = Integer.valueOf(830001)

    recordUserAction(req, Integer.valueOf(ownerUid), shared, Wf, ActionType.Like)
    recordUserAction(req, Integer.valueOf(ownerUid), shared, Ds, ActionType.Unlike)

    auditRows().map { case (_, resourceType, _, action, _) => (resourceType, action) } shouldBe
      Seq(("workflow", "like"), ("dataset", "unlike"))
  }

  "recordLikeAction" should "insert a like the first time and remove it on unlike" in {
    recordLikeAction(
      req,
      Integer.valueOf(likerUid),
      UserRequest(wid, Wf),
      isLike = true
    ) shouldBe true
    likeRows shouldBe 1
    isLiked(likerUid) shouldBe true

    recordLikeAction(
      req,
      Integer.valueOf(likerUid),
      UserRequest(wid, Wf),
      isLike = false
    ) shouldBe true
    likeRows shouldBe 0
    isLiked(likerUid) shouldBe false
  }

  it should "return false when liking something already liked" in {
    recordLikeAction(
      req,
      Integer.valueOf(likerUid),
      UserRequest(wid, Wf),
      isLike = true
    ) shouldBe true
    recordLikeAction(
      req,
      Integer.valueOf(likerUid),
      UserRequest(wid, Wf),
      isLike = true
    ) shouldBe false
    likeRows shouldBe 1
  }

  it should "audit one row per accepted like or unlike and none for a rejected duplicate" in {
    val id = seedWorkflow(810201, "wf_like_audit")
    val request = UserRequest(id, Wf)

    recordLikeAction(req, Integer.valueOf(ownerUid), request, isLike = true) shouldBe true
    recordLikeAction(req, Integer.valueOf(ownerUid), request, isLike = true) shouldBe false
    recordLikeAction(req, Integer.valueOf(ownerUid), request, isLike = false) shouldBe true

    // The rejected duplicate like contributes nothing: two accepted calls, two rows.
    auditRows() shouldBe Seq(
      (Integer.valueOf(ownerUid), "workflow", 810201, "like", "127.0.0.1"),
      (Integer.valueOf(ownerUid), "workflow", 810201, "unlike", "127.0.0.1")
    )
    workflowLikeUids(810201) shouldBe empty
  }

  it should "return false and audit nothing when unliking an entity that was never liked" in {
    val id = seedWorkflow(810204, "wf_unlike_cold")

    recordLikeAction(
      req,
      Integer.valueOf(ownerUid),
      UserRequest(id, Wf),
      isLike = false
    ) shouldBe false

    auditRows() shouldBe empty
    workflowLikeUids(810204) shouldBe empty
  }

  it should "only remove the calling user's like, leaving other users' likes intact" in {
    val id = seedWorkflow(810205, "wf_like_two_users")
    seedWorkflowLike(810205, ownerUid)
    seedWorkflowLike(810205, likerUid)

    recordLikeAction(
      req,
      Integer.valueOf(ownerUid),
      UserRequest(id, Wf),
      isLike = false
    ) shouldBe true

    workflowLikeUids(810205) shouldBe Set(likerUid)
  }

  it should "route the like to the table for its entity type when a workflow and a dataset share an id" in {
    seedWorkflow(830001, "wf_like_shared_id")
    seedDataset(830001, "ds_like_shared_id")

    recordLikeAction(
      req,
      Integer.valueOf(ownerUid),
      UserRequest(Integer.valueOf(830001), Ds),
      isLike = true
    ) shouldBe true

    datasetLikeUids(830001) shouldBe Set(ownerUid)
    workflowLikeUids(830001) shouldBe empty
  }

  it should "route the like to the model table when a model shares the id" in {
    seedWorkflow(830002, "wf_like_shared_with_model")
    seedDataset(830002, "ds_like_shared_with_model")
    seedModel(830002, "md_like_shared_id")

    recordLikeAction(
      req,
      Integer.valueOf(ownerUid),
      UserRequest(Integer.valueOf(830002), Md),
      isLike = true
    ) shouldBe true

    modelLikeUids(830002) shouldBe Set(ownerUid)
    datasetLikeUids(830002) shouldBe empty
    workflowLikeUids(830002) shouldBe empty
  }

  it should "unlike a model, leaving other likers alone" in {
    seedModel(840101, "md_unlike")
    seedModelLike(840101, likerUid)
    seedModelLike(840101, ownerUid)

    recordLikeAction(
      req,
      Integer.valueOf(ownerUid),
      UserRequest(Integer.valueOf(840101), Md),
      isLike = false
    ) shouldBe true

    modelLikeUids(840101) shouldBe Set(likerUid)
  }

  "recordCloneAction" should "record a clone row and a user_action row" in {
    recordCloneAction(req, Integer.valueOf(likerUid), Integer.valueOf(wid), Wf)
    cloneRows shouldBe 1
    userActionRows shouldBe 1
  }

  it should "audit every clone attempt but keep a single clone row per user" in {
    val id = seedWorkflow(810402, "wf_clone_twice")
    recordCloneAction(req, Integer.valueOf(ownerUid), id, Wf)
    recordCloneAction(req, Integer.valueOf(ownerUid), id, Wf)

    // The asymmetry is deliberate: the audit trail records both attempts while the
    // existence check keeps workflow_user_clones at one row for the (uid, wid) pair.
    workflowCloneUids(810402) shouldBe Seq(ownerUid)
    auditRows() shouldBe Seq(
      (Integer.valueOf(ownerUid), "workflow", 810402, "clone", "127.0.0.1"),
      (Integer.valueOf(ownerUid), "workflow", 810402, "clone", "127.0.0.1")
    )
  }

  "postLike / postUnlike / isLiked" should "toggle the like through the resource endpoints" in {
    val resource = new HubResource()
    resource.postLike(session(likerUid), req, UserRequest(wid, Wf)) shouldBe true
    resource.isLiked(session(likerUid), ids(wid), types(Wf)).asScala.head.isLiked shouldBe true

    resource.postUnlike(session(likerUid), req, UserRequest(wid, Wf)) shouldBe true
    resource.isLiked(session(likerUid), ids(wid), types(Wf)).asScala.head.isLiked shouldBe false
  }

  "isLikedHelper" should "flag each requested pair, keeping the two entity types apart" in {
    seedWorkflow(810301, "wf_liked")
    seedWorkflow(810302, "wf_not_liked")
    seedDataset(820301, "ds_liked")
    seedDataset(820302, "ds_not_liked")
    seedWorkflowLike(810301, ownerUid)
    seedDatasetLike(820301, ownerUid)

    val responses = isLikedHelper(
      Integer.valueOf(ownerUid),
      ids(810301, 810302, 820301, 820302),
      types(Wf, Wf, Ds, Ds)
    ).asScala

    // Responses are emitted grouped by entity type, so compare as a set.
    responses should have size 4
    responses.map(r => (r.entityType, r.entityId.intValue(), r.isLiked)).toSet shouldBe Set(
      (Wf, 810301, true),
      (Wf, 810302, false),
      (Ds, 820301, true),
      (Ds, 820302, false)
    )
  }

  it should "keep model likes apart from the other entity types" in {
    seedModel(840501, "md_is_liked")
    seedModelLike(840501, ownerUid)
    seedDataset(820920, "ds_is_liked")

    val responses = isLikedHelper(
      Integer.valueOf(ownerUid),
      ids(840501, 820920),
      types(Md, Ds)
    ).asScala.map(r => (r.entityType, r.entityId.intValue()) -> r.isLiked).toMap

    responses shouldBe Map((Md, 840501) -> true, (Ds, 820920) -> false)
  }

  it should "not report another user's like as the caller's" in {
    seedWorkflow(810303, "wf_liked_by_other")
    seedWorkflowLike(810303, likerUid)

    isLikedHelper(Integer.valueOf(ownerUid), ids(810303), types(Wf)).asScala
      .map(_.isLiked) shouldBe Seq(false)
  }

  // Like the two empty-input tests further down, this pins the contract rather than a
  // branch: isLikedHelper has no empty-input guard at all, so empty-in/empty-out falls
  // out of grouping an empty list. Worth keeping as a contract lock, but it cannot fail.
  it should "return an empty list for empty input lists" in {
    isLikedHelper(Integer.valueOf(ownerUid), ids(), types()).asScala shouldBe empty
  }

  "isLiked" should "resolve the liked flag against the session user, not another user" in {
    seedWorkflow(810304, "wf_session_like")
    seedWorkflowLike(810304, ownerUid)

    hub.isLiked(session(ownerUid), ids(810304), types(Wf)).asScala.map(_.isLiked) shouldBe Seq(true)
    hub.isLiked(session(likerUid), ids(810304), types(Wf)).asScala.map(_.isLiked) shouldBe Seq(
      false
    )
  }

  "getCount" should "count the public workflows" in {
    // this suite's isolated database holds exactly one public workflow
    new HubResource().getCount(Wf).intValue() shouldBe 1
  }

  it should "reject a missing entity type" in {
    intercept[BadRequestException](hub.getCount(null))
    intercept[BadRequestException](hub.getTops(null, null, null, null))
  }

  it should "leave non-public workflows out of the count" in {
    // On top of the public fixture workflow (wid 7001) seeded in beforeAll.
    seedWorkflow(810501, "wf_public_a", isPublic = true, withOwnerRows = false)
    seedWorkflow(810502, "wf_public_b", isPublic = true, withOwnerRows = false)
    seedWorkflow(810503, "wf_private", isPublic = false, withOwnerRows = false)

    hub.getCount(Wf).intValue() shouldBe 3
  }

  it should "count models from the model table" in {
    seedModel(840201, "md_public")
    seedModel(840202, "md_private", isPublic = false)
    seedDataset(820510, "ds_noise")

    hub.getCount(Md).intValue() shouldBe 1
  }

  it should "count datasets from the dataset table, not the workflow table" in {
    // Deliberately unequal cardinalities so a table mix-up cannot pass.
    seedWorkflow(810504, "wf_noise_a", isPublic = true, withOwnerRows = false)
    seedWorkflow(810505, "wf_noise_b", isPublic = true, withOwnerRows = false)
    seedDataset(820501, "ds_public")
    seedDataset(820502, "ds_private", isPublic = false)

    hub.getCount(Ds).intValue() shouldBe 1
    // 810504, 810505 and the fixture workflow 7001.
    hub.getCount(Wf).intValue() shouldBe 3
  }

  "postView" should "increment the view count and record a view action" in {
    val resource = new HubResource()
    resource.postView(
      req,
      ViewRequest(Integer.valueOf(wid), Integer.valueOf(likerUid), Wf)
    ) shouldBe 1
    resource.postView(
      req,
      ViewRequest(Integer.valueOf(wid), Integer.valueOf(likerUid), Wf)
    ) shouldBe 2
    userActionRows shouldBe 2
  }

  it should "audit the view against the uid carried by the request body" in {
    val id = seedWorkflow(810602, "wf_view_audit")
    // The viewer (likerUid) is deliberately not the owner (ownerUid), so an
    // implementation that audited the owner instead would fail here.
    hub.postView(req, ViewRequest(id, Integer.valueOf(likerUid), Wf))

    auditRows() shouldBe Seq(
      (Integer.valueOf(likerUid), "workflow", 810602, "view", "127.0.0.1")
    )
  }

  it should "keep workflow and dataset view counts apart when the ids collide" in {
    seedWorkflow(830001, "wf_view_shared_id")
    seedDataset(830001, "ds_view_shared_id")
    val shared = Integer.valueOf(830001)

    hub.postView(req, ViewRequest(shared, Integer.valueOf(ownerUid), Wf))
    hub.postView(req, ViewRequest(shared, Integer.valueOf(ownerUid), Wf))
    hub.postView(req, ViewRequest(shared, Integer.valueOf(ownerUid), Ds))

    workflowViewCount(830001) shouldBe Some(2)
    datasetViewCount(830001) shouldBe Some(1)
  }

  it should "keep model view counts apart from the dataset and workflow rows" in {
    seedWorkflow(830003, "wf_view_shared_with_model")
    seedDataset(830003, "ds_view_shared_with_model")
    seedModel(830003, "md_view_shared_id")
    val shared = Integer.valueOf(830003)

    hub.postView(req, ViewRequest(shared, Integer.valueOf(ownerUid), Md))
    hub.postView(req, ViewRequest(shared, Integer.valueOf(ownerUid), Md))
    hub.postView(req, ViewRequest(shared, Integer.valueOf(ownerUid), Ds))

    modelViewCount(830003) shouldBe Some(2)
    datasetViewCount(830003) shouldBe Some(1)
    workflowViewCount(830003) shouldBe None
  }

  "getCounts" should "report the like count for the entity" in {
    recordLikeAction(req, Integer.valueOf(likerUid), UserRequest(wid, Wf), isLike = true)

    val response =
      new HubResource().getCounts(types(Wf), ids(wid), actions(ActionType.Like)).asScala.head
    response.entityId shouldBe Integer.valueOf(wid)
    response.counts.get(ActionType.Like) shouldBe 1
  }

  it should "reject missing or mismatched entity type / id lists" in {
    intercept[BadRequestException](hub.getCounts(null, ids(wid), null))
    intercept[BadRequestException](hub.getCounts(types(Wf), null, null))
    intercept[BadRequestException](hub.getCounts(types(), ids(), null))
    intercept[BadRequestException](hub.getCounts(types(Wf), ids(wid, 810000), null))
  }

  it should "return view, like and clone counts by default" in {
    val id = seedWorkflow(810801, "wf_counts")
    seedWorkflowViewCount(810801, 5)
    Seq(ownerUid, likerUid).foreach(seedWorkflowLike(810801, _))
    seedWorkflowClone(810801, ownerUid)

    val responses = hub.getCounts(types(Wf), ids(810801), null).asScala

    responses should have size 1
    responses.head.entityId shouldBe id
    // Distinct 5 / 2 / 1 so swapping two of the three maps cannot pass.
    responses.head.counts.asScala.toMap shouldBe Map(
      (ActionType.View: ActionType) -> 5,
      (ActionType.Like: ActionType) -> 2,
      (ActionType.Clone: ActionType) -> 1
    )
  }

  it should "return only the requested action types" in {
    seedWorkflow(810802, "wf_counts_filtered")
    seedWorkflowViewCount(810802, 5)
    Seq(ownerUid, likerUid).foreach(seedWorkflowLike(810802, _))
    seedWorkflowClone(810802, ownerUid)

    val counts = hub
      .getCounts(types(Wf), ids(810802), actions(ActionType.Like))
      .asScala
      .head
      .counts
      .asScala
      .toMap

    // View and clone data exists for this workflow; the filter must drop both keys.
    counts shouldBe Map((ActionType.Like: ActionType) -> 2)
  }

  it should "backfill a zero view-count row for an entity that has never been viewed" in {
    seedWorkflow(810803, "wf_counts_backfill")
    workflowViewCount(810803) shouldBe None

    val counts = hub
      .getCounts(types(Wf), ids(810803), actions(ActionType.View))
      .asScala
      .head
      .counts
      .asScala
      .toMap

    counts shouldBe Map((ActionType.View: ActionType) -> 0)
    // The backfill is a real side effect: the row is written by getCounts itself.
    workflowViewCount(810803) shouldBe Some(0)
  }

  it should "not touch the view-count table when view is not among the requested actions" in {
    seedWorkflow(810804, "wf_counts_no_view")

    hub.getCounts(types(Wf), ids(810804), actions(ActionType.Like))

    workflowViewCount(810804) shouldBe None
  }

  it should "report zero clones for a dataset rather than failing on the absent clone table" in {
    seedDataset(820801, "ds_counts")
    seedDatasetViewCount(820801, 7)
    Seq(ownerUid, likerUid, thirdUid).foreach(seedDatasetLike(820801, _))

    val counts = hub.getCounts(types(Ds), ids(820801), null).asScala.head.counts

    counts.asScala.toMap shouldBe Map(
      (ActionType.View: ActionType) -> 7,
      (ActionType.Like: ActionType) -> 3,
      (ActionType.Clone: ActionType) -> 0
    )
  }

  it should "report view and like counts for a model, and zero clones" in {
    seedModel(840301, "md_counts")
    seedModelViewCount(840301, 5)
    Seq(ownerUid, likerUid).foreach(seedModelLike(840301, _))

    val counts = hub.getCounts(types(Md), ids(840301), null).asScala.head.counts

    counts.asScala.toMap shouldBe Map(
      (ActionType.View: ActionType) -> 5,
      (ActionType.Like: ActionType) -> 2,
      (ActionType.Clone: ActionType) -> 0
    )
  }

  it should "answer a mixed batch with one response per requested pair" in {
    seedWorkflow(810805, "wf_mixed")
    seedDataset(820802, "ds_mixed")
    seedWorkflowLike(810805, ownerUid)
    Seq(ownerUid, likerUid).foreach(seedDatasetLike(820802, _))

    val responses =
      hub.getCounts(types(Wf, Ds), ids(810805, 820802), actions(ActionType.Like)).asScala

    // Assert the raw size before collapsing to a Map: "one response per requested pair"
    // is half the contract, and a regression emitting duplicates would survive the
    // Map comparison alone.
    responses should have size 2

    val byEntity = responses
      .map(r => (r.entityType, r.entityId.intValue()) -> r.counts.asScala(ActionType.Like))
      .toMap

    byEntity shouldBe Map((Wf, 810805) -> 1, (Ds, 820802) -> 2)
  }

  "getTops" should "list the liked public entity under the 'like' key" in {
    recordLikeAction(req, Integer.valueOf(likerUid), UserRequest(wid, Wf), isLike = true)

    val tops = new HubResource().getTops(Wf, actions(ActionType.Like), null, null)
    tops.keySet.asScala should contain("like")
    val likedWids = tops.get("like").asScala.flatMap(_.workflow.map(_.workflow.getWid.intValue()))
    likedWids should contain(wid)
  }

  it should "default to the like and clone buckets when no action types are given" in {
    hub.getTops(Wf, null, null, null).asScala.keySet shouldBe Set("like", "clone")
  }

  it should "reject a missing entity type as a bad request" in {
    val thrown = intercept[BadRequestException](hub.getTops(null, null, null, null))
    thrown.getMessage shouldBe "entityType is required"
  }

  // Regression: the default bucket list ([like, clone]) used to drive getTops into a
  // throwing CloneTable(Dataset), making this plain request a 500.
  it should "answer for a cloneless entity type instead of failing on its absent clone table" in {
    val tops = hub.getTops(Ds, null, null, null).asScala

    tops.keySet shouldBe Set("like", "clone")
    tops("clone").asScala shouldBe empty
  }

  it should "answer for a model without failing on its absent clone table" in {
    // Models are cloneless like datasets; with no likes seeded the descriptor path is not
    // asked to size a lakeFS repository, which this suite has no stub for.
    val tops = hub.getTops(Md, null, null, null).asScala

    tops.keySet shouldBe Set("like", "clone")
    tops("like").asScala shouldBe empty
    tops("clone").asScala shouldBe empty
  }

  it should "return an empty clone bucket when clone is asked for explicitly" in {
    seedDataset(820850, "ds_no_clones")
    seedDatasetLike(820850, likerUid)

    hub.getTops(Ds, actions(ActionType.Clone), null, null).get("clone").asScala shouldBe empty
  }

  it should "select the most-liked public workflows and honour the limit" in {
    val hot = seedWorkflow(810701, "wf_hot")
    val warm = seedWorkflow(810702, "wf_warm")
    seedWorkflow(810703, "wf_cold")
    Seq(ownerUid, likerUid, thirdUid).foreach(seedWorkflowLike(810701, _))
    Seq(ownerUid, likerUid).foreach(seedWorkflowLike(810702, _))
    seedWorkflowLike(810703, ownerUid)

    val liked = hub
      .getTops(Wf, actions(ActionType.Like), null, Integer.valueOf(2))
      .get("like")
      .asScala

    // The final entries are re-fetched by wid without an ORDER BY, so only the
    // *selection* (top 2 by like count) is guaranteed -- assert membership, not order.
    liked.map(_.workflow.get.workflow.getWid).toSet shouldBe Set(hot, warm)
    liked.map(_.resourceType).toSet shouldBe Set("workflow")
  }

  it should "exclude non-public workflows even when they are the most liked" in {
    val visible = seedWorkflow(810704, "wf_public_liked", isPublic = true)
    seedWorkflow(810705, "wf_private_liked", isPublic = false)
    seedWorkflowLike(810704, ownerUid)
    Seq(ownerUid, likerUid, thirdUid).foreach(seedWorkflowLike(810705, _))

    val liked = hub.getTops(Wf, actions(ActionType.Like), null, null).get("like").asScala

    liked.map(_.workflow.get.workflow.getWid) shouldBe Seq(visible)
  }

  it should "fall back to a limit of 8 when the limit is missing or non-positive" in {
    (1 to 9).foreach { i =>
      seedWorkflow(810710 + i, s"wf_top_$i")
      seedWorkflowLike(810710 + i, ownerUid)
    }

    def topCount(limit: Integer): Int =
      hub.getTops(Wf, actions(ActionType.Like), null, limit).get("like").size()

    topCount(null) shouldBe 8
    topCount(Integer.valueOf(0)) shouldBe 8
    topCount(Integer.valueOf(-5)) shouldBe 8
    topCount(Integer.valueOf(3)) shouldBe 3
  }

  it should "reject action types that have no top-list table" in {
    val ex = intercept[BadRequestException] {
      hub.getTops(Wf, actions(ActionType.View), null, null)
    }
    ex.getMessage should include("view")
  }

  it should "rank the clone bucket independently of the like bucket" in {
    val mostLiked = seedWorkflow(810707, "wf_most_liked")
    val mostCloned = seedWorkflow(810708, "wf_most_cloned")
    Seq(ownerUid, likerUid).foreach(seedWorkflowLike(810707, _))
    Seq(ownerUid, likerUid).foreach(seedWorkflowClone(810708, _))

    val tops = hub.getTops(Wf, null, null, Integer.valueOf(1)).asScala

    tops("like").asScala.map(_.workflow.get.workflow.getWid) shouldBe Seq(mostLiked)
    tops("clone").asScala.map(_.workflow.get.workflow.getWid) shouldBe Seq(mostCloned)
  }

  "userAccess" should "return the user ids granted access to the entity" in {
    val response = new HubResource().userAccess(types(Wf), ids(wid)).asScala.head
    response.entityId shouldBe Integer.valueOf(wid)
    response.userIds.asScala should contain(Integer.valueOf(ownerUid))
  }

  it should "return each entity's grantees from that entity's own access table" in {
    seedWorkflow(810901, "wf_access", withOwnerRows = false)
    seedDataset(820901, "ds_access")
    // Disjoint grantee sets so reading the wrong access table cannot pass.
    grantWorkflowAccess(810901, ownerUid, PrivilegeEnum.WRITE)
    grantWorkflowAccess(810901, likerUid, PrivilegeEnum.READ)
    grantDatasetAccess(820901, thirdUid, PrivilegeEnum.READ)

    val byEntity = hub
      .userAccess(types(Wf, Ds), ids(810901, 820901))
      .asScala
      .map(r => (r.entityType, r.entityId.intValue()) -> r.userIds.asScala.map(_.intValue()).toSet)
      .toMap

    byEntity shouldBe Map(
      (Wf, 810901) -> Set(ownerUid, likerUid),
      (Ds, 820901) -> Set(thirdUid)
    )
  }

  it should "read a model's grantees from model_user_access" in {
    seedModel(840401, "md_access")
    seedDataset(820910, "ds_access_noise")
    // Disjoint grantee sets so reading the wrong access table cannot pass.
    grantModelAccess(840401, likerUid, PrivilegeEnum.WRITE)
    grantDatasetAccess(820910, thirdUid, PrivilegeEnum.READ)

    val byEntity = hub
      .userAccess(types(Md, Ds), ids(840401, 820910))
      .asScala
      .map(r => (r.entityType, r.entityId.intValue()) -> r.userIds.asScala.map(_.intValue()).toSet)
      .toMap

    byEntity shouldBe Map(
      (Md, 840401) -> Set(likerUid),
      (Ds, 820910) -> Set(thirdUid)
    )
  }

  it should "still emit a response with an empty uid list for an entity nobody can access" in {
    seedWorkflow(810902, "wf_no_access", withOwnerRows = false)

    hub
      .userAccess(types(Wf), ids(810902))
      .asScala
      .map(r => (r.entityId.intValue(), r.userIds.asScala.toList)) shouldBe
      Seq((810902, List.empty[Integer]))
  }

  it should "emit a single response when the same entity id is requested twice" in {
    seedWorkflow(810903, "wf_dup_request", withOwnerRows = false)
    grantWorkflowAccess(810903, ownerUid, PrivilegeEnum.READ)

    val responses = hub.userAccess(types(Wf, Wf), ids(810903, 810903)).asScala

    responses should have size 1
    responses.head.userIds.asScala.map(_.intValue()) shouldBe Seq(ownerUid)
  }

  "fetchDashboardWorkflowsByWids" should "return the seeded workflow" in {
    val results =
      fetchDashboardWorkflowsByWids(Seq(Integer.valueOf(wid)), Integer.valueOf(ownerUid))
    results.map(_.workflow.getWid.intValue()) should contain(wid)
  }

  // As with the dataset twin below, this pins the contract (empty in, empty out) rather
  // than the early-return branch itself: jOOQ renders an empty `WID.in()` as a false
  // predicate, so the query comes back empty even with the guard removed.
  it should "return an empty list for an empty wid list" in {
    fetchDashboardWorkflowsByWids(Seq.empty, Integer.valueOf(ownerUid)) shouldBe empty
  }

  it should "hydrate owner name, owner id and access level, and skip unknown wids" in {
    val id = seedWorkflow(811001, "wf_hydrate", owner = ownerUid)

    val fetched =
      fetchDashboardWorkflowsByWids(Seq(id, Integer.valueOf(899999)), Integer.valueOf(ownerUid))

    fetched should have size 1
    val entry = fetched.head
    entry.workflow.getWid shouldBe id
    entry.workflow.getName shouldBe "wf_hydrate"
    entry.ownerName shouldBe "hub_owner"
    entry.ownerId shouldBe Integer.valueOf(ownerUid)
    entry.accessLevel shouldBe "WRITE"
    entry.coverImage shouldBe empty
  }

  // NOTE: `isOwner == true` is deliberately not asserted anywhere. WorkflowResource's
  // mapWorkflowEntries compares the two uids with `AnyRef.eq` (reference equality on
  // boxed Integers), so it is true only for uids that land in the java.lang.Integer
  // cache (-128..127); this suite's uids (9001-9003) are outside it, so the owner's own
  // request reports false today. Asserting it either way would either cement that bug or
  // force this suite onto unrealistically small uids. The two negatives below hold for
  // the current implementation and for a value-equality fix alike.
  it should "clear isOwner for a different uid and for an anonymous (null) uid" in {
    val id = seedWorkflow(811002, "wf_not_mine", owner = ownerUid)

    fetchDashboardWorkflowsByWids(Seq(id), Integer.valueOf(likerUid)).head.isOwner shouldBe false
    fetchDashboardWorkflowsByWids(Seq(id), null).head.isOwner shouldBe false
  }

  // Every resolved id hits LakeFS, so an empty list is the only input testable without one.
  "fetchDashboardVersionedResourcesByIds" should "return an empty list for an empty id list" in {
    fetchDashboardVersionedResourcesByIds(
      VersionedResourceTables.DatasetTables,
      Seq.empty,
      Integer.valueOf(ownerUid)
    ) shouldBe empty
  }
}
