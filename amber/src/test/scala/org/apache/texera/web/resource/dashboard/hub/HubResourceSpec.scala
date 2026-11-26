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
import org.apache.texera.dao.DatabaseFlatSpec
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.web.resource.dashboard.{
  DashboardWorkflowFixtureSupport,
  VersionedResourceTables
}
import org.apache.texera.web.resource.dashboard.hub.HubResource._
import org.jooq.{Field, Record, Table}
import org.scalamock.scalatest.MockFactory

import javax.servlet.http.HttpServletRequest
import javax.ws.rs.BadRequestException
import scala.jdk.CollectionConverters._

class HubResourceSpec
    extends DatabaseFlatSpec()
    with MockFactory
    with DashboardWorkflowFixtureSupport {

  // MockTexeraDB gives this suite its own UUID-named database, so fixed ids are
  // safe and keep failures reproducible.
  private val ownerUid = 9001
  private val likerUid = 9002
  private val wid = 7001
  private val Wf: EntityType = EntityType.Workflow

  // A request whose remote address is a valid IPv4 so recordUserAction stores it.
  private def req: HttpServletRequest = requestFrom("127.0.0.1")

  private def requestFrom(remoteAddress: String): HttpServletRequest = {
    val r = stub[HttpServletRequest]
    (r.getRemoteAddr _).when().returns(remoteAddress)
    r
  }

  private def session(uid: Int): SessionUser = {
    val u = new User
    u.setUid(Integer.valueOf(uid))
    new SessionUser(u)
  }

  private def ids(values: Int*): java.util.List[Integer] = values.map(Integer.valueOf).asJava
  private def types(values: EntityType*): java.util.List[EntityType] = values.toList.asJava
  private def actions(values: ActionType*): java.util.List[ActionType] = values.toList.asJava

  private def recordLike(uid: Int, entity: UserRequest, isLike: Boolean): Boolean =
    recordLikeAction(req, Integer.valueOf(uid), entity, isLike)

  private def view(id: Integer, uid: Integer, kind: EntityType = Wf): Int =
    hub.postView(req, ViewRequest(id, uid, kind))

  private def countsFor(
      kind: EntityType,
      id: Int,
      requested: java.util.List[ActionType] = null
  ): Map[ActionType, Int] =
    hub.getCounts(types(kind), ids(id), requested).asScala.head.counts.asScala.toMap

  private def likeRows: Int =
    getDSLContext.fetchCount(
      WORKFLOW_USER_LIKES,
      WORKFLOW_USER_LIKES.WID.eq(wid).and(WORKFLOW_USER_LIKES.UID.eq(likerUid))
    )

  private def cloneRows: Int =
    getDSLContext.fetchCount(
      WORKFLOW_USER_CLONES,
      WORKFLOW_USER_CLONES.WID.eq(wid).and(WORKFLOW_USER_CLONES.UID.eq(likerUid))
    )

  private def userActionRows: Int =
    getDSLContext.fetchCount(USER_ACTION, USER_ACTION.RESOURCE_ID.eq(wid))

  private def isLiked(uid: Int): Boolean =
    isLikedHelper(Integer.valueOf(uid), ids(wid), types(Wf)).asScala.head.isLiked

  // ---------------------------------------------------------------------------
  // Additional fixtures. These sit alongside the ones above and are used only by
  // the cases further down that need a second entity type, a second workflow, a
  // third liker or a non-IPv4 caller. Every id they introduce is deliberately far
  // away from wid 7001 / uid 9001 / uid 9002 so the fixture above is untouched.
  // ---------------------------------------------------------------------------

  private val thirdUid = 9003
  // Owners inside the default java.lang.Integer autoboxing cache (-128..127), where
  // reference equality on boxed values happens to agree with value equality. The
  // 9001-9003 uids above sit outside it.
  private val cachedUid = 42
  private val lastCachedUid = 127
  private val firstUncachedUid = 128
  private val Ds: EntityType = EntityType.Dataset
  private val Md: EntityType = EntityType.Model

  private def hub: HubResource = new HubResource()

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
      seedWorkflowOwner(wfId, owner)
    }
    Integer.valueOf(wfId)
  }

  private def seedWorkflowOwner(wfId: Int, owner: Int): Unit = {
    seedOwnership(owner, wfId)
    seedAccess(owner, wfId, PrivilegeEnum.WRITE)
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

  private case class GrantRows(
      table: Table[_ <: Record],
      id: Field[Integer],
      user: Field[Integer],
      privilege: Field[PrivilegeEnum]
  ) {
    def insert(entityId: Int, uid: Int, value: PrivilegeEnum): Unit =
      getDSLContext
        .insertInto(table)
        .set(id, Int.box(entityId))
        .set(user, Int.box(uid))
        .set(privilege, value)
        .execute()
  }

  private case class InteractionRows(
      table: Table[_ <: Record],
      id: Field[Integer],
      value: Field[Integer]
  ) {
    def insert(entityId: Int, n: Int): Unit =
      getDSLContext.insertInto(table).set(id, Int.box(entityId)).set(value, Int.box(n)).execute()

    def insertAll(entityId: Int, values: Int*): Unit = values.foreach(insert(entityId, _))

    def delete(entityId: Int): Unit =
      getDSLContext.deleteFrom(table).where(id.eq(Int.box(entityId))).execute()

    private def select(entityId: Int) =
      getDSLContext.select(value).from(table).where(id.eq(Int.box(entityId)))

    def all(entityId: Int): Seq[Int] =
      select(entityId).fetchInto(classOf[Integer]).asScala.map(_.intValue()).toSeq

    def count(entityId: Int): Option[Int] =
      Option(select(entityId).fetchOne(value)).map(_.intValue())
  }

  // Fixture routing stays independent of the production EntityTables registries.
  private val datasetGrants =
    GrantRows(
      DATASET_USER_ACCESS,
      DATASET_USER_ACCESS.DID,
      DATASET_USER_ACCESS.UID,
      DATASET_USER_ACCESS.PRIVILEGE
    )
  private val modelGrants =
    GrantRows(
      MODEL_USER_ACCESS,
      MODEL_USER_ACCESS.MID,
      MODEL_USER_ACCESS.UID,
      MODEL_USER_ACCESS.PRIVILEGE
    )

  private val workflowLikes =
    InteractionRows(WORKFLOW_USER_LIKES, WORKFLOW_USER_LIKES.WID, WORKFLOW_USER_LIKES.UID)
  private val datasetLikes =
    InteractionRows(DATASET_USER_LIKES, DATASET_USER_LIKES.DID, DATASET_USER_LIKES.UID)
  private val modelLikes =
    InteractionRows(MODEL_USER_LIKES, MODEL_USER_LIKES.MID, MODEL_USER_LIKES.UID)
  private val workflowClones =
    InteractionRows(WORKFLOW_USER_CLONES, WORKFLOW_USER_CLONES.WID, WORKFLOW_USER_CLONES.UID)
  private val workflowViews =
    InteractionRows(WORKFLOW_VIEW_COUNT, WORKFLOW_VIEW_COUNT.WID, WORKFLOW_VIEW_COUNT.VIEW_COUNT)
  private val datasetViews =
    InteractionRows(DATASET_VIEW_COUNT, DATASET_VIEW_COUNT.DID, DATASET_VIEW_COUNT.VIEW_COUNT)
  private val modelViews =
    InteractionRows(MODEL_VIEW_COUNT, MODEL_VIEW_COUNT.MID, MODEL_VIEW_COUNT.VIEW_COUNT)

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

  override protected def onBeforeAll(): Unit = {
    val userDao = new UserDao(getDSLContext.configuration())

    userDao.insert(fixtureUser(ownerUid, "hub_owner"))
    userDao.insert(fixtureUser(likerUid, "hub_liker"))
    // A third liker, needed by the ranking cases below to make "most liked" a
    // strict order (3 > 2 > 1) rather than a tie between two users' likes.
    userDao.insert(fixtureUser(thirdUid, "hub_third"))
    userDao.insert(fixtureUser(cachedUid, "hub_cached_uid"))
    userDao.insert(fixtureUser(lastCachedUid, "hub_uid_127"))
    userDao.insert(fixtureUser(firstUncachedUid, "hub_uid_128"))

    workflowDao.insert(fixtureWorkflow(wid, "hub_wf_" + uniqueSuffix, "hub test", isPublic = true))

    seedWorkflowOwner(wid, ownerUid)
  }

  // Reset the per-entity hub rows so every test starts from a clean slate.
  override protected def cleanup(): Unit = {
    val ctx = getDSLContext
    workflowLikes.delete(wid)
    workflowClones.delete(wid)
    workflowViews.delete(wid)
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
    recordUserAction(
      req,
      Some(Integer.valueOf(likerUid)),
      Integer.valueOf(wid),
      Wf,
      ActionType.Like
    )
    userActionRows shouldBe 1
  }

  it should "persist uid, resource type, resource id, action and an IPv4 address" in {
    val id = seedWorkflow(810101, "wf_audit")
    recordUserAction(req, Some(Integer.valueOf(likerUid)), id, Wf, ActionType.View)

    auditRows() shouldBe Seq(
      (Integer.valueOf(likerUid), "workflow", 810101, "view", "127.0.0.1")
    )
  }

  it should "leave the ip column null when the remote address is not IPv4-shaped" in {
    val id = seedWorkflow(810102, "wf_audit_v6")
    recordUserAction(requestFrom("::1"), Some(Integer.valueOf(ownerUid)), id, Wf, ActionType.Clone)

    // Everything but the address is still recorded; only the ip column is skipped.
    auditRows() shouldBe Seq((Integer.valueOf(ownerUid), "workflow", 810102, "clone", null))
  }

  it should "store a NULL uid when userId is omitted" in {
    val id = seedWorkflow(810103, "wf_audit_anonymous")
    recordUserAction(req, entityId = id, entityType = Wf, action = ActionType.View)

    auditRows() shouldBe Seq((null, "workflow", 810103, "view", "127.0.0.1"))
  }

  it should "tag each row with its own entity type, keeping equal ids apart" in {
    // Same numeric id on purpose: only resource_type separates the two rows.
    seedWorkflow(830001, "wf_shared_id")
    seedDataset(830001, "ds_shared_id")
    val shared = Integer.valueOf(830001)

    recordUserAction(req, Some(Integer.valueOf(ownerUid)), shared, Wf, ActionType.Like)
    recordUserAction(req, Some(Integer.valueOf(ownerUid)), shared, Ds, ActionType.Unlike)

    auditRows().map { case (_, resourceType, _, action, _) => (resourceType, action) } shouldBe
      Seq(("workflow", "like"), ("dataset", "unlike"))
  }

  "recordLikeAction" should "insert a like the first time and remove it on unlike" in {
    recordLike(likerUid, UserRequest(wid, Wf), isLike = true) shouldBe true
    likeRows shouldBe 1
    isLiked(likerUid) shouldBe true

    recordLike(likerUid, UserRequest(wid, Wf), isLike = false) shouldBe true
    likeRows shouldBe 0
    isLiked(likerUid) shouldBe false
  }

  it should "return false when liking something already liked" in {
    recordLike(likerUid, UserRequest(wid, Wf), isLike = true) shouldBe true
    recordLike(likerUid, UserRequest(wid, Wf), isLike = true) shouldBe false
    likeRows shouldBe 1
  }

  it should "audit one row per accepted like or unlike and none for a rejected duplicate" in {
    val id = seedWorkflow(810201, "wf_like_audit")
    val request = UserRequest(id, Wf)

    recordLike(ownerUid, request, isLike = true) shouldBe true
    recordLike(ownerUid, request, isLike = true) shouldBe false
    recordLike(ownerUid, request, isLike = false) shouldBe true

    // The rejected duplicate like contributes nothing: two accepted calls, two rows.
    auditRows() shouldBe Seq(
      (Integer.valueOf(ownerUid), "workflow", 810201, "like", "127.0.0.1"),
      (Integer.valueOf(ownerUid), "workflow", 810201, "unlike", "127.0.0.1")
    )
    workflowLikes.all(810201).toSet shouldBe empty
  }

  it should "return false and audit nothing when unliking an entity that was never liked" in {
    val id = seedWorkflow(810204, "wf_unlike_cold")

    recordLike(ownerUid, UserRequest(id, Wf), isLike = false) shouldBe false

    auditRows() shouldBe empty
    workflowLikes.all(810204).toSet shouldBe empty
  }

  it should "only remove the calling user's like, leaving other users' likes intact" in {
    val id = seedWorkflow(810205, "wf_like_two_users")
    workflowLikes.insert(810205, ownerUid)
    workflowLikes.insert(810205, likerUid)

    recordLike(ownerUid, UserRequest(id, Wf), isLike = false) shouldBe true

    workflowLikes.all(810205).toSet shouldBe Set(likerUid)
  }

  it should "route the like to the table for its entity type when a workflow and a dataset share an id" in {
    seedWorkflow(830001, "wf_like_shared_id")
    seedDataset(830001, "ds_like_shared_id")

    recordLike(ownerUid, UserRequest(Integer.valueOf(830001), Ds), isLike = true) shouldBe true

    datasetLikes.all(830001).toSet shouldBe Set(ownerUid)
    workflowLikes.all(830001).toSet shouldBe empty
  }

  it should "route the like to the model table when a model shares the id" in {
    seedWorkflow(830002, "wf_like_shared_with_model")
    seedDataset(830002, "ds_like_shared_with_model")
    seedModel(830002, "md_like_shared_id")

    recordLike(ownerUid, UserRequest(Integer.valueOf(830002), Md), isLike = true) shouldBe true

    modelLikes.all(830002).toSet shouldBe Set(ownerUid)
    datasetLikes.all(830002).toSet shouldBe empty
    workflowLikes.all(830002).toSet shouldBe empty
  }

  it should "unlike a model, leaving other likers alone" in {
    seedModel(840101, "md_unlike")
    modelLikes.insert(840101, likerUid)
    modelLikes.insert(840101, ownerUid)

    recordLike(ownerUid, UserRequest(Integer.valueOf(840101), Md), isLike = false) shouldBe true

    modelLikes.all(840101).toSet shouldBe Set(likerUid)
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
    workflowClones.all(810402) shouldBe Seq(ownerUid)
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
    workflowLikes.insert(810301, ownerUid)
    datasetLikes.insert(820301, ownerUid)

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
    modelLikes.insert(840501, ownerUid)
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
    workflowLikes.insert(810303, likerUid)

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
    workflowLikes.insert(810304, ownerUid)

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

  "getPublicOwners" should "offer the owners of published workflows, and no one else" in {
    // hub_liker's workflow is private, so only hub_owner has anything the hub can show.
    seedWorkflow(810601, "wf_private_of_liker", isPublic = false, owner = likerUid)

    hub.getPublicOwners(Wf).asScala should contain theSameElementsAs Seq("hub_owner@test.com")
  }

  it should "name an owner once however many public entities they have" in {
    seedWorkflow(810602, "wf_public_b", isPublic = true)
    seedWorkflow(810603, "wf_public_c", isPublic = true)

    hub.getPublicOwners(Wf).asScala shouldBe Seq("hub_owner@test.com")
  }

  it should "ignore a grant, which is what the per-kind owners endpoints answer instead" in {
    // The bug itself: a grant without anything published must not make you an owner here.
    seedAccess(likerUid, wid, PrivilegeEnum.READ)

    hub.getPublicOwners(Wf).asScala should not contain "hub_liker@test.com"
  }

  it should "keep each kind to its own table" in {
    // Deliberately different owners per kind, so a table mix-up cannot pass.
    seedDataset(820601, "ds_public", owner = likerUid)
    seedModel(840601, "md_public", owner = thirdUid)

    hub.getPublicOwners(Ds).asScala shouldBe Seq("hub_liker@test.com")
    hub.getPublicOwners(Md).asScala shouldBe Seq("hub_third@test.com")
    hub.getPublicOwners(Wf).asScala shouldBe Seq("hub_owner@test.com")
  }

  it should "leave out the owners of private entities" in {
    seedDataset(820602, "ds_private", isPublic = false, owner = likerUid)
    seedModel(840602, "md_private", isPublic = false, owner = thirdUid)

    hub.getPublicOwners(Ds).asScala shouldBe empty
    hub.getPublicOwners(Md).asScala shouldBe empty
  }

  it should "name nobody for a public workflow that has no owner row" in {
    seedWorkflow(810604, "wf_ownerless", isPublic = true, withOwnerRows = false)

    // The fixture workflow's owner, and not a null for the ownerless one.
    hub.getPublicOwners(Wf).asScala shouldBe Seq("hub_owner@test.com")
  }

  "postView" should "increment the view count and record a view action" in {
    view(Integer.valueOf(wid), Integer.valueOf(likerUid)) shouldBe 1
    view(Integer.valueOf(wid), Integer.valueOf(likerUid)) shouldBe 2
    userActionRows shouldBe 2
  }

  it should "audit the view against the uid carried by the request body" in {
    val id = seedWorkflow(810602, "wf_view_audit")
    // The viewer (likerUid) is deliberately not the owner (ownerUid), so an
    // implementation that audited the owner instead would fail here.
    view(id, Integer.valueOf(likerUid))

    auditRows() shouldBe Seq(
      (Integer.valueOf(likerUid), "workflow", 810602, "view", "127.0.0.1")
    )
  }

  it should "record an anonymous view (uid 0 or none) with a NULL uid" in {
    val id = seedWorkflow(810603, "wf_view_anonymous")
    view(id, Integer.valueOf(0)) shouldBe 1
    view(id, null) shouldBe 2

    auditRows() shouldBe Seq(
      (null, "workflow", 810603, "view", "127.0.0.1"),
      (null, "workflow", 810603, "view", "127.0.0.1")
    )
  }

  it should "leave the count unchanged when the view cannot be recorded" in {
    val id = seedWorkflow(810605, "wf_view_missing_user")
    view(id, Integer.valueOf(likerUid)) shouldBe 1

    // A uid with no user row, e.g. an account deleted while another tab was still open.
    an[Exception] should be thrownBy view(id, Integer.valueOf(999999))

    workflowViews.count(810605) shouldBe Some(1)
  }

  it should "keep workflow and dataset view counts apart when the ids collide" in {
    seedWorkflow(830001, "wf_view_shared_id")
    seedDataset(830001, "ds_view_shared_id")
    val shared = Integer.valueOf(830001)

    view(shared, Integer.valueOf(ownerUid))
    view(shared, Integer.valueOf(ownerUid))
    view(shared, Integer.valueOf(ownerUid), Ds)

    workflowViews.count(830001) shouldBe Some(2)
    datasetViews.count(830001) shouldBe Some(1)
  }

  it should "keep model view counts apart from the dataset and workflow rows" in {
    seedWorkflow(830003, "wf_view_shared_with_model")
    seedDataset(830003, "ds_view_shared_with_model")
    seedModel(830003, "md_view_shared_id")
    val shared = Integer.valueOf(830003)

    view(shared, Integer.valueOf(ownerUid), Md)
    view(shared, Integer.valueOf(ownerUid), Md)
    view(shared, Integer.valueOf(ownerUid), Ds)

    modelViews.count(830003) shouldBe Some(2)
    datasetViews.count(830003) shouldBe Some(1)
    workflowViews.count(830003) shouldBe None
  }

  private def seedWorkflowWithCounts(wid: Int, name: String): Integer = {
    val id = seedWorkflow(wid, name)
    workflowViews.insert(wid, 5)
    workflowLikes.insertAll(wid, ownerUid, likerUid)
    workflowClones.insert(wid, ownerUid)
    id
  }

  "getCounts" should "report the like count for the entity" in {
    recordLike(likerUid, UserRequest(wid, Wf), isLike = true)

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
    val id = seedWorkflowWithCounts(810801, "wf_counts")

    val responses = hub.getCounts(types(Wf), ids(810801), null).asScala

    responses should have size 1
    responses.head.entityId shouldBe id
    // Distinct 5 / 2 / 1 so swapping two of the three maps cannot pass.
    responses.head.counts.asScala.toMap shouldBe Map(
      ActionType.View -> 5,
      ActionType.Like -> 2,
      ActionType.Clone -> 1
    )
  }

  it should "return only the requested action types" in {
    seedWorkflowWithCounts(810802, "wf_counts_filtered")

    val counts = countsFor(Wf, 810802, actions(ActionType.Like))

    // View and clone data exists for this workflow; the filter must drop both keys.
    counts shouldBe Map(ActionType.Like -> 2)
  }

  it should "backfill a zero view-count row for an entity that has never been viewed" in {
    seedWorkflow(810803, "wf_counts_backfill")
    workflowViews.count(810803) shouldBe None

    val counts = countsFor(Wf, 810803, actions(ActionType.View))

    counts shouldBe Map(ActionType.View -> 0)
    // The backfill is a real side effect: the row is written by getCounts itself.
    workflowViews.count(810803) shouldBe Some(0)
  }

  it should "not touch the view-count table when view is not among the requested actions" in {
    seedWorkflow(810804, "wf_counts_no_view")

    hub.getCounts(types(Wf), ids(810804), actions(ActionType.Like))

    workflowViews.count(810804) shouldBe None
  }

  it should "report zero clones for a dataset rather than failing on the absent clone table" in {
    seedDataset(820801, "ds_counts")
    datasetViews.insert(820801, 7)
    datasetLikes.insertAll(820801, ownerUid, likerUid, thirdUid)

    countsFor(Ds, 820801) shouldBe Map(
      ActionType.View -> 7,
      ActionType.Like -> 3,
      ActionType.Clone -> 0
    )
  }

  it should "report view and like counts for a model, and zero clones" in {
    seedModel(840301, "md_counts")
    modelViews.insert(840301, 5)
    modelLikes.insertAll(840301, ownerUid, likerUid)

    countsFor(Md, 840301) shouldBe Map(
      ActionType.View -> 5,
      ActionType.Like -> 2,
      ActionType.Clone -> 0
    )
  }

  it should "answer a mixed batch with one response per requested pair" in {
    seedWorkflow(810805, "wf_mixed")
    seedDataset(820802, "ds_mixed")
    workflowLikes.insert(810805, ownerUid)
    datasetLikes.insertAll(820802, ownerUid, likerUid)

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

  "versionedResourcesByIdsQuery" should "carry only the viewer's own grant, never another user's" in {
    val did = seedDataset(820860, "ds_two_grants")
    for ((uid, privilege) <- Seq(ownerUid -> PrivilegeEnum.WRITE, likerUid -> PrivilegeEnum.READ)) {
      datasetGrants.insert(did, uid, privilege)
    }
    val tables = VersionedResourceTables.DatasetTables
    def grantsSeenBy(uid: Integer): Seq[PrivilegeEnum] =
      HubResource
        .versionedResourcesByIdsQuery(tables, Seq(did), uid)
        .fetch()
        .asScala
        .map(_.get(DATASET_USER_ACCESS.PRIVILEGE))
        .toSeq

    grantsSeenBy(Integer.valueOf(likerUid)) shouldBe Seq(PrivilegeEnum.READ)
    // A user with no grant, and an anonymous viewer, see one row with no grant at all.
    grantsSeenBy(Integer.valueOf(thirdUid)) shouldBe Seq(null)
    grantsSeenBy(null) shouldBe Seq(null)
  }

  "getTops" should "list the liked public entity under the 'like' key" in {
    recordLike(likerUid, UserRequest(wid, Wf), isLike = true)

    val tops = new HubResource().getTops(Wf, actions(ActionType.Like), null, null)
    tops.keySet.asScala should contain("like")
    val likedWids = tops.get("like").asScala.flatMap(_.workflow.map(_.workflow.getWid.intValue()))
    likedWids should contain(wid)
  }

  it should "default to the like and clone buckets when no action types are given" in {
    hub.getTops(Wf, null, null, null).asScala.keySet shouldBe Set("like", "clone")
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
    datasetLikes.insert(820850, likerUid)

    hub.getTops(Ds, actions(ActionType.Clone), null, null).get("clone").asScala shouldBe empty
  }

  it should "select the most-liked public workflows and honour the limit" in {
    val hot = seedWorkflow(810701, "wf_hot")
    val warm = seedWorkflow(810702, "wf_warm")
    seedWorkflow(810703, "wf_cold")
    workflowLikes.insertAll(810701, ownerUid, likerUid, thirdUid)
    workflowLikes.insertAll(810702, ownerUid, likerUid)
    workflowLikes.insert(810703, ownerUid)

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
    workflowLikes.insert(810704, ownerUid)
    workflowLikes.insertAll(810705, ownerUid, likerUid, thirdUid)

    val liked = hub.getTops(Wf, actions(ActionType.Like), null, null).get("like").asScala

    liked.map(_.workflow.get.workflow.getWid) shouldBe Seq(visible)
  }

  it should "fall back to a limit of 8 when the limit is missing or non-positive" in {
    (1 to 9).foreach { i =>
      seedWorkflow(810710 + i, s"wf_top_$i")
      workflowLikes.insert(810710 + i, ownerUid)
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
    workflowLikes.insertAll(810707, ownerUid, likerUid)
    workflowClones.insertAll(810708, ownerUid, likerUid)

    val tops = hub.getTops(Wf, null, null, Integer.valueOf(1)).asScala

    tops("like").asScala.map(_.workflow.get.workflow.getWid) shouldBe Seq(mostLiked)
    tops("clone").asScala.map(_.workflow.get.workflow.getWid) shouldBe Seq(mostCloned)
  }

  private def accessByEntity(
      kinds: java.util.List[EntityType],
      entityIds: java.util.List[Integer]
  ): Map[(EntityType, Int), Set[Int]] =
    hub
      .userAccess(kinds, entityIds)
      .asScala
      .map(r => (r.entityType, r.entityId.intValue()) -> r.userIds.asScala.map(_.intValue()).toSet)
      .toMap

  "userAccess" should "return the user ids granted access to the entity" in {
    val response = new HubResource().userAccess(types(Wf), ids(wid)).asScala.head
    response.entityId shouldBe Integer.valueOf(wid)
    response.userIds.asScala should contain(Integer.valueOf(ownerUid))
  }

  it should "return each entity's grantees from that entity's own access table" in {
    seedWorkflow(810901, "wf_access", withOwnerRows = false)
    seedDataset(820901, "ds_access")
    // Disjoint grantee sets so reading the wrong access table cannot pass.
    seedAccess(ownerUid, 810901, PrivilegeEnum.WRITE)
    seedAccess(likerUid, 810901, PrivilegeEnum.READ)
    datasetGrants.insert(820901, thirdUid, PrivilegeEnum.READ)

    accessByEntity(types(Wf, Ds), ids(810901, 820901)) shouldBe Map(
      (Wf, 810901) -> Set(ownerUid, likerUid),
      (Ds, 820901) -> Set(thirdUid)
    )
  }

  it should "read a model's grantees from model_user_access" in {
    seedModel(840401, "md_access")
    seedDataset(820910, "ds_access_noise")
    // Disjoint grantee sets so reading the wrong access table cannot pass.
    modelGrants.insert(840401, likerUid, PrivilegeEnum.WRITE)
    datasetGrants.insert(820910, thirdUid, PrivilegeEnum.READ)

    accessByEntity(types(Md, Ds), ids(840401, 820910)) shouldBe Map(
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
    seedAccess(ownerUid, 810903, PrivilegeEnum.READ)

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

  // Cover boxed owner identities on both sides of the Integer cache boundary.
  Seq(
    (
      "set isOwner for the owner's own request when the uid is outside the Integer cache",
      811003,
      "wf_mine_uncached",
      ownerUid
    ),
    (
      "set isOwner for the owner's own request when the uid is inside the Integer cache",
      811004,
      "wf_mine_cached",
      cachedUid
    ),
    (
      "set isOwner for the highest uid inside the Integer cache (127)",
      811006,
      "wf_mine_uid_127",
      lastCachedUid
    ),
    (
      "set isOwner for the lowest uid outside the Integer cache (128)",
      811007,
      "wf_mine_uid_128",
      firstUncachedUid
    )
  ).foreach {
    case (description, workflowId, name, uid) =>
      it should description in {
        val id = seedWorkflow(workflowId, name, owner = uid)
        fetchDashboardWorkflowsByWids(Seq(id), Integer.valueOf(uid)).head.isOwner shouldBe true
      }
  }

  it should "clear isOwner for a different uid and for an anonymous (null) uid" in {
    val id = seedWorkflow(811002, "wf_not_mine", owner = ownerUid)

    fetchDashboardWorkflowsByWids(Seq(id), Integer.valueOf(likerUid)).head.isOwner shouldBe false
    fetchDashboardWorkflowsByWids(Seq(id), null).head.isOwner shouldBe false
  }

  // workflow_of_user is LEFT JOINed, so a workflow with no ownership row has a null owner
  // uid. For the anonymous caller both uids
  // are null, and Scala's `==` treats two nulls as equal, so the `uid != null` guard in
  // mapWorkflowEntries is what keeps that one false.
  it should "clear isOwner for a workflow with no ownership row, for an authenticated and an anonymous caller" in {
    val id = seedWorkflow(811005, "wf_unowned", withOwnerRows = false)
    seedAccess(ownerUid, id, PrivilegeEnum.READ)

    val entry = fetchDashboardWorkflowsByWids(Seq(id), Integer.valueOf(ownerUid)).head
    entry.ownerId shouldBe null
    entry.isOwner shouldBe false
    fetchDashboardWorkflowsByWids(Seq(id), null).head.isOwner shouldBe false
  }

  it should "return one entry per workflow, carrying only the viewer's access level" in {
    val id = seedWorkflow(811008, "wf_shared_twice")
    seedAccess(likerUid, id, PrivilegeEnum.READ)
    seedAccess(thirdUid, id, PrivilegeEnum.WRITE)

    def levelsSeenBy(uid: Integer) =
      fetchDashboardWorkflowsByWids(Seq(id), uid).map(_.accessLevel)
    levelsSeenBy(Integer.valueOf(likerUid)) shouldBe List("READ")
    levelsSeenBy(Integer.valueOf(cachedUid)) shouldBe List("NONE")
    levelsSeenBy(null) shouldBe List("NONE")
  }

  it should "report WRITE for the owner even without an access row" in {
    val id = seedWorkflow(811009, "wf_owner_row_removed")
    getDSLContext
      .deleteFrom(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(id))
      .execute()

    fetchDashboardWorkflowsByWids(Seq(id), Integer.valueOf(ownerUid)).map(_.accessLevel) shouldBe
      List("WRITE")
  }

  // Every resolved id is sized through LakeFS, and a resource whose repository LakeFS cannot size
  // is dropped. Connection-refused and 404 both surface as an ApiException, so the second case
  // behaves the same whether or not a LakeFS answers here.
  "fetchDashboardVersionedResourcesByIds" should "return an empty list for an empty id list" in {
    fetchDashboardVersionedResourcesByIds(
      VersionedResourceTables.DatasetTables,
      Seq.empty,
      Integer.valueOf(ownerUid)
    ) shouldBe empty
  }

  it should "drop a resource whose LakeFS repository cannot be sized" in {
    val did = seedDataset(820870, "ds_unsized")
    getDSLContext
      .update(DATASET)
      .set(DATASET.REPOSITORY_NAME, "hub-spec-unsized-820870")
      .where(DATASET.DID.eq(did))
      .execute()

    fetchDashboardVersionedResourcesByIds(
      VersionedResourceTables.DatasetTables,
      Seq(did),
      Integer.valueOf(ownerUid)
    ) shouldBe empty
  }
}
