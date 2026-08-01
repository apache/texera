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
import org.apache.texera.web.resource.dashboard.hub.HubResource._
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.util.UUID
import javax.servlet.http.HttpServletRequest
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
    u.setPassword("password")
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

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    userDao = new UserDao(getDSLContext.configuration())
    workflowDao = new WorkflowDao(getDSLContext.configuration())
    workflowOfUserDao = new WorkflowOfUserDao(getDSLContext.configuration())
    workflowUserAccessDao = new WorkflowUserAccessDao(getDSLContext.configuration())

    userDao.insert(makeUser(ownerUid, "hub_owner"))
    userDao.insert(makeUser(likerUid, "hub_liker"))

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
  }

  "recordUserAction" should "insert a user_action row for the entity" in {
    recordUserAction(req, Integer.valueOf(likerUid), Integer.valueOf(wid), Wf, ActionType.Like)
    userActionRows shouldBe 1
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

  "recordCloneAction" should "record a clone row and a user_action row" in {
    recordCloneAction(req, Integer.valueOf(likerUid), Integer.valueOf(wid), Wf)
    cloneRows shouldBe 1
    userActionRows shouldBe 1
  }

  "postLike / postUnlike / isLiked" should "toggle the like through the resource endpoints" in {
    val resource = new HubResource()
    resource.postLike(session(likerUid), req, UserRequest(wid, Wf)) shouldBe true
    resource.isLiked(session(likerUid), ids(wid), types(Wf)).asScala.head.isLiked shouldBe true

    resource.postUnlike(session(likerUid), req, UserRequest(wid, Wf)) shouldBe true
    resource.isLiked(session(likerUid), ids(wid), types(Wf)).asScala.head.isLiked shouldBe false
  }

  "getCount" should "count the public workflows" in {
    // this suite's isolated database holds exactly one public workflow
    new HubResource().getCount(Wf).intValue() shouldBe 1
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

  "getCounts" should "report the like count for the entity" in {
    recordLikeAction(req, Integer.valueOf(likerUid), UserRequest(wid, Wf), isLike = true)

    val response =
      new HubResource().getCounts(types(Wf), ids(wid), actions(ActionType.Like)).asScala.head
    response.entityId shouldBe Integer.valueOf(wid)
    response.counts.get(ActionType.Like) shouldBe 1
  }

  "getTops" should "list the liked public entity under the 'like' key" in {
    recordLikeAction(req, Integer.valueOf(likerUid), UserRequest(wid, Wf), isLike = true)

    val tops = new HubResource().getTops(Wf, actions(ActionType.Like), null, null)
    tops.keySet.asScala should contain("like")
    val likedWids = tops.get("like").asScala.flatMap(_.workflow.map(_.workflow.getWid.intValue()))
    likedWids should contain(wid)
  }

  "userAccess" should "return the user ids granted access to the entity" in {
    val response = new HubResource().userAccess(types(Wf), ids(wid)).asScala.head
    response.entityId shouldBe Integer.valueOf(wid)
    response.userIds.asScala should contain(Integer.valueOf(ownerUid))
  }

  "fetchDashboardWorkflowsByWids" should "return the seeded workflow" in {
    val results =
      fetchDashboardWorkflowsByWids(Seq(Integer.valueOf(wid)), Integer.valueOf(ownerUid))
    results.map(_.workflow.getWid.intValue()) should contain(wid)
  }
}
