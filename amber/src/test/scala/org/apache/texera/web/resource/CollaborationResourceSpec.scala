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

package org.apache.texera.web.resource

import org.apache.texera.amber.util.JSONUtils
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.{USER, WORKFLOW, WORKFLOW_USER_ACCESS}
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowDao,
  WorkflowUserAccessDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{User, Workflow, WorkflowUserAccess}
import org.apache.texera.web.model.collab.request._
import org.apache.texera.web.resource.CollaborationResource._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.util.concurrent.{Future => JFuture}
import javax.websocket.{RemoteEndpoint, Session}
import scala.collection.mutable.ArrayBuffer

// Unit tests for CollaborationResource's session bookkeeping, message fan-out
// and lock-request handling. Most cases drive a mocked javax.websocket.Session
// with no external collaborators; the TryLockRequest cases that reach
// WorkflowAccessResource.hasWriteAccess additionally mix in MockTexeraDB and
// seed a workflow_user_access row so the privilege check reads a real value.
// The lock hand-off inside myOnClose still needs SqlServer and is left uncovered.
class CollaborationResourceSpec
    extends AnyFlatSpec
    with Matchers
    with MockFactory
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private var resource: CollaborationResource = _

  // Fixed ids for the DB-backed lock tests; a user, a workflow and one
  // workflow_user_access row are seeded per test so checkIsReadOnly ->
  // WorkflowAccessResource.hasWriteAccess reads a real privilege.
  private val accessUid = 8201
  private val accessWid = 8301

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def afterAll(): Unit = {
    closeConnectionPool()
  }

  // The five maps live on the companion object, i.e. they are JVM-wide mutable
  // state shared by every test in the suite. Clearing them here is what keeps
  // this spec order-independent.
  override def beforeEach(): Unit = {
    sessionIdSessionMap.clear()
    sessionIdWIdMap.clear()
    sessionIdUIdMap.clear()
    wIdSessionIdsMap.clear()
    wIdLockHolderSessionIdMap.clear()
    cleanupAccess()
    resource = new CollaborationResource()
  }

  override def afterEach(): Unit = {
    cleanupAccess()
  }

  // Seeds user accessUid + workflow accessWid + a workflow_user_access row with
  // the given privilege, so WorkflowAccessResource.getPrivilege(accessWid,
  // accessUid) returns it.
  private def seedAccess(privilege: PrivilegeEnum): Unit = {
    val user = new User
    user.setUid(Integer.valueOf(accessUid))
    user.setName("collab_lock_user")
    user.setRole(UserRoleEnum.REGULAR)
    user.setPassword("pw")
    new UserDao(getDSLContext.configuration()).insert(user)

    val workflow = new Workflow
    workflow.setWid(Integer.valueOf(accessWid))
    workflow.setName("collab-lock-wf")
    workflow.setContent("{}")
    workflow.setDescription("")
    workflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    new WorkflowDao(getDSLContext.configuration()).insert(workflow)

    new WorkflowUserAccessDao(getDSLContext.configuration())
      .insert(
        new WorkflowUserAccess(Integer.valueOf(accessUid), Integer.valueOf(accessWid), privilege)
      )
  }

  private def cleanupAccess(): Unit = {
    getDSLContext
      .deleteFrom(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(accessWid))
      .execute()
    getDSLContext.deleteFrom(WORKFLOW).where(WORKFLOW.WID.eq(accessWid)).execute()
    getDSLContext.deleteFrom(USER).where(USER.UID.eq(accessUid)).execute()
  }

  // A session already registered on accessWid as accessUid, ready to TryLock.
  private def lockingSession(id: String): (Session, ArrayBuffer[String]) = {
    val (session, sent) = mockSession(id, uId = Some(accessUid))
    resource.myOnOpen(session)
    resource.myOnMsg(session, send(WIdRequest(accessWid)))
    sent.clear()
    (session, sent)
  }

  /**
    * A mocked Session whose getId is fixed and whose outgoing messages are
    * collected into the returned buffer. `uId` seeds the authenticated user in
    * the session's user properties; None models an anonymous session.
    */
  private def mockSession(id: String, uId: Option[Int] = None): (Session, ArrayBuffer[String]) = {
    val sent = ArrayBuffer[String]()

    val async = mock[RemoteEndpoint.Async]
    (async
      .sendText(_: String))
      .expects(*)
      .onCall { (text: String) =>
        sent += text
        null.asInstanceOf[JFuture[Void]]
      }
      .anyNumberOfTimes()

    val properties = new java.util.HashMap[String, Object]()
    uId.foreach { uid =>
      val user = new User()
      user.setUid(Integer.valueOf(uid))
      properties.put(classOf[User].getName, user)
    }

    val session = mock[Session]
    (() => session.getId).expects().returning(id).anyNumberOfTimes()
    (() => session.getAsyncRemote).expects().returning(async).anyNumberOfTimes()
    (() => session.getUserProperties).expects().returning(properties).anyNumberOfTimes()

    (session, sent)
  }

  private def send(request: CollabWebSocketRequest): String =
    JSONUtils.objectMapper.writeValueAsString(request)

  // -- session lifecycle ------------------------------------------------------

  "myOnOpen" should "register the session" in {
    val (session, _) = mockSession("s1")

    resource.myOnOpen(session)

    sessionIdSessionMap should contain key "s1"
    sessionIdSessionMap("s1") shouldBe session
  }

  "myOnClose" should "drop the session and its workflow bookkeeping" in {
    val (session, _) = mockSession("s1")
    resource.myOnOpen(session)
    resource.myOnMsg(session, send(WIdRequest(7)))
    sessionIdWIdMap should contain key "s1"

    resource.myOnClose(session)

    sessionIdSessionMap should not contain key("s1")
    sessionIdWIdMap should not contain key("s1")
    wIdSessionIdsMap(DUMMY_WID) shouldBe empty
  }

  it should "leave the maps alone for a session that never sent a WIdRequest" in {
    val (session, _) = mockSession("s1")
    resource.myOnOpen(session)

    resource.myOnClose(session)

    sessionIdSessionMap shouldBe empty
    sessionIdWIdMap shouldBe empty
    wIdSessionIdsMap shouldBe empty
  }

  // -- WIdRequest -------------------------------------------------------------

  "WIdRequest" should "record the uid and the requested wid for an authenticated session" in {
    val (session, _) = mockSession("s1", uId = Some(42))
    resource.myOnOpen(session)

    resource.myOnMsg(session, send(WIdRequest(7)))

    sessionIdUIdMap("s1") shouldBe 42
    sessionIdWIdMap("s1") shouldBe 7
    wIdSessionIdsMap(7) should contain only "s1"
  }

  it should "fall back to DUMMY_WID for an anonymous session" in {
    val (session, _) = mockSession("s1")
    resource.myOnOpen(session)

    resource.myOnMsg(session, send(WIdRequest(7)))

    sessionIdWIdMap("s1") shouldBe DUMMY_WID
    sessionIdUIdMap should not contain key("s1")
    wIdSessionIdsMap(DUMMY_WID) should contain only "s1"
  }

  it should "accumulate every session that joins the same wid" in {
    val (first, _) = mockSession("s1", uId = Some(1))
    val (second, _) = mockSession("s2", uId = Some(2))
    resource.myOnOpen(first)
    resource.myOnOpen(second)

    resource.myOnMsg(first, send(WIdRequest(7)))
    resource.myOnMsg(second, send(WIdRequest(7)))

    // The union() call returns a fresh set rather than mutating in place, so
    // this only holds because the result is reassigned into the map.
    wIdSessionIdsMap(7) should contain theSameElementsAs Set("s1", "s2")
  }

  // -- fan-out ----------------------------------------------------------------

  /**
    * Three open sessions: s1 and s2 share wid 1, s3 sits on wid 2.
    */
  private def threeSessions(): (
      (Session, ArrayBuffer[String]),
      (Session, ArrayBuffer[String]),
      (Session, ArrayBuffer[String])
  ) = {
    val first = mockSession("s1", uId = Some(1))
    val second = mockSession("s2", uId = Some(2))
    val third = mockSession("s3", uId = Some(3))
    List(first, second, third).foreach { case (session, _) => resource.myOnOpen(session) }
    resource.myOnMsg(first._1, send(WIdRequest(1)))
    resource.myOnMsg(second._1, send(WIdRequest(1)))
    resource.myOnMsg(third._1, send(WIdRequest(2)))
    (first, second, third)
  }

  "CommandRequest" should "reach only the peers on the same workflow" in {
    val ((sender, senderSent), (peer, peerSent), (other, otherSent)) = threeSessions()
    senderSent.clear()
    peerSent.clear()
    otherSent.clear()

    resource.myOnMsg(sender, send(CommandRequest("do-something")))

    peerSent should have size 1
    peerSent.head should include("CommandEvent")
    peerSent.head should include("do-something")
    senderSent shouldBe empty
    otherSent shouldBe empty
    peer.getId shouldBe "s2"
    other.getId shouldBe "s3"
  }

  "RestoreVersionRequest" should "reach only the peers on the same workflow" in {
    val ((sender, senderSent), (_, peerSent), (_, otherSent)) = threeSessions()
    senderSent.clear()
    peerSent.clear()
    otherSent.clear()

    resource.myOnMsg(sender, send(RestoreVersionRequest()))

    peerSent should have size 1
    peerSent.head should include("RestoreVersionEvent")
    senderSent shouldBe empty
    otherSent shouldBe empty
  }

  // -- heartbeat --------------------------------------------------------------

  "HeartBeatRequest" should "answer the sender only" in {
    val ((sender, senderSent), (_, peerSent), _) = threeSessions()
    senderSent.clear()
    peerSent.clear()

    resource.myOnMsg(sender, send(HeartBeatRequest()))

    senderSent should have size 1
    senderSent.head should include("HeartBeatResponse")
    peerSent shouldBe empty
  }

  // -- locking ----------------------------------------------------------------

  "TryLockRequest" should "grant the lock unconditionally on the DUMMY_WID workflow" in {
    val (session, sent) = mockSession("s1")
    resource.myOnOpen(session)
    resource.myOnMsg(session, send(WIdRequest(7)))
    sessionIdWIdMap("s1") shouldBe DUMMY_WID
    sent.clear()

    resource.myOnMsg(session, send(TryLockRequest()))

    sent should have size 2
    sent.head should include("WorkflowAccessEvent")
    sent.head should include("\"workflowReadonly\":false")
    sent(1) should include("LockGrantedEvent")
  }

  "AcquireLockRequest" should "hand the lock over from the previous holder" in {
    val (holder, holderSent) = mockSession("s1", uId = Some(1))
    val (requester, requesterSent) = mockSession("s2", uId = Some(2))
    resource.myOnOpen(holder)
    resource.myOnOpen(requester)
    resource.myOnMsg(holder, send(WIdRequest(1)))
    resource.myOnMsg(requester, send(WIdRequest(1)))
    wIdLockHolderSessionIdMap(1) = "s1"
    holderSent.clear()
    requesterSent.clear()

    resource.myOnMsg(requester, send(AcquireLockRequest()))

    holderSent should have size 1
    holderSent.head should include("ReleaseLockEvent")
    requesterSent should have size 1
    requesterSent.head should include("LockGrantedEvent")
    wIdLockHolderSessionIdMap(1) shouldBe "s2"
  }

  it should "re-grant the lock to the session that already holds it" in {
    val (session, sent) = mockSession("s1", uId = Some(1))
    resource.myOnOpen(session)
    resource.myOnMsg(session, send(WIdRequest(1)))
    wIdLockHolderSessionIdMap(1) = "s1"
    sent.clear()

    resource.myOnMsg(session, send(AcquireLockRequest()))

    sent should have size 1
    sent.head should include("LockGrantedEvent")
    wIdLockHolderSessionIdMap(1) shouldBe "s1"
  }

  it should "rethrow when the holder slot holds the null sentinel" in {
    val (session, _) = mockSession("s1", uId = Some(1))
    resource.myOnOpen(session)
    resource.myOnMsg(session, send(WIdRequest(1)))
    // `null` means "no holder"; it is a distinct state from an absent key and
    // the hand-off branch cannot look a null session id up.
    wIdLockHolderSessionIdMap(1) = null

    a[NoSuchElementException] should be thrownBy
      resource.myOnMsg(session, send(AcquireLockRequest()))
  }

  // -- locking that consults WorkflowAccessResource (DB-backed) ---------------

  "TryLockRequest from a read-only user" should "reject the lock and mark the workflow read-only" in {
    seedAccess(PrivilegeEnum.READ)
    val (session, sent) = lockingSession("s1")

    resource.myOnMsg(session, send(TryLockRequest()))

    sent should have size 2
    sent.head should include("LockRejectedEvent")
    sent(1) should include("WorkflowAccessEvent")
    sent(1) should include("\"workflowReadonly\":true")
    // a read-only attempt records the null sentinel so the holder slot exists
    wIdLockHolderSessionIdMap should contain key accessWid
    wIdLockHolderSessionIdMap(accessWid) shouldBe null
  }

  "TryLockRequest from a writable user with no current holder" should "grant the lock" in {
    seedAccess(PrivilegeEnum.WRITE)
    val (session, sent) = lockingSession("s1")

    resource.myOnMsg(session, send(TryLockRequest()))

    sent should have size 2
    sent.head should include("WorkflowAccessEvent")
    sent.head should include("\"workflowReadonly\":false")
    sent(1) should include("LockGrantedEvent")
    wIdLockHolderSessionIdMap(accessWid) shouldBe "s1"
  }

  "TryLockRequest from a writable user" should "be rejected when another session holds the lock" in {
    seedAccess(PrivilegeEnum.WRITE)
    val (session, sent) = lockingSession("s1")
    wIdLockHolderSessionIdMap(accessWid) = "other-session" // a different holder

    resource.myOnMsg(session, send(TryLockRequest()))

    sent should have size 2
    sent.head should include("WorkflowAccessEvent")
    sent.head should include("\"workflowReadonly\":false")
    sent(1) should include("LockRejectedEvent")
    wIdLockHolderSessionIdMap(accessWid) shouldBe "other-session" // unchanged
  }
}
