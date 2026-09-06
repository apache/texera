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
// The lock hand-off inside myOnClose consults the same privilege check, so some
// of the myOnClose cases at the bottom of this file also mix in MockTexeraDB.
// When the candidate privilege needs to be observable, they seed two users (one
// WRITE, one READ) on the same workflow.
//
// A session that sends WIdRequest twice is added to the new wid's bucket but
// never removed from the old one, leaving a stale hand-off candidate that
// outlives the session. That defect is deliberately not pinned here.
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
  // A second user on the SAME workflow, seeded with READ only. Needed because
  // the hand-off loop in myOnClose consults checkIsReadOnly per candidate
  // session: with one uid the privilege predicate cannot be observed at all.
  private val readOnlyUid = 8202

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
    val workflow = new Workflow
    workflow.setWid(Integer.valueOf(accessWid))
    workflow.setName("collab-lock-wf")
    workflow.setContent("{}")
    workflow.setDescription("")
    workflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    new WorkflowDao(getDSLContext.configuration()).insert(workflow)

    seedUserAccess(accessUid, "collab_lock_user", privilege)
  }

  // Adds one more (uid, accessWid) row. WORKFLOW_USER_ACCESS is keyed on the
  // pair, so several users can hold different privileges on one workflow.
  private def seedUserAccess(uid: Int, name: String, privilege: PrivilegeEnum): Unit = {
    val user = new User
    user.setUid(Integer.valueOf(uid))
    user.setName(name)
    user.setRole(UserRoleEnum.REGULAR)
    new UserDao(getDSLContext.configuration()).insert(user)

    new WorkflowUserAccessDao(getDSLContext.configuration())
      .insert(
        new WorkflowUserAccess(Integer.valueOf(uid), Integer.valueOf(accessWid), privilege)
      )
  }

  private def cleanupAccess(): Unit = {
    getDSLContext
      .deleteFrom(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(accessWid))
      .execute()
    getDSLContext.deleteFrom(WORKFLOW).where(WORKFLOW.WID.eq(accessWid)).execute()
    getDSLContext.deleteFrom(USER).where(USER.UID.eq(accessUid)).execute()
    getDSLContext.deleteFrom(USER).where(USER.UID.eq(readOnlyUid)).execute()
  }

  // A session already registered on accessWid, ready to TryLock. Defaults to
  // accessUid; pass readOnlyUid for a session that cannot write the workflow.
  private def lockingSession(
      id: String,
      uId: Int = accessUid
  ): (Session, ArrayBuffer[String]) = {
    val (session, sent) = mockSession(id, uId = Some(uId))
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

  it should "update the recorded wid when a session re-registers" in {
    val (session, _) = mockSession("s1", uId = Some(42))
    resource.myOnOpen(session)

    resource.myOnMsg(session, send(WIdRequest(7)))
    resource.myOnMsg(session, send(WIdRequest(8)))

    // The second request replaces the recorded wid rather than being ignored.
    sessionIdWIdMap("s1") shouldBe 8
    wIdSessionIdsMap(8) should contain only "s1"
    // Deliberately NOT asserted: what wIdSessionIdsMap(7) holds afterwards.
    // The handler never removes the session from the bucket it left, so the
    // stale entry survives and stays a hand-off candidate for wid 7 -- a real
    // defect (see the class comment). Pinning today's value here would make
    // that leak harder to fix, so this test stays silent about it.
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
    // The DUMMY_WID fast path answers the sender WITHOUT taking ownership of
    // the holder slot. Every anonymous/reconnecting session shares wid -1, so
    // recording a holder there would make unrelated clients contend for it.
    wIdLockHolderSessionIdMap should not contain key(DUMMY_WID)
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

  it should "grant the lock when the holder slot is absent or null" in {
    Seq((1, "s1", false), (2, "s2", true)).foreach {
      case (wid, id, nullSlot) =>
        val (session, sent) = mockSession(id, uId = Some(1))
        resource.myOnOpen(session)
        resource.myOnMsg(session, send(WIdRequest(wid)))
        if (nullSlot) wIdLockHolderSessionIdMap(wid) = null

        resource.myOnMsg(session, send(AcquireLockRequest()))

        sent should have size 1
        sent.head should include("LockGrantedEvent")
        wIdLockHolderSessionIdMap(wid) shouldBe id
    }
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

  "TryLockRequest from a read-only user" should "leave an existing holder entry alone" in {
    seedAccess(PrivilegeEnum.READ)
    val (session, sent) = lockingSession("s1")
    // Somebody already holds the lock; a read-only viewer arriving afterwards
    // must not overwrite the holder slot with the "nobody holds it" sentinel.
    wIdLockHolderSessionIdMap(accessWid) = "someone-else"

    resource.myOnMsg(session, send(TryLockRequest()))

    sent should have size 2
    sent.head should include("LockRejectedEvent")
    sent(1) should include("WorkflowAccessEvent")
    sent(1) should include("\"workflowReadonly\":true")
    wIdLockHolderSessionIdMap(accessWid) shouldBe "someone-else"
  }

  "TryLockRequest from a writable user" should "grant the lock over the null sentinel" in {
    seedAccess(PrivilegeEnum.WRITE)
    val (session, sent) = lockingSession("s1")
    // The key exists but holds `null`, i.e. a read-only viewer has been here
    // and nobody holds the lock. A writable user must still get it.
    wIdLockHolderSessionIdMap(accessWid) = null

    resource.myOnMsg(session, send(TryLockRequest()))

    sent should have size 2
    sent.head should include("WorkflowAccessEvent")
    sent.head should include("\"workflowReadonly\":false")
    sent(1) should include("LockGrantedEvent")
    wIdLockHolderSessionIdMap(accessWid) shouldBe "s1"
  }

  it should "re-grant the lock to the session that already holds it" in {
    seedAccess(PrivilegeEnum.WRITE)
    val (session, sent) = lockingSession("s1")
    wIdLockHolderSessionIdMap(accessWid) = "s1"

    resource.myOnMsg(session, send(TryLockRequest()))

    sent should have size 2
    sent.head should include("WorkflowAccessEvent")
    sent.head should include("\"workflowReadonly\":false")
    sent(1) should include("LockGrantedEvent")
    wIdLockHolderSessionIdMap(accessWid) shouldBe "s1"
  }

  // -- myOnClose releasing the lock -------------------------------------------

  "myOnClose" should "tolerate a workflow whose session bucket has gone" in {
    val (session, _) = mockSession("s1", uId = Some(1))
    resource.myOnOpen(session)
    resource.myOnMsg(session, send(WIdRequest(7)))
    // The guard on wIdSessionIdsMap is what keeps the two maps from having to
    // agree; without it this close would blow up on a missing key.
    wIdSessionIdsMap.remove(7)

    noException should be thrownBy resource.myOnClose(session)

    sessionIdWIdMap should not contain key("s1")
    sessionIdSessionMap should not contain key("s1")
  }

  it should "leave a null holder sentinel alone" in {
    seedAccess(PrivilegeEnum.WRITE)
    // `null` is the "nobody holds it" sentinel; comparing it against the
    // departing session id must not dereference it, and must not be mistaken
    // for a match. A WRITABLE PEER is deliberately left in the bucket: without
    // one, "branch skipped" and "branch entered" are indistinguishable,
    // because entering it would only rewrite null over null and find nobody to
    // grant to. With s2 present, entering it hands s2 the lock.
    val (session, _) = lockingSession("s1")
    val (_, peerSent) = lockingSession("s2")
    wIdLockHolderSessionIdMap(accessWid) = null
    peerSent.clear()

    noException should be thrownBy resource.myOnClose(session)

    wIdLockHolderSessionIdMap should contain key accessWid
    wIdLockHolderSessionIdMap(accessWid) shouldBe null
    peerSent shouldBe empty
  }

  "myOnClose by the lock holder" should "hand the lock to a remaining writable peer" in {
    seedAccess(PrivilegeEnum.WRITE)
    // Two sessions of the same user, e.g. two browser tabs, both writable.
    val (holder, _) = lockingSession("s1")
    val (_, peerSent) = lockingSession("s2")
    wIdLockHolderSessionIdMap(accessWid) = "s1"
    peerSent.clear()

    resource.myOnClose(holder)

    wIdLockHolderSessionIdMap(accessWid) shouldBe "s2"
    peerSent should have size 1
    peerSent.head should include("LockGrantedEvent")
    wIdSessionIdsMap(accessWid) should contain only "s2"
  }

  it should "not hand the lock to a read-only peer" in {
    seedAccess(PrivilegeEnum.WRITE)
    seedUserAccess(readOnlyUid, "collab_lock_reader", PrivilegeEnum.READ)
    val (holder, _) = lockingSession("s1")
    // The only session left behind belongs to a user with READ only.
    val (_, readerSent) = lockingSession("s2", uId = readOnlyUid)
    wIdLockHolderSessionIdMap(accessWid) = "s1"
    readerSent.clear()

    resource.myOnClose(holder)

    // Nobody eligible remains, so the lock stays unheld. This is what makes
    // the privilege check in the hand-off loop observable: it is checked for
    // the CANDIDATE, not for the departing holder.
    wIdLockHolderSessionIdMap(accessWid) shouldBe null
    readerSent shouldBe empty
  }

  it should "grant the lock to exactly one of several writable peers" in {
    seedAccess(PrivilegeEnum.WRITE)
    val (holder, _) = lockingSession("s1")
    val (_, peer2Sent) = lockingSession("s2")
    val (_, peer3Sent) = lockingSession("s3")
    wIdLockHolderSessionIdMap(accessWid) = "s1"
    peer2Sent.clear()
    peer3Sent.clear()

    resource.myOnClose(holder)

    // The once-only latch must stop the loop after the first eligible peer;
    // two grants would leave two clients each believing they hold the lock.
    // Asserted order-agnostically because the bucket is an unordered Set.
    (peer2Sent.size + peer3Sent.size) shouldBe 1
    val winner = wIdLockHolderSessionIdMap(accessWid)
    Set("s2", "s3") should contain(winner)
    val winnerSent = if (winner == "s2") peer2Sent else peer3Sent
    winnerSent should have size 1
    winnerSent.head should include("LockGrantedEvent")
  }

  it should "ignore sessions sitting on a different workflow" in {
    seedAccess(PrivilegeEnum.WRITE)
    val (holder, _) = lockingSession("s1")
    // Same writable user, but registered on another workflow, so it is not a
    // candidate for accessWid's lock even though its session is still open.
    val (other, otherSent) = mockSession("s3", uId = Some(accessUid))
    resource.myOnOpen(other)
    resource.myOnMsg(other, send(WIdRequest(9999)))
    wIdLockHolderSessionIdMap(accessWid) = "s1"
    otherSent.clear()

    resource.myOnClose(holder)

    // The hand-off must read the departing workflow's own bucket, which is now
    // empty -- not the set of every open session on the server.
    wIdLockHolderSessionIdMap(accessWid) shouldBe null
    otherSent shouldBe empty
  }

  it should "clear the holder to the null sentinel when no session is left" in {
    val (session, sent) = mockSession("s1", uId = Some(1))
    resource.myOnOpen(session)
    resource.myOnMsg(session, send(WIdRequest(7)))
    wIdLockHolderSessionIdMap(7) = "s1"
    sent.clear()

    resource.myOnClose(session)

    wIdLockHolderSessionIdMap(7) shouldBe null
    wIdSessionIdsMap(7) shouldBe empty
    sent shouldBe empty
  }

  "myOnClose by a non-holder" should "leave the lock with its holder" in {
    val (first, _) = mockSession("s1", uId = Some(1))
    val (second, secondSent) = mockSession("s2", uId = Some(2))
    resource.myOnOpen(first)
    resource.myOnOpen(second)
    resource.myOnMsg(first, send(WIdRequest(7)))
    resource.myOnMsg(second, send(WIdRequest(7)))
    wIdLockHolderSessionIdMap(7) = "s2"
    secondSent.clear()

    resource.myOnClose(first)

    wIdLockHolderSessionIdMap(7) shouldBe "s2"
    secondSent shouldBe empty
    wIdSessionIdsMap(7) should contain only "s2"
  }
}
