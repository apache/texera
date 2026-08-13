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

import org.apache.texera.amber.core.virtualidentity.WorkflowIdentity
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.auth.util.HeaderField
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.web.SessionState
import org.apache.texera.web.model.websocket.event.TexeraWebSocketEvent
import org.apache.texera.web.model.websocket.request.{
  HeartBeatRequest,
  TexeraWebSocketRequest,
  WorkflowExecuteRequest
}
import org.apache.texera.web.service.WorkflowService
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import io.reactivex.rxjava3.disposables.Disposable

import java.net.URI
import java.util.UUID
import java.util.concurrent.{Future => JFuture}
import javax.websocket.{RemoteEndpoint, Session}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.IteratorHasAsScala

/**
  * Unit tests for the websocket endpoint's message handling.
  *
  * The endpoint itself is thin, but two pieces of it are real logic that nothing else guards:
  *
  *   - the write-access gate on `WorkflowExecuteRequest`. It is the only consumer of the privilege
  *     `myOnOpen` parsed off the handshake, and denying it must both surface an error to the client
  *     and rethrow. Note this branch is unreachable in single-node mode, where
  *     `ServletAwareConfigurator` hard-codes WRITE, but live under Kubernetes where the privilege
  *     comes from the `x-user-computing-unit-access` header.
  *   - the catch-all error mapper, which turns any exception into a `WorkflowFatalError`, routes it
  *     either to the execution's metadata store or (with no execution) straight to the socket, and
  *     then rethrows so the container sees it too. Both halves are asserted: dropping either the
  *     send or the rethrow would leave the other silently missing.
  *
  * Worth knowing, and the reason there is no malformed-frame test here: `objectMapper.readValue`
  * sits OUTSIDE the try, so an unparseable frame escapes un-mapped and the client is told nothing.
  * That looks like an oversight, but pinning today's behaviour would cement it.
  *
  * Everything here drives a mocked `javax.websocket.Session` (the pattern `CollaborationResourceSpec`
  * established) and registers a `SessionState` directly, so no real workflow is created.
  *
  * Deliberately not covered, because each would pin an accidental failure rather than a contract:
  *   - `myOnOpen`'s missing-`wid`/`cuid` and bogus-privilege paths, which fail with NPE /
  *     IndexOutOfBounds / IllegalArgumentException. A harmless improvement (a default, or a clear
  *     message) would break such a test.
  *   - `ModifyLogicRequest`, which reaches `executionReconfigurationService` — null until
  *     `executeWorkflow()` has run, so the only way to exercise it here is via an NPE that
  *     production never reaches.
  *   - `ResultPaginationRequest`, whose payload line needs a DB, and whose no-workflow case is a
  *     discarded `Option.foreach` — "nothing sent, nothing thrown" asserts nothing.
  *   - the deserialization line, already owned by `TexeraWebSocketRequestSpec`.
  */
class WorkflowWebsocketResourceSpec
    extends AnyFlatSpec
    with Matchers
    with MockFactory
    with BeforeAndAfterEach {

  private var resource: WorkflowWebsocketResource = _
  private val registeredSessions = ArrayBuffer[String]()

  override protected def beforeEach(): Unit = {
    resource = new WorkflowWebsocketResource()
  }

  // SessionState's registry is a JVM-global map. Remove only the ids this test registered, so the
  // suite cannot disturb (or be disturbed by) SessionStateSpec running in the same JVM.
  //
  // Best-effort: `removeState` throws NoSuchElementException on an id that is already gone, and the
  // myOnClose test removes its own entry by design.
  override protected def afterEach(): Unit = {
    registeredSessions.foreach(id => scala.util.Try(SessionState.removeState(id)))
    registeredSessions.clear()
  }

  /**
    * A mocked session. `sent` collects everything the endpoint writes back, which is the only
    * observable for most of these handlers.
    */
  private def mockSession(
      id: String = UUID.randomUUID().toString,
      uid: Option[Int] = None
  ): (Session, ArrayBuffer[String]) = {
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
    properties.put(HeaderField.UserComputingUnitAccess, PrivilegeEnum.WRITE.name())
    uid.foreach { u =>
      val user = new User()
      user.setUid(Integer.valueOf(u))
      properties.put(classOf[User].getName, user)
    }

    val session = mock[Session]
    (() => session.getId).expects().returning(id).anyNumberOfTimes()
    (() => session.getAsyncRemote).expects().returning(async).anyNumberOfTimes()
    (() => session.getUserProperties).expects().returning(properties).anyNumberOfTimes()
    (() => session.getRequestURI)
      .expects()
      .returning(new URI("ws://localhost/wsapi/workflow-websocket"))
      .anyNumberOfTimes()

    (session, sent)
  }

  /** Registers a SessionState for `session` and remembers it for cleanup. */
  private def registerState(session: Session, access: PrivilegeEnum): SessionState = {
    val state = new SessionState(session)
    state.setUserComputingUnitAccess(access)
    SessionState.setState(session.getId, state)
    registeredSessions += session.getId
    state
  }

  /**
    * Stands in for a real WorkflowService. Overriding the three subscription members keeps the
    * lifecycle manager out of it: a real `disconnect()` drops the user count to zero and reaches
    * `AmberRuntime.scheduleCallThroughActorSystem`, whose actor system is null in a unit JVM.
    */
  private class TestWorkflowService(id: Long) extends WorkflowService(WorkflowIdentity(id), 1, 10) {
    var initCalls: List[(WorkflowExecuteRequest, Option[User], URI)] = Nil

    override def connect(onNext: TexeraWebSocketEvent => Unit): Disposable = Disposable.empty()
    override def connectToExecution(onNext: TexeraWebSocketEvent => Unit): Disposable =
      Disposable.empty()
    override def disconnect(): Unit = ()

    override def initExecutionService(
        req: WorkflowExecuteRequest,
        userOpt: Option[User],
        sessionUri: URI
    ): Unit = initCalls = initCalls :+ ((req, userOpt, sessionUri))
  }

  private def executeRequest: WorkflowExecuteRequest =
    objectMapper
      .readValue(
        """{"type":"WorkflowExecuteRequest","executionName":"exec","engineVersion":"v",
          |"logicalPlan":{"operators":[],"links":[],"opsToViewResult":[],"opsToReuseResult":[]},
          |"workflowSettings":{},"emailNotificationEnabled":false,"computingUnitId":1}""".stripMargin,
        classOf[TexeraWebSocketRequest]
      )
      .asInstanceOf[WorkflowExecuteRequest]

  private def frameOf(request: TexeraWebSocketRequest): String =
    objectMapper.writeValueAsString(request)

  /** The `type` discriminator of each frame the endpoint wrote back. */
  private def sentTypes(sent: ArrayBuffer[String]): Seq[String] =
    sent.toSeq.map(objectMapper.readTree(_).get("type").asText())

  private def fatalErrorMessages(sent: ArrayBuffer[String]): Seq[String] =
    sent.toSeq
      .map(objectMapper.readTree)
      .filter(_.get("type").asText() == "WorkflowErrorEvent")
      .flatMap(_.get("fatalErrors").elements().asScala)
      .map(_.get("message").asText())

  // -- heartbeat ----------------------------------------------------------------

  "myOnMsg" should "answer a heartbeat without needing a workflow" in {
    val (session, sent) = mockSession()
    registerState(session, PrivilegeEnum.WRITE)

    resource.myOnMsg(session, frameOf(HeartBeatRequest()))

    sentTypes(sent) shouldBe Seq("HeartBeatResponse")
  }

  // -- the write-access gate ----------------------------------------------------

  it should "refuse a WorkflowExecuteRequest when the session has no write access" in {
    // READ, not WRITE: the gate must fire before any workflow lookup. The handler both reports the
    // failure to the client AND rethrows, so the container sees it too — assert both halves,
    // because dropping the rethrow (or the send) would leave one of them silently missing.
    val (session, sent) = mockSession(uid = Some(42))
    registerState(session, PrivilegeEnum.READ)

    val ex = intercept[IllegalStateException] {
      resource.myOnMsg(session, frameOf(executeRequest))
    }
    ex.getMessage should include("write access")

    sentTypes(sent) shouldBe Seq("WorkflowErrorEvent")
    val errors = objectMapper.readTree(sent.head).get("fatalErrors")
    errors.size() shouldBe 1
    // Every mapped error is stamped COMPILATION_ERROR with a placeholder operator; the frontend's
    // error panel keys on both.
    // The scalapb enum does not serialize as a bare string: it becomes an object, so the client
    // reads `type.name`. Pinning the nested shape is the point — flattening it would break the
    // frontend's error panel silently.
    errors.get(0).get("type").get("name").asText() shouldBe "COMPILATION_ERROR"
    errors.get(0).get("operatorId").asText() shouldBe "unknown operator"
    errors.get(0).get("message").asText() should include("write access")
    errors.get(0).get("details").asText() should not be empty
  }

  it should "refuse a WorkflowExecuteRequest with write access but no workflow attached" in {
    // WRITE, so the gate passes; the request then has nowhere to go. A distinct message is what
    // proves the gate is its own branch rather than the same failure reported twice.
    val (session, sent) = mockSession(uid = Some(42))
    registerState(session, PrivilegeEnum.WRITE)

    val ex = intercept[IllegalStateException] {
      resource.myOnMsg(session, frameOf(executeRequest))
    }
    ex.getMessage should include("workflow is not initialized")

    sentTypes(sent) shouldBe Seq("WorkflowErrorEvent")
    fatalErrorMessages(sent).head should include("workflow is not initialized")
  }

  it should "announce Initializing before handing the request to the workflow" in {
    val (session, sent) = mockSession(uid = Some(42))
    val state = registerState(session, PrivilegeEnum.WRITE)
    val workflow = new TestWorkflowService(9101L)
    state.subscribe(workflow)

    val request = executeRequest
    resource.myOnMsg(session, frameOf(request))

    // Order matters: the frontend flips its run button on this event, so it has to go out before
    // the (synchronous, potentially slow) init call rather than after it.
    sentTypes(sent) shouldBe Seq("WorkflowStateEvent")
    objectMapper.readTree(sent.head).get("state").asText() shouldBe "Initializing"

    workflow.initCalls should have size 1
    val (forwardedReq, forwardedUser, forwardedUri) = workflow.initCalls.head
    forwardedReq.executionName shouldBe request.executionName
    // The uid is read out of the session's user-properties map; a wrong key would silently
    // degrade every execution to anonymous.
    forwardedUser.map(_.getUid.intValue()) shouldBe Some(42)
    forwardedUri.toString should endWith("/wsapi/workflow-websocket")
  }

  // -- the default dispatch arm -------------------------------------------------

  it should "report a runtime command that arrives before any workflow is attached" in {
    // Anything that is not one of the four named requests falls through to `wsInput`, which only
    // exists once an execution has been created.
    //
    // Note this drives the case with NO workflow attached. With a workflow attached but no
    // execution, `executionService.getValue` returns null and `workflowStateOpt.map(...)` yields
    // Some(null), which slips past the `case None` guard and NPEs on `value.wsInput`. That is a
    // real gap in the guard, but it is an accidental failure mode rather than a contract, so it is
    // reported rather than pinned here.
    val (session, sent) = mockSession(uid = Some(42))
    registerState(session, PrivilegeEnum.WRITE)

    val ex = intercept[IllegalStateException] {
      resource.myOnMsg(session, """{"type":"WorkflowPauseRequest"}""")
    }
    ex.getMessage should include("workflow execution is not initialized")

    sentTypes(sent) shouldBe Seq("WorkflowErrorEvent")
    fatalErrorMessages(sent).head should include("workflow execution is not initialized")
  }

  // -- session lifecycle --------------------------------------------------------

  "myOnClose" should "drop the state registered under that session id" in {
    val (session, _) = mockSession()
    val state = registerState(session, PrivilegeEnum.WRITE)
    SessionState.getState(session.getId) shouldBe state

    resource.myOnClose(session, null)

    // getState throws once the entry is gone, which is how the endpoint's own handlers would fail
    // if a frame arrived after close.
    a[NoSuchElementException] should be thrownBy SessionState.getState(session.getId)
  }
}
