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

import com.google.protobuf.timestamp.Timestamp
import org.apache.texera.amber.clustering.ClusterListener
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType}
import org.apache.texera.amber.core.virtualidentity.WorkflowIdentity
import org.apache.texera.amber.core.workflow.WorkflowContext
import org.apache.texera.amber.core.workflowruntimestate.FatalErrorType.{
  COMPILATION_ERROR,
  EXECUTION_FAILURE
}
import org.apache.texera.amber.core.workflowruntimestate.WorkflowFatalError
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.auth.util.HeaderField
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.web.SessionState
import org.apache.texera.web.model.websocket.event.{PaginatedResultEvent, TexeraWebSocketEvent}
import org.apache.texera.web.model.websocket.request.{
  HeartBeatRequest,
  ModifyLogicRequest,
  ResultPaginationRequest,
  RetryRequest,
  TexeraWebSocketRequest,
  WorkflowExecuteRequest
}
import org.apache.texera.web.model.websocket.response.ModifyLogicResponse
import org.apache.texera.web.service.{
  ExecutionReconfigurationService,
  ExecutionResultService,
  WorkflowExecutionService,
  WorkflowService
}
import org.apache.texera.web.storage.{ExecutionStateStore, WorkflowStateStore}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import io.reactivex.rxjava3.disposables.Disposable

import java.net.URI
import java.time.Instant
import java.util.UUID
import java.util.concurrent.{Future => JFuture}
import javax.websocket.{RemoteEndpoint, Session}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.{IteratorHasAsScala, MapHasAsJava, SeqHasAsJava}

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
  *   - `ModifyLogicRequest` with a workflow but no execution: `executionService.getValue` is null,
  *     so the handler NPEs instead of reporting "workflow execution is not initialized" — the same
  *     guard gap `case other` used to have, reported rather than pinned. It is also the only input
  *     that would tell `if (workflowStateOpt.isDefined)` apart from `if (executionStateOpt.isDefined)`,
  *     so that guard's exact condition is left unpinned on purpose; see the no-workflow case below.
  *   - `ResultPaginationRequest`'s real payload, which needs a DB; only the request/response
  *     pass-through is asserted here, against a stubbed result service.
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
  // Best-effort: `getState` throws NoSuchElementException on an id that is already gone, and the
  // myOnClose test removes its own entry by design.
  //
  // This runs the real production teardown — `removeState` disposes the session's rx subscriptions
  // and calls `WorkflowService.disconnect` — rather than swapping in a workflow-less stand-in
  // first. That matters: a swap removes the registry entry but leaves the ORIGINAL state's
  // subscription undisposed, so the mock session stays reachable from the workflow's state store
  // for the life of the JVM. The reason a swap looked necessary is that `disconnect` drops the user
  // count to zero and reaches `AmberRuntime.scheduleCallThroughActorSystem`, whose actor system is
  // null in a unit JVM; the myOnOpen tests take a user count of their own on the service they
  // create precisely so that this call never reaches zero. See `holdWorkflowService`.
  override protected def afterEach(): Unit = {
    registeredSessions.foreach { id =>
      scala.util.Try(SessionState.removeState(id))
      // Checked, not assumed. `removeState` unsubscribes BEFORE it removes, so anything that throws
      // in `unsubscribe` — a user count reaching zero and touching the null actor system being the
      // one that bites here — leaves the mock session in the JVM-global registry for the rest of
      // the run, where a later suite's cluster broadcast would call an expired mock.
      withClue(s"session $id was still registered after afterEach: ") {
        scala.util.Try(SessionState.getState(id)).isFailure shouldBe true
      }
    }
    registeredSessions.clear()
  }

  /**
    * Returns the shared `WorkflowService` for `wid`, creating it if needed, and takes a user count
    * on it that is never released.
    *
    * Two things follow, both wanted. `myOnOpen` must JOIN this instance rather than build its own,
    * which is what makes the identity assertions below meaningful; and the count never reaches zero
    * in `afterEach`, so the real teardown can run without touching a null actor system.
    *
    * The cost is one `WorkflowService` per wid left in `WorkflowService.workflowServiceMapping` with
    * a user count of 1 for the life of the JVM. Nothing test-scoped is reachable from it once the
    * session is removed, and the wid namespace 9701xx is reserved for this suite — note that
    * `getOrCreate` keys that map on the workflow id ALONE (`mkWorkflowStateId`), so a sibling suite
    * reusing one of these ids would silently get this instance back.
    */
  private def holdWorkflowService(wid: Long, cuid: Int): WorkflowService = {
    val service = WorkflowService.getOrCreate(WorkflowIdentity(wid), cuid)
    service.lifeCycleManager.increaseUserCount()
    service
  }

  /**
    * A mocked session. `sent` collects everything the endpoint writes back, which is the only
    * observable for most of these handlers.
    */
  private def mockSession(
      id: String = UUID.randomUUID().toString,
      uid: Option[Int] = None,
      cuAccess: String = PrivilegeEnum.WRITE.name(),
      wid: Long = 1L,
      cuid: Int = 1
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
    properties.put(HeaderField.UserComputingUnitAccess, cuAccess)
    uid.foreach { u =>
      val user = new User()
      user.setUid(Integer.valueOf(u))
      properties.put(classOf[User].getName, user)
    }

    // What `myOnOpen` parses the workflow and computing-unit ids out of.
    val parameters: java.util.Map[String, java.util.List[String]] =
      Map("wid" -> Seq(wid.toString).asJava, "cuid" -> Seq(cuid.toString).asJava).asJava

    val session = mock[Session]
    (() => session.getId).expects().returning(id).anyNumberOfTimes()
    (() => session.getAsyncRemote).expects().returning(async).anyNumberOfTimes()
    (() => session.getUserProperties).expects().returning(properties).anyNumberOfTimes()
    (() => session.getRequestParameterMap).expects().returning(parameters).anyNumberOfTimes()
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

    /**
      * When set, `initExecutionService` publishes this execution and then fails — the shape a real
      * `executeWorkflow()` failure takes, and the only way to reach the endpoint's catch arm with an
      * execution that did NOT exist when the frame arrived.
      */
    var publishThenFail: Option[WorkflowExecutionService] = None

    val results = new StubResultService(workflowId, computingUnitId, stateStore)
    override val resultService: ExecutionResultService = results

    override def connect(onNext: TexeraWebSocketEvent => Unit): Disposable = Disposable.empty()
    override def connectToExecution(onNext: TexeraWebSocketEvent => Unit): Disposable =
      Disposable.empty()
    override def disconnect(): Unit = ()

    override def initExecutionService(
        req: WorkflowExecuteRequest,
        userOpt: Option[User],
        sessionUri: URI
    ): Unit = {
      initCalls = initCalls :+ ((req, userOpt, sessionUri))
      publishThenFail.foreach { execution =>
        executionService.onNext(execution)
        throw new IllegalStateException("failed while starting the execution")
      }
    }
  }

  /**
    * The real `handleResultPagination` resolves the latest execution out of the database and opens
    * an Iceberg document; the endpoint's own contribution is only that the request travels in and
    * the returned event travels back out. The echoed fields make both directions observable.
    *
    * The table and schema carry values on purpose. Production ships a companion
    * `PaginatedResultEvent(req, table, schema)` that rebuilds requestID/operatorID/pageIndex FROM the
    * request, so an endpoint that called the service and then constructed its own event would match
    * on those three fields anyway. These two payload fields are the only ones that can only have
    * come from the returned event.
    */
  private class StubResultService(
      workflowId: WorkflowIdentity,
      computingUnitId: Int,
      stateStore: WorkflowStateStore
  ) extends ExecutionResultService(workflowId, computingUnitId, stateStore) {
    var captured: Option[ResultPaginationRequest] = None

    override def handleResultPagination(request: ResultPaginationRequest): TexeraWebSocketEvent = {
      captured = Some(request)
      PaginatedResultEvent(
        request.requestID,
        request.operatorID,
        request.pageIndex,
        List(objectMapper.createObjectNode().put("c", 5)),
        List(new Attribute("c", AttributeType.INTEGER))
      )
    }
  }

  /**
    * Production only builds an `ExecutionReconfigurationService` inside `executeWorkflow()`, but
    * the field it lands in is a public var, so a test can put a stub there directly. The two
    * `register*` overrides are the seams the service already ships for constructing it without a
    * live `AmberClient` (see `ExecutionReconfigurationServiceSpec`).
    */
  private class CapturingReconfigurationService(stateStore: ExecutionStateStore)
      extends ExecutionReconfigurationService(client = null, stateStore, workflow = null) {
    var captured: Option[ModifyLogicRequest] = None

    override protected def registerWorkerCompletionCallback(): Unit = ()
    override protected def registerCompletionDiffHandler(): Unit = ()

    override def modifyOperatorLogic(request: ModifyLogicRequest): TexeraWebSocketEvent = {
      captured = Some(request)
      // Both fields are derived from the request that arrived, so the response the endpoint
      // forwards proves which request the service actually saw.
      ModifyLogicResponse(
        request.operator.operatorIdentifier.id,
        isValid = true,
        errorMessage = request.operator.asInstanceOf[LimitOpDesc].limit.toString
      )
    }
  }

  /**
    * A real execution, built the way `WorkflowExecutionServiceSpec`/`WorkflowServiceSpec` do:
    * construction does no external work, so the coordinator config and result service can be null.
    */
  private def newExecution(): WorkflowExecutionService =
    new WorkflowExecutionService(
      null,
      new WorkflowContext(),
      null,
      executeRequest,
      new ExecutionStateStore(),
      (_: Throwable) => (),
      None,
      new URI("vfs:///test")
    )

  /** A session state holding `workflow`, with `execution` published into it when given. */
  private def attach(
      session: Session,
      access: PrivilegeEnum,
      workflow: TestWorkflowService,
      execution: Option[WorkflowExecutionService] = None
  ): SessionState = {
    val state = registerState(session, access)
    state.subscribe(workflow)
    execution.foreach(workflow.executionService.onNext)
    state
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

  /** Shared by the "forwarded" and "no workflow attached" halves of each guarded arm. */
  private val modifyLogicFrame =
    """{"type":"ModifyLogicRequest","operator":{"operatorType":"Limit","limit":7}}"""

  private val paginationFrame =
    """{"type":"ResultPaginationRequest","requestID":"req-77","operatorID":"op-88",
      |"pageIndex":3,"pageSize":25}""".stripMargin

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
    // exists once an execution has been created. This drives the case with NO workflow attached;
    // the next test drives the workflow-attached-but-not-yet-executing half of the same guard.
    val (session, sent) = mockSession(uid = Some(42))
    registerState(session, PrivilegeEnum.WRITE)

    val ex = intercept[IllegalStateException] {
      resource.myOnMsg(session, """{"type":"WorkflowPauseRequest"}""")
    }
    ex.getMessage should include("workflow execution is not initialized")

    sentTypes(sent) shouldBe Seq("WorkflowErrorEvent")
    fatalErrorMessages(sent).head should include("workflow execution is not initialized")
  }

  it should "report a runtime command that arrives before the execution is created" in {
    // The workflow is attached, so `workflowStateOpt` is defined, but nothing has published an
    // execution yet: `executionService` is a BehaviorSubject seeded with null, so its current value
    // is null rather than absent. Reading it with `map` would wrap that null into a Some and slip
    // past the `case None` guard, so the command would NPE on `value.wsInput` instead of reporting
    // the uninitialized execution. The message, not just the throw, is what this pins.
    val (session, sent) = mockSession(uid = Some(42))
    val workflow = new TestWorkflowService(9103L)
    attach(session, PrivilegeEnum.WRITE, workflow)
    workflow.executionService.getValue shouldBe null

    val ex = intercept[IllegalStateException] {
      resource.myOnMsg(session, """{"type":"WorkflowPauseRequest"}""")
    }
    ex.getMessage should include("workflow execution is not initialized")

    // With no execution there is no metadata store to record into, so the error mapper falls back
    // to the socket -- the same route the no-workflow case takes.
    sentTypes(sent) shouldBe Seq("WorkflowErrorEvent")
    fatalErrorMessages(sent).head should include("workflow execution is not initialized")
  }

  it should "hand an unrecognised runtime command to the execution's websocket input" in {
    // The other half of the same arm: with an execution attached the command is forwarded rather
    // than rejected, and the sender's uid rides along with it.
    val (session, sent) = mockSession(uid = Some(42))
    val workflow = new TestWorkflowService(9102L)
    val execution = newExecution()
    attach(session, PrivilegeEnum.WRITE, workflow, Some(execution))

    val received = ArrayBuffer.empty[(TexeraWebSocketRequest, Option[Integer])]
    execution.wsInput.subscribe[TexeraWebSocketRequest]((req, uid) => received += ((req, uid)))

    // A payload-carrying request, deliberately. `WorkflowPauseRequest` is a field-less case class,
    // so its generated equals matches ANY instance: asserting on one would stay green with the
    // deserialized request dropped and a fresh constant forwarded in its place.
    resource.myOnMsg(session, """{"type":"RetryRequest","workers":["worker-a","worker-b"]}""")

    received should have size 1
    received.head._1 shouldBe RetryRequest(Seq("worker-a", "worker-b"))
    // Some(42), not None: None is what a missing user-properties entry yields anyway, so asserting
    // it would be green with the whole uid lookup deleted.
    received.head._2 shouldBe Some(Integer.valueOf(42))
    // Forwarding is the whole handling — nothing goes back to the socket, and no exception escapes.
    sent shouldBe empty
  }

  // -- reconfiguration ----------------------------------------------------------

  it should "forward a ModifyLogicRequest to the execution's reconfiguration service" in {
    val (session, sent) = mockSession(uid = Some(42))
    val workflow = new TestWorkflowService(9103L)
    val execution = newExecution()
    val reconfiguration = new CapturingReconfigurationService(execution.executionStateStore)
    execution.executionReconfigurationService = reconfiguration
    attach(session, PrivilegeEnum.WRITE, workflow, Some(execution))

    resource.myOnMsg(session, modifyLogicFrame)

    // The deserialized operator has to arrive intact: asserting only that SOME ModifyLogicResponse
    // came back would stay green with a different (or default) request forwarded.
    val captured = reconfiguration.captured.getOrElse(fail("no request reached the service"))
    captured.operator shouldBe a[LimitOpDesc]
    captured.operator.asInstanceOf[LimitOpDesc].limit shouldBe 7

    sentTypes(sent) shouldBe Seq("ModifyLogicResponse")
    val response = objectMapper.readTree(sent.head)
    // Both values were computed by the stub FROM the request it received, so this pins the whole
    // round trip rather than the shape of a canned reply.
    response.get("opId").asText() shouldBe captured.operator.operatorIdentifier.id
    response.get("errorMessage").asText() shouldBe "7"
  }

  it should "ignore a ModifyLogicRequest that arrives before any workflow is attached" in {
    // The `if (workflowStateOpt.isDefined)` guard, observed false. Without this case the guard could
    // be replaced by `if (true)` and nothing would notice: the `.get` on the empty Option would
    // throw NoSuchElementException, the catch-all would turn it into a WorkflowErrorEvent, and the
    // rethrow would escape — so both assertions here have teeth.
    //
    // NOT pinned, and it cannot be: rewriting the guard as `if (executionStateOpt.isDefined)` is
    // indistinguishable from the current one to every test in this suite. The only input that
    // separates them is a workflow with no execution, where today's guard NPEs on
    // `executionService.getValue` and the rewrite quietly does nothing. Pinning either answer would
    // cement one of them, and the rewrite is arguably the fix.
    val (session, sent) = mockSession(uid = Some(42))
    registerState(session, PrivilegeEnum.WRITE)

    noException should be thrownBy resource.myOnMsg(session, modifyLogicFrame)

    sent shouldBe empty
  }

  // -- pagination ---------------------------------------------------------------

  it should "answer a pagination request with what the result service returns" in {
    val (session, sent) = mockSession()
    val workflow = new TestWorkflowService(9104L)
    attach(session, PrivilegeEnum.WRITE, workflow)

    resource.myOnMsg(session, paginationFrame)

    // Four distinct field values, so the request cannot have been rebuilt from defaults on the way
    // in; the endpoint has to pass the deserialized one straight through.
    val captured =
      workflow.results.captured.getOrElse(fail("no request reached the result service"))
    captured.requestID shouldBe "req-77"
    captured.operatorID shouldBe "op-88"
    captured.pageIndex shouldBe 3
    captured.pageSize shouldBe 25

    sentTypes(sent) shouldBe Seq("PaginatedResultEvent")
    val event = objectMapper.readTree(sent.head)
    event.get("requestID").asText() shouldBe "req-77"
    event.get("operatorID").asText() shouldBe "op-88"
    event.get("pageIndex").asInt() shouldBe 3
    // The other direction, and the half the three fields above cannot see: those are all
    // reconstructible from the request, and production has a `PaginatedResultEvent(req, ...)`
    // companion that does exactly that. The table and schema exist only in the event the service
    // returned, so they are what proves the endpoint forwards the RETURN VALUE.
    event.get("table").get(0).get("c").asInt() shouldBe 5
    event.get("schema").get(0).get("attributeName").asText() shouldBe "c"
  }

  it should "ignore a pagination request that arrives before any workflow is attached" in {
    // `workflowStateOpt.foreach(...)`, observed empty. Replacing that with
    // `workflowStateOpt.get.resultService...` throws NoSuchElementException here, which the
    // catch-all turns into a WorkflowErrorEvent and then rethrows — so both assertions bite.
    val (session, sent) = mockSession()
    registerState(session, PrivilegeEnum.WRITE)

    noException should be thrownBy resource.myOnMsg(session, paginationFrame)

    sent shouldBe empty
  }

  // -- the error mapper's metadata-store arm ------------------------------------

  it should "record a failure in the execution's metadata store, replacing the stale compilation error" in {
    // With an execution attached the mapper writes into its metadata store instead of the socket,
    // and drops any earlier COMPILATION_ERROR so the panel shows one compilation failure at a time.
    val (session, sent) = mockSession(uid = Some(42))
    val workflow = new TestWorkflowService(9105L)
    val execution = newExecution()
    attach(session, PrivilegeEnum.READ, workflow, Some(execution))

    val store = execution.executionStateStore.metadataStore
    // Two seeded errors of two different types. With only the new error in play the filter would
    // have nothing to remove, so deleting it entirely would leave the test green.
    store.updateState(
      _.addFatalErrors(
        WorkflowFatalError(
          COMPILATION_ERROR,
          Timestamp(Instant.now),
          "stale compilation",
          "",
          "op-a"
        ),
        WorkflowFatalError(EXECUTION_FAILURE, Timestamp(Instant.now), "runtime failure", "", "op-b")
      )
    )

    // READ access, so the write-access gate throws inside the try.
    intercept[IllegalStateException] {
      resource.myOnMsg(session, frameOf(executeRequest))
    }

    val errors = store.getState.fatalErrors
    // The unrelated execution failure survives; only the compilation error is superseded.
    errors.map(_.message) should contain("runtime failure")
    errors.map(_.message) should not contain "stale compilation"
    val compilationErrors = errors.filter(_.`type` == COMPILATION_ERROR)
    compilationErrors should have size 1
    compilationErrors.head.message should include("write access")
    // `details` is what the frontend's error panel shows a developer, and it has to be the stack
    // trace, not the message again: naming the throwing frame is what separates
    // `getStackTraceWithAllCauses(err)` from `err.toString` / `err.getMessage`, both of which would
    // satisfy any "is non-empty" check.
    compilationErrors.head.details should include("WorkflowWebsocketResource.myOnMsg")
    // A real clock reading, not `Timestamp.defaultInstance` — the panel orders errors by this.
    compilationErrors.head.timestamp.seconds should be > 0L

    // Nothing reaches the socket: this is the arm that replaces the WorkflowErrorEvent send.
    sent shouldBe empty
  }

  it should "route a failure raised while the execution is being created to the socket" in {
    // `executionStateOpt` is captured BEFORE the try, so it names the execution that existed when
    // the frame arrived. An execution created DURING the request therefore does not receive the
    // error — it goes to the socket instead. Recomputing that capture inside the catch (it is used
    // nowhere else) would reverse the routing, and no other case in this suite can see the
    // difference, because every other one either has an execution throughout or never gains one.
    val (session, sent) = mockSession(uid = Some(42))
    val workflow = new TestWorkflowService(9106L)
    val execution = newExecution()
    workflow.publishThenFail = Some(execution)
    // Attached with NO execution: the capture is None.
    attach(session, PrivilegeEnum.WRITE, workflow)

    intercept[IllegalStateException] {
      resource.myOnMsg(session, frameOf(executeRequest))
    }

    // The execution exists by the time the handler fails ...
    workflow.executionService.getValue shouldBe execution
    // ... and is nonetheless not where the error was recorded.
    execution.executionStateStore.metadataStore.getState.fatalErrors shouldBe empty
    sentTypes(sent) shouldBe Seq("WorkflowStateEvent", "WorkflowErrorEvent")
    fatalErrorMessages(sent).head should include("failed while starting the execution")
  }

  // -- session lifecycle --------------------------------------------------------

  "myOnOpen" should "bind the session to the requested workflow, computing unit and privilege" in {
    // Two rows, because one cannot tell a real parse from a constant: the ids differ from each
    // other and from 1, and the privilege differs between rows.
    Seq((970101L, 7, PrivilegeEnum.READ), (970102L, 13, PrivilegeEnum.WRITE)).foreach {
      case (wid, cuid, access) =>
        withClue(s"wid=$wid cuid=$cuid access=$access: ") {
          val (session, sent) = mockSession(cuAccess = access.name(), wid = wid, cuid = cuid)
          // The service this call has to JOIN. Created up front so the assertion below can be about
          // instance identity rather than field values.
          val shared = holdWorkflowService(wid, cuid)
          // Registered before the call so afterEach still cleans up if myOnOpen throws.
          registeredSessions += session.getId

          // A sentinel node count, so the event's payload is pinned to the global the endpoint is
          // supposed to read rather than to 0, which is that global's initializer. Restored in a
          // finally, the way ClusterListenerSpec handles the same var — amber serialises its suites
          // (amber/build.sbt: `Tags.limit(Tags.Test, 1)`), so the window is this call alone.
          val previousNodeCount = ClusterListener.numWorkerNodesInCluster
          ClusterListener.numWorkerNodesInCluster = 4242
          try resource.myOnOpen(session, null)
          finally ClusterListener.numWorkerNodesInCluster = previousNodeCount

          val state = SessionState.getState(session.getId)
          // The same INSTANCE, not merely the same ids. `WorkflowService.getOrCreate` is what makes
          // one workflow shared by every client that opens it; `new WorkflowService(...)` in its
          // place would satisfy every field assertion below while silently handing each websocket a
          // private workflow of its own — i.e. deleting multi-user collaboration.
          state.getCurrentWorkflowState.getOrElse(
            fail("no workflow bound")
          ) should be theSameInstanceAs shared
          state.getCurrentWorkflowState.map(s => (s.workflowId.id, s.computingUnitId)) shouldBe
            Some((wid, cuid))
          // READ/WRITE, never NONE: NONE is SessionState's own field default, so feeding it would
          // pass with the privilege never being set at all.
          state.getUserComputingUnitAccess shouldBe access

          // Ordered, not merely present. The state event is the "hack to refresh frontend run
          // button state" and has to go out before the workflow subscription is established.
          sentTypes(sent) shouldBe Seq("WorkflowStateEvent", "ClusterStatusUpdateEvent")
          objectMapper.readTree(sent.head).get("state").asText() shouldBe "Uninitialized"
          objectMapper.readTree(sent(1)).get("numWorkers").asInt() shouldBe 4242
        }
    }
  }

  it should "join a workflow that already exists, ignoring the computing unit it was opened with" in {
    // The cached arm of `getOrCreate`, which the rows above never take. `workflowServiceMapping` is
    // keyed on `mkWorkflowStateId(workflowId)` — the workflow id ALONE — so `computingUnitId` is
    // used only when the entry is constructed and is silently dropped for every later opener.
    //
    // This is characterization, not endorsement: a second websocket asking for the same workflow on
    // a DIFFERENT computing unit is bound to the first one's, with nothing reported. Recorded here
    // because it is the behaviour the endpoint actually has, and because the test above cannot see
    // it — both of its rows construct.
    val existing = holdWorkflowService(wid = 970103L, cuid = 5)
    val (session, _) = mockSession(cuAccess = PrivilegeEnum.WRITE.name(), wid = 970103L, cuid = 99)
    registeredSessions += session.getId

    resource.myOnOpen(session, null)

    val bound = SessionState
      .getState(session.getId)
      .getCurrentWorkflowState
      .getOrElse(fail("no workflow bound"))
    // Identity is the assertion, deliberately: it is what pins the cached arm (keying `getOrCreate`
    // on wid+cuid would construct a second service and fail this). The dropped computing unit id is
    // described above but NOT asserted -- pinning it would turn a defect into a contract and make
    // this spec an obstacle to fixing it.
    bound should be theSameInstanceAs existing
  }

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
