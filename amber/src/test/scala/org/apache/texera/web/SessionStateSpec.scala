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

package org.apache.texera.web

import io.reactivex.rxjava3.disposables.Disposable
import org.apache.texera.amber.core.virtualidentity.WorkflowIdentity
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.web.model.websocket.event.{TexeraWebSocketEvent, WorkflowStateEvent}
import org.apache.texera.web.service.WorkflowService
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.{Future => JFuture}
import javax.websocket.{RemoteEndpoint, Session}
import scala.collection.mutable.ArrayBuffer

class SessionStateSpec extends AnyFlatSpec with Matchers with MockFactory {

  private class TrackingDisposable extends Disposable {
    var disposeCalls = 0

    override def dispose(): Unit = disposeCalls += 1

    override def isDisposed: Boolean = disposeCalls > 0
  }

  private class TestWorkflowService(id: Long) extends WorkflowService(WorkflowIdentity(id), 1, 10) {
    val workflowSubscription = new TrackingDisposable
    val executionSubscription = new TrackingDisposable
    var workflowConnectCalls = 0
    var executionConnectCalls = 0
    var disconnectCalls = 0

    override def connect(onNext: TexeraWebSocketEvent => Unit): Disposable = {
      workflowConnectCalls += 1
      workflowSubscription
    }

    override def connectToExecution(onNext: TexeraWebSocketEvent => Unit): Disposable = {
      executionConnectCalls += 1
      executionSubscription
    }

    override def disconnect(): Unit = disconnectCalls += 1
  }

  private def messageCollectingSession(): (Session, ArrayBuffer[String]) = {
    val messages = ArrayBuffer[String]()
    val async = mock[RemoteEndpoint.Async]
    (async
      .sendText(_: String))
      .expects(*)
      .onCall { (message: String) =>
        messages += message
        null.asInstanceOf[JFuture[Void]]
      }
      .anyNumberOfTimes()

    val session = mock[Session]
    (() => session.getAsyncRemote).expects().returning(async).anyNumberOfTimes()
    (session, messages)
  }

  private def removeStateIfPresent(sessionId: String): Unit = {
    try {
      SessionState.removeState(sessionId)
    } catch {
      case _: NoSuchElementException =>
    }
  }

  "SessionState" should "send websocket events as typed JSON messages" in {
    val (session, messages) = messageCollectingSession()
    val state = new SessionState(session)

    state.send(WorkflowStateEvent("RUNNING"))

    messages should have size 1
    val payload = objectMapper.readTree(messages.head)
    payload.get("type").asText() shouldBe "WorkflowStateEvent"
    payload.get("state").asText() shouldBe "RUNNING"
  }

  it should "replace subscriptions before attaching a new workflow service" in {
    val state = new SessionState(stub[Session])
    val firstService = new TestWorkflowService(1L)
    val secondService = new TestWorkflowService(2L)

    state.subscribe(firstService)
    state.getCurrentWorkflowState shouldBe Some(firstService)
    firstService.workflowConnectCalls shouldBe 1
    firstService.executionConnectCalls shouldBe 1

    state.subscribe(secondService)
    firstService.workflowSubscription.disposeCalls shouldBe 1
    firstService.executionSubscription.disposeCalls shouldBe 1
    firstService.disconnectCalls shouldBe 1
    state.getCurrentWorkflowState shouldBe Some(secondService)
    secondService.workflowConnectCalls shouldBe 1
    secondService.executionConnectCalls shouldBe 1

    state.unsubscribe()
    secondService.workflowSubscription.disposeCalls shouldBe 1
    secondService.executionSubscription.disposeCalls shouldBe 1
    secondService.disconnectCalls shouldBe 1
    state.getCurrentWorkflowState shouldBe None
  }

  it should "remove registered sessions and retain their computing-unit access level" in {
    val sessionId = UUID.randomUUID().toString
    val state = new SessionState(stub[Session])
    SessionState.setState(sessionId, state)
    try {
      SessionState.getState(sessionId) shouldBe state
      SessionState.getAllSessionStates should contain(state)

      state.getUserComputingUnitAccess shouldBe PrivilegeEnum.NONE
      state.setUserComputingUnitAccess(PrivilegeEnum.WRITE)
      state.getUserComputingUnitAccess shouldBe PrivilegeEnum.WRITE

      SessionState.removeState(sessionId)
      SessionState.getAllSessionStates should not contain state
      a[NoSuchElementException] should be thrownBy SessionState.getState(sessionId)
    } finally {
      removeStateIfPresent(sessionId)
    }
  }
}
