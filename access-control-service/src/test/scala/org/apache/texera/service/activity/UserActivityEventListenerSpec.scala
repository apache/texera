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

package org.apache.texera.service.activity

import jakarta.ws.rs.core.SecurityContext
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.glassfish.jersey.server.ContainerRequest
import org.glassfish.jersey.server.monitoring.{ApplicationEvent, RequestEvent}
import org.mockito.Mockito.{mock, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.security.Principal
import java.util.concurrent.ConcurrentLinkedQueue

class UserActivityEventListenerSpec extends AnyFlatSpec with Matchers {

  private def sessionUser(uid: Integer): SessionUser = {
    val u = new User(uid, "u", null, null, null, null, UserRoleEnum.REGULAR, null, null, null, null)
    new SessionUser(u)
  }

  private def buildEvent(eventType: RequestEvent.Type, sc: SecurityContext): RequestEvent = {
    val req = mock(classOf[ContainerRequest])
    when(req.getSecurityContext).thenReturn(sc)
    val event = mock(classOf[RequestEvent])
    when(event.getType).thenReturn(eventType)
    when(event.getContainerRequest).thenReturn(req)
    event
  }

  private def buildSecurityContext(principal: Principal): SecurityContext = {
    val sc = mock(classOf[SecurityContext])
    when(sc.getUserPrincipal).thenReturn(principal)
    sc
  }

  private def newRecorder(): ConcurrentLinkedQueue[Integer] = new ConcurrentLinkedQueue[Integer]()
  private def trackTo(q: ConcurrentLinkedQueue[Integer]): Integer => Unit =
    uid => { q.add(uid); () }

  "UserActivityEventListener.handle" should "invoke the tracker on RESOURCE_METHOD_FINISHED with a SessionUser principal" in {
    val recorded = newRecorder()
    UserActivityEventListener.handle(
      buildEvent(RequestEvent.Type.RESOURCE_METHOD_FINISHED, buildSecurityContext(sessionUser(42))),
      trackTo(recorded)
    )
    recorded.size shouldBe 1
    recorded.peek() shouldBe 42
  }

  it should "ignore RequestEvent types other than RESOURCE_METHOD_FINISHED" in {
    val recorded = newRecorder()
    val sc = buildSecurityContext(sessionUser(42))
    UserActivityEventListener.handle(buildEvent(RequestEvent.Type.START, sc), trackTo(recorded))
    UserActivityEventListener.handle(
      buildEvent(RequestEvent.Type.RESOURCE_METHOD_START, sc),
      trackTo(recorded)
    )
    UserActivityEventListener.handle(buildEvent(RequestEvent.Type.FINISHED, sc), trackTo(recorded))
    recorded.isEmpty shouldBe true
  }

  it should "ignore non-SessionUser principals" in {
    val recorded = newRecorder()
    val anon: Principal = new Principal { override def getName: String = "anon" }
    UserActivityEventListener.handle(
      buildEvent(RequestEvent.Type.RESOURCE_METHOD_FINISHED, buildSecurityContext(anon)),
      trackTo(recorded)
    )
    recorded.isEmpty shouldBe true
  }

  it should "ignore SessionUser with null uid" in {
    val recorded = newRecorder()
    UserActivityEventListener.handle(
      buildEvent(
        RequestEvent.Type.RESOURCE_METHOD_FINISHED,
        buildSecurityContext(sessionUser(null))
      ),
      trackTo(recorded)
    )
    recorded.isEmpty shouldBe true
  }

  it should "ignore null SecurityContext" in {
    val recorded = newRecorder()
    UserActivityEventListener.handle(
      buildEvent(RequestEvent.Type.RESOURCE_METHOD_FINISHED, null),
      trackTo(recorded)
    )
    recorded.isEmpty shouldBe true
  }

  // Listener-level smoke tests: verify the SAM lambda + dispatch glue,
  // not the per-event branching (which lives in `handle`).
  "UserActivityEventListener" should "dispatch RequestEvent to the handle function" in {
    val recorded = newRecorder()
    val listener = new UserActivityEventListener(trackTo(recorded))
    val rel = listener.onRequest(mock(classOf[RequestEvent]))
    rel.onEvent(
      buildEvent(RequestEvent.Type.RESOURCE_METHOD_FINISHED, buildSecurityContext(sessionUser(7)))
    )
    recorded.peek() shouldBe 7
  }

  it should "no-op on ApplicationEvent (lifecycle hook unused)" in {
    val recorded = newRecorder()
    val listener = new UserActivityEventListener(trackTo(recorded))
    listener.onEvent(mock(classOf[ApplicationEvent]))
    recorded.isEmpty shouldBe true
  }

  it should "construct with the default tracker without invoking it" in {
    new UserActivityEventListener() should not be null
  }
}
