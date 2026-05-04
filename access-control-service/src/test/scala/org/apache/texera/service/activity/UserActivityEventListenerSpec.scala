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
import org.glassfish.jersey.server.monitoring.RequestEvent
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

  private def setup() = {
    val recorded = new ConcurrentLinkedQueue[Integer]()
    val listener = new UserActivityEventListener(uid => { recorded.add(uid); () })
    val rel = listener.onRequest(mock(classOf[RequestEvent]))
    (rel, recorded)
  }

  "UserActivityEventListener" should "invoke the tracker on RESOURCE_METHOD_FINISHED with a SessionUser principal" in {
    val (rel, recorded) = setup()
    rel.onEvent(
      buildEvent(
        RequestEvent.Type.RESOURCE_METHOD_FINISHED,
        buildSecurityContext(sessionUser(42))
      )
    )
    recorded.size shouldBe 1
    recorded.peek() shouldBe 42
  }

  it should "ignore RequestEvent types other than RESOURCE_METHOD_FINISHED" in {
    val (rel, recorded) = setup()
    val sc = buildSecurityContext(sessionUser(42))
    rel.onEvent(buildEvent(RequestEvent.Type.START, sc))
    rel.onEvent(buildEvent(RequestEvent.Type.RESOURCE_METHOD_START, sc))
    rel.onEvent(buildEvent(RequestEvent.Type.FINISHED, sc))
    recorded.isEmpty shouldBe true
  }

  it should "ignore non-SessionUser principals" in {
    val (rel, recorded) = setup()
    val anon: Principal = new Principal {
      override def getName: String = "anon"
    }
    rel.onEvent(
      buildEvent(RequestEvent.Type.RESOURCE_METHOD_FINISHED, buildSecurityContext(anon))
    )
    recorded.isEmpty shouldBe true
  }

  it should "ignore SessionUser with null uid" in {
    val (rel, recorded) = setup()
    rel.onEvent(
      buildEvent(
        RequestEvent.Type.RESOURCE_METHOD_FINISHED,
        buildSecurityContext(sessionUser(null))
      )
    )
    recorded.isEmpty shouldBe true
  }

  it should "ignore null SecurityContext" in {
    val (rel, recorded) = setup()
    rel.onEvent(buildEvent(RequestEvent.Type.RESOURCE_METHOD_FINISHED, null))
    recorded.isEmpty shouldBe true
  }
}
