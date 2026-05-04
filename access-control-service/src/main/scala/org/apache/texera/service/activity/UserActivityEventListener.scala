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

import jakarta.ws.rs.ext.Provider
import org.apache.texera.auth.{SessionUser, UserActivityTracker}
import org.glassfish.jersey.server.monitoring.{
  ApplicationEvent,
  ApplicationEventListener,
  RequestEvent,
  RequestEventListener
}

/** Records user activity (USER_LAST_ACTIVE_TIME) once per matched, completed
  * request. Intentionally NOT a ContainerRequestFilter:
  *
  *   - It cannot reject or transform a request — it only observes.
  *   - It runs at Jersey's monitoring layer, not the auth pipeline, so
  *     activity tracking is decoupled from authentication concerns.
  *   - It listens for RESOURCE_METHOD_FINISHED only, so requests that
  *     fail before reaching a handler (no auth, 404, 4xx in earlier
  *     filters) do not count as user activity.
  *
  * The DB write itself is throttled per-uid by [[UserActivityTracker]].
  *
  * Lives in access-control-service because USER_LAST_ACTIVE_TIME is a
  * user-management concern; the assumption is that any authenticated
  * client session contacts this service often enough (UI navigation,
  * permission checks, LiteLLM proxy) to capture activity with high
  * recall, so other services do not need to mirror this listener.
  */
@Provider
class UserActivityEventListener(track: Integer => Unit = UserActivityTracker.markActive)
    extends ApplicationEventListener {

  override def onEvent(event: ApplicationEvent): Unit = ()

  // SAM-converted lambda: avoids an inner anonymous class so coverage
  // tooling sees a flat method body. Logic lives in the companion's
  // `handle` so tests can drive it directly.
  override def onRequest(requestEvent: RequestEvent): RequestEventListener =
    (event: RequestEvent) => UserActivityEventListener.handle(event, track)
}

object UserActivityEventListener {

  /** Process a single Jersey request event. Public-package for tests so the
    * per-request branching is exercised without a Jersey runtime.
    */
  private[activity] def handle(event: RequestEvent, track: Integer => Unit): Unit = {
    // `eq` (reference equality) is correct here because Type is a Java enum
    // — its constants are singletons. It also compiles to a single
    // `if_acmpne`, sidestepping Scala's BoxesRunTime.equals branch fan-out.
    if (!(event.getType eq RequestEvent.Type.RESOURCE_METHOD_FINISHED)) return
    val sc = event.getContainerRequest.getSecurityContext
    if (sc == null) return
    sc.getUserPrincipal match {
      case u: SessionUser if u.getUid != null => track(u.getUid)
      case _                                  =>
    }
  }
}
