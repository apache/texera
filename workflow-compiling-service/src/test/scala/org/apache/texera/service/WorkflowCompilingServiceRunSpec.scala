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

package org.apache.texera.service

import io.dropwizard.auth.{AuthDynamicFeature, AuthValueFactoryProvider}
import io.dropwizard.core.setup.Environment
import io.dropwizard.jersey.setup.JerseyEnvironment
import io.dropwizard.jetty.MutableServletContextHandler
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WorkflowCompilingServiceRunSpec extends AnyFlatSpec with Matchers {

  // Verifies that the @RolesAllowed annotations on resource methods are actually
  // enforced by Jersey, which requires RolesAllowedDynamicFeature, AuthDynamicFeature,
  // and AuthValueFactoryProvider.Binder to be registered on the Jersey environment.
  "WorkflowCompilingService.run" should "register auth + RolesAllowedDynamicFeature on the Jersey environment" in {
    val jersey = mock(classOf[JerseyEnvironment])
    val context = mock(classOf[MutableServletContextHandler])
    val env = mock(classOf[Environment])
    when(env.jersey).thenReturn(jersey)
    when(env.getApplicationContext).thenReturn(context)

    val service = new WorkflowCompilingService
    // run() may throw if SqlServer/StorageConfig or other side effects fail
    // under mocks. Auth + RolesAllowedDynamicFeature registrations happen
    // before any such failure, so the verifications below remain valid.
    try {
      service.run(mock(classOf[WorkflowCompilingServiceConfiguration]), env)
    } catch {
      case _: Throwable => // expected under mocks
    }

    verify(jersey).register(classOf[RolesAllowedDynamicFeature])
    verify(jersey).register(org.mockito.ArgumentMatchers.any(classOf[AuthDynamicFeature]))
    verify(jersey).register(
      org.mockito.ArgumentMatchers.any(classOf[AuthValueFactoryProvider.Binder[_]])
    )
  }
}
