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

import io.dropwizard.auth.AuthDynamicFeature
import io.dropwizard.core.setup.Environment
import io.dropwizard.jersey.setup.JerseyEnvironment
import io.dropwizard.jetty.MutableServletContextHandler
import io.dropwizard.jetty.setup.ServletEnvironment
import org.apache.texera.auth.UnauthorizedExceptionMapper
import org.apache.texera.service.resource.{HealthCheckResource, NotebookMigrationResource}
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature
import org.mockito.ArgumentMatchers.isA
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class NotebookMigrationServiceRunSpec extends AnyFlatSpec with Matchers {

  "NotebookMigrationService.run" should "register the resources and the JWT auth stack on the Jersey environment" in {
    val jersey = mock(classOf[JerseyEnvironment])
    val servlets = mock(classOf[ServletEnvironment])
    val context = mock(classOf[MutableServletContextHandler])
    val env = mock(classOf[Environment])
    when(env.jersey).thenReturn(jersey)
    when(env.servlets).thenReturn(servlets)
    when(env.getApplicationContext).thenReturn(context)

    val service = new NotebookMigrationService
    service.run(mock(classOf[NotebookMigrationServiceConfiguration]), env)

    verify(jersey).setUrlPattern("/api/*")
    verify(jersey).register(classOf[HealthCheckResource])
    verify(jersey).register(classOf[NotebookMigrationResource])
    // Auth stack from registerAuthFeatures — without these, @RolesAllowed / @Auth are ignored.
    verify(jersey).register(isA(classOf[AuthDynamicFeature]))
    verify(jersey).register(classOf[UnauthorizedExceptionMapper])
    verify(jersey).register(classOf[RolesAllowedDynamicFeature])
  }
}
