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

import io.dropwizard.core.setup.Environment
import io.dropwizard.jersey.DropwizardResourceConfig
import io.dropwizard.jersey.setup.JerseyEnvironment
import io.dropwizard.jetty.MutableServletContextHandler
import io.dropwizard.jetty.setup.ServletEnvironment
import org.apache.texera.auth.{RoleAnnotationEnforcer, UnauthorizedExceptionMapper}
import org.apache.texera.service.resource.{HealthCheckResource, WorkflowCompilationResource}
import org.eclipse.jetty.servlet.FilterHolder
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature
import org.mockito.ArgumentMatchers.{any, eq => eqTo, isA}
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WorkflowCompilingServiceRunSpec extends AnyFlatSpec with Matchers {

  // Mirrors AccessControlServiceRunSpec: run() is driven against a mocked Dropwizard
  // environment so the wiring it installs can be asserted without starting a server.
  private def runService(): (JerseyEnvironment, MutableServletContextHandler) = {
    val jersey = mock(classOf[JerseyEnvironment])
    val servlets = mock(classOf[ServletEnvironment])
    val context = mock(classOf[MutableServletContextHandler])
    val env = mock(classOf[Environment])
    when(env.jersey).thenReturn(jersey)
    when(env.servlets).thenReturn(servlets)
    when(env.getApplicationContext).thenReturn(context)
    when(jersey.getResourceConfig).thenReturn(DropwizardResourceConfig.forTesting())

    val service = new WorkflowCompilingService
    service.run(mock(classOf[WorkflowCompilingServiceConfiguration]), env)

    (jersey, context)
  }

  "WorkflowCompilingService.run" should "serve the API under /api/*" in {
    val (jersey, _) = runService()
    verify(jersey).setUrlPattern("/api/*")
  }

  it should "register the health check and compilation endpoints" in {
    val (jersey, _) = runService()
    verify(jersey).register(classOf[HealthCheckResource])
    verify(jersey).register(classOf[WorkflowCompilationResource])
  }

  it should "install the auth stack" in {
    val (jersey, _) = runService()
    // AuthFeatures.register: without RolesAllowedDynamicFeature Jersey ignores @RolesAllowed
    verify(jersey).register(classOf[RolesAllowedDynamicFeature])
    verify(jersey).register(classOf[UnauthorizedExceptionMapper])
  }

  it should "add the request-logging filter to the application context" in {
    val (_, context) = runService()
    verify(context).addFilter(isA(classOf[FilterHolder]), eqTo("/*"), any())
  }

  // Every endpoint this service registers declares @RolesAllowed/@PermitAll/@DenyAll.
  "WorkflowCompilingService's registered resources" should "all declare access control" in {
    RoleAnnotationEnforcer.findUnannotatedEndpoints(
      Seq(classOf[WorkflowCompilationResource], classOf[HealthCheckResource])
    ) shouldBe empty
  }
}
