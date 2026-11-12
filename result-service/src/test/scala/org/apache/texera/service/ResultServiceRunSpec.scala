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
import io.dropwizard.configuration.SubstitutingSourceProvider
import io.dropwizard.core.setup.{Bootstrap, Environment}
import io.dropwizard.jersey.DropwizardResourceConfig
import io.dropwizard.jersey.setup.JerseyEnvironment
import io.dropwizard.jetty.MutableServletContextHandler
import io.dropwizard.jetty.setup.ServletEnvironment
import org.apache.texera.auth.{RoleAnnotationEnforcer, UnauthorizedExceptionMapper}
import org.apache.texera.dao.SqlServer
import org.apache.texera.service.resource.{HealthCheckResource, ResultResource}
import org.eclipse.jetty.server.session.SessionHandler
import org.eclipse.jetty.servlet.FilterHolder
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature
import org.mockito.ArgumentMatchers.{any, eq => eqTo, isA}
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Try

class ResultServiceRunSpec extends AnyFlatSpec with Matchers {

  private case class Wiring(
      jersey: JerseyEnvironment,
      servlets: ServletEnvironment,
      context: MutableServletContextHandler
  )

  /** One `run` against mocked Dropwizard wiring. */
  private def run(): Wiring = {
    val wiring = Wiring(
      mock(classOf[JerseyEnvironment]),
      mock(classOf[ServletEnvironment]),
      mock(classOf[MutableServletContextHandler])
    )
    val env = mock(classOf[Environment])
    when(env.jersey).thenReturn(wiring.jersey)
    when(env.servlets).thenReturn(wiring.servlets)
    when(env.getApplicationContext).thenReturn(wiring.context)
    when(wiring.jersey.getResourceConfig).thenReturn(DropwizardResourceConfig.forTesting())

    new ResultService().run(mock(classOf[ResultServiceConfiguration]), env)
    wiring
  }

  "ResultService.run" should "install the API prefix, session handling and its resources" in {
    val Wiring(jersey, servlets, _) = run()

    // Everything the service serves lives under /api; losing this silently moves every
    // endpoint to the root.
    verify(jersey).setUrlPattern("/api/*")
    verify(jersey).register(classOf[SessionHandler])
    verify(servlets).setSessionHandler(isA(classOf[SessionHandler]))
    verify(jersey).register(classOf[HealthCheckResource])
    verify(jersey).register(classOf[ResultResource])
  }

  it should "open the database connection" in {
    val before = Try(SqlServer.getInstance()).toOption

    run()

    // initConnection replaces the shared instance, so a run that skipped it leaves the old one.
    Some(SqlServer.getInstance()) should not be before
  }

  it should "install the auth stack and the request logging filter" in {
    val Wiring(jersey, _, context) = run()

    // AuthFeatures.register: without these, @Auth parameters do not resolve and
    // @RolesAllowed on the result endpoints is ignored.
    verify(jersey).register(isA(classOf[AuthDynamicFeature]))
    verify(jersey).register(classOf[UnauthorizedExceptionMapper])
    verify(jersey).register(classOf[RolesAllowedDynamicFeature])

    verify(context).addFilter(isA(classOf[FilterHolder]), eqTo("/*"), any())
  }

  "ResultService's registered resources" should "all declare access control" in {
    RoleAnnotationEnforcer.findUnannotatedEndpoints(
      Seq(classOf[ResultResource], classOf[HealthCheckResource])
    ) shouldBe empty
  }

  "ResultService.initialize" should "substitute variables and register the Scala module" in {
    val application = new ResultService
    val bootstrap = new Bootstrap[ResultServiceConfiguration](application)

    application.initialize(bootstrap)

    bootstrap.getConfigurationSourceProvider shouldBe a[SubstitutingSourceProvider]
    bootstrap.getObjectMapper.getRegisteredModuleIds should contain(
      "com.fasterxml.jackson.module.scala.DefaultScalaModule$"
    )
  }
}
