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
import io.dropwizard.jersey.DropwizardResourceConfig
import io.dropwizard.jersey.setup.JerseyEnvironment
import io.dropwizard.jetty.MutableServletContextHandler
import io.dropwizard.jetty.setup.ServletEnvironment
import org.apache.texera.auth.{RoleAnnotationEnforcer, UnauthorizedExceptionMapper}
import org.apache.texera.common.config.DefaultsConfig
import org.apache.texera.dao.{MockTexeraDB, SqlServer}
import org.apache.texera.dao.jooq.generated.Tables.SITE_SETTINGS
import org.apache.texera.service.resource.{ConfigResource, HealthCheckResource}
import org.eclipse.jetty.server.session.SessionHandler
import org.eclipse.jetty.servlet.FilterHolder
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature
import org.jooq.{ConnectionProvider, SQLDialect}
import org.jooq.impl.{DSL, DefaultConfiguration}
import org.mockito.ArgumentMatchers.{any, eq => eqTo, isA}
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Connection, SQLException}

// `run` ends by preloading default.conf into site_settings, so it needs a live
// SqlServer: MockTexeraDB gives this suite its own embedded database and points
// SqlServer at it, which lets the whole method — including the request-logging
// filter installed after the preload — run against mocked Dropwizard wiring.
class ConfigServiceRunSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  override protected def beforeAll(): Unit = initializeDBAndReplaceDSLContext()

  override protected def afterAll(): Unit = shutdownDB()

  "ConfigService.run" should "install the API prefix, session handling and its resources" in {
    val jersey = mock(classOf[JerseyEnvironment])
    val servlets = mock(classOf[ServletEnvironment])
    val context = mock(classOf[MutableServletContextHandler])
    val env = mock(classOf[Environment])
    when(env.jersey).thenReturn(jersey)
    when(env.servlets).thenReturn(servlets)
    when(env.getApplicationContext).thenReturn(context)
    when(jersey.getResourceConfig).thenReturn(DropwizardResourceConfig.forTesting())

    new ConfigService().run(mock(classOf[ConfigServiceConfiguration]), env)

    // Everything the service serves lives under /api; losing this silently moves every
    // endpoint to the root.
    verify(jersey).setUrlPattern("/api/*")
    verify(jersey).register(classOf[SessionHandler])
    verify(servlets).setSessionHandler(isA(classOf[SessionHandler]))
    verify(jersey).register(classOf[HealthCheckResource])
    verify(jersey).register(isA(classOf[ConfigResource]))
  }

  it should "install the auth stack and the request logging filter" in {
    val jersey = mock(classOf[JerseyEnvironment])
    val servlets = mock(classOf[ServletEnvironment])
    val context = mock(classOf[MutableServletContextHandler])
    val env = mock(classOf[Environment])
    when(env.jersey).thenReturn(jersey)
    when(env.servlets).thenReturn(servlets)
    when(env.getApplicationContext).thenReturn(context)
    when(jersey.getResourceConfig).thenReturn(DropwizardResourceConfig.forTesting())

    new ConfigService().run(mock(classOf[ConfigServiceConfiguration]), env)

    // AuthFeatures.register: without these, @Auth parameters do not resolve and
    // @RolesAllowed on the settings endpoints is ignored.
    verify(jersey).register(isA(classOf[AuthDynamicFeature]))
    verify(jersey).register(classOf[UnauthorizedExceptionMapper])
    verify(jersey).register(classOf[RolesAllowedDynamicFeature])

    // RequestLoggingFilter.register, which runs only after the preload below succeeds.
    verify(context).addFilter(isA(classOf[FilterHolder]), eqTo("/*"), any())
  }

  it should "preload the default settings into site_settings" in {
    val jersey = mock(classOf[JerseyEnvironment])
    val env = mock(classOf[Environment])
    when(env.jersey).thenReturn(jersey)
    when(env.servlets).thenReturn(mock(classOf[ServletEnvironment]))
    when(env.getApplicationContext).thenReturn(mock(classOf[MutableServletContextHandler]))
    when(jersey.getResourceConfig).thenReturn(DropwizardResourceConfig.forTesting())

    // The fixture does not truncate between tests and the runs above already seeded the
    // table, so start from empty: otherwise this passes even for a run() that skipped the
    // preload entirely.
    getDSLContext.deleteFrom(SITE_SETTINGS).execute()

    new ConfigService().run(mock(classOf[ConfigServiceConfiguration]), env)

    DefaultsConfig.allDefaults should not be empty
    DefaultsConfig.allDefaults.foreach {
      case (key, value) =>
        val stored = getDSLContext
          .select(SITE_SETTINGS.VALUE)
          .from(SITE_SETTINGS)
          .where(SITE_SETTINGS.KEY.eq(key))
          .fetchOne()
        withClue(s"site_settings row for '$key': ") {
          stored should not be null
          stored.value1() shouldBe value
        }
    }
  }

  it should "surface a failed preload instead of starting without the defaults" in {
    val jersey = mock(classOf[JerseyEnvironment])
    val env = mock(classOf[Environment])
    when(env.jersey).thenReturn(jersey)
    when(env.servlets).thenReturn(mock(classOf[ServletEnvironment]))
    when(env.getApplicationContext).thenReturn(mock(classOf[MutableServletContextHandler]))
    when(jersey.getResourceConfig).thenReturn(DropwizardResourceConfig.forTesting())

    // Point SqlServer at a context that cannot acquire a connection. MockTexeraDB's fixture
    // reinstalls the suite's healthy context before the next test, so this stays local.
    val unusable = new DefaultConfiguration()
    unusable.set(SQLDialect.POSTGRES)
    unusable.set(new ConnectionProvider {
      override def acquire(): Connection = throw new SQLException("database unavailable")
      override def release(connection: Connection): Unit = ()
    })
    SqlServer.getInstance().replaceDSLContext(DSL.using(unusable))

    // Rethrown rather than swallowed: a service that came up with no settings would look
    // healthy while serving none of them.
    a[RuntimeException] should be thrownBy new ConfigService()
      .run(mock(classOf[ConfigServiceConfiguration]), env)
  }

  // Every endpoint this service registers declares @RolesAllowed/@PermitAll/@DenyAll.
  "ConfigService's registered resources" should "all declare access control" in {
    RoleAnnotationEnforcer.findUnannotatedEndpoints(
      Seq(classOf[ConfigResource], classOf[HealthCheckResource])
    ) shouldBe empty
  }
}
