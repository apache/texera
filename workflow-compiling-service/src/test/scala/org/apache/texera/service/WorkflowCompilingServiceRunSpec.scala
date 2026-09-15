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

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, Logger => LogbackLogger}
import ch.qos.logback.core.read.ListAppender
import io.dropwizard.configuration.ConfigurationSourceProvider
import io.dropwizard.core.setup.{Bootstrap, Environment}
import io.dropwizard.jersey.DropwizardResourceConfig
import io.dropwizard.jersey.setup.JerseyEnvironment
import io.dropwizard.jetty.MutableServletContextHandler
import io.dropwizard.jetty.setup.ServletEnvironment
import jakarta.servlet.{DispatcherType, Filter, FilterChain}
import jakarta.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.apache.texera.auth.{RoleAnnotationEnforcer, UnauthorizedExceptionMapper}
import org.apache.texera.service.WorkflowCompilingServiceRunSpec.SpecPayload
import org.apache.texera.service.resource.{
  HealthCheckResource,
  WorkflowCompilationResource,
  WorkflowToPythonResource
}
import org.eclipse.jetty.servlet.{FilterHolder, ServletHandler}
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, eq => eqTo, isA}
import org.mockito.Mockito.{mock, never, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import java.io.{ByteArrayInputStream, FileNotFoundException, InputStream}
import java.nio.charset.StandardCharsets.UTF_8
import scala.jdk.CollectionConverters._
import scala.util.Using

class WorkflowCompilingServiceRunSpec extends AnyFlatSpec with Matchers {

  // Mirrors AccessControlServiceRunSpec: run() is driven against a mocked Dropwizard
  // environment so the wiring it installs can be asserted without starting a server.
  // run() starts an operator-metadata warmup thread and swaps the global SqlServer
  // instance, so it is executed once for the whole suite and the resulting mocks are
  // shared; the assertions below only read recorded interactions.
  private lazy val ranService: (JerseyEnvironment, MutableServletContextHandler) = {
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
    val (jersey, _) = ranService
    verify(jersey).setUrlPattern("/api/*")
  }

  it should "register the health check, compilation and export endpoints" in {
    val (jersey, _) = ranService
    verify(jersey).register(classOf[HealthCheckResource])
    verify(jersey).register(classOf[WorkflowCompilationResource])
    verify(jersey).register(classOf[WorkflowToPythonResource])
  }

  it should "install the auth stack" in {
    val (jersey, _) = ranService
    // AuthFeatures.register: without RolesAllowedDynamicFeature Jersey ignores @RolesAllowed
    verify(jersey).register(classOf[RolesAllowedDynamicFeature])
    verify(jersey).register(classOf[UnauthorizedExceptionMapper])
  }

  it should "add the request-logging filter to the application context" in {
    val (_, context) = ranService
    // Every path and every dispatcher type: a narrower EnumSet registers the filter but leaves
    // forwarded, included, async and error dispatches unlogged.
    verify(context).addFilter(
      isA(classOf[FilterHolder]),
      eqTo("/*"),
      eqTo(java.util.EnumSet.allOf(classOf[DispatcherType]))
    )
  }

  /**
    * The filter run() installed, read back out of the holder it registered rather than looked up
    * by class name. That keeps these assertions about behaviour: replacing this service's inlined
    * copy with the `RequestLoggingFilter.register` the other four services call registers the same
    * filter contract and leaves them green.
    *
    * A FilterHolder only publishes its instance once it has been started and initialised (Jetty
    * does that when the context starts), so the holder is walked through that lifecycle here
    * instead of binding a server. `wrap` reads the holder's ServletHandler, hence the bare one.
    */
  private lazy val requestLogFilter: Filter = {
    val (_, context) = ranService
    val holder = ArgumentCaptor.forClass(classOf[FilterHolder])
    verify(context).addFilter(holder.capture(), any[String](), any())
    val captured = holder.getValue
    captured.setServletHandler(new ServletHandler)
    captured.start()
    captured.initialize()
    captured.getFilter
  }

  /** Runs `body` with the request-log logger pinned at `level`, returning what it logged. */
  private def eventsLoggedAt(level: Level)(body: => Unit): Seq[ILoggingEvent] = {
    val logger =
      LoggerFactory.getLogger("org.eclipse.jetty.server.RequestLog").asInstanceOf[LogbackLogger]
    val appender = new ListAppender[ILoggingEvent]
    appender.setContext(logger.getLoggerContext)
    appender.start()
    val previousLevel = logger.getLevel
    logger.setLevel(level)
    logger.addAppender(appender)
    try body
    finally {
      // Restored: the suites of this module share one JVM and this logger is global to it.
      logger.detachAppender(appender)
      logger.setLevel(previousLevel)
      appender.stop()
    }
    appender.list.asScala.toSeq
  }

  "WorkflowCompilingService's request-logging filter" should "forward the request and log one access line at info" in {
    val request = mock(classOf[HttpServletRequest])
    when(request.getRemoteAddr).thenReturn("10.0.0.7")
    when(request.getMethod).thenReturn("POST")
    when(request.getRequestURI).thenReturn("/api/compile")
    when(request.getProtocol).thenReturn("HTTP/1.1")
    val response = mock(classOf[HttpServletResponse])
    when(response.getStatus).thenReturn(200)
    val chain = mock(classOf[FilterChain])

    val events = eventsLoggedAt(Level.INFO) {
      requestLogFilter.doFilter(request, response, chain)
    }

    // Forwarding is the filter's first job: without it every request ends here with an empty
    // response instead of reaching the compilation endpoint.
    verify(chain).doFilter(request, response)
    // The level is asserted with the message: access lines have to stay at info so that raising
    // TEXERA_SERVICE_LOG_LEVEL silences them.
    events.map(event => (event.getLevel, event.getFormattedMessage)) shouldBe Seq(
      (Level.INFO, """10.0.0.7 - "POST /api/compile HTTP/1.1" 200""")
    )
  }

  it should "forward the request without reading it when info logging is off" in {
    val request = mock(classOf[HttpServletRequest])
    val response = mock(classOf[HttpServletResponse])
    val chain = mock(classOf[FilterChain])

    val events = eventsLoggedAt(Level.WARN) {
      requestLogFilter.doFilter(request, response, chain)
    }

    verify(chain).doFilter(request, response)
    events shouldBe empty
    // Logback would drop the info event on its own, so an absent event proves nothing about the
    // isInfoEnabled guard. What the guard buys is not paying to build the line at all, which is
    // only observable as the request never being interrogated.
    verify(request, never()).getRemoteAddr
  }

  // Every endpoint this service registers declares @RolesAllowed/@PermitAll/@DenyAll.
  "WorkflowCompilingService's registered resources" should "all declare access control" in {
    RoleAnnotationEnforcer.findUnannotatedEndpoints(
      Seq(
        classOf[WorkflowCompilationResource],
        classOf[HealthCheckResource],
        classOf[WorkflowToPythonResource]
      )
    ) shouldBe empty
  }

  // --- initialize() ---
  // Dropwizard's Bootstrap only allocates in-memory scaffolding (an object mapper, a metric
  // registry, a file-reading configuration source provider), so initialize() can be driven
  // against a real one without binding a port or opening a connection.

  private val seededConfigPath = "workflow-compiling-service-spec.yml"

  // The `:-` default form is the only substitution shape this service actually ships: both
  // `${...}` in workflow-compiling-service-config.yaml (lines 28 and 30) carry a default.
  private val seededConfig =
    "defined: ${PATH}\ndefaulted: ${TEXERA_WORKFLOW_COMPILING_SERVICE_SPEC_UNSET:-fallback}\n"

  private val unsetConfigPath = "workflow-compiling-service-spec-unset.yml"
  private val unsetConfig = "unset: ${TEXERA_WORKFLOW_COMPILING_SERVICE_SPEC_UNSET}\n"

  /**
    * Runs initialize() over a bootstrap that already carries a recognisable in-memory
    * configuration source. Seeding it is what makes the wrapping observable: initialize() is
    * required to wrap the provider that is already installed, and a version that instead built a
    * fresh file-reading provider would fail to find these paths at all.
    */
  private def initializedBootstrap(): Bootstrap[WorkflowCompilingServiceConfiguration] = {
    val application = new WorkflowCompilingService
    val bootstrap = new Bootstrap[WorkflowCompilingServiceConfiguration](application)
    bootstrap.setConfigurationSourceProvider(new ConfigurationSourceProvider {
      override def open(path: String): InputStream =
        path match {
          case `seededConfigPath` => new ByteArrayInputStream(seededConfig.getBytes(UTF_8))
          case `unsetConfigPath`  => new ByteArrayInputStream(unsetConfig.getBytes(UTF_8))
          case other              => throw new FileNotFoundException(other)
        }
    })
    application.initialize(bootstrap)
    bootstrap
  }

  private def resolve(path: String): String =
    Using.resource(initializedBootstrap().getConfigurationSourceProvider.open(path)) { stream =>
      new String(stream.readAllBytes(), UTF_8)
    }

  "WorkflowCompilingService.initialize" should "substitute environment variables into the configuration source it was handed" in {
    // Whole-string rather than a pair of `include`s, so a substitutor that also mangled the rest
    // of the document could not pass. The second line is the shape the service's own YAML uses:
    // it only resolves while the substitutor keeps commons-text's `:-` value delimiter, and a
    // deployment that lost it would hand logback the literal "${TEXERA_SERVICE_LOG_LEVEL:-INFO}"
    // as a level.
    resolve(seededConfigPath) shouldBe s"defined: ${System.getenv("PATH")}\ndefaulted: fallback\n"
  }

  it should "leave a variable with neither a value nor a default as a literal" in {
    // The substitutor is built non-strict. A strict one raises UndefinedEnvironmentVariableException
    // out of open(), i.e. refuses to boot a deployment whose config names a variable it does not
    // set; asserting the resolved text here makes that a stated claim rather than an incidental
    // error thrown from the middle of the test above.
    resolve(unsetConfigPath) shouldBe "unset: ${TEXERA_WORKFLOW_COMPILING_SERVICE_SPEC_UNSET}\n"
  }

  it should "register the Scala module on Dropwizard's object mapper" in {
    val mapper = initializedBootstrap().getObjectMapper
    // The whole module, not only the Option support that `Some("x")` alone would prove: this is
    // the mapper Dropwizard hands to Jersey, so every payload the API returns goes through it —
    // including WorkflowCompilationSuccess's Map of Option-valued output schemas.
    mapper.getRegisteredModuleIds should contain(
      "com.fasterxml.jackson.module.scala.DefaultScalaModule$"
    )

    val payload = SpecPayload("op-1", Map("port0" -> Some(Seq(1, 2)), "port1" -> None))
    // Option unwrapped, Map and Seq emitted as JSON, and the camelCase property names left alone
    // (Dropwizard's own naming strategy is annotation-sensitive; a global one would rename them).
    val json = mapper.writeValueAsString(payload)
    json shouldBe """{"operatorId":"op-1","outputSchemas":{"port0":[1,2],"port1":null}}"""
    // Reading, too: this is also the mapper Dropwizard parses the YAML configuration with.
    mapper.readValue(json, classOf[SpecPayload]) shouldBe payload
  }
}

object WorkflowCompilingServiceRunSpec {

  /** Stands in for the payloads this service returns: camelCase names, a Map, a Seq and an Option. */
  final case class SpecPayload(operatorId: String, outputSchemas: Map[String, Option[Seq[Int]]])
}
