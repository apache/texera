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

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, Logger => LogbackLogger}
import ch.qos.logback.core.AppenderBase
import com.codahale.metrics.MetricRegistry
import com.github.dirkraft.dropwizard.fileassets.FileAssetServlet
import io.dropwizard.auth.{AuthDynamicFeature, AuthValueFactoryProvider}
import io.dropwizard.jackson.Jackson
import io.dropwizard.jersey.validation.Validators
import io.dropwizard.setup.{Bootstrap, Environment}
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.dao.SqlServer
import org.apache.texera.web.resource._
import org.apache.texera.web.resource.auth.{AuthResource, GoogleAuthResource}
import org.apache.texera.web.resource.dashboard.DashboardResource
import org.apache.texera.web.resource.dashboard.admin.execution.AdminExecutionResource
import org.apache.texera.web.resource.dashboard.admin.user.AdminUserResource
import org.apache.texera.web.resource.dashboard.hub.HubResource
import org.apache.texera.web.resource.dashboard.user.UserResource
import org.apache.texera.web.resource.dashboard.user.quota.UserQuotaResource
import org.apache.texera.web.resource.dashboard.user.warehouse.WarehouseResource
import org.apache.texera.web.resource.dashboard.user.workflow.{
  WorkflowAccessResource,
  WorkflowExecutionsResource,
  WorkflowResource,
  WorkflowVersionResource
}
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.session.SessionHandler
import org.eclipse.jetty.servlet.{ErrorPageErrorHandler, FilterHolder, FilterMapping}
import org.eclipse.jetty.util.component.{ContainerLifeCycle, LifeCycle}
import org.eclipse.jetty.websocket.jsr356.server.{ServerContainer => JsrServerContainer}
import org.eclipse.jetty.websocket.server.{NativeWebSocketConfiguration, WebSocketUpgradeFilter}
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.sql.DriverManager
import java.util.EnumSet
import java.util.concurrent.atomic.AtomicBoolean
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import javax.servlet.{DispatcherType, Filter, FilterChain, ServletRequest, ServletResponse}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}

/**
  * Drives the web application's two entry-point hooks against a real Dropwizard
  * [[Environment]] rather than a mock. The sibling service run-specs use Mockito, but amber has no
  * Mockito on its test classpath, and more importantly `run()` calls
  * `WebSocketUpgradeFilter.configureContext`, which walks into the servlet context and NPEs against
  * a mock. A real Environment allocates only in-memory Jetty/Jersey scaffolding and binds no port,
  * so every registration below is asserted as observable state instead of as a verified call.
  *
  * `main` is deliberately not covered: it is Dropwizard's `Application.run(String*)`, which parses
  * web-config.yml and binds Jetty to a port.
  */
class TexeraWebApplicationSpec extends AnyFlatSpec with Matchers {

  private def newEnvironment(): Environment =
    new Environment(
      "texera-web-application-spec",
      Jackson.newObjectMapper(),
      Validators.newValidator(),
      new MetricRegistry(),
      getClass.getClassLoader
    )

  private def initializedBootstrap(): Bootstrap[TexeraWebConfiguration] = {
    val application = new TexeraWebApplication
    val bootstrap = new Bootstrap[TexeraWebConfiguration](application)
    application.initialize(bootstrap)
    bootstrap
  }

  // Booting the application opens a real connection pool and bootstraps the admin account, so the
  // run()-driven assertions are only exercisable where Postgres is provisioned at the configured
  // JDBC URL (as in CI).
  private def databaseReachable: Boolean =
    try {
      DriverManager
        .getConnection(
          StorageConfig.jdbcUrl,
          StorageConfig.jdbcUsername,
          StorageConfig.jdbcPassword
        )
        .close()
      true
    } catch {
      case _: Throwable => false
    }

  /** The JDBC URL the pool `run()` installed was actually dialled, read before it is torn down. */
  private var pooledJdbcUrl: Option[String] = None

  /**
    * The environment left behind by a single full `run()`, shared by every assertion below so the
    * admin-user bootstrap and the operator-metadata warmup happen once.
    *
    * `run()` calls `SqlServer.initConnection`, which swaps a JVM-global singleton. amber runs its
    * suites in parallel in one unforked JVM, so the pool is closed and the previously installed
    * DSLContext (typically a MockTexeraDB scoped one) is put back the instant `run()` returns,
    * keeping the window in which another suite could be routed at the real texera_db as short as
    * possible. Closing our pool is safe for that restored context: it carries its own DataSource.
    */
  private lazy val ranEnvironment: Environment = {
    val environment = newEnvironment()
    val priorContext = Try(SqlServer.getInstance().createDSLContext()).toOption
    try {
      new TexeraWebApplication().run(new TexeraWebConfiguration, environment)
      pooledJdbcUrl = Try(
        SqlServer
          .getInstance()
          .createDSLContext()
          .connectionResult(connection => connection.getMetaData.getURL)
      ).toOption
    } finally {
      Try(SqlServer.getInstance().close())
      priorContext.foreach(context => Try(SqlServer.getInstance().replaceDSLContext(context)))
    }
    environment
  }

  private def resourceClasses: Set[Class[_]] =
    ranEnvironment.jersey.getResourceConfig.getClasses.asScala.toSet

  private def resourceInstances: Set[AnyRef] =
    ranEnvironment.jersey.getResourceConfig.getInstances.asScala.toSet

  // --- initialize() ---

  "TexeraWebApplication.initialize" should "substitute environment variables into the configuration source" in {
    val bootstrap = initializedBootstrap()
    val yaml = Files.createTempFile("texera-web-application-spec", ".yml")
    try {
      Files.write(
        yaml,
        "defined: ${PATH}\nunset: ${TEXERA_WEB_APPLICATION_SPEC_UNSET}\n".getBytes(UTF_8)
      )
      val resolved = Using.resource(bootstrap.getConfigurationSourceProvider.open(yaml.toString)) {
        stream => new String(stream.readAllBytes(), UTF_8)
      }
      resolved should include(s"defined: ${System.getenv("PATH")}")
      // The substitutor is built non-strict, so an unset variable is left as a literal rather than
      // aborting the boot of a deployment that does not set every optional override.
      resolved should include("unset: ${TEXERA_WEB_APPLICATION_SPEC_UNSET}")
    } finally Files.deleteIfExists(yaml)
  }

  it should "register the Scala module on Dropwizard's object mapper" in {
    // Without DefaultScalaModule, Jackson treats Option as a bean and emits its accessors
    // instead of unwrapping it, which corrupts every optional field the API returns.
    initializedBootstrap().getObjectMapper.writeValueAsString(Some("x")) shouldBe "\"x\""
  }

  it should "serve the built frontend from the filesystem at the context root" in {
    val environment = newEnvironment()
    initializedBootstrap().run(new TexeraWebConfiguration, environment)

    val handler = environment.getApplicationContext.getServletHandler
    val assets = handler.getServlets.toSeq
      .find(holder => Option(holder.getServletInstance).exists(_.isInstanceOf[FileAssetServlet]))
      .getOrElse(fail("no FileAssetServlet was registered"))
    // Mapped at the root so that Angular's deep links reach the asset servlet (and, missing there,
    // the 404-to-index rule installed by run()).
    handler.getServletMappings.toSeq
      .filter(_.getServletName == assets.getName)
      .flatMap(_.getPathSpecs)
      .toSet shouldBe Set("/*")

    // All three FileAssetsBundle arguments, not just the uriPath. With only the mapping asserted,
    // the source directory and the index file are free -- point the bundle at a directory that
    // does not exist and the servlet is still registered at "/*", it just serves nothing, while
    // this test's name still claims it serves the frontend from the filesystem.
    val servlet = assets.getServletInstance.asInstanceOf[FileAssetServlet]
    servlet.getIndexFile shouldBe "index.html"
    val resourcePathField = classOf[FileAssetServlet].getDeclaredField("resourcePath")
    resourcePathField.setAccessible(true)
    resourcePathField.get(servlet).asInstanceOf[String] should include("frontend/dist")
  }

  it should "deploy the collaboration websocket endpoint at /wsapi/collab" in {
    val environment = newEnvironment()
    initializedBootstrap().run(new TexeraWebConfiguration, environment)

    // The bundle defers all of its work to a lifecycle listener, and Jetty's JSR-356 container
    // defers endpoint deployment until it starts, so both have to be driven to observe the path.
    val listeners = mutable.ListBuffer.empty[LifeCycle.Listener]
    val container = new ContainerLifeCycle {
      override def addLifeCycleListener(listener: LifeCycle.Listener): Unit = {
        listeners += listener
        super.addLifeCycleListener(listener)
      }
    }
    environment.getApplicationContext.setServer(new Server)
    environment.lifecycle().attach(container)
    val websocketListener = listeners
      .find(_.getClass.getName.startsWith("io.dropwizard.websockets.WebsocketBundle"))
      .getOrElse(fail("the websocket bundle registered no lifecycle listener"))

    websocketListener.lifeCycleStarting(container)
    val serverContainer = environment.getApplicationContext
      .getAttribute(classOf[javax.websocket.server.ServerContainer].getName)
      .asInstanceOf[JsrServerContainer]
    serverContainer.start()
    try {
      val mappings = serverContainer.getBean(classOf[NativeWebSocketConfiguration])
      Option(mappings.getMatch("/wsapi/collab")).map(_.getPathSpec.getDeclaration) shouldBe Some(
        "/wsapi/collab"
      )
      // The other two @ServerEndpoint classes belong to other entry points; wiring one of them
      // here would leave collaborative editing unreachable.
      mappings.getMatch("/wsapi/pve") shouldBe null
      mappings.getMatch("/wsapi/workflow-websocket") shouldBe null
    } finally serverContainer.stop()
  }

  // --- run() ---

  "TexeraWebApplication.run" should "serve the backend under /api" in {
    assume(
      databaseReachable,
      "run() requires a reachable Postgres at the configured JDBC URL (provided in CI)"
    )
    ranEnvironment.jersey.getUrlPattern shouldBe "/api/*"
  }

  it should "open the connection pool against the configured database" in {
    assume(
      databaseReachable,
      "run() requires a reachable Postgres at the configured JDBC URL (provided in CI)"
    )
    // Not the test-cases database: pointing the running server at it would have every dashboard
    // read and write a schema that the CI e2e specs truncate underneath it.
    ranEnvironment
    pooledJdbcUrl shouldBe Some(StorageConfig.jdbcUrl)
  }

  it should "register every dashboard and system resource on Jersey" in {
    assume(
      databaseReachable,
      "run() requires a reachable Postgres at the configured JDBC URL (provided in CI)"
    )
    resourceClasses should contain allElementsOf Set[Class[_]](
      classOf[SystemMetadataResource],
      classOf[HealthCheckResource],
      classOf[AuthResource],
      classOf[GoogleAuthResource],
      classOf[UserConfigResource],
      classOf[FeedbackResource],
      classOf[AdminUserResource],
      classOf[WorkflowAccessResource],
      classOf[WorkflowResource],
      classOf[HubResource],
      classOf[UserResource],
      classOf[WorkflowVersionResource],
      classOf[WorkflowExecutionsResource],
      classOf[DashboardResource],
      classOf[GmailResource],
      classOf[AdminExecutionResource],
      classOf[UserQuotaResource],
      classOf[AIAssistantResource],
      classOf[HuggingFaceModelResource],
      classOf[WarehouseResource]
    )
  }

  it should "install the JWT auth stack and role enforcement" in {
    assume(
      databaseReachable,
      "run() requires a reachable Postgres at the configured JDBC URL (provided in CI)"
    )
    // The filter that turns a Bearer token into a SessionUser principal.
    resourceInstances.map(_.getClass) should contain(classOf[AuthDynamicFeature])
    // The binder that lets resource methods inject that principal via @Auth.
    resourceInstances.count(_.isInstanceOf[AuthValueFactoryProvider.Binder[_]]) shouldBe 1
    // Without this feature Jersey ignores every @RolesAllowed, exposing the admin endpoints to
    // any authenticated user.
    resourceClasses should contain(classOf[RolesAllowedDynamicFeature])
  }

  it should "route unmatched requests to the Angular index page" in {
    assume(
      databaseReachable,
      "run() requires a reachable Postgres at the configured JDBC URL (provided in CI)"
    )
    ranEnvironment.getApplicationContext.getErrorHandler match {
      case handler: ErrorPageErrorHandler =>
        handler.getErrorPages.asScala shouldBe Map("404" -> "/")
      case other => fail(s"expected an ErrorPageErrorHandler, got $other")
    }
  }

  it should "enable HTTP sessions on both Jersey and the servlet context" in {
    assume(
      databaseReachable,
      "run() requires a reachable Postgres at the configured JDBC URL (provided in CI)"
    )
    resourceClasses should contain(classOf[SessionHandler])
    val context = ranEnvironment.getApplicationContext
    context.getSessionHandler should not be null
    context.isSessionsEnabled shouldBe true
  }

  it should "install the websocket upgrade filter with a one-hour idle timeout" in {
    assume(
      databaseReachable,
      "run() requires a reachable Postgres at the configured JDBC URL (provided in CI)"
    )
    val upgradeFilter = ranEnvironment.getApplicationContext
      .getAttribute(classOf[WebSocketUpgradeFilter].getName)
      .asInstanceOf[WebSocketUpgradeFilter]
    // Spelled out rather than derived from Duration.ofHours(1): an editor session that sits idle
    // between keystrokes must outlive Jetty's 5-minute default, or collaboration silently drops.
    upgradeFilter.getFactory.getPolicy.getIdleTimeout shouldBe 3600000L
  }

  it should "install the cache-control and request-log filters across every dispatch" in {
    assume(
      databaseReachable,
      "run() requires a reachable Postgres at the configured JDBC URL (provided in CI)"
    )
    Seq(
      mappingOf(cacheControlFilter),
      mappingOf(requestLogFilter)
    ).foreach { mapping =>
      mapping.getPathSpecs.toSeq shouldBe Seq("/*")
      // Errors and forwards are dispatched too, so the cache headers and the access line must not
      // be limited to REQUEST — the 404-to-index fallback arrives as an ERROR dispatch.
      mapping.getDispatcherTypes shouldBe EnumSet.allOf(classOf[DispatcherType])
    }
  }

  // --- the anonymous request-log filter installed by run() ---

  "the request-log filter" should "emit one access line per request when the request log is at INFO" in {
    assume(
      databaseReachable,
      "run() requires a reachable Postgres at the configured JDBC URL (provided in CI)"
    )
    val chained = new AtomicBoolean(false)
    val emitted = withRequestLog(Level.INFO) {
      requestLogFilter.doFilter(
        httpRequest("10.0.0.7", "GET", "/api/workflow", "HTTP/1.1"),
        httpResponse(204),
        recordingChain(chained)
      )
    }
    emitted.map(_.getFormattedMessage) shouldBe Seq(
      """10.0.0.7 - "GET /api/workflow HTTP/1.1" 204"""
    )
    // The level as well as the text. Without this the emission could be moved to WARN and both
    // request-log tests would still pass: the guard would stop the line at WARN, and at INFO the
    // appender would collect an identical one.
    emitted.map(_.getLevel) shouldBe Seq(Level.INFO)
    chained.get() shouldBe true
  }

  it should "suppress the access line, and the casts behind it, when the request log is above INFO" in {
    assume(
      databaseReachable,
      "run() requires a reachable Postgres at the configured JDBC URL (provided in CI)"
    )
    val chained = new AtomicBoolean(false)
    // A non-HTTP request/response pair: the level guard is what keeps the two asInstanceOf casts
    // from running, so dropping it turns this into a ClassCastException rather than a no-op.
    val emitted = withRequestLog(Level.WARN) {
      requestLogFilter.doFilter(
        proxy(classOf[ServletRequest])(PartialFunction.empty),
        proxy(classOf[ServletResponse])(PartialFunction.empty),
        recordingChain(chained)
      )
    }
    emitted shouldBe empty
    chained.get() shouldBe true
  }

  // --- fixtures ---

  private def filterHolders: Seq[FilterHolder] =
    ranEnvironment.getApplicationContext.getServletHandler.getFilters.toSeq

  private def cacheControlFilter: Filter =
    filterHolders
      .flatMap(holder => Option(holder.getFilter))
      .find(_.isInstanceOf[StaticAssetCacheFilter])
      .getOrElse(fail("StaticAssetCacheFilter was not registered"))

  private def requestLogFilter: Filter =
    filterHolders
      .flatMap(holder => Option(holder.getFilter))
      .find(_.getClass.getName.startsWith(classOf[TexeraWebApplication].getName + "$$anon$"))
      .getOrElse(fail("the anonymous request-log filter was not registered"))

  private def mappingOf(filter: Filter): FilterMapping = {
    val handler = ranEnvironment.getApplicationContext.getServletHandler
    val holder = filterHolders
      .find(h => Option(h.getFilter).exists(_ eq filter))
      .getOrElse(fail(s"no holder for $filter"))
    handler.getFilterMappings.toSeq
      .find(_.getFilterName == holder.getName)
      .getOrElse(fail(s"no mapping for ${holder.getName}"))
  }

  /**
    * Runs `body` with Jetty's request-log logger pinned to `level` and a collecting appender
    * attached, returning the formatted messages it saw. Only this one logger name is touched, and
    * its level is restored, so parallel suites in the shared JVM are unaffected.
    */
  /** Returns the events the request logger emitted, so both the text and the LEVEL can be pinned. */
  private def withRequestLog(level: Level)(body: => Unit): Seq[ILoggingEvent] = {
    val logger =
      LoggerFactory.getLogger("org.eclipse.jetty.server.RequestLog").asInstanceOf[LogbackLogger]
    val events = mutable.ListBuffer.empty[ILoggingEvent]
    val appender = new AppenderBase[ILoggingEvent] {
      override def append(event: ILoggingEvent): Unit = events += event
    }
    appender.setContext(logger.getLoggerContext)
    appender.start()
    val priorLevel = logger.getLevel
    logger.setLevel(level)
    logger.addAppender(appender)
    try body
    finally {
      logger.detachAppender(appender)
      appender.stop()
      logger.setLevel(priorLevel)
    }
    events.toSeq
  }

  // A proxy that answers the handled methods and returns nulls/zeros for everything else,
  // following StaticAssetCacheFilterSpec so the servlet doubles stay dependency-free.
  private def proxy[T](
      cls: Class[T]
  )(handler: PartialFunction[(String, Seq[AnyRef]), AnyRef]): T = {
    val invocationHandler = new InvocationHandler {
      override def invoke(p: Any, m: Method, args: Array[AnyRef]): AnyRef = {
        val a = if (args == null) Seq.empty[AnyRef] else args.toSeq
        handler.applyOrElse(
          (m.getName, a),
          (_: (String, Seq[AnyRef])) => defaultValue(m.getReturnType)
        )
      }
    }
    Proxy
      .newProxyInstance(cls.getClassLoader, Array[Class[_]](cls), invocationHandler)
      .asInstanceOf[T]
  }

  private def defaultValue(t: Class[_]): AnyRef =
    if (t == java.lang.Boolean.TYPE) java.lang.Boolean.FALSE
    else if (t == java.lang.Integer.TYPE) java.lang.Integer.valueOf(0)
    else if (t == java.lang.Long.TYPE) java.lang.Long.valueOf(0L)
    else null

  private def httpRequest(
      remoteAddr: String,
      method: String,
      uri: String,
      protocol: String
  ): HttpServletRequest =
    proxy(classOf[HttpServletRequest]) {
      case ("getRemoteAddr", _) => remoteAddr
      case ("getMethod", _)     => method
      case ("getRequestURI", _) => uri
      case ("getProtocol", _)   => protocol
    }

  private def httpResponse(status: Int): HttpServletResponse =
    proxy(classOf[HttpServletResponse]) { case ("getStatus", _) => Integer.valueOf(status) }

  private def recordingChain(invoked: AtomicBoolean): FilterChain =
    (_: ServletRequest, _: ServletResponse) => invoked.set(true)
}
