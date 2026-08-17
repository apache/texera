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
import ch.qos.logback.core.read.ListAppender
import com.codahale.metrics.MetricRegistry
import com.fasterxml.jackson.databind.ObjectMapper
import io.dropwizard.Configuration
import io.dropwizard.auth.{AuthDynamicFeature, AuthValueFactoryProvider}
import io.dropwizard.configuration.SubstitutingSourceProvider
import io.dropwizard.jersey.validation.Validators
import io.dropwizard.setup.{Bootstrap, Environment}
import io.dropwizard.websockets.WebsocketBundle
import org.apache.commons.jcs3.access.exception.InvalidArgumentException
import org.apache.pekko.actor.ActorSystem
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.common.config.ApplicationConfig
import org.apache.texera.dao.{MockTexeraDB, SqlServer}
import org.apache.texera.dao.jooq.generated.Tables.{
  USER,
  WORKFLOW,
  WORKFLOW_EXECUTIONS,
  WORKFLOW_VERSION
}
import org.apache.texera.dao.jooq.generated.tables.pojos.WorkflowExecutions
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource
import org.apache.texera.web.resource.pythonvirtualenvironment.{PveResource, PveWebsocketResource}
import org.apache.texera.web.resource.{
  SyncExecutionResource,
  WebsocketPayloadSizeTuner,
  WorkflowWebsocketResource
}
import org.eclipse.jetty.server.session.SessionHandler
import org.eclipse.jetty.servlet.FilterHolder
import org.eclipse.jetty.websocket.server.WebSocketUpgradeFilter
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.lang.reflect.Proxy
import java.nio.file.{Files, Path}
import java.sql.Timestamp
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import javax.servlet.{DispatcherType, FilterChain, ServletContext, ServletContextEvent}
import javax.websocket.server.ServerContainer
import scala.jdk.CollectionConverters._

class ComputingUnitMasterSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  // ComputingUnitMaster.run schedules the result-cleanup task through AmberRuntime's
  // process-wide actor system, and only touches `.scheduler`, so a plain ActorSystem is
  // enough here; AmberRuntime.startActorMaster would additionally bind pekko artery on
  // 2552 and join a cluster. Preserve and restore the shared reference because amber
  // suites share one JVM (same approach as WorkflowLifecycleManagerSpec).
  private lazy val testSystem: ActorSystem = ActorSystem("computing-unit-master-spec")
  private var previousActorSystem: AnyRef = _

  private def amberRuntimeActorSystemField = {
    val field = AmberRuntime.getClass.getDeclaredField("_actorSystem")
    field.setAccessible(true)
    field
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    previousActorSystem = amberRuntimeActorSystemField.get(AmberRuntime)
    amberRuntimeActorSystemField.set(AmberRuntime, testSystem)
  }

  override protected def afterAll(): Unit = {
    amberRuntimeActorSystemField.set(AmberRuntime, previousActorSystem)
    // Terminating the system also cancels the recurring cleanup task run() scheduled.
    testSystem.terminate()
    // run() repoints the JVM-wide SqlServer singleton at StorageConfig.jdbcUrl and closes the
    // pool it replaced. Point it back at this suite's embedded database before tearing that down,
    // so the singleton is never left aimed at production storage for whatever runs next.
    initializeDBAndReplaceDSLContext()
    super.afterAll()
  }

  private val master = new ComputingUnitMaster()

  private def newEnvironment(name: String): Environment =
    new Environment(
      name,
      new ObjectMapper(),
      Validators.newValidator(),
      new MetricRegistry(),
      getClass.getClassLoader
    )

  /** The SqlServer singletons seen either side of the single run() call. */
  private var sqlServerBeforeRun: AnyRef = _
  private var sqlServerAfterRun: AnyRef = _

  /**
    * run() is driven exactly once against a real Dropwizard Environment (a mock would not
    * do: WebSocketUpgradeFilter.configureContext needs a live MutableServletContextHandler);
    * every assertion below inspects the resulting wiring. Note that run() also repoints the
    * JVM-wide SqlServer singleton at StorageConfig.jdbcUrl, so this suite requires the same
    * reachable Postgres the amber CI job provisions; MockTexeraDB.withFixture points the
    * singleton back at this suite's own database before each subsequent test, and afterAll points
    * it back once more before shutting the pool down.
    *
    * One residual hazard is worth naming rather than hiding: initConnection CLOSES the pool it
    * replaces, so a suite running concurrently in this JVM could lose its connection mid-test.
    * amber sets neither `Test / fork` nor `Test / parallelExecution := false`, unlike every other
    * module that mixes MockTexeraDB, so nothing in the build prevents that overlap.
    */
  private lazy val ranEnvironment: Environment = {
    val environment = newEnvironment("computing-unit-master-spec-run")
    sqlServerBeforeRun = SqlServer.getInstance()
    master.run(new Configuration(), environment)
    sqlServerAfterRun = SqlServer.getInstance()
    environment
  }

  /** The anonymous request-logging Filter that run() installs on the application context. */
  private def requestLogFilterHolder: FilterHolder =
    ranEnvironment.getApplicationContext.getServletHandler.getFilters
      .find(_.getFilter.getClass.getName.startsWith(classOf[ComputingUnitMaster].getName + "$"))
      .getOrElse(fail("run() did not install the anonymous request-logging filter"))

  /**
    * Minimal dynamic-proxy stub. `answers` handles the calls a test cares about; anything
    * else yields the return type's zero value. Used instead of a mocking library because
    * amber declares scalamock, which cannot stub Java interfaces of this shape cheaply.
    */
  private def stub[T](iface: Class[T])(
      answers: PartialFunction[(String, Seq[AnyRef]), AnyRef]
  ): T =
    Proxy
      .newProxyInstance(
        iface.getClassLoader,
        Array(iface),
        (_, method, args) => {
          val callArgs = Option(args).map(_.toSeq).getOrElse(Seq.empty)
          answers.applyOrElse(
            (method.getName, callArgs),
            (_: (String, Seq[AnyRef])) => zeroValue(method.getReturnType)
          )
        }
      )
      .asInstanceOf[T]

  private def zeroValue(returnType: Class[_]): AnyRef =
    if (!returnType.isPrimitive) null
    else if (returnType == java.lang.Boolean.TYPE) java.lang.Boolean.FALSE
    else if (returnType == java.lang.Integer.TYPE) java.lang.Integer.valueOf(0)
    else if (returnType == java.lang.Long.TYPE) java.lang.Long.valueOf(0L)
    else null

  /** Collects the events a named logger emits at `level` or above while `body` runs. */
  private def eventsLoggedAt[T](loggerName: String, level: Level)(
      body: => T
  ): Seq[ILoggingEvent] = {
    val logger = org.slf4j.LoggerFactory.getLogger(loggerName).asInstanceOf[LogbackLogger]
    val appender = new ListAppender[ILoggingEvent]
    val previousLevel = logger.getLevel
    appender.start()
    logger.addAppender(appender)
    logger.setLevel(level)
    try {
      body
      appender.list.asScala.toSeq
    } finally {
      logger.setLevel(previousLevel)
      logger.detachAppender(appender)
      appender.stop()
    }
  }

  private def loggedAt[T](loggerName: String, level: Level)(body: => T): Seq[String] =
    eventsLoggedAt(loggerName, level)(body).map(_.getFormattedMessage)

  private val masterLoggerName = classOf[ComputingUnitMaster].getName

  /**
    * The cleanup helpers are private, and in production they are only reached through the
    * scheduled task (which fires on a day-long interval) or the CLEANUP_ALL_EXECUTION_RESULTS
    * branch (gated on a load-time val amber's unforked Test config cannot set), so they are
    * invoked reflectively here.
    */
  private def invokeCleanupHelper(
      name: String,
      parameterTypes: Seq[Class[_]],
      args: Seq[AnyRef]
  ): Unit = {
    val method = classOf[ComputingUnitMaster].getDeclaredMethod(name, parameterTypes: _*)
    method.setAccessible(true)
    try method.invoke(master, args: _*)
    catch {
      case invocation: java.lang.reflect.InvocationTargetException => throw invocation.getCause
    }
  }

  private def dropCollections(result: String): Unit =
    invokeCleanupHelper("dropCollections", Seq(classOf[String]), Seq(result))

  private def deleteReplayLog(logLocation: String): Unit =
    invokeCleanupHelper("deleteReplayLog", Seq(classOf[String]), Seq(logLocation))

  private def recurringCheckExpiredResults(timeToLive: Int): Unit =
    invokeCleanupHelper(
      "recurringCheckExpiredResults",
      Seq(java.lang.Integer.TYPE),
      Seq(java.lang.Integer.valueOf(timeToLive))
    )

  private def cleanExecutions(
      executions: List[WorkflowExecutions],
      statusChangeFunc: Short => Short
  ): Unit =
    invokeCleanupHelper(
      "cleanExecutions",
      Seq(classOf[List[_]], classOf[Function1[_, _]]),
      Seq(executions, statusChangeFunc)
    )

  private def execution(eid: Int, result: String, logLocation: String): WorkflowExecutions = {
    val entry = new WorkflowExecutions()
    entry.setEid(eid)
    entry.setResult(result)
    entry.setLogLocation(logLocation)
    entry
  }

  "parseArgs" should "return no options when the master receives no arguments" in {
    ComputingUnitMaster.parseArgs(Array.empty[String]) shouldBe Map.empty
  }

  it should "parse a true cluster flag into a boolean" in {
    ComputingUnitMaster.parseArgs(Array("--cluster", "true")) shouldBe
      Map(Symbol("cluster") -> true)
  }

  it should "parse a false cluster flag into a boolean" in {
    ComputingUnitMaster.parseArgs(Array("--cluster", "false")) shouldBe
      Map(Symbol("cluster") -> false)
  }

  it should "use the last cluster value when the option is repeated" in {
    ComputingUnitMaster.parseArgs(
      Array("--cluster", "true", "--cluster", "false")
    ) shouldBe
      Map(Symbol("cluster") -> false)
  }

  it should "reject an unknown command-line option" in {
    val exception = intercept[InvalidArgumentException] {
      ComputingUnitMaster.parseArgs(Array("--serverAddr", "master.internal:8080"))
    }

    exception.getMessage shouldBe "unknown command-line arg"
  }

  it should "reject a cluster option with no value" in {
    val exception = intercept[InvalidArgumentException] {
      ComputingUnitMaster.parseArgs(Array("--cluster"))
    }

    exception.getMessage shouldBe "unknown command-line arg"
  }

  it should "parse a mixed-case cluster value case-insensitively" in {
    ComputingUnitMaster.parseArgs(Array("--cluster", "TRUE")) shouldBe
      Map(Symbol("cluster") -> true)
  }

  it should "reject an unknown option that follows a valid cluster pair" in {
    val exception = intercept[InvalidArgumentException] {
      ComputingUnitMaster.parseArgs(Array("--cluster", "true", "--bogus"))
    }

    exception.getMessage shouldBe "unknown command-line arg"
  }

  it should "consume a repeated flag as the cluster value" in {
    // The two-element pattern greedily takes the next token as the value, so a second
    // "--cluster" is fed to String.toBoolean and fails with IllegalArgumentException
    // rather than InvalidArgumentException (current behavior). This would flip to
    // InvalidArgumentException if a value-looks-like-a-flag guard were ever added.
    an[IllegalArgumentException] should be thrownBy {
      ComputingUnitMaster.parseArgs(Array("--cluster", "--cluster"))
    }
  }

  it should "fail with an IllegalArgumentException on a non-boolean cluster value" in {
    // The value is parsed with String.toBoolean, so a malformed boolean surfaces as an
    // IllegalArgumentException instead of the InvalidArgumentException used for unknown
    // options (current behavior).
    an[IllegalArgumentException] should be thrownBy {
      ComputingUnitMaster.parseArgs(Array("--cluster", "notabool"))
    }
  }

  "initialize" should "wrap the configuration source provider in the environment substitutor" in {
    val bootstrap = new Bootstrap[Configuration](master)

    master.initialize(bootstrap)

    bootstrap.getConfigurationSourceProvider shouldBe a[SubstitutingSourceProvider]
  }

  it should "register the websocket bundle for both websocket endpoints" in {
    val bootstrap = new Bootstrap[Configuration](master)

    master.initialize(bootstrap)

    // Bootstrap exposes no accessor for the bundles it was given, and WebsocketBundle keeps
    // its endpoints as ServerEndpointConfigs, so both have to be read reflectively; the
    // endpoint classes themselves come back off the public ServerEndpointConfig API.
    val bundlesField = classOf[Bootstrap[_]].getDeclaredField("bundles")
    bundlesField.setAccessible(true)
    val websocketBundles = bundlesField
      .get(bootstrap)
      .asInstanceOf[java.util.List[_]]
      .asScala
      .collect { case bundle: WebsocketBundle => bundle }
    websocketBundles should have size 1

    val configsField = classOf[WebsocketBundle].getDeclaredField("endpointConfigs")
    configsField.setAccessible(true)
    val endpointClasses = configsField
      .get(websocketBundles.head)
      .asInstanceOf[java.util.Collection[javax.websocket.server.ServerEndpointConfig]]
      .asScala
      .map(_.getEndpointClass)
    endpointClasses should contain theSameElementsAs Seq(
      classOf[WorkflowWebsocketResource],
      classOf[PveWebsocketResource]
    )
  }

  it should "register the Scala module on the bootstrap object mapper" in {
    val bootstrap = new Bootstrap[Configuration](master)

    master.initialize(bootstrap)

    bootstrap.getObjectMapper.getRegisteredModuleIds.asScala should contain(
      com.fasterxml.jackson.module.scala.DefaultScalaModule.getClass.getName
    )
  }

  "run" should "serve the Jersey resources under the /api prefix" in {
    ranEnvironment.jersey.getUrlPattern shouldBe "/api/*"
  }

  it should "open its own connection to the configured storage database" in {
    ranEnvironment

    // MockTexeraDB has already installed a singleton for this suite, and nothing else runs
    // between the two reads, so a second instance can only come from run() itself calling
    // SqlServer.initConnection.
    sqlServerAfterRun should not be theSameInstanceAs(sqlServerBeforeRun)
  }

  it should "register the master's Jersey resource classes" in {
    ranEnvironment.jersey.getResourceConfig.getClasses.asScala should contain allOf (
      classOf[PveResource],
      classOf[WorkflowExecutionsResource],
      classOf[SyncExecutionResource],
      classOf[SessionHandler],
      classOf[RolesAllowedDynamicFeature]
    )
  }

  it should "install the JWT auth feature and the session-user value factory" in {
    val singletons = ranEnvironment.jersey.getResourceConfig.getSingletons.asScala

    singletons.count(_.isInstanceOf[AuthDynamicFeature]) shouldBe 1
    singletons.count(_.isInstanceOf[AuthValueFactoryProvider.Binder[_]]) shouldBe 1
  }

  it should "attach a session handler to the servlet context" in {
    // The application context starts out without one; setSessionHandler puts it there.
    ranEnvironment.getApplicationContext.getSessionHandler should not be null
  }

  it should "configure the websocket upgrade filter with a one-hour idle timeout" in {
    val upgradeFilter = ranEnvironment.getApplicationContext
      .getAttribute(classOf[WebSocketUpgradeFilter].getName)
      .asInstanceOf[WebSocketUpgradeFilter]

    upgradeFilter should not be null
    // Jetty's own default is 5 minutes, so 1 hour can only come from run().
    upgradeFilter.getFactory.getPolicy.getIdleTimeout shouldBe 3600000L
  }

  it should "raise the websocket payload buffers to the configured size" in {
    val tuner = ranEnvironment.getApplicationContext.getEventListeners
      .collectFirst { case tuner: WebsocketPayloadSizeTuner => tuner }
      .getOrElse(fail("run() did not register the websocket payload size tuner"))

    var textBufferSize = -1
    var binaryBufferSize = -1
    val container = stub(classOf[ServerContainer]) {
      case ("setDefaultMaxTextMessageBufferSize", args) =>
        textBufferSize = args.head.asInstanceOf[Integer]; null
      case ("setDefaultMaxBinaryMessageBufferSize", args) =>
        binaryBufferSize = args.head.asInstanceOf[Integer]; null
    }
    val servletContext = stub(classOf[ServletContext]) {
      case ("getAttribute", _) => container.asInstanceOf[AnyRef]
    }

    tuner.contextInitialized(new ServletContextEvent(servletContext))

    val expected = ApplicationConfig.maxWorkflowWebsocketRequestPayloadSizeKb * 1024
    textBufferSize shouldBe expected
    binaryBufferSize shouldBe expected
  }

  it should "map the request-logging filter to every path and dispatcher type" in {
    val holder = requestLogFilterHolder
    val mapping = ranEnvironment.getApplicationContext.getServletHandler.getFilterMappings
      .find(_.getFilterName == holder.getName)
      .getOrElse(fail("the request-logging filter was added without a mapping"))

    mapping.getPathSpecs.toSeq shouldBe Seq("/*")
    mapping.getDispatcherTypes shouldBe java.util.EnumSet.allOf(classOf[DispatcherType])
  }

  "the request-logging filter" should "forward the request and log one access line at info" in {
    val filter = requestLogFilterHolder.getFilter
    var chainCalls = 0
    val chain = stub(classOf[FilterChain]) { case ("doFilter", _) => chainCalls += 1; null }

    val events = eventsLoggedAt("org.eclipse.jetty.server.RequestLog", Level.INFO) {
      filter.doFilter(request("10.0.0.7", "GET", "/api/workflow"), response(200), chain)
    }

    chainCalls shouldBe 1
    // The level is asserted too: access lines must stay at info so that raising
    // TEXERA_SERVICE_LOG_LEVEL silences them.
    events.map(event => (event.getLevel, event.getFormattedMessage)) shouldBe Seq(
      (Level.INFO, """10.0.0.7 - "GET /api/workflow HTTP/1.1" 200""")
    )
  }

  it should "forward the request without inspecting it when info logging is off" in {
    val filter = requestLogFilterHolder.getFilter
    var chainCalls = 0
    var remoteAddrReads = 0
    val chain = stub(classOf[FilterChain]) { case ("doFilter", _) => chainCalls += 1; null }
    val countingRequest = stub(classOf[HttpServletRequest]) {
      case ("getRemoteAddr", _) => remoteAddrReads += 1; "10.0.0.7"
    }

    val events = eventsLoggedAt("org.eclipse.jetty.server.RequestLog", Level.WARN) {
      filter.doFilter(countingRequest, response(200), chain)
    }

    chainCalls shouldBe 1
    events shouldBe empty
    // Logback would drop the info event on its own, so an absent event proves nothing about
    // the isInfoEnabled guard. What the guard buys is not paying for the access line at all,
    // which is only observable as the request never being interrogated.
    remoteAddrReads shouldBe 0
  }

  private def request(remoteAddr: String, method: String, uri: String): HttpServletRequest =
    stub(classOf[HttpServletRequest]) {
      case ("getRemoteAddr", _) => remoteAddr
      case ("getMethod", _)     => method
      case ("getRequestURI", _) => uri
      case ("getProtocol", _)   => "HTTP/1.1"
    }

  private def response(status: Int): HttpServletResponse =
    stub(classOf[HttpServletResponse]) {
      case ("getStatus", _) => java.lang.Integer.valueOf(status)
    }

  "dropCollections" should "skip an execution whose result pointer is empty" in {
    // Without the early return an empty string parses to a missing node and the
    // "results" lookup would blow up into a cleanup-failed warning.
    loggedAt(masterLoggerName, Level.WARN)(dropCollections("")) shouldBe empty
    loggedAt(masterLoggerName, Level.WARN)(dropCollections(null)) shouldBe empty
  }

  it should "accept an iceberg result pointer without warning" in {
    val result =
      """{"results":[{"storageType":"iceberg","storageKey":"k1"},
        |{"storageType":"iceberg","storageKey":"k2"}]}""".stripMargin

    loggedAt(masterLoggerName, Level.WARN)(dropCollections(result)) shouldBe empty
  }

  it should "warn instead of propagating when the result pointer is unparsable" in {
    val messages = loggedAt(masterLoggerName, Level.WARN)(dropCollections("{not json"))

    messages shouldBe Seq("result collection cleanup failed.")
  }

  it should "warn instead of propagating on an unsupported storage type" in {
    // The unsupported entry is the last one, so a loop that stopped early would also
    // fail this expectation rather than silently reporting success.
    val result =
      """{"results":[{"storageType":"iceberg","storageKey":"k1"},
        |{"storageType":"mongodb","storageKey":"k2"}]}""".stripMargin

    val messages = loggedAt(masterLoggerName, Level.WARN)(dropCollections(result))

    messages shouldBe Seq("result collection cleanup failed.")
  }

  "deleteReplayLog" should "skip an execution with no log location" in {
    // Without the early return the empty location becomes a scheme-less URI and the
    // storage lookup would blow up into a delete-failed warning.
    loggedAt(masterLoggerName, Level.WARN)(deleteReplayLog("")) shouldBe empty
    loggedAt(masterLoggerName, Level.WARN)(deleteReplayLog(null)) shouldBe empty
  }

  it should "delete the log folder the location points at" in {
    val logFolder: Path = Files.createTempDirectory("computing-unit-master-spec-log")
    Files.createFile(logFolder.resolve("0.log"))

    val messages =
      loggedAt(masterLoggerName, Level.WARN)(deleteReplayLog(logFolder.toUri.toString))

    messages shouldBe empty
    Files.exists(logFolder) shouldBe false
  }

  it should "warn instead of propagating when the log location is unusable" in {
    // A scheme-less location cannot be resolved to any record storage.
    val messages = loggedAt(masterLoggerName, Level.WARN)(deleteReplayLog("no-scheme/logs"))

    messages shouldBe Seq("failed to delete log at no-scheme/logs")
  }

  "cleanExecutions" should "clean every execution it is handed" in {
    val executions = List(
      execution(eid = 90001, result = "{not json", logLocation = "no-scheme/first"),
      execution(eid = 90002, result = "{not json", logLocation = "no-scheme/second")
    )

    val messages =
      loggedAt(masterLoggerName, Level.WARN)(cleanExecutions(executions, identity))

    // Two entries, so a fold that stopped after the head would show up here.
    messages shouldBe Seq(
      "result collection cleanup failed.",
      "failed to delete log at no-scheme/first",
      "result collection cleanup failed.",
      "failed to delete log at no-scheme/second"
    )
  }

  it should "clear the stored pointers and rewrite the status of the persisted execution" in {
    // ExecutionsMetadataPersistService swallows every throwable, so with no row behind the
    // eid the update step is indistinguishable from a no-op; only a real row shows that the
    // pointers are cleared and the status is run through statusChangeFunc.
    val eid = seedExecution(
      result = """{"results":[]}""",
      logLocation = "file:///seeded-log",
      status = 3
    )

    cleanExecutions(
      // Empty pointers on the POJO: the update step reads the row, not the POJO, and this
      // keeps dropCollections/deleteReplayLog out of the way of what is being asserted.
      List(execution(eid.intValue(), result = "", logLocation = "")),
      statusByte => (statusByte + 1).toShort
    )

    val updated = storedExecution(eid)
    updated.getResult shouldBe ""
    updated.getLogLocation shouldBe null
    updated.getStatus shouldBe 4.toShort
  }

  /**
    * Seeds one workflow_executions row plus the user/workflow/version parents its foreign
    * keys require, and returns the generated eid. `lastUpdateAgeMillis` stays far below
    * result-cleanup.ttl-in-seconds (a day), so the cleanup task run() scheduled never
    * selects these rows and cannot clear one behind a test's back.
    */
  private def seedExecution(
      result: String,
      logLocation: String,
      status: Short,
      lastUpdateAgeMillis: Long = 0
  ): Integer = {
    val context = getDSLContext
    val now = System.currentTimeMillis()
    // The user email is UNIQUE, so every seeded row needs its own parents.
    val tag = "computing-unit-master-spec-" + java.util.UUID.randomUUID()
    val uid = context
      .insertInto(USER)
      .set(USER.NAME, "computing-unit-master-spec")
      .set(USER.EMAIL, tag + "@example.com")
      .returning(USER.UID)
      .fetchOne()
      .getUid
    val wid = context
      .insertInto(WORKFLOW)
      .set(WORKFLOW.NAME, "computing-unit-master-spec")
      .set(WORKFLOW.CONTENT, "{}")
      .returning(WORKFLOW.WID)
      .fetchOne()
      .getWid
    val vid = context
      .insertInto(WORKFLOW_VERSION)
      .set(WORKFLOW_VERSION.WID, wid)
      .set(WORKFLOW_VERSION.CONTENT, "{}")
      .returning(WORKFLOW_VERSION.VID)
      .fetchOne()
      .getVid
    context
      .insertInto(WORKFLOW_EXECUTIONS)
      .set(WORKFLOW_EXECUTIONS.VID, vid)
      .set(WORKFLOW_EXECUTIONS.UID, uid)
      .set(WORKFLOW_EXECUTIONS.STATUS, java.lang.Short.valueOf(status))
      .set(WORKFLOW_EXECUTIONS.RESULT, result)
      .set(WORKFLOW_EXECUTIONS.LOG_LOCATION, logLocation)
      .set(WORKFLOW_EXECUTIONS.ENVIRONMENT_VERSION, "computing-unit-master-spec")
      .set(WORKFLOW_EXECUTIONS.STARTING_TIME, new Timestamp(now - lastUpdateAgeMillis))
      .set(WORKFLOW_EXECUTIONS.LAST_UPDATE_TIME, new Timestamp(now - lastUpdateAgeMillis))
      .returning(WORKFLOW_EXECUTIONS.EID)
      .fetchOne()
      .getEid
  }

  private def storedExecution(eid: Integer): WorkflowExecutions =
    getDSLContext
      .selectFrom(WORKFLOW_EXECUTIONS)
      .where(WORKFLOW_EXECUTIONS.EID.eq(eid))
      .fetchOneInto(classOf[WorkflowExecutions])

  "recurringCheckExpiredResults" should "clean the executions past the time to live without touching their status" in {
    val eid = seedExecution(
      result = """{"results":[]}""",
      logLocation = "file:///seeded-recurring-log",
      status = 3,
      lastUpdateAgeMillis = 10000
    )

    // A time to live of zero makes this row expired for the call under test while leaving it
    // well inside the day-long window the task run() scheduled uses, so the two can never
    // select the same row and the background task cannot make this assertion pass.
    recurringCheckExpiredResults(timeToLive = 0)

    val updated = storedExecution(eid)
    updated.getResult shouldBe ""
    updated.getLogLocation shouldBe null
    // The recurring path passes the status through unchanged, unlike the post-restart path,
    // which flips anything incomplete to FAILED.
    updated.getStatus shouldBe 3.toShort
  }

  it should "leave an execution that is still inside the time to live alone" in {
    val eid = seedExecution(
      result = """{"results":[]}""",
      logLocation = "file:///seeded-fresh-log",
      status = 3,
      lastUpdateAgeMillis = 10000
    )

    recurringCheckExpiredResults(timeToLive = 3600)

    val untouched = storedExecution(eid)
    untouched.getResult shouldBe """{"results":[]}"""
    untouched.getLogLocation shouldBe "file:///seeded-fresh-log"
  }
}
