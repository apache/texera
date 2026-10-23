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
import io.dropwizard.configuration.ConfigurationSourceProvider
import io.dropwizard.core.setup.{Bootstrap, Environment}
import io.dropwizard.jersey.DropwizardResourceConfig
import io.dropwizard.jersey.setup.JerseyEnvironment
import io.dropwizard.jetty.MutableServletContextHandler
import io.dropwizard.jetty.setup.ServletEnvironment
import org.apache.texera.auth.{RoleAnnotationEnforcer, UnauthorizedExceptionMapper}
import org.apache.texera.common.config.{DefaultsConfig, StorageConfig}
import org.apache.texera.dao.{MockTexeraDB, SqlServer}
import org.apache.texera.dao.jooq.generated.Tables.SITE_SETTINGS
import org.apache.texera.service.ConfigServiceRunSpec.SpecPayload
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

import java.io.{ByteArrayInputStream, FileNotFoundException, InputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.{Connection, DriverManager, SQLException}
import scala.util.{Try, Using}

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

  // --- initialize() ---

  /**
    * Registering the driver is explicit because sbt gives each test project a layered classloader
    * that DriverManager's own ServiceLoader scan does not always reach; a deployed service has one
    * flat classpath and never hits this. Both the probe below and HikariCP resolve the driver
    * through DriverManager, so this has to happen before initialize() runs. A missing class throws
    * out of here rather than reading as "no database".
    */
  private lazy val postgresDriverLoaded: Class[_] = Class.forName("org.postgresql.Driver")

  // Only the pool assertion needs the deployment's own database (this suite's embedded
  // MockTexeraDB one is a different URL). This service, access-control-service and
  // notebook-migration-service are the three entry points that call SqlServer.initConnection from
  // initialize(); the other five call it from run(). That inconsistency is not endorsed here:
  // if the call moves into run(), the pool test below moves with it (and goes red until it does,
  // along with the two run() tests above, which would then be preloading the deployment's
  // site_settings instead of the fixture's).
  private def databaseReachable: Boolean =
    try {
      postgresDriverLoaded
      DriverManager
        .getConnection(
          StorageConfig.jdbcUrl,
          StorageConfig.jdbcUsername,
          StorageConfig.jdbcPassword
        )
        .close()
      true
    } catch {
      // Narrowed to SQLException on purpose: a missing driver or a linkage error is a broken test
      // classpath and has to fail loudly rather than silently cancel the assertion.
      case _: SQLException => false
    }

  private val seededConfigPath = "config-service-spec.yml"

  // The `:-` default form is the only substitution shape this service actually ships: both
  // `${...}` in config-service-web-config.yaml (lines 28 and 31) carry a default.
  private val seededConfig =
    "defined: ${PATH}\ndefaulted: ${TEXERA_CONFIG_SERVICE_SPEC_UNSET:-fallback}\n"

  private val unsetConfigPath = "config-service-spec-unset.yml"
  private val unsetConfig = "unset: ${TEXERA_CONFIG_SERVICE_SPEC_UNSET}\n"

  /** What one full initialize() left behind. */
  private case class Initialized(
      bootstrap: Bootstrap[ConfigServiceConfiguration],
      outcome: Try[Unit],
      sqlServerBefore: Option[SqlServer],
      sqlServerAfter: Option[SqlServer],
      pooledJdbcUrl: Try[String]
  )

  /**
    * A single full initialize(), shared by every assertion below so the connection pool is opened
    * once.
    *
    * The bootstrap is seeded with a recognisable in-memory configuration source first: that is what
    * makes the wrapping observable, because initialize() is required to wrap the provider already
    * installed, and a version that instead built a fresh file-reading provider would not find these
    * paths.
    *
    * initialize() is called inside a Try because opening the pool is its last statement: the
    * configuration-source and object-mapper effects have already landed by the time it can fail, so
    * the assertions that need no database still run on a host with no Postgres instead of silently
    * cancelling. `outcome` carries the failure to the one assertion that does need it.
    *
    * `SqlServer.initConnection` swaps the same JVM-global singleton this suite's MockTexeraDB
    * fixture owns, so the pool is closed and the fixture's DSLContext is put back the instant
    * initialize() returns. Closing our pool is safe for that restored context: it carries its own
    * DataSource.
    */
  private lazy val initialized: Initialized = {
    val application = new ConfigService
    val bootstrap = new Bootstrap[ConfigServiceConfiguration](application)
    bootstrap.setConfigurationSourceProvider(new ConfigurationSourceProvider {
      override def open(path: String): InputStream =
        path match {
          case `seededConfigPath` => new ByteArrayInputStream(seededConfig.getBytes(UTF_8))
          case `unsetConfigPath`  => new ByteArrayInputStream(unsetConfig.getBytes(UTF_8))
          case other              => throw new FileNotFoundException(other)
        }
    })
    postgresDriverLoaded
    val priorContext = Try(SqlServer.getInstance().createDSLContext()).toOption
    val sqlServerBefore = Try(SqlServer.getInstance()).toOption
    try {
      Initialized(
        bootstrap,
        Try(application.initialize(bootstrap)),
        sqlServerBefore,
        Try(SqlServer.getInstance()).toOption,
        Try(
          SqlServer
            .getInstance()
            .createDSLContext()
            .connectionResult(connection => connection.getMetaData.getURL)
        )
      )
    } finally {
      Try(SqlServer.getInstance().close())
      priorContext.foreach(context => Try(SqlServer.getInstance().replaceDSLContext(context)))
    }
  }

  private def resolve(path: String): String =
    Using.resource(initialized.bootstrap.getConfigurationSourceProvider.open(path)) { stream =>
      new String(stream.readAllBytes(), UTF_8)
    }

  "ConfigService.initialize" should "substitute environment variables into the configuration source it was handed" in {
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
    resolve(unsetConfigPath) shouldBe "unset: ${TEXERA_CONFIG_SERVICE_SPEC_UNSET}\n"
  }

  it should "register the Scala module on Dropwizard's object mapper" in {
    val mapper = initialized.bootstrap.getObjectMapper
    // The whole module, not only the Option support that `Some("x")` alone would prove: this is
    // the mapper Dropwizard hands to Jersey, so every payload the API returns goes through it.
    mapper.getRegisteredModuleIds should contain(
      "com.fasterxml.jackson.module.scala.DefaultScalaModule$"
    )

    val payload =
      SpecPayload("forum_enabled", Map("default" -> Some(Seq(1, 2)), "override" -> None))
    // Option unwrapped, Map and Seq emitted as JSON, and the camelCase property names left alone
    // (Dropwizard's own naming strategy is annotation-sensitive; a global one would rename them).
    val json = mapper.writeValueAsString(payload)
    json shouldBe """{"settingKey":"forum_enabled","storedValues":{"default":[1,2],"override":null}}"""
    // Reading, too: this is also the mapper Dropwizard parses the YAML configuration with.
    mapper.readValue(json, classOf[SpecPayload]) shouldBe payload
  }

  it should "open a connection pool against the configured JDBC URL" in {
    assume(
      databaseReachable,
      "initialize() opens a connection pool against the configured JDBC URL (provided in CI)"
    )
    // Rethrown rather than collapsed into a missing URL, so a pool that failed to open reports
    // why.
    initialized.outcome.get
    // A *new* SqlServer, not merely a URL read back off the singleton: without this the assertion
    // below also passes for an initialize() that opens nothing at all, off a pool some earlier
    // test in this JVM installed.
    initialized.sqlServerAfter.map(_ ne initialized.sqlServerBefore.orNull) shouldBe Some(true)
    // Not the test-cases database: preloading the defaults into it would leave the deployment's
    // own site_settings empty while the CI e2e specs truncate the rows that were written. Only
    // the URL is pinned — storage.conf ships username and password as the same string
    // ("postgres"), so nothing here can tell the two credential arguments apart.
    initialized.pooledJdbcUrl.get shouldBe StorageConfig.jdbcUrl
  }
}

object ConfigServiceRunSpec {

  /** Stands in for the payloads this service returns: camelCase names, a Map, a Seq and an Option. */
  final case class SpecPayload(settingKey: String, storedValues: Map[String, Option[Seq[Int]]])
}
