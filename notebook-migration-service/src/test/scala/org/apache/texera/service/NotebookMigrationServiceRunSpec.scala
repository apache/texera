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
import org.apache.texera.auth.UnauthorizedExceptionMapper
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.dao.SqlServer
import org.apache.texera.service.NotebookMigrationServiceRunSpec.SpecPayload
import org.apache.texera.service.resource.{HealthCheckResource, NotebookMigrationResource}
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature
import org.mockito.ArgumentMatchers.isA
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, FileNotFoundException, InputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.{DriverManager, SQLException}
import scala.util.{Try, Using}

class NotebookMigrationServiceRunSpec extends AnyFlatSpec with Matchers {

  "NotebookMigrationService.run" should "register the resources and the JWT auth stack on the Jersey environment" in {
    val jersey = mock(classOf[JerseyEnvironment])
    val servlets = mock(classOf[ServletEnvironment])
    val context = mock(classOf[MutableServletContextHandler])
    val env = mock(classOf[Environment])
    when(env.jersey).thenReturn(jersey)
    when(env.servlets).thenReturn(servlets)
    when(env.getApplicationContext).thenReturn(context)
    when(jersey.getResourceConfig).thenReturn(new DropwizardResourceConfig())

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

  // --- initialize() ---

  /**
    * Registering the driver is explicit because sbt gives each test project a layered classloader
    * that DriverManager's own ServiceLoader scan does not always reach; a deployed service has one
    * flat classpath and never hits this. Both the probe below and HikariCP resolve the driver
    * through DriverManager, so this has to happen before initialize() runs. A missing class throws
    * out of here rather than reading as "no database".
    */
  private lazy val postgresDriverLoaded: Class[_] = Class.forName("org.postgresql.Driver")

  // Only the pool assertion needs a database. This service, access-control-service and
  // config-service are the three entry points that call SqlServer.initConnection from
  // initialize(); the other five call it from run(). That inconsistency is not endorsed here:
  // if the call moves into run(), the pool test below moves with it (and goes red until it does).
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

  private val seededConfigPath = "notebook-migration-service-spec.yml"

  // The `:-` default form is the only substitution shape this service actually ships: both
  // `${...}` in notebook-migration-service-web-config.yaml (lines 28 and 31) carry a default.
  private val seededConfig =
    "defined: ${PATH}\ndefaulted: ${TEXERA_NOTEBOOK_MIGRATION_SERVICE_SPEC_UNSET:-fallback}\n"

  private val unsetConfigPath = "notebook-migration-service-spec-unset.yml"
  private val unsetConfig = "unset: ${TEXERA_NOTEBOOK_MIGRATION_SERVICE_SPEC_UNSET}\n"

  /** What one full initialize() left behind. */
  private case class Initialized(
      bootstrap: Bootstrap[NotebookMigrationServiceConfiguration],
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
    * `SqlServer.initConnection` swaps a JVM-global singleton. The suites of this module share one
    * unforked JVM and NotebookMigrationResourceSpec points that singleton at its own MockTexeraDB
    * database, so the pool is closed and the previously installed DSLContext is put back the
    * instant initialize() returns. Closing our pool is safe for that restored context: it carries
    * its own DataSource.
    */
  private lazy val initialized: Initialized = {
    val application = new NotebookMigrationService
    val bootstrap = new Bootstrap[NotebookMigrationServiceConfiguration](application)
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

  "NotebookMigrationService.initialize" should "substitute environment variables into the configuration source it was handed" in {
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
    resolve(unsetConfigPath) shouldBe "unset: ${TEXERA_NOTEBOOK_MIGRATION_SERVICE_SPEC_UNSET}\n"
  }

  it should "register the Scala module on Dropwizard's object mapper" in {
    val mapper = initialized.bootstrap.getObjectMapper
    // The whole module, not only the Option support that `Some("x")` alone would prove: this is
    // the mapper Dropwizard hands to Jersey, so every payload the API returns goes through it.
    mapper.getRegisteredModuleIds should contain(
      "com.fasterxml.jackson.module.scala.DefaultScalaModule$"
    )

    val payload =
      SpecPayload("notebook.ipynb", Map("migrated" -> Some(Seq(1, 2)), "failed" -> None))
    // Option unwrapped, Map and Seq emitted as JSON, and the camelCase property names left alone
    // (Dropwizard's own naming strategy is annotation-sensitive; a global one would rename them).
    val json = mapper.writeValueAsString(payload)
    json shouldBe """{"notebookName":"notebook.ipynb","cellOutcomes":{"migrated":[1,2],"failed":null}}"""
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
    // Not the test-cases database: pointing the running service at it would have every migration
    // record land in a schema that the CI e2e specs truncate underneath it. Only the URL is
    // pinned — storage.conf ships username and password as the same string ("postgres"), so
    // nothing here can tell the two credential arguments apart.
    initialized.pooledJdbcUrl.get shouldBe StorageConfig.jdbcUrl
  }
}

object NotebookMigrationServiceRunSpec {

  /** Stands in for the payloads this service returns: camelCase names, a Map, a Seq and an Option. */
  final case class SpecPayload(notebookName: String, cellOutcomes: Map[String, Option[Seq[Int]]])
}
