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
import org.apache.texera.auth.RoleAnnotationEnforcer
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.dao.SqlServer
import org.apache.texera.service.ComputingUnitManagingServiceRunSpec.SpecPayload
import org.apache.texera.service.resource.{
  AdminComputingUnitResource,
  ComputingUnitAccessResource,
  ComputingUnitManagingResource,
  HealthCheckResource
}
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature
import org.mockito.ArgumentMatchers.isA
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, FileNotFoundException, InputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.DriverManager
import scala.util.Using

class ComputingUnitManagingServiceRunSpec extends AnyFlatSpec with Matchers {

  // Booting the service opens a real connection pool, so this path is only exercisable where
  // Postgres is provisioned at the configured JDBC URL (as in CI).
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

  // Every endpoint this service registers declares @RolesAllowed/@PermitAll/@DenyAll.
  "ComputingUnitManagingService's registered resources" should "all declare access control" in {
    RoleAnnotationEnforcer.findUnannotatedEndpoints(
      Seq(
        classOf[ComputingUnitManagingResource],
        classOf[ComputingUnitAccessResource],
        classOf[AdminComputingUnitResource],
        classOf[HealthCheckResource]
      )
    ) shouldBe empty
  }

  "ComputingUnitManagingService.run" should "register the admin resource on the Jersey environment" in {
    assume(
      databaseReachable,
      "run() requires a reachable Postgres at the configured JDBC URL (provided in CI)"
    )

    val jersey = mock(classOf[JerseyEnvironment])
    val context = mock(classOf[MutableServletContextHandler])
    val env = mock(classOf[Environment])
    when(env.jersey).thenReturn(jersey)
    when(env.getApplicationContext).thenReturn(context)
    when(jersey.getResourceConfig).thenReturn(DropwizardResourceConfig.forTesting())

    try {
      new ComputingUnitManagingService()
        .run(mock(classOf[ComputingUnitManagingServiceConfiguration]), env)

      verify(jersey).register(isA(classOf[ComputingUnitManagingResource]))
      verify(jersey).register(isA(classOf[ComputingUnitAccessResource]))
      verify(jersey).register(isA(classOf[AdminComputingUnitResource]))
      verify(jersey).setUrlPattern("/api/*")
      // Without these two, Jersey never enforces AdminComputingUnitResource's
      // @RolesAllowed(ADMIN) and the cross-user listing is readable by any authenticated user.
      verify(jersey).register(isA(classOf[AuthDynamicFeature]))
      verify(jersey).register(classOf[RolesAllowedDynamicFeature])
    } finally {
      // run() calls SqlServer.initConnection, which opens a real HikariCP pool against the
      // configured JDBC URL. Close it so the pool's threads/connections don't outlive this suite
      // in the shared forked JVM. Swallowed because run() may have failed before initConnection,
      // and a throw here would mask that failure.
      try SqlServer.getInstance().close()
      catch { case _: Throwable => () }
    }
  }

  // --- initialize() ---
  // Unlike run(), initialize() opens no connection pool: Dropwizard's Bootstrap only allocates
  // in-memory scaffolding (an object mapper, a metric registry, a file-reading configuration
  // source provider), so these run everywhere, with no database and no port.

  private val seededConfigPath = "computing-unit-managing-service-spec.yml"

  // The `:-` default form is the only substitution shape this service actually ships: both
  // `${...}` in computing-unit-managing-service-config.yaml (lines 31 and 33) carry a default.
  private val seededConfig =
    "defined: ${PATH}\ndefaulted: ${TEXERA_COMPUTING_UNIT_MANAGING_SERVICE_SPEC_UNSET:-fallback}\n"

  private val unsetConfigPath = "computing-unit-managing-service-spec-unset.yml"
  private val unsetConfig = "unset: ${TEXERA_COMPUTING_UNIT_MANAGING_SERVICE_SPEC_UNSET}\n"

  /**
    * Runs initialize() over a bootstrap that already carries a recognisable in-memory
    * configuration source. Seeding it is what makes the wrapping observable: initialize() is
    * required to wrap the provider that is already installed, and a version that instead built a
    * fresh file-reading provider would fail to find these paths at all.
    */
  private def initializedBootstrap(): Bootstrap[ComputingUnitManagingServiceConfiguration] = {
    val application = new ComputingUnitManagingService
    val bootstrap = new Bootstrap[ComputingUnitManagingServiceConfiguration](application)
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

  "ComputingUnitManagingService.initialize" should "substitute environment variables into the configuration source it was handed" in {
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
    resolve(
      unsetConfigPath
    ) shouldBe "unset: ${TEXERA_COMPUTING_UNIT_MANAGING_SERVICE_SPEC_UNSET}\n"
  }

  it should "register the Scala module on Dropwizard's object mapper" in {
    val mapper = initializedBootstrap().getObjectMapper
    // The whole module, not only the Option support that `Some("x")` alone would prove: this is
    // the mapper Dropwizard hands to Jersey, so every payload the API returns goes through it.
    mapper.getRegisteredModuleIds should contain(
      "com.fasterxml.jackson.module.scala.DefaultScalaModule$"
    )

    val payload = SpecPayload("cu-1", Map("running" -> Some(Seq(1, 2)), "terminated" -> None))
    // Option unwrapped, Map and Seq emitted as JSON, and the camelCase property names left alone
    // (Dropwizard's own naming strategy is annotation-sensitive; a global one would rename them).
    val json = mapper.writeValueAsString(payload)
    json shouldBe """{"unitName":"cu-1","unitAccess":{"running":[1,2],"terminated":null}}"""
    // Reading, too: this is also the mapper Dropwizard parses the YAML configuration with.
    mapper.readValue(json, classOf[SpecPayload]) shouldBe payload
  }
}

object ComputingUnitManagingServiceRunSpec {

  /** Stands in for the payloads this service returns: camelCase names, a Map, a Seq and an Option. */
  final case class SpecPayload(unitName: String, unitAccess: Map[String, Option[Seq[Int]]])
}
