// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.texera.service

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import io.dropwizard.auth.AuthDynamicFeature
import io.dropwizard.configuration.{EnvironmentVariableSubstitutor, SubstitutingSourceProvider}
import io.dropwizard.core.Application
import io.dropwizard.core.setup.{Bootstrap, Environment}
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.auth.{
  JwtAuthFilter,
  RequestLoggingFilter,
  SessionUser,
  UnauthorizedExceptionMapper
}
import org.apache.texera.dao.SqlServer
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature
import java.nio.file.Path
import org.apache.texera.service.resource.{HealthCheckResource, NotebookMigrationResource}

class NotebookMigrationService
    extends Application[NotebookMigrationServiceConfiguration]
    with LazyLogging {
  override def initialize(bootstrap: Bootstrap[NotebookMigrationServiceConfiguration]): Unit = {
    // enable environment variable substitution in YAML config
    bootstrap.setConfigurationSourceProvider(
      new SubstitutingSourceProvider(
        bootstrap.getConfigurationSourceProvider,
        new EnvironmentVariableSubstitutor(false)
      )
    )
    // Register Scala module to Dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)

    SqlServer.initConnection(
      StorageConfig.jdbcUrl,
      StorageConfig.jdbcUsername,
      StorageConfig.jdbcPassword
    )
  }

  override def run(
      configuration: NotebookMigrationServiceConfiguration,
      environment: Environment
  ): Unit = {
    // Serve backend at /api
    environment.jersey.setUrlPattern("/api/*")

    environment.jersey.register(classOf[HealthCheckResource])

    NotebookMigrationService.registerAuthFeatures(environment)

    environment.jersey.register(classOf[NotebookMigrationResource])

    // Route request logs through SLF4J, controlled by TEXERA_SERVICE_LOG_LEVEL
    RequestLoggingFilter.register(environment.getApplicationContext)
  }
}
object NotebookMigrationService {
  // Registers JWT auth, @Auth injection, and @RolesAllowed enforcement.
  // Mirrors the other Dropwizard services' registerAuthFeatures so they don't drift apart.
  def registerAuthFeatures(environment: Environment): Unit = {
    // Register JWT authentication filter
    environment.jersey.register(new AuthDynamicFeature(classOf[JwtAuthFilter]))
    environment.jersey.register(classOf[UnauthorizedExceptionMapper])

    // Enable @Auth annotation for injecting SessionUser
    environment.jersey.register(
      new io.dropwizard.auth.AuthValueFactoryProvider.Binder(classOf[SessionUser])
    )

    // Enforce @RolesAllowed annotations on resource methods
    environment.jersey.register(classOf[RolesAllowedDynamicFeature])
  }

  def main(args: Array[String]): Unit = {
    val notebookMigrationPath = Path
      .of(sys.env.getOrElse("TEXERA_HOME", "."))
      .resolve("notebook-migration-service")
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve("notebook-migration-service-web-config.yaml")
      .toAbsolutePath
      .toString

    // Start the Dropwizard application
    new NotebookMigrationService().run("server", notebookMigrationPath)
  }
}
