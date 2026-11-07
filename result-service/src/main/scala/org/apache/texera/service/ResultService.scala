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

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import io.dropwizard.configuration.{EnvironmentVariableSubstitutor, SubstitutingSourceProvider}
import io.dropwizard.core.Application
import io.dropwizard.core.setup.{Bootstrap, Environment}
import org.apache.texera.auth.{AuthFeatures, RequestLoggingFilter, RoleAnnotationEnforcer}
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.dao.SqlServer
import org.apache.texera.service.resource.{HealthCheckResource, ResultResource}
import org.eclipse.jetty.server.session.SessionHandler

import java.nio.file.Path

/**
  * Serves execution results, so they can be read without a computing unit (#5363). Results live
  * in Iceberg tables behind Lakekeeper, which manages table metadata but cannot return rows, so
  * this service does the reading, the way file-service does in front of LakeFS.
  */
class ResultService extends Application[ResultServiceConfiguration] with LazyLogging {
  override def initialize(bootstrap: Bootstrap[ResultServiceConfiguration]): Unit = {
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

  override def run(configuration: ResultServiceConfiguration, environment: Environment): Unit = {
    // Bridge this service's logs to the OTel collector under its own service.name.
    org.apache.texera.observability.OtelInit.init("result-service")
    // Serve backend at /api
    environment.jersey.setUrlPattern("/api/*")

    environment.jersey.register(classOf[SessionHandler])
    environment.servlets.setSessionHandler(new SessionHandler)

    environment.jersey.register(classOf[HealthCheckResource])

    AuthFeatures.register(environment)

    environment.jersey.register(new ResultResource)

    RoleAnnotationEnforcer.enforce(environment.jersey.getResourceConfig, "ResultService")

    // Route request logs through SLF4J, controlled by TEXERA_SERVICE_LOG_LEVEL
    RequestLoggingFilter.register(environment.getApplicationContext)
  }
}

object ResultService {
  def main(args: Array[String]): Unit = {
    val configFilePath = Path
      .of(sys.env.getOrElse("TEXERA_HOME", "."))
      .resolve("result-service")
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve("result-service-web-config.yaml")
      .toAbsolutePath
      .toString

    // Start the Dropwizard application
    new ResultService().run("server", configFilePath)
  }
}
