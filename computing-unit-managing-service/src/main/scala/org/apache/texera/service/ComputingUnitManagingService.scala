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
import io.dropwizard.configuration.{EnvironmentVariableSubstitutor, SubstitutingSourceProvider}
import io.dropwizard.core.Application
import io.dropwizard.core.setup.{Bootstrap, Environment}
import org.apache.texera.common.config.{KubernetesConfig, StorageConfig}
import org.apache.texera.auth.{AuthFeatures, RequestLoggingFilter, RoleAnnotationEnforcer}
import org.apache.texera.dao.SqlServer
import org.apache.texera.service.resource.{
  AdminComputingUnitResource,
  ComputingUnitAccessResource,
  ComputingUnitManagingResource,
  CuratedImageResource,
  HealthCheckResource
}
import org.apache.texera.service.util.IdleComputingUnitCleanupJob
import org.slf4j.LoggerFactory
import java.nio.file.Path

class ComputingUnitManagingService extends Application[ComputingUnitManagingServiceConfiguration] {
  private val logger = LoggerFactory.getLogger(classOf[ComputingUnitManagingService])

  private def initSqlServer(): Unit =
    SqlServer.initConnection(
      StorageConfig.jdbcUrl,
      StorageConfig.jdbcUsername,
      StorageConfig.jdbcPassword
    )

  /**
    * Registers the periodic idle computing unit cleanup job on the application lifecycle when
    * enabled. Extracted from `run` (and kept free of any global config reads) so the conditional
    * wiring can be unit-tested with a standalone `Environment`.
    */
  private[service] def registerIdleComputingUnitCleanup(
      environment: Environment,
      enabled: Boolean,
      idleTimeoutMinutes: Long,
      intervalMinutes: Long
  ): Unit =
    if (enabled) {
      // The job's scheduler rejects a non-positive delay, and a misconfigured sweep should leave
      // the rest of the service usable, so log it and skip rather than abort startup.
      if (idleTimeoutMinutes <= 0 || intervalMinutes <= 0) {
        logger.warn(
          s"Idle Kubernetes computing unit cleanup is disabled: timeout and check interval must " +
            s"both be positive but are $idleTimeoutMinutes and $intervalMinutes minute(s)"
        )
      } else {
        environment
          .lifecycle()
          .manage(new IdleComputingUnitCleanupJob(idleTimeoutMinutes, intervalMinutes))
      }
    }

  override def initialize(
      bootstrap: Bootstrap[ComputingUnitManagingServiceConfiguration]
  ): Unit = {
    // enable environment variable substitution in YAML config
    bootstrap.setConfigurationSourceProvider(
      new SubstitutingSourceProvider(
        bootstrap.getConfigurationSourceProvider,
        new EnvironmentVariableSubstitutor(false)
      )
    )
    // register scala module to dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)
  }
  override def run(
      configuration: ComputingUnitManagingServiceConfiguration,
      environment: Environment
  ): Unit = {
    // Register http resources
    environment.jersey.setUrlPattern("/api/*")
    environment.jersey.register(classOf[HealthCheckResource])

    AuthFeatures.register(environment)

    initSqlServer()

    environment.jersey().register(new ComputingUnitManagingResource)
    environment.jersey().register(new ComputingUnitAccessResource)
    environment.jersey().register(new AdminComputingUnitResource)
    environment.jersey().register(new CuratedImageResource)

    RoleAnnotationEnforcer.enforce(
      environment.jersey.getResourceConfig,
      "ComputingUnitManagingService"
    )

    // Periodically terminate Kubernetes computing units their owners have stopped using
    registerIdleComputingUnitCleanup(
      environment,
      KubernetesConfig.kubernetesComputingUnitEnabled &&
        KubernetesConfig.computingUnitIdleCleanupEnabled,
      KubernetesConfig.computingUnitIdleTimeoutMinutes,
      KubernetesConfig.computingUnitIdleCheckIntervalMinutes
    )

    // Route request logs through SLF4J, controlled by TEXERA_SERVICE_LOG_LEVEL
    RequestLoggingFilter.register(environment.getApplicationContext)
  }
}

object ComputingUnitManagingService {
  def main(args: Array[String]): Unit = {
    val configFilePath = Path
      .of(sys.env.getOrElse("TEXERA_HOME", "."))
      .resolve("computing-unit-managing-service")
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve("computing-unit-managing-service-config.yaml")
      .toAbsolutePath
      .toString

    new ComputingUnitManagingService().run("server", configFilePath)
  }
}
