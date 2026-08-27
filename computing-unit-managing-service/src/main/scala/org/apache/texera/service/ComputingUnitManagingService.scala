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
  HealthCheckResource
}
import org.apache.texera.service.resource.ComputingUnitManagingResource.TerminatedComputingUnitInfo
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.util.concurrent.TimeUnit

class ComputingUnitManagingService extends Application[ComputingUnitManagingServiceConfiguration] {
  private val logger = LoggerFactory.getLogger(classOf[ComputingUnitManagingService])

  private def initSqlServer(): Unit =
    SqlServer.initConnection(
      StorageConfig.jdbcUrl,
      StorageConfig.jdbcUsername,
      StorageConfig.jdbcPassword
    )

  private[service] def registerIdleComputingUnitCleanup(
      environment: Environment,
      kubernetesComputingUnitEnabled: Boolean = KubernetesConfig.kubernetesComputingUnitEnabled,
      idleTimeoutMinutes: Long = KubernetesConfig.computingUnitIdleTimeoutMinutes,
      idleCheckIntervalMinutes: Long = KubernetesConfig.computingUnitIdleCheckIntervalMinutes,
      terminateIdleComputingUnits: () => List[TerminatedComputingUnitInfo] = () =>
        ComputingUnitManagingResource.terminateIdleKubernetesComputingUnits(),
      logTerminatedUnits: String => Unit = message => logger.info(message),
      logCleanupFailure: Throwable => Unit = throwable =>
        logger.warn("Failed to terminate idle Kubernetes computing units", throwable),
      scheduleWithFixedDelay: Option[(Runnable, Long, Long, TimeUnit) => Unit] = None
  ): Unit = {
    if (!kubernetesComputingUnitEnabled || idleTimeoutMinutes <= 0) {
      return
    }
    // scheduleWithFixedDelay rejects a non-positive delay, which would abort service startup.
    // A misconfigured interval leaves the rest of the service usable, so log it and skip the sweep.
    if (idleCheckIntervalMinutes <= 0) {
      logger.warn(
        s"Idle Kubernetes computing unit cleanup is disabled: check interval must be positive " +
          s"but is $idleCheckIntervalMinutes minute(s)"
      )
      return
    }

    val scheduler = scheduleWithFixedDelay.getOrElse((command, initialDelay, delay, unit) =>
      environment.lifecycle
        .scheduledExecutorService("idle-computing-unit-terminator")
        .threads(1)
        .build()
        .scheduleWithFixedDelay(command, initialDelay, delay, unit)
    )
    scheduler(
      () =>
        ComputingUnitManagingService.runIdleComputingUnitCleanup(
          terminateIdleComputingUnits,
          logTerminatedUnits,
          logCleanupFailure
        ),
      idleCheckIntervalMinutes,
      idleCheckIntervalMinutes,
      TimeUnit.MINUTES
    )
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

    RoleAnnotationEnforcer.enforce(
      environment.jersey.getResourceConfig,
      "ComputingUnitManagingService"
    )

    registerIdleComputingUnitCleanup(environment)

    // Route request logs through SLF4J, controlled by TEXERA_SERVICE_LOG_LEVEL
    RequestLoggingFilter.register(environment.getApplicationContext)
  }
}

object ComputingUnitManagingService {
  private[service] def runIdleComputingUnitCleanup(
      terminateIdleComputingUnits: () => List[TerminatedComputingUnitInfo],
      logTerminatedUnits: String => Unit,
      logCleanupFailure: Throwable => Unit
  ): Unit =
    try {
      val terminated = terminateIdleComputingUnits()
      if (terminated.nonEmpty) {
        val terminatedDetails = terminated
          .map(unit =>
            s"cuid=${unit.cuid}, name=${unit.name}, uid=${unit.uid}, username=${unit.username
              .getOrElse("unknown")}, reason=${unit.reason.getLiteral}"
          )
          .mkString("; ")
        logTerminatedUnits(
          s"Terminated ${terminated.size} idle Kubernetes computing unit(s): $terminatedDetails"
        )
      }
    } catch {
      case t: Throwable =>
        logCleanupFailure(t)
    }

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
