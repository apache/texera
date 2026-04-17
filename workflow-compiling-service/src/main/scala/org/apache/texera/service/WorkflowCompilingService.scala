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
import org.apache.texera.amber.config.StorageConfig
import org.apache.texera.amber.util.ObjectMapperUtils
import org.apache.texera.dao.SqlServer
import org.apache.texera.service.resource.{HealthCheckResource, WorkflowCompilationResource}
import org.eclipse.jetty.servlet.FilterHolder

import java.nio.file.Path

class WorkflowCompilingService extends Application[WorkflowCompilingServiceConfiguration] {
  override def initialize(bootstrap: Bootstrap[WorkflowCompilingServiceConfiguration]): Unit = {
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
      configuration: WorkflowCompilingServiceConfiguration,
      environment: Environment
  ): Unit = {
    ObjectMapperUtils.warmupObjectMapperForOperatorsSerde()

    // serve backend at /api
    environment.jersey.setUrlPattern("/api/*")

    SqlServer.initConnection(
      StorageConfig.jdbcUrl,
      StorageConfig.jdbcUsername,
      StorageConfig.jdbcPassword
    )

    environment.jersey.register(classOf[HealthCheckResource])

    // register the compilation endpoint
    environment.jersey.register(classOf[WorkflowCompilationResource])

    // Route request logs through SLF4J, controlled by TEXERA_SERVICE_LOG_LEVEL
    val requestLogger = org.slf4j.LoggerFactory.getLogger("org.eclipse.jetty.server.RequestLog")
    environment.getApplicationContext.addFilter(
      new FilterHolder(new jakarta.servlet.Filter {
        override def doFilter(
            request: jakarta.servlet.ServletRequest,
            response: jakarta.servlet.ServletResponse,
            chain: jakarta.servlet.FilterChain
        ): Unit = {
          chain.doFilter(request, response)
          if (requestLogger.isInfoEnabled) {
            val req = request.asInstanceOf[jakarta.servlet.http.HttpServletRequest]
            val resp = response.asInstanceOf[jakarta.servlet.http.HttpServletResponse]
            requestLogger.info(
              s"""${req.getRemoteAddr} - "${req.getMethod} ${req.getRequestURI} ${req.getProtocol}" ${resp.getStatus}"""
            )
          }
        }
      }),
      "/*",
      java.util.EnumSet.allOf(classOf[jakarta.servlet.DispatcherType])
    )
  }
}

object WorkflowCompilingService {
  def main(args: Array[String]): Unit = {
    // set the configuration file's path
    val configFilePath = Path
      .of(sys.env.getOrElse("TEXERA_HOME", "."))
      .resolve("workflow-compiling-service")
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve("workflow-compiling-service-config.yaml")
      .toAbsolutePath
      .toString

    // Start the Dropwizard application
    new WorkflowCompilingService().run("server", configFilePath)
  }
}
