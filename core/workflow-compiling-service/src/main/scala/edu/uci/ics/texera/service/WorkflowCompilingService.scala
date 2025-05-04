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

package edu.uci.ics.texera.service

import io.dropwizard.core.Application
import io.dropwizard.core.setup.{Bootstrap, Environment}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.util.PathUtils.workflowCompilingServicePath
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.service.resource.WorkflowCompilationResource
import edu.uci.ics.texera.service.resource.HealthCheckResource

class WorkflowCompilingService extends Application[WorkflowCompilingServiceConfiguration] {
  override def initialize(bootstrap: Bootstrap[WorkflowCompilingServiceConfiguration]): Unit = {
    // register scala module to dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)
  }

  override def run(
      configuration: WorkflowCompilingServiceConfiguration,
      environment: Environment
  ): Unit = {
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
  }
}

object WorkflowCompilingService {
  def main(args: Array[String]): Unit = {
    // set the configuration file's path
    val configFilePath = workflowCompilingServicePath
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
