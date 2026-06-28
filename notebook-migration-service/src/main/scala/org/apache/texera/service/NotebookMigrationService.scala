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

import com.typesafe.scalalogging.LazyLogging
import io.dropwizard.core.Application
import io.dropwizard.core.setup.{Bootstrap, Environment}
import org.apache.texera.auth.{AuthFeatures, RequestLoggingFilter, ServiceBootstrap}
import org.apache.texera.service.resource.{HealthCheckResource, NotebookMigrationResource}

class NotebookMigrationService
    extends Application[NotebookMigrationServiceConfiguration]
    with LazyLogging {
  override def initialize(bootstrap: Bootstrap[NotebookMigrationServiceConfiguration]): Unit = {
    ServiceBootstrap.configure(bootstrap)
    ServiceBootstrap.initDatabase()
  }

  override def run(
      configuration: NotebookMigrationServiceConfiguration,
      environment: Environment
  ): Unit = {
    // Serve backend at /api
    environment.jersey.setUrlPattern("/api/*")

    environment.jersey.register(classOf[HealthCheckResource])

    AuthFeatures.register(environment)

    environment.jersey.register(classOf[NotebookMigrationResource])

    // Route request logs through SLF4J, controlled by TEXERA_SERVICE_LOG_LEVEL
    RequestLoggingFilter.register(environment.getApplicationContext)
  }
}
object NotebookMigrationService {
  def main(args: Array[String]): Unit = {
    val notebookMigrationPath = ServiceBootstrap.configFilePath(
      "notebook-migration-service",
      "notebook-migration-service-web-config.yaml"
    )

    // Start the Dropwizard application
    new NotebookMigrationService().run("server", notebookMigrationPath)
  }
}
