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

import io.dropwizard.core.Application
import io.dropwizard.core.setup.{Bootstrap, Environment}
import org.apache.texera.amber.util.ObjectMapperUtils
import org.apache.texera.auth.{
  AuthFeatures,
  RequestLoggingFilter,
  RoleAnnotationEnforcer,
  ServiceBootstrap
}
import org.apache.texera.service.resource.{HealthCheckResource, WorkflowCompilationResource}

class WorkflowCompilingService extends Application[WorkflowCompilingServiceConfiguration] {
  override def initialize(bootstrap: Bootstrap[WorkflowCompilingServiceConfiguration]): Unit = {
    ServiceBootstrap.configure(bootstrap)
  }

  override def run(
      configuration: WorkflowCompilingServiceConfiguration,
      environment: Environment
  ): Unit = {
    ObjectMapperUtils.warmupObjectMapperForOperatorsSerde()

    // serve backend at /api
    environment.jersey.setUrlPattern("/api/*")

    environment.jersey.register(classOf[HealthCheckResource])

    AuthFeatures.register(environment)

    ServiceBootstrap.initDatabase()

    // register the compilation endpoint
    environment.jersey.register(classOf[WorkflowCompilationResource])

    RoleAnnotationEnforcer.enforce(
      environment.jersey.getResourceConfig,
      "WorkflowCompilingService"
    )

    // Route request logs through SLF4J, controlled by TEXERA_SERVICE_LOG_LEVEL
    RequestLoggingFilter.register(environment.getApplicationContext)
  }
}

object WorkflowCompilingService {
  def main(args: Array[String]): Unit =
    ServiceBootstrap.start(
      new WorkflowCompilingService,
      "workflow-compiling-service",
      "workflow-compiling-service-config.yaml"
    )
}
