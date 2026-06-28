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
import org.apache.texera.auth.{
  AuthFeatures,
  RequestLoggingFilter,
  RoleAnnotationEnforcer,
  ServiceBootstrap
}
import org.apache.texera.service.resource.{
  ComputingUnitAccessResource,
  ComputingUnitManagingResource,
  HealthCheckResource
}

class ComputingUnitManagingService extends Application[ComputingUnitManagingServiceConfiguration] {

  override def initialize(
      bootstrap: Bootstrap[ComputingUnitManagingServiceConfiguration]
  ): Unit = {
    ServiceBootstrap.configure(bootstrap)
  }
  override def run(
      configuration: ComputingUnitManagingServiceConfiguration,
      environment: Environment
  ): Unit = {
    // Register http resources
    environment.jersey.setUrlPattern("/api/*")
    environment.jersey.register(classOf[HealthCheckResource])

    AuthFeatures.register(environment)

    ServiceBootstrap.initDatabase()

    environment.jersey().register(new ComputingUnitManagingResource)
    environment.jersey().register(new ComputingUnitAccessResource)

    RoleAnnotationEnforcer.enforce(
      environment.jersey.getResourceConfig,
      "ComputingUnitManagingService"
    )

    // Route request logs through SLF4J, controlled by TEXERA_SERVICE_LOG_LEVEL
    RequestLoggingFilter.register(environment.getApplicationContext)
  }
}

object ComputingUnitManagingService {
  def main(args: Array[String]): Unit = {
    val configFilePath = ServiceBootstrap.configFilePath(
      "computing-unit-managing-service",
      "computing-unit-managing-service-config.yaml"
    )

    new ComputingUnitManagingService().run("server", configFilePath)
  }
}
