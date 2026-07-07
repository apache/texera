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
import org.apache.texera.auth.{
  AuthFeatures,
  RequestLoggingFilter,
  RoleAnnotationEnforcer,
  ServiceBootstrap
}
import org.apache.texera.service.activity.UserActivityEventListener
import org.apache.texera.service.resource.{
  AccessControlResource,
  HealthCheckResource,
  LiteLLMModelsResource,
  LiteLLMProxyResource
}
import org.eclipse.jetty.server.session.SessionHandler

class AccessControlService extends Application[AccessControlServiceConfiguration] with LazyLogging {
  override def initialize(bootstrap: Bootstrap[AccessControlServiceConfiguration]): Unit = {
    ServiceBootstrap.configure(bootstrap)
    ServiceBootstrap.initDatabase()
  }

  override def run(
      configuration: AccessControlServiceConfiguration,
      environment: Environment
  ): Unit = {
    // Serve backend at /api
    environment.jersey.setUrlPattern("/api/*")

    environment.jersey.register(classOf[SessionHandler])
    environment.servlets.setSessionHandler(new SessionHandler)

    environment.jersey.register(classOf[HealthCheckResource])
    environment.jersey.register(classOf[AccessControlResource])
    environment.jersey.register(classOf[LiteLLMProxyResource])
    environment.jersey.register(classOf[LiteLLMModelsResource])

    AuthFeatures.register(environment)

    // Record USER_LAST_ACTIVE_TIME on every matched, completed request.
    // Lives only in this service because authenticated client sessions
    // contact access-control-service often enough to capture activity
    // with high recall.
    environment.jersey.register(new UserActivityEventListener())

    RoleAnnotationEnforcer.enforce(environment.jersey.getResourceConfig, "AccessControlService")

    // Route request logs through SLF4J, controlled by TEXERA_SERVICE_LOG_LEVEL
    RequestLoggingFilter.register(environment.getApplicationContext)
  }
}
object AccessControlService {
  def main(args: Array[String]): Unit =
    ServiceBootstrap.start(
      new AccessControlService,
      "access-control-service",
      "access-control-service-web-config.yaml"
    )
}
