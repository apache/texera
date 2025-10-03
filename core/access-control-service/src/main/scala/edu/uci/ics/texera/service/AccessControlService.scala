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

package edu.uci.ics.texera.service

import io.dropwizard.core.Application
import io.dropwizard.core.setup.{Bootstrap, Environment}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.config.StorageConfig
import edu.uci.ics.amber.util.PathUtils.{configServicePath, accessControlServicePath}
import edu.uci.ics.texera.auth.{JwtAuthFilter, SessionUser}
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.service.resource.{HealthCheckResource, AccessControlResource}
import io.dropwizard.auth.AuthDynamicFeature
import org.eclipse.jetty.server.session.SessionHandler
import org.jooq.impl.DSL


class AccessControlService extends Application[AccessControlServiceConfiguration] with LazyLogging {
  override def initialize(bootstrap: Bootstrap[AccessControlServiceConfiguration]): Unit = {
    // Register Scala module to Dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)

    SqlServer.initConnection(
      StorageConfig.jdbcUrl,
      StorageConfig.jdbcUsername,
      StorageConfig.jdbcPassword
    )
  }

  override def run(configuration: AccessControlServiceConfiguration, environment: Environment): Unit = {
    // Serve backend at /api
    environment.jersey.setUrlPattern("/api/*")

    environment.jersey.register(classOf[SessionHandler])
    environment.servlets.setSessionHandler(new SessionHandler)

    environment.jersey.register(classOf[HealthCheckResource])
    environment.jersey.register(classOf[AccessControlResource])

    // Register JWT authentication filter
    environment.jersey.register(new AuthDynamicFeature(classOf[JwtAuthFilter]))

    // Enable @Auth annotation for injecting SessionUser
    environment.jersey.register(
      new io.dropwizard.auth.AuthValueFactoryProvider.Binder(classOf[SessionUser])
    )
  }
}
object AccessControlService {
  def main(args: Array[String]): Unit = {
    val accessControlPath = accessControlServicePath
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve("access-control-service-web-config.yaml")
      .toAbsolutePath
      .toString

    // Start the Dropwizard application
    new AccessControlService().run("server", accessControlPath)
  }
}
