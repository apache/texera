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

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.util.PathUtils.workflowComputingUnitManagingServicePath
import edu.uci.ics.texera.auth.{JwtAuthFilter, SessionUser}
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.service.resource.{ComputingUnitManagingResource, HealthCheckResource}
import io.dropwizard.auth.AuthDynamicFeature
import io.dropwizard.core.setup.{Bootstrap, Environment}
import io.dropwizard.core.Application

class ComputingUnitManagingService extends Application[ComputingUnitManagingServiceConfiguration] {

  override def initialize(
      bootstrap: Bootstrap[ComputingUnitManagingServiceConfiguration]
  ): Unit = {
    // register scala module to dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)
  }
  override def run(
      configuration: ComputingUnitManagingServiceConfiguration,
      environment: Environment
  ): Unit = {
    SqlServer.initConnection(
      StorageConfig.jdbcUrl,
      StorageConfig.jdbcUsername,
      StorageConfig.jdbcPassword
    )
    // Register http resources
    environment.jersey.setUrlPattern("/api/*")
    environment.jersey.register(classOf[HealthCheckResource])

    // Register JWT authentication filter
    environment.jersey.register(new AuthDynamicFeature(classOf[JwtAuthFilter]))

    // Enable @Auth annotation for injecting SessionUser
    environment.jersey.register(
      new io.dropwizard.auth.AuthValueFactoryProvider.Binder(classOf[SessionUser])
    )

    environment.jersey().register(new ComputingUnitManagingResource)
  }
}

object ComputingUnitManagingService {
  def main(args: Array[String]): Unit = {
    val configFilePath = workflowComputingUnitManagingServicePath
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve("computing-unit-managing-service-config.yaml")
      .toAbsolutePath
      .toString

    new ComputingUnitManagingService().run("server", configFilePath)
  }
}
