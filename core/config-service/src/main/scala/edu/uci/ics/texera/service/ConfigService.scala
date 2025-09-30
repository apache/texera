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
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.config.StorageConfig
import edu.uci.ics.amber.util.PathUtils.configServicePath
import edu.uci.ics.texera.auth.{JwtAuthFilter, SessionUser}
import edu.uci.ics.texera.config.DefaultsConfig
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.service.resource.{ConfigResource, HealthCheckResource}
import io.dropwizard.auth.AuthDynamicFeature
import org.eclipse.jetty.server.session.SessionHandler
import org.jooq.impl.DSL

class ConfigService extends Application[ConfigServiceConfiguration] with LazyLogging {
  override def initialize(bootstrap: Bootstrap[ConfigServiceConfiguration]): Unit = {
    // Register Scala module to Dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)

    SqlServer.initConnection(
      StorageConfig.jdbcUrl,
      StorageConfig.jdbcUsername,
      StorageConfig.jdbcPassword
    )
  }

  override def run(configuration: ConfigServiceConfiguration, environment: Environment): Unit = {
    // Serve backend at /api
    environment.jersey.setUrlPattern("/api/*")

    environment.jersey.register(classOf[SessionHandler])
    environment.servlets.setSessionHandler(new SessionHandler)

    environment.jersey.register(classOf[HealthCheckResource])

    // Register JWT authentication filter
    environment.jersey.register(new AuthDynamicFeature(classOf[JwtAuthFilter]))

    // Enable @Auth annotation for injecting SessionUser
    environment.jersey.register(
      new io.dropwizard.auth.AuthValueFactoryProvider.Binder(classOf[SessionUser])
    )

    environment.jersey.register(new ConfigResource)

    // Preload default.conf into site_setting tables
    try {
      val ctx = SqlServer.getInstance().createDSLContext()

      SqlServer.withTransaction(ctx) { tx =>
        if (DefaultsConfig.reinit) {
          tx.deleteFrom(DSL.table("site_settings")).execute()
        }

        DefaultsConfig.allDefaults.foreach {
          case (key, value) =>
            tx
              .insertInto(DSL.table("site_settings"))
              .columns(
                DSL.field("key"),
                DSL.field("value"),
                DSL.field("updated_by"),
                DSL.field("updated_at")
              )
              .values(key, value, "texera", DSL.currentTimestamp())
              .onDuplicateKeyIgnore()
              .execute()
        }
      }
    } catch {
      case ex: Exception =>
        logger.error("Failed to preload default settings", ex)
        throw ex
    }
  }
}

object ConfigService {
  def main(args: Array[String]): Unit = {
    val configFilePath = configServicePath
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve("config-service-web-config.yaml")
      .toAbsolutePath
      .toString

    // Start the Dropwizard application
    new ConfigService().run("server", configFilePath)
  }
}
