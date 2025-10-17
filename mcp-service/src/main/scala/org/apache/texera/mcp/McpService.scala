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

package org.apache.texera.mcp

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.dropwizard.auth.{AuthDynamicFeature, AuthValueFactoryProvider}
import io.dropwizard.core.Application
import io.dropwizard.core.Configuration
import io.dropwizard.core.setup.{Bootstrap, Environment}
import org.apache.amber.config.StorageConfig
import org.apache.texera.auth.{JwtAuthFilter, SessionUser}
import org.apache.texera.config.McpConfig
import org.apache.texera.dao.SqlServer
import org.apache.texera.mcp.resource.HealthCheckResource
import org.apache.texera.mcp.server.TexeraMcpServerImpl

import java.nio.file.Path

/**
  * Main MCP Service application for Texera.
  * Exposes Texera metadata and capabilities through Model Context Protocol.
  * Uses the official MCP SDK implementation in Java.
  * Configuration is loaded from common/config/src/main/resources/mcp.conf via McpConfig.
  */
class McpService extends Application[Configuration] {

  override def initialize(bootstrap: Bootstrap[Configuration]): Unit = {
    // Register Scala module for Jackson
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)
  }

  override def run(config: Configuration, env: Environment): Unit = {
    // Set API URL pattern
    env.jersey.setUrlPattern("/api/*")

    // Initialize database connection if needed
    if (McpConfig.databaseEnabled) {
      SqlServer.initConnection(
        StorageConfig.jdbcUrl,
        StorageConfig.jdbcUsername,
        StorageConfig.jdbcPassword
      )
    }

    // Initialize MCP server using Java SDK implementation
    val mcpServer = new TexeraMcpServerImpl()
    mcpServer.start()

    // Register the MCP SDK servlet at /api/mcp and /api/mcp/*
    // The servlet handles all MCP protocol requests (GET for SSE, POST for messages)
    // Both paths are needed: /api/mcp for base endpoint, /api/mcp/* for any subpaths
    val mcpServletRegistration = env
      .servlets()
      .addServlet("mcp-protocol", mcpServer.getServlet)
    mcpServletRegistration.addMapping("/api/mcp")
    mcpServletRegistration.addMapping("/api/mcp/*")

    // Register health check resource (for service monitoring only)
    env.jersey.register(new HealthCheckResource)

    // Add authentication if enabled
    if (McpConfig.authEnabled) {
      env.jersey.register(new AuthDynamicFeature(classOf[JwtAuthFilter]))
      env.jersey.register(
        new AuthValueFactoryProvider.Binder(classOf[SessionUser])
      )
    }

    // Add shutdown hook for MCP server
    env.lifecycle.addServerLifecycleListener(server => {
      Runtime.getRuntime.addShutdownHook(new Thread(() => {
        mcpServer.stop()
      }))
    })
  }
}

object McpService {
  def main(args: Array[String]): Unit = {
    val configPath = Path
      .of(sys.env.getOrElse("TEXERA_HOME", "."))
      .resolve("mcp-service")
      .resolve("src/main/resources")
      .resolve("mcp-service-config.yaml")
      .toAbsolutePath
      .toString

    new McpService().run("server", configPath)
  }
}
