/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.texera.config

import com.typesafe.config.{Config, ConfigFactory}
import scala.jdk.CollectionConverters._

/**
  * Configuration for the MCP (Model Context Protocol) Service.
  * Settings are loaded from mcp.conf.
  */
object McpConfig {

  private val conf: Config = ConfigFactory.parseResources("mcp.conf").resolve()

  // Server settings
  val serverPort: Int = conf.getInt("mcp.server.port")
  val serverName: String = conf.getString("mcp.server.name")
  val serverVersion: String = conf.getString("mcp.server.version")

  // Transport protocol
  val transport: String = conf.getString("mcp.transport")

  // Authentication and database
  val authEnabled: Boolean = conf.getBoolean("mcp.auth.enabled")
  val databaseEnabled: Boolean = conf.getBoolean("mcp.database.enabled")

  // MCP Capabilities
  val toolsEnabled: Boolean = conf.getBoolean("mcp.capabilities.tools")
  val resourcesEnabled: Boolean = conf.getBoolean("mcp.capabilities.resources")
  val promptsEnabled: Boolean = conf.getBoolean("mcp.capabilities.prompts")
  val samplingEnabled: Boolean = conf.getBoolean("mcp.capabilities.sampling")
  val loggingEnabled: Boolean = conf.getBoolean("mcp.capabilities.logging")

  // Performance settings
  val maxConcurrentRequests: Int = conf.getInt("mcp.performance.max-concurrent-requests")
  val requestTimeoutMs: Long = conf.getLong("mcp.performance.request-timeout-ms")

  // Enabled features
  val enabledTools: List[String] = conf.getStringList("mcp.enabled-tools").asScala.toList
  val enabledResources: List[String] = conf.getStringList("mcp.enabled-resources").asScala.toList
  val enabledPrompts: List[String] = conf.getStringList("mcp.enabled-prompts").asScala.toList
}
