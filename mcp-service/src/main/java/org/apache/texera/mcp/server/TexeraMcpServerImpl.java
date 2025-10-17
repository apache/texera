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

package org.apache.texera.mcp.server;

import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.HttpServletStreamableServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.http.HttpServlet;
import lombok.Getter;
import org.apache.texera.config.McpConfig;
import org.apache.texera.mcp.tools.OperatorToolProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * MCP Server implementation using the official Model Context Protocol SDK.
 * Provides Texera operator metadata and capabilities to AI agents via HTTP Streamable transport.
 * Configuration is loaded from McpConfig (common/config/src/main/resources/mcp.conf).
 */
public class TexeraMcpServerImpl {
    private static final Logger logger = LoggerFactory.getLogger(TexeraMcpServerImpl.class);

    private final OperatorToolProvider operatorToolProvider;
    private McpSyncServer mcpServer;
    private HttpServletStreamableServerTransportProvider transportProvider;

    /**
     * -- GETTER --
     *  Check if server is running
     */
    @Getter
    private boolean running = false;

    public TexeraMcpServerImpl() {
        this.operatorToolProvider = new OperatorToolProvider();
    }

    /**
     * Start the MCP server with configured capabilities using SDK
     */
    public void start() {
        logger.info("Starting Texera MCP Server: {} v{}", McpConfig.serverName(), McpConfig.serverVersion());

        try {
            // Use the SDK's default JSON mapper supplier - this ensures proper MCP protocol deserialization
            // The SDK provides JacksonMcpJsonMapperSupplier which creates a properly configured mapper
            logger.info("Using SDK's default JacksonMcpJsonMapperSupplier for MCP protocol");

            // Build HTTP Streamable transport provider using SDK builder
            // Don't specify jsonMapper - let the SDK use its default
            transportProvider = HttpServletStreamableServerTransportProvider.builder()
                    .build();

            // Build server capabilities based on configuration
            var capabilitiesBuilder = McpSchema.ServerCapabilities.builder();

            if (McpConfig.toolsEnabled()) {
                capabilitiesBuilder.tools(true);
            }
            if (McpConfig.loggingEnabled()) {
                capabilitiesBuilder.logging();
            }
            if (McpConfig.resourcesEnabled()) {
                capabilitiesBuilder.resources(true, true);
            }
            if (McpConfig.promptsEnabled()) {
                capabilitiesBuilder.prompts(true);
            }

            var capabilities = capabilitiesBuilder.build();

            // Build the MCP server with transport and capabilities using SDK
            var serverBuilder = McpServer
                    .sync(transportProvider)
                    .serverInfo(McpConfig.serverName(), McpConfig.serverVersion())
                    .capabilities(capabilities);

            // Register operator tools if enabled
            if (McpConfig.toolsEnabled() && McpConfig.enabledTools().contains("operators")) {
                registerOperatorTools(serverBuilder);
            }

            mcpServer = serverBuilder.build();

            running = true;
            logger.info("MCP Server started successfully with 8 operator tools on HTTP Streamable transport");
            logger.info("Server listening for MCP protocol messages - ready to handle requests");

        } catch (Exception e) {
            logger.error("FATAL: Failed to start MCP Server - {}", e.getMessage(), e);
            throw new RuntimeException("Failed to start MCP Server", e);
        }
    }

    /**
     * Get the HTTP servlet for Dropwizard integration.
     * The transport provider IS a servlet that handles MCP protocol requests.
     */
    public HttpServlet getServlet() {
        if (transportProvider == null) {
            throw new IllegalStateException("Server not started - call start() first");
        }
        return transportProvider;  // Transport provider IS the servlet
    }

    /**
     * Stop the MCP server
     */
    public void stop() {
        logger.info("Stopping Texera MCP Server");
        running = false;
        if (mcpServer != null) {
            try {
                mcpServer.close();
            } catch (Exception e) {
                logger.error("Error closing MCP server", e);
            }
        }
        if (transportProvider != null) {
            transportProvider.destroy();
        }
        logger.info("MCP Server stopped");
    }

    /**
     * Register all operator tools using MCP SDK.
     * Delegates to OperatorToolProvider for tool registration.
     */
    private void registerOperatorTools(McpServer.SyncSpecification<McpServer.StreamableSyncSpecification> serverBuilder) {
        operatorToolProvider.registerAllTools(serverBuilder);
    }

    /**
     * Get server information
     */
    public Map<String, String> getServerInfo() {
        Map<String, String> info = new HashMap<>();
        info.put("name", McpConfig.serverName());
        info.put("version", McpConfig.serverVersion());
        info.put("status", running ? "running" : "stopped");
        return info;
    }

    /**
     * Get server capabilities
     */
    public Map<String, Object> getCapabilities() {
        Map<String, Object> caps = new HashMap<>();
        caps.put("tools", McpConfig.toolsEnabled());
        caps.put("resources", McpConfig.resourcesEnabled());
        caps.put("prompts", McpConfig.promptsEnabled());
        caps.put("sampling", McpConfig.samplingEnabled());
        caps.put("logging", McpConfig.loggingEnabled());
        return caps;
    }
}
