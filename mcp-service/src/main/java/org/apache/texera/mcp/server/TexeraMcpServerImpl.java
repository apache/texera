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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
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

    private final ObjectMapper objectMapper;  // For serializing our data (operator metadata)
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
        // ObjectMapper for our data (operator metadata) - includes Scala module
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new DefaultScalaModule());

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
     * Register all operator tools using MCP SDK
     */
    private void registerOperatorTools(McpServer.SyncSpecification<McpServer.StreamableSyncSpecification> serverBuilder) {
        logger.info("Registering operator tools");

        // Tool 1: List all operators
        serverBuilder.toolCall(
                McpSchema.Tool.builder()
                        .name("list_operators")
                        .description("List all available Texera operators with their metadata, groups, and schemas")
                        .inputSchema(createEmptyInputSchema())
                        .build(),
                (exchange, request) -> {
                    try {
                        var result = operatorToolProvider.listOperators();
                        String jsonResult = objectMapper.writeValueAsString(result);
                        return new McpSchema.CallToolResult(jsonResult, false);
                    } catch (Exception e) {
                        logger.error("Error in list_operators tool: {}", e.getMessage(), e);
                        return new McpSchema.CallToolResult("Error: " + e.getMessage(), true);
                    }
                }
        );

        // Tool 2: Get specific operator
        serverBuilder.toolCall(
                McpSchema.Tool.builder()
                        .name("get_operator")
                        .description("Get detailed metadata for a specific operator by its type identifier")
                        .inputSchema(createInputSchema("operatorType", "The operator type identifier (e.g., 'CSVScanSource')"))
                        .build(),
                (exchange, request) -> {
                    try {
                        String operatorType = getStringArg(request, "operatorType");
                        var result = operatorToolProvider.getOperator(operatorType);

                        if (result.isDefined()) {
                            String jsonResult = objectMapper.writeValueAsString(result.get());
                            return new McpSchema.CallToolResult(jsonResult, false);
                        } else {
                            return new McpSchema.CallToolResult("Operator not found: " + operatorType, true);
                        }
                    } catch (Exception e) {
                        logger.error("Error in get_operator tool: {}", e.getMessage(), e);
                        return new McpSchema.CallToolResult("Error: " + e.getMessage(), true);
                    }
                }
        );

        // Tool 3: Get operator schema
        serverBuilder.toolCall(
                McpSchema.Tool.builder()
                        .name("get_operator_schema")
                        .description("Get the JSON schema for a specific operator's configuration")
                        .inputSchema(createInputSchema("operatorType", "The operator type identifier"))
                        .build(),
                (exchange, request) -> {
                    try {
                        String operatorType = getStringArg(request, "operatorType");
                        var result = operatorToolProvider.getOperatorSchema(operatorType);

                        if (result.isDefined()) {
                            String jsonResult = objectMapper.writeValueAsString(result.get());
                            return new McpSchema.CallToolResult(jsonResult, false);
                        } else {
                            return new McpSchema.CallToolResult("Operator schema not found: " + operatorType, true);
                        }
                    } catch (Exception e) {
                        logger.error("Error in get_operator_schema tool: {}", e.getMessage(), e);
                        return new McpSchema.CallToolResult("Error: " + e.getMessage(), true);
                    }
                }
        );

        // Tool 4: Search operators
        serverBuilder.toolCall(
                McpSchema.Tool.builder()
                        .name("search_operators")
                        .description("Search operators by name, description, or type using a query string")
                        .inputSchema(createInputSchema("query", "Search query string"))
                        .build(),
                (exchange, request) -> {
                    try {
                        String query = getStringArg(request, "query");
                        var result = operatorToolProvider.searchOperators(query);
                        String jsonResult = objectMapper.writeValueAsString(result);
                        return new McpSchema.CallToolResult(jsonResult, false);
                    } catch (Exception e) {
                        logger.error("Error in search_operators tool: {}", e.getMessage(), e);
                        return new McpSchema.CallToolResult("Error: " + e.getMessage(), true);
                    }
                }
        );

        // Tool 5: Get operators by group
        serverBuilder.toolCall(
                McpSchema.Tool.builder()
                        .name("get_operators_by_group")
                        .description("Get all operators in a specific group")
                        .inputSchema(createInputSchema("groupName", "The operator group name (e.g., 'Data Input', 'Machine Learning')"))
                        .build(),
                (exchange, request) -> {
                    try {
                        String groupName = getStringArg(request, "groupName");
                        var result = operatorToolProvider.getOperatorsByGroup(groupName);
                        String jsonResult = objectMapper.writeValueAsString(result);
                        return new McpSchema.CallToolResult(jsonResult, false);
                    } catch (Exception e) {
                        logger.error("Error in get_operators_by_group tool: {}", e.getMessage(), e);
                        return new McpSchema.CallToolResult("Error: " + e.getMessage(), true);
                    }
                }
        );

        // Tool 6: Get operator groups
        serverBuilder.toolCall(
                McpSchema.Tool.builder()
                        .name("get_operator_groups")
                        .description("Get all operator groups in their hierarchical structure")
                        .inputSchema(createEmptyInputSchema())
                        .build(),
                (exchange, request) -> {
                    try {
                        var result = operatorToolProvider.getOperatorGroups();
                        String jsonResult = objectMapper.writeValueAsString(result);
                        return new McpSchema.CallToolResult(jsonResult, false);
                    } catch (Exception e) {
                        logger.error("Error in get_operator_groups tool: {}", e.getMessage(), e);
                        return new McpSchema.CallToolResult("Error: " + e.getMessage(), true);
                    }
                }
        );

        // Tool 7: Describe operator
        serverBuilder.toolCall(
                McpSchema.Tool.builder()
                        .name("describe_operator")
                        .description("Get a detailed human-readable description of an operator including ports, capabilities, and schema")
                        .inputSchema(createInputSchema("operatorType", "The operator type identifier"))
                        .build(),
                (exchange, request) -> {
                    try {
                        String operatorType = getStringArg(request, "operatorType");
                        var result = operatorToolProvider.describeOperator(operatorType);

                        if (result.isDefined()) {
                            return new McpSchema.CallToolResult(result.get(), false);
                        } else {
                            return new McpSchema.CallToolResult("Operator not found: " + operatorType, true);
                        }
                    } catch (Exception e) {
                        logger.error("Error in describe_operator tool: {}", e.getMessage(), e);
                        return new McpSchema.CallToolResult("Error: " + e.getMessage(), true);
                    }
                }
        );

        // Tool 8: Get operators by capability
        serverBuilder.toolCall(
                McpSchema.Tool.builder()
                        .name("get_operators_by_capability")
                        .description("Get operators that support specific capabilities")
                        .inputSchema(createInputSchema("capability", "The capability to filter by (reconfiguration, dynamic_input, dynamic_output, port_customization)"))
                        .build(),
                (exchange, request) -> {
                    try {
                        String capability = getStringArg(request, "capability");
                        var result = operatorToolProvider.getOperatorsByCapability(capability);
                        String jsonResult = objectMapper.writeValueAsString(result);
                        return new McpSchema.CallToolResult(jsonResult, false);
                    } catch (Exception e) {
                        logger.error("Error in get_operators_by_capability tool: {}", e.getMessage(), e);
                        return new McpSchema.CallToolResult("Error: " + e.getMessage(), true);
                    }
                }
        );

        logger.info("Registered 8 operator tools");
    }

    /**
     * Helper to create empty input schema for tools with no parameters
     */
    private McpSchema.JsonSchema createEmptyInputSchema() {
        try {
            // Create a plain ObjectMapper for schema parsing (no custom modules needed)
            ObjectMapper schemaMapper = new ObjectMapper();
            String schemaJson = "{\"type\":\"object\",\"properties\":{},\"required\":[]}";
            return schemaMapper.readValue(schemaJson, McpSchema.JsonSchema.class);
        } catch (Exception e) {
            logger.error("Error creating empty input schema", e);
            return null;
        }
    }

    /**
     * Helper to create input schema for a single string parameter
     */
    private McpSchema.JsonSchema createInputSchema(String paramName, String description) {
        try {
            // Create a plain ObjectMapper for schema parsing (no custom modules needed)
            ObjectMapper schemaMapper = new ObjectMapper();
            String schemaJson = String.format(
                "{\"type\":\"object\",\"properties\":{\"%s\":{\"type\":\"string\",\"description\":\"%s\"}},\"required\":[\"%s\"]}",
                paramName, description, paramName
            );
            return schemaMapper.readValue(schemaJson, McpSchema.JsonSchema.class);
        } catch (Exception e) {
            logger.error("Error creating input schema", e);
            return null;
        }
    }

    /**
     * Helper to extract string argument from CallToolRequest
     */
    private String getStringArg(McpSchema.CallToolRequest request, String key) {
        if (request.arguments() == null) {
            return "";
        }
        Object value = request.arguments().get(key);
        return value != null ? value.toString() : "";
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
