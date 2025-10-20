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

package org.apache.texera.mcp.tool.operator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.spec.McpSchema;
import org.apache.amber.operator.metadata.AllOperatorMetadata;
import org.apache.amber.operator.metadata.GroupInfo;
import org.apache.amber.operator.metadata.OperatorMetadata;
import org.apache.amber.operator.metadata.OperatorMetadataGenerator;
import org.apache.texera.mcp.tool.McpSchemaGenerator;
import org.apache.texera.mcp.tool.operator.input.CapabilityInput;
import org.apache.texera.mcp.tool.operator.input.GroupNameInput;
import org.apache.texera.mcp.tool.operator.input.OperatorTypeInput;
import org.apache.texera.mcp.tool.operator.input.SearchQueryInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Java implementation of Operator Tool operations.
 * Provides direct access to Texera operator metadata using the MCP SDK.
 */
public class OperatorToolProvider {

    private static final Logger logger = LoggerFactory.getLogger(OperatorToolProvider.class);
    private final ObjectMapper objectMapper;

    public OperatorToolProvider() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new DefaultScalaModule());
    }

    /**
     * List all available Texera operators
     */
    public AllOperatorMetadata listOperators() {
        return OperatorMetadataGenerator.allOperatorMetadata();
    }

    /**
     * Get specific operator by type
     */
    public Option<OperatorMetadata> getOperator(String operatorType) {
        List<OperatorMetadata> operators = CollectionConverters.asJava(
            OperatorMetadataGenerator.allOperatorMetadata().operators()
        );

        return operators.stream()
            .filter(op -> op.operatorType().equals(operatorType))
            .findFirst()
            .map(Option::apply)
            .orElse(Option.empty());
    }

    /**
     * Get operator JSON schema
     */
    public Option<JsonNode> getOperatorSchema(String operatorType) {
        Option<OperatorMetadata> operator = getOperator(operatorType);

        if (operator.isDefined()) {
            return Option.apply(operator.get().jsonSchema());
        }
        return Option.empty();
    }

    /**
     * Get operators by group name
     */
    public List<OperatorMetadata> getOperatorsByGroup(String groupName) {
        List<OperatorMetadata> operators = CollectionConverters.asJava(
            OperatorMetadataGenerator.allOperatorMetadata().operators()
        );

        return operators.stream()
            .filter(op -> op.additionalMetadata().operatorGroupName().equals(groupName))
            .collect(Collectors.toList());
    }

    /**
     * Search operators by query string
     */
    public List<OperatorMetadata> searchOperators(String query) {
        logger.info("MCP Tool: Searching operators with query: {}", query);
        String lowerQuery = query.toLowerCase();
        List<OperatorMetadata> operators = CollectionConverters.asJava(
            OperatorMetadataGenerator.allOperatorMetadata().operators()
        );

        return operators.stream()
            .filter(op ->
                op.additionalMetadata().userFriendlyName().toLowerCase().contains(lowerQuery) ||
                op.additionalMetadata().operatorDescription().toLowerCase().contains(lowerQuery) ||
                op.operatorType().toLowerCase().contains(lowerQuery)
            )
            .collect(Collectors.toList());
    }

    /**
     * Get all operator groups
     */
    public List<GroupInfo> getOperatorGroups() {
        logger.info("MCP Tool: Getting operator groups");
        return CollectionConverters.asJava(
            OperatorMetadataGenerator.allOperatorMetadata().groups()
        );
    }

    /**
     * Get detailed description of operator
     */
    public Option<String> describeOperator(String operatorType) {
        logger.info("MCP Tool: Describing operator: {}", operatorType);
        Option<OperatorMetadata> operatorOpt = getOperator(operatorType);

        if (operatorOpt.isDefined()) {
            OperatorMetadata op = operatorOpt.get();
            var info = op.additionalMetadata();

            StringBuilder description = new StringBuilder();
            description.append("\nOperator: ").append(info.userFriendlyName()).append("\n");
            description.append("Type: ").append(op.operatorType()).append("\n");
            description.append("Version: ").append(op.operatorVersion()).append("\n");
            description.append("Group: ").append(info.operatorGroupName()).append("\n");
            description.append("Description: ").append(info.operatorDescription()).append("\n\n");

            description.append("Input Ports: ").append(info.inputPorts().size()).append("\n");
            CollectionConverters.asJava(info.inputPorts()).forEach(port ->
                description.append("  - ").append(port.displayName())
                    .append(" (multi-links: ").append(port.allowMultiLinks()).append(")").append("\n")
            );

            description.append("\nOutput Ports: ").append(info.outputPorts().size()).append("\n");
            CollectionConverters.asJava(info.outputPorts()).forEach(port ->
                description.append("  - ").append(port.displayName()).append("\n")
            );

            description.append("\nDynamic Input Ports: ").append(info.dynamicInputPorts()).append("\n");
            description.append("Dynamic Output Ports: ").append(info.dynamicOutputPorts()).append("\n");
            description.append("Supports Reconfiguration: ").append(info.supportReconfiguration()).append("\n");
            description.append("Allow Port Customization: ").append(info.allowPortCustomization()).append("\n\n");

            description.append("Configuration Schema:\n");
            description.append(op.jsonSchema().toPrettyString()).append("\n");

            return Option.apply(description.toString());
        }

        return Option.empty();
    }

    /**
     * Get operators by capability
     */
    public List<OperatorMetadata> getOperatorsByCapability(String capability) {
        logger.info("MCP Tool: Getting operators by capability: {}", capability);
        List<OperatorMetadata> operators = CollectionConverters.asJava(
            OperatorMetadataGenerator.allOperatorMetadata().operators()
        );

        switch (capability.toLowerCase()) {
            case "reconfiguration":
                return operators.stream()
                    .filter(op -> op.additionalMetadata().supportReconfiguration())
                    .collect(Collectors.toList());

            case "dynamic_input":
                return operators.stream()
                    .filter(op -> op.additionalMetadata().dynamicInputPorts())
                    .collect(Collectors.toList());

            case "dynamic_output":
                return operators.stream()
                    .filter(op -> op.additionalMetadata().dynamicOutputPorts())
                    .collect(Collectors.toList());

            case "port_customization":
                return operators.stream()
                    .filter(op -> op.additionalMetadata().allowPortCustomization())
                    .collect(Collectors.toList());

            default:
                logger.warn("Unknown capability: {}", capability);
                return List.of();
        }
    }

    /**
     * Register all operator tools with the MCP server builder.
     * Uses annotation-based schema generation for type-safe input schemas.
     */
    public void registerAllTools(McpServer.SyncSpecification<McpServer.StreamableSyncSpecification> serverBuilder) {
        logger.info("Registering operator tools");

        // Tool 1: List all operators
        serverBuilder.toolCall(
                McpSchema.Tool.builder()
                        .name("list_operators")
                        .description("List all available Texera operators with their metadata, groups, and schemas")
                        .inputSchema(McpSchemaGenerator.generateEmptySchema())
                        .build(),
                (exchange, request) -> {
                    try {
                        var result = listOperators();
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
                        .inputSchema(McpSchemaGenerator.generateSchema(OperatorTypeInput.class))
                        .build(),
                (exchange, request) -> {
                    try {
                        String operatorType = getStringArg(request, "operatorType");
                        var result = getOperator(operatorType);

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
                        .inputSchema(McpSchemaGenerator.generateSchema(OperatorTypeInput.class))
                        .build(),
                (exchange, request) -> {
                    try {
                        String operatorType = getStringArg(request, "operatorType");
                        var result = getOperatorSchema(operatorType);

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
                        .inputSchema(McpSchemaGenerator.generateSchema(SearchQueryInput.class))
                        .build(),
                (exchange, request) -> {
                    try {
                        String query = getStringArg(request, "query");
                        var result = searchOperators(query);
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
                        .inputSchema(McpSchemaGenerator.generateSchema(GroupNameInput.class))
                        .build(),
                (exchange, request) -> {
                    try {
                        String groupName = getStringArg(request, "groupName");
                        var result = getOperatorsByGroup(groupName);
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
                        .inputSchema(McpSchemaGenerator.generateEmptySchema())
                        .build(),
                (exchange, request) -> {
                    try {
                        var result = getOperatorGroups();
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
                        .inputSchema(McpSchemaGenerator.generateSchema(OperatorTypeInput.class))
                        .build(),
                (exchange, request) -> {
                    try {
                        String operatorType = getStringArg(request, "operatorType");
                        var result = describeOperator(operatorType);

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
                        .inputSchema(McpSchemaGenerator.generateSchema(CapabilityInput.class))
                        .build(),
                (exchange, request) -> {
                    try {
                        String capability = getStringArg(request, "capability");
                        var result = getOperatorsByCapability(capability);
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
     * Helper to extract string argument from CallToolRequest
     */
    private String getStringArg(McpSchema.CallToolRequest request, String key) {
        if (request.arguments() == null) {
            return "";
        }
        Object value = request.arguments().get(key);
        return value != null ? value.toString() : "";
    }
}
