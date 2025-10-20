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

package org.apache.texera.mcp.tool;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kjetland.jackson.jsonSchema.JsonSchemaConfig;
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator;
import io.modelcontextprotocol.spec.McpSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for generating MCP JSON schemas from annotated Java classes.
 * Uses Jackson's JSON Schema generation to create schemas from classes annotated with
 * @JsonProperty, @JsonSchemaTitle, @JsonPropertyDescription, etc.
 */
public class McpSchemaGenerator {

    private static final Logger logger = LoggerFactory.getLogger(McpSchemaGenerator.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final JsonSchemaGenerator schemaGenerator;

    static {
        // Configure JSON Schema generator with appropriate settings
        JsonSchemaConfig config = JsonSchemaConfig.vanillaJsonSchemaDraft4();
        schemaGenerator = new JsonSchemaGenerator(objectMapper, config);
    }

    /**
     * Generate MCP JSON schema from a Java class using Jackson annotations.
     *
     * @param inputClass The class annotated with Jackson schema annotations
     * @return McpSchema.JsonSchema object representing the input schema
     */
    public static McpSchema.JsonSchema generateSchema(Class<?> inputClass) {
        try {
            // Generate JSON schema from the class
            JsonNode schemaNode = schemaGenerator.generateJsonSchema(inputClass);

            // Convert JsonNode to McpSchema.JsonSchema
            String schemaJson = objectMapper.writeValueAsString(schemaNode);
            return objectMapper.readValue(schemaJson, McpSchema.JsonSchema.class);

        } catch (Exception e) {
            logger.error("Error generating schema for class {}: {}", inputClass.getName(), e.getMessage(), e);
            throw new RuntimeException("Failed to generate schema for " + inputClass.getName(), e);
        }
    }

    /**
     * Generate an empty schema for tools with no input parameters.
     *
     * @return McpSchema.JsonSchema with empty properties and no required fields
     */
    public static McpSchema.JsonSchema generateEmptySchema() {
        try {
            String schemaJson = "{\"type\":\"object\",\"properties\":{},\"required\":[]}";
            return objectMapper.readValue(schemaJson, McpSchema.JsonSchema.class);
        } catch (Exception e) {
            logger.error("Error generating empty schema: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to generate empty schema", e);
        }
    }
}
