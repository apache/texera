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

package org.apache.texera.mcp.tools;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.amber.operator.metadata.AllOperatorMetadata;
import org.apache.amber.operator.metadata.GroupInfo;
import org.apache.amber.operator.metadata.OperatorMetadata;
import org.apache.amber.operator.metadata.OperatorMetadataGenerator;
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
}
