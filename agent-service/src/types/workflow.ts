/**
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

/**
 * Core workflow types for Texera Agent Service.
 * These types are derived from the frontend definitions but simplified for server-side use.
 */

// ============================================================================
// Port and Link Types
// ============================================================================

/**
 * Reference to an operator's port (input or output)
 */
export interface LogicalPort {
  readonly operatorID: string;
  readonly portID: string;
}

/**
 * Port identity for backend API
 */
export interface PortIdentity {
  readonly id: number;
  readonly internal: boolean;
}

/**
 * Data partitioning strategies for operator ports
 */
export type PartitionInfo =
  | { readonly type: "hash"; readonly hashAttributeNames: string[] }
  | {
      readonly type: "range";
      readonly rangeAttributeNames: string[];
      readonly rangeMin: number;
      readonly rangeMax: number;
    }
  | { readonly type: "single" }
  | { readonly type: "broadcast" }
  | { readonly type: "none" };

/**
 * Port description with metadata
 */
export interface PortDescription {
  readonly portID: string;
  readonly displayName?: string;
  readonly allowMultiInputs?: boolean;
  readonly isDynamicPort?: boolean;
  readonly partitionRequirement?: PartitionInfo;
  readonly dependencies?: { id: number; internal: boolean }[];
}

// ============================================================================
// Operator Types
// ============================================================================

/**
 * Complete operator definition for workflow graph
 */
export interface OperatorPredicate {
  readonly operatorID: string;
  readonly operatorType: string;
  readonly operatorVersion: string;
  readonly operatorProperties: Record<string, any>;
  readonly inputPorts: PortDescription[];
  readonly outputPorts: PortDescription[];
  readonly dynamicInputPorts?: boolean;
  readonly dynamicOutputPorts?: boolean;
  readonly showAdvanced: boolean;
  readonly isDisabled?: boolean;
  readonly viewResult?: boolean;
  readonly markedForReuse?: boolean;
  readonly customDisplayName?: string;
}

/**
 * Operator for backend LogicalPlan (simplified)
 */
export interface LogicalOperator {
  readonly operatorID: string;
  readonly operatorType: string;
  readonly [key: string]: any;
}

// ============================================================================
// Link Types
// ============================================================================

/**
 * Connection between operators in workflow graph (frontend format)
 */
export interface OperatorLink {
  readonly linkID: string;
  readonly source: LogicalPort;
  readonly target: LogicalPort;
}

/**
 * Link for backend LogicalPlan
 */
export interface LogicalLink {
  readonly fromOpId: string;
  readonly fromPortId: PortIdentity;
  readonly toOpId: string;
  readonly toPortId: PortIdentity;
}

// ============================================================================
// Workflow Plan Types
// ============================================================================

/**
 * Logical plan for backend execution
 */
export interface LogicalPlan {
  readonly operators: LogicalOperator[];
  readonly links: LogicalLink[];
  readonly opsToViewResult?: string[];
  readonly opsToReuseResult?: string[];
}

/**
 * 2D point for operator positions
 */
export interface Point {
  readonly x: number;
  readonly y: number;
}

/**
 * Comment box on the workflow canvas
 */
export interface CommentBox {
  readonly commentBoxID: string;
  readonly comments: string;
  readonly x: number;
  readonly y: number;
  readonly width: number;
  readonly height: number;
}

/**
 * Workflow settings
 */
export interface WorkflowSettings {
  readonly dataTransferBatchSize: number;
}

/**
 * Complete workflow content (operators + links + positions in frontend format)
 * This must match the frontend's WorkflowContent interface for compatibility.
 */
export interface WorkflowContent {
  readonly operators: OperatorPredicate[];
  readonly operatorPositions: { [key: string]: Point };
  readonly links: OperatorLink[];
  readonly commentBoxes: CommentBox[];
  readonly settings: WorkflowSettings;
}

// ============================================================================
// Schema Types
// ============================================================================

/**
 * Attribute type for schema definitions
 */
export type AttributeType = "string" | "integer" | "double" | "boolean" | "long" | "timestamp" | "binary";

/**
 * Single attribute in a schema
 */
export interface SchemaAttribute {
  readonly attributeName: string;
  readonly attributeType: AttributeType;
}

/**
 * Schema for a port (array of attributes)
 */
export type PortSchema = readonly SchemaAttribute[];

/**
 * Map of port identity to schema
 */
export type OperatorPortSchemaMap = Record<string, PortSchema | undefined>;

// ============================================================================
// Operator Detail (for tool results)
// ============================================================================

/**
 * Operator detail information including properties and ports
 */
export interface OperatorDetail {
  operatorId: string;
  operatorType: string;
  customDisplayName?: string;
  operatorProperties: Record<string, any>;
  inputPorts: PortDescription[];
  outputPorts: PortDescription[];
}

// ============================================================================
// Validation Types (matching frontend ValidationWorkflowService)
// ============================================================================

/**
 * Validation error with messages
 */
export type ValidationError = {
  isValid: false;
  messages: Record<string, string>;
};

/**
 * Validation result (either valid or error)
 */
export type Validation = { isValid: true } | ValidationError;
