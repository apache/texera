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
 * Texera Agent Service - Main Entry Point
 *
 * This is the main entry point for the Texera Agent Service.
 * It provides the TexeraAgent class and all necessary types for building
 * AI-powered workflow manipulation agents.
 */

// Export types
export * from "./types";

// Export workflow state
export { WorkflowState } from "./workflow/workflow-state";

// Export tools
export * from "./tools";

// Export agent
export { TexeraAgent, type TexeraAgentConfig, type AgentMessageResult } from "./agent/texera-agent";
export { BASE_SYSTEM_PROMPT, buildGeneralModeSystemPrompt } from "./agent/prompts";

// ============================================================================
// Simple Example Usage
// ============================================================================

/**
 * Example of how to use the TexeraAgent:
 *
 * ```typescript
 * import { createOpenAI } from "@ai-sdk/openai";
 * import { TexeraAgent } from "texera-agent-service";
 *
 * // Create OpenAI client
 * const openai = createOpenAI({
 *   apiKey: process.env.OPENAI_API_KEY,
 * });
 *
 * // Create agent
 * const agent = new TexeraAgent({
 *   model: openai("gpt-4-turbo"),
 *   agentId: "agent-1",
 *   agentName: "My Workflow Agent",
 * });
 *
 * // Register some operator schemas
 * agent.getMetadataStore().registerOperator(
 *   "PythonTableUDF",
 *   { properties: { code: { type: "string" } }, required: ["code"] },
 *   "Python UDF for multi-table data processing"
 * );
 *
 * // Send a message
 * const result = await agent.sendMessage("Add a Python UDF that filters rows where value > 10");
 *
 * console.log("Response:", result.response);
 * console.log("Steps:", result.steps.length);
 * console.log("Tokens used:", result.usage.totalTokens);
 *
 * // Get the workflow
 * const workflow = agent.getWorkflowState().getWorkflowContent();
 * console.log("Operators:", workflow.operators);
 * console.log("Links:", workflow.links);
 * ```
 */

// ============================================================================
// Development Server (for testing)
// ============================================================================

if (import.meta.main) {
  console.log("=".repeat(60));
  console.log("Texera Agent Service");
  console.log("=".repeat(60));
  console.log("");
  console.log("This is a library for building AI-powered workflow agents.");
  console.log("Import and use the TexeraAgent class in your application.");
  console.log("");
  console.log("Available exports:");
  console.log("  - TexeraAgent: Core agent class");
  console.log("  - WorkflowState: Workflow state management");
  console.log("  - OperatorMetadataStore: Operator schema management");
  console.log("  - Tool creators: createAddOperatorTool, createModifyOperatorTool, etc.");
  console.log("  - Types: All TypeScript types for workflow, execution, agent");
  console.log("");
  console.log("See src/index.ts for example usage.");
  console.log("=".repeat(60));
}
