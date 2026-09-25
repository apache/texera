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

import { PortSchema } from "../../../types/workflow-compiling.interface";

/**
 * Builds the single user message sent to the LLM for one failed operator.
 *
 * The model is asked for exactly one minimal fix and for raw JSON. The reply is
 * still parsed defensively (fenced blocks are unwrapped) because "no markdown"
 * is a request, not a guarantee.
 */
export function buildFixPrompt(
  errorMessage: string,
  operatorCode: string | undefined,
  schema: PortSchema | undefined,
  operatorProperties: Readonly<Record<string, unknown>>
): string {
  return `You are a debugging assistant for a Python data processing operator in a workflow engine.

Error
${errorMessage}

Operator code
\`\`\`python
${operatorCode ?? "(this operator has no user code)"}
\`\`\`

Input schema (column names and types)
${JSON.stringify(schema ?? [], null, 2)}

Operator configuration
${JSON.stringify(operatorProperties, null, 2)}

Task
Diagnose the root cause of the error. Then propose exactly ONE fix.

Rules:
- If it is a missing column error: suggest the closest column name from the schema.
- If it is a type error: add an explicit cast (e.g., int(), str(), float()) at the point of failure.
- If it is a null/NaN error: add a .dropna() call or an explicit null guard.
- If it is a model name error: correct the model field in the configuration.
- Do NOT rewrite the whole function. Change the minimum number of lines.

Respond ONLY with valid JSON, no markdown, no explanation outside the JSON:
{
  "explanation": "one sentence describing the root cause",
  "fix_type": "code_change" | "property_change",
  "original_snippet": "exact lines to replace (for code_change) or field name (for property_change)",
  "suggested_snippet": "replacement lines or new field value",
  "confidence": "high" | "medium" | "low"
}`;
}
