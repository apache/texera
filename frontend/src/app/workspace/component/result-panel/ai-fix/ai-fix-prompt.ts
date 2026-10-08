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

// Names that carry a credential even when the operator forgot the password widget.
const SECRET_NAME = /token|password|secret|credential|api[-_]?key/i;

/**
 * Drops operator properties whose values are credentials, so they never reach the model.
 *
 * Operators keep secrets alongside ordinary settings -- `hfApiToken` on HuggingFaceInference,
 * `password` on the SQL sources -- and the whole configuration used to be serialized into the
 * prompt. The operator's own schema marks those fields with the password widget, which is the
 * reliable signal; the name pattern is a second barrier so a schema that cannot be read, or an
 * operator missing the annotation, does not leak anyway.
 *
 * Dropped rather than masked: none of the four supported fixes touches a credential, so the
 * model has no reason to know the field is there.
 */
export function withoutSecrets(
  operatorProperties: Readonly<Record<string, unknown>>,
  schemaProperties?: Readonly<Record<string, unknown>>
): Record<string, unknown> {
  const isPasswordWidget = (fieldSchema: unknown): boolean =>
    (fieldSchema as any)?.widget?.formlyConfig?.templateOptions?.type === "password";

  return Object.fromEntries(
    Object.entries(operatorProperties).filter(
      ([name]) => !SECRET_NAME.test(name) && !isPasswordWidget(schemaProperties?.[name])
    )
  );
}

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
