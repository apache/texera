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

import { OperatorPortSchemaMap } from "../../../types/workflow-compiling.interface";

/**
 * Renders every input port, not just the first.
 *
 * A Python UDF can have several input ports and `process_tuple` is told which one a tuple
 * came from, so a column that only exists on port 1 is not a typo for one on port 0. Sending
 * port 0 alone leaves the model guessing for anything downstream of a join.
 *
 * A single port renders with no label at all, since that is nearly every operator and there
 * is nothing to disambiguate.
 */
export function formatInputSchema(inputSchemas: OperatorPortSchemaMap | undefined): string {
  const ports = Object.entries(inputSchemas ?? {}).filter(([, schema]) => schema !== undefined);
  if (ports.length <= 1) {
    return JSON.stringify(ports[0]?.[1] ?? [], null, 2);
  }
  return ports.map(([key, schema]) => `Port ${key.split("_")[0]}:\n${JSON.stringify(schema, null, 2)}`).join("\n");
}

/**
 * Builds the single user message sent to the LLM for one failed operator.
 *
 * The operator's configuration is not among the things sent. Every fix this panel writes is
 * a change to `code`, so the block served nothing -- and it could not be made safe by
 * filtering field names: a Python UDF's `uiParameters` is a list of attribute/value pairs,
 * so the name says nothing about whether a value is an API key the user typed in.
 *
 * The model is asked for exactly one minimal fix and for raw JSON. The reply is still parsed
 * defensively (fenced blocks are unwrapped) because "no markdown" is a request, not a
 * guarantee.
 */
export function buildFixPrompt(
  errorMessage: string,
  operatorCode: string | undefined,
  inputSchemas: OperatorPortSchemaMap | undefined
): string {
  return `You are a debugging assistant for a Python data processing operator in a workflow engine.

Error
${errorMessage}

Operator code
\`\`\`python
${operatorCode ?? "(this operator has no user code)"}
\`\`\`

Input schema (column names and types)
${formatInputSchema(inputSchemas)}


Task
Diagnose the root cause of the error. Then propose exactly ONE fix.

Rules:
- If it is a missing column error: suggest the closest column name from the schema.
- If it is a type error: add an explicit cast (e.g., int(), str(), float()) at the point of failure.
- If it is a null/NaN error: add a .dropna() call or an explicit null guard.
- Do NOT rewrite the whole function. Change the minimum number of lines.
- original_snippet and suggested_snippet must be WHOLE LINES, copied starting at the
  beginning of a line, with the leading indentation included exactly as it appears. Do not
  start a snippet in the middle of a line, and do not re-indent: every line you return must
  already carry the indentation it should have in the file. Ending a snippet with a newline
  is fine; just end both snippets the same way.

Respond ONLY with valid JSON, no markdown, no explanation outside the JSON:
{
  "explanation": "one sentence describing the root cause",
  "original_snippet": "whole lines to replace, indentation included",
  "suggested_snippet": "replacement lines, each already indented as it should appear",
  "confidence": "high" | "medium" | "low"
}`;
}
