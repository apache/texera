/**
 * Prompt builders for the AI wizard. Each builder returns a single string
 * that we send to /aiassistant/openai via AiAnalystService.sendPromptToOpenAI.
 *
 * Structure mirrors the design-doc §4.2 Prompt Builder table:
 *   System / Operators / Methodology / Guardrails / Data / Examples / Task
 */

import { WorkflowContent } from "../../../common/type/workflow";
import { OperatorMetadata } from "../../types/operator-schema.interface";
import { DataProfile, ValidationResult, WizardState } from "./types";
import { getFrameworkPrompt } from "./data/frameworks";
import { getGuardrailsPrompt } from "./data/guardrails";
import { getFewShotPrompt } from "./data/few-shot-examples";

const PORT_AND_FORMAT_RULES = `## CRITICAL: Port ID Convention (must follow exactly)
Every operator MUST declare its input and output ports using this exact naming scheme:
- inputPorts: an array with one entry per input port. The i-th entry has portID "input-{i}" (zero-indexed).
- outputPorts: an array with one entry per output port. The i-th entry has portID "output-{i}" (zero-indexed).

Every link MUST reference these exact portIDs:
- link.source.portID = "output-{i}" referencing an output port that actually exists on the source operator
- link.target.portID = "input-{i}" referencing an input port that actually exists on the target operator

Source operators (0 input ports) can only appear as a link source, never a target.

## CRITICAL: Operator Property Completeness (the workflow MUST compile)
Every operator must be IMMEDIATELY runnable. Texera shows "invalid workflow" if any required property
is missing or references a column that doesn't exist. Therefore:

1. For each operator, consult the operator catalog above and identify which properties are marked "required".
2. Fill EVERY required property. Do NOT leave any required field as null, empty string, or a placeholder.
3. When a property references a column (Filter.attribute, Aggregate.groupByKeys, BarChart.x, Visualization axes,
   Sklearn target/feature columns, etc.), use the EXACT column name from the Data Profile section above.
   Never invent column names. Match capitalization exactly.
4. When the analysis goal implies a target column (predictive modeling → outcome/label, EDA on time series
   → date column), pick the most plausible match from the Data Profile.
5. For each property you auto-filled where you had to make a judgment (especially when multiple columns
   could fit), record a short rationale in whyExplanations so the user can audit your choice. Example:
   "Filter-operator-1: Filters on Species=Iris-setosa. (Auto-filled: Species is the only categorical
   column in the profile.)"
6. Do NOT add a Filter or any column-referencing operator with empty predicates.

## CRITICAL: Why Explanations
Include a top-level "whyExplanations" object mapping every operatorID to a short (1-3 sentence) plain-English
explanation suitable for a biomedical researcher who does not write code. Reference framework phase,
guardrail, or data column when applicable. For any property where you had to choose among alternatives,
note "(Auto-filled: <one-line rationale>)" inside the same explanation.

## Output Format
Return ONLY a single JSON object (no markdown fences, no commentary):

{
  "operators": [...],
  "operatorPositions": { "operatorID": { "x": number, "y": number } },
  "links": [...],
  "commentBoxes": [],
  "settings": { "dataTransferBatchSize": 400, "executionMode": "PIPELINED" },
  "whyExplanations": { "operatorID": "explanation string" }
}`;

export function operatorCatalogText(metadata: OperatorMetadata | null | undefined): string {
  if (!metadata || metadata.operators.length === 0) {
    return "(operator catalog unavailable — generation may be unreliable)";
  }
  const lines: string[] = [];
  for (const op of metadata.operators) {
    const am = op.additionalMetadata;
    const definitions = (op.jsonSchema as any)?.definitions ?? {};
    const body = renderJsonSchemaProps(op.jsonSchema as any, definitions, "    ", 0);
    lines.push(
      `### ${op.operatorType} - ${am.userFriendlyName}\nCategory: ${am.operatorGroupName}\nDescription: ${am.operatorDescription ?? ""}\nInput Ports: ${am.inputPorts.length}\nOutput Ports: ${am.outputPorts.length}\nProperties:\n${body || "    (none)"}`
    );
  }
  return lines.join("\n\n");
}

/** Resolve a "#/definitions/X" $ref against the operator schema's definitions block. */
function resolveRef(schema: any, definitions: Record<string, any>): any {
  if (!schema || typeof schema !== "object") return schema;
  if (typeof schema.$ref === "string") {
    const m = /^#\/definitions\/(.+)$/.exec(schema.$ref);
    if (m) {
      const target = definitions[m[1]];
      if (target) return { ...target, ...schema, $ref: undefined };
    }
  }
  return schema;
}

/**
 * Recursively render a JSON schema's properties (incl. resolved $refs) so the
 * LLM sees every nested required field. Texera's schemas use $ref into the
 * top-level definitions block — without resolving them the LLM has no way to
 * know that aggregations[].attribute / aggFunction / "result attribute" exist.
 */
/** Skip Texera's scaffolding placeholder properties so the LLM doesn't fill workflows
 *  with dummyProperty / dummyValue (legitimate workflows never use them). */
function isPlaceholderProp(name: string): boolean {
  return /^dummy/i.test(name);
}

function renderJsonSchemaProps(
  schemaRaw: any,
  definitions: Record<string, any>,
  indent: string,
  depth: number
): string {
  if (depth > 6) return ""; // safety cap
  const schema = resolveRef(schemaRaw, definitions);
  if (!schema || typeof schema !== "object" || !schema.properties) return "";
  const required = new Set<string>(Array.isArray(schema.required) ? schema.required : []);
  const lines: string[] = [];
  for (const [name, subRaw] of Object.entries<any>(schema.properties)) {
    if (isPlaceholderProp(name) && !required.has(name)) continue;
    const sub = resolveRef(subRaw, definitions);
    const type = sub?.type ?? "any";
    const isReq = required.has(name);
    const desc = sub?.description ?? sub?.title ?? "";
    lines.push(`${indent}- ${name} (${type}${isReq ? ", required" : ", optional"}): ${desc}`);
    if (Array.isArray(sub?.enum)) {
      lines.push(`${indent}  enum: ${sub.enum.join(" | ")}`);
    }
    if (sub?.type === "object") {
      const nested = renderJsonSchemaProps(sub, definitions, indent + "  ", depth + 1);
      if (nested) lines.push(`${indent}  fields:\n${nested}`);
    } else if (sub?.type === "array") {
      const items = resolveRef(sub.items, definitions);
      if (items?.type === "object") {
        const nested = renderJsonSchemaProps(items, definitions, indent + "  ", depth + 1);
        if (nested) lines.push(`${indent}  items[]:\n${nested}`);
      } else if (items?.type) {
        lines.push(`${indent}  items[]: ${items.type}`);
        if (Array.isArray(items.enum)) lines.push(`${indent}  items enum: ${items.enum.join(" | ")}`);
      }
    }
  }
  return lines.join("\n");
}

/**
 * Walk a generated operator's properties against its schema and report any
 * required fields that are unset, empty, or array-with-empty-items. Used by
 * the wizard's review UI to surface "needs your input" gaps. Resolves $refs
 * against the top-level definitions block so nested required fields under
 * Texera's schema-by-reference style (e.g. aggregations[].attribute) get
 * checked too.
 */
export function findMissingRequiredPaths(value: any, schema: any, path: string): string[] {
  const definitions = collectDefinitions(schema);
  return walkRequired(value, schema, path, definitions, 0);
}

function collectDefinitions(rootSchema: any): Record<string, any> {
  return rootSchema?.definitions && typeof rootSchema.definitions === "object" ? rootSchema.definitions : {};
}

function walkRequired(
  value: any,
  schemaRaw: any,
  path: string,
  definitions: Record<string, any>,
  depth: number
): string[] {
  if (depth > 6) return [];
  const schema = resolveRef(schemaRaw, definitions);
  if (!schema || typeof schema !== "object") return [];
  const required: string[] = Array.isArray(schema.required) ? schema.required : [];
  const missing: string[] = [];
  if (schema.type === "object" && schema.properties) {
    for (const key of required) {
      const sub = resolveRef(schema.properties[key], definitions);
      const v = (value ?? {})[key];
      const subPath = path ? `${path}.${key}` : key;
      if (v === undefined || v === null || v === "") {
        missing.push(subPath);
        continue;
      }
      if (Array.isArray(v) && v.length === 0) {
        missing.push(`${subPath} (empty array)`);
        continue;
      }
      if (sub?.type === "object" && typeof v === "object") {
        missing.push(...walkRequired(v, sub, subPath, definitions, depth + 1));
      } else if (sub?.type === "array" && Array.isArray(v)) {
        const items = resolveRef(sub.items, definitions);
        if (items?.type === "object") {
          v.forEach((item: any, i: number) => {
            missing.push(...walkRequired(item, items, `${subPath}[${i}]`, definitions, depth + 1));
          });
        }
      }
    }
  }
  return missing;
}

function dataProfileText(profile: DataProfile | undefined): string {
  if (!profile) return "(no data profile available — LLM should not assume column names)";
  if (profile.source === "unavailable") return "(profile unavailable for this data source)";
  const cols = profile.columns
    .map(
      c =>
        `  - ${c.name} (${c.dtype}, ${c.nullRate * 100}% null, ${c.uniqueCount} unique). Sample: [${c.sampleValues.slice(0, 5).join(", ")}]`
    )
    .join("\n");
  return `Row count: ${profile.rowCount}\nColumns:\n${cols}`;
}

function dataSourceConfigText(state: WizardState): string {
  const { dataSource, existingDatasetPath, dknetDataset } = state;
  if (dataSource === "Existing Dataset") {
    return existingDatasetPath
      ? `Existing Texera dataset file. Use CSVFileScan with fileName "${existingDatasetPath}".`
      : "Existing dataset (path to be specified by user via Datasets picker)";
  }
  if (dataSource === "dkNET Dataset" && dknetDataset) {
    return `dkNET curated biomedical dataset: ${dknetDataset.name}\nSchema: ${dknetDataset.schema}\nUse CSVFileScan with fileName "${dknetDataset.fileName}".`;
  }
  return "";
}

export function buildGeneratePrompt(state: WizardState, operatorCatalog: OperatorMetadata | null): string {
  const { analysisGoal, customAnalysisGoal, dataSource, framework, frameworkPrompt, guardrails, dataProfile } = state;

  const goalText =
    analysisGoal === "Custom"
      ? `Custom (free-text): ${customAnalysisGoal?.trim() ?? ""}`
      : (analysisGoal ?? "");

  const additionalContext =
    analysisGoal && analysisGoal !== "Custom" && customAnalysisGoal?.trim() ? customAnalysisGoal.trim() : "";

  const fwPromptText = frameworkPrompt?.trim()
    ? frameworkPrompt
    : framework
      ? getFrameworkPrompt(framework)
      : "";

  return `You are a Texera workflow generation expert. Generate a complete Texera workflow JSON for the following requirements:

## Analysis Goal
${goalText}
${additionalContext ? `\n## Additional Context (user-provided domain notes — soft guidance)\n${additionalContext}\n` : ""}
## Data Source
${dataSource ?? "(none)"}
${dataSourceConfigText(state)}

## Data Profile (REAL column names — use these, do NOT guess)
${dataProfileText(dataProfile)}

## Scientific Framework
${framework ?? "None specified"}
${fwPromptText}

${getGuardrailsPrompt(guardrails)}

## Available Operators
${operatorCatalogText(operatorCatalog)}

${PORT_AND_FORMAT_RULES}

${getFewShotPrompt()}

Generate the workflow now.`;
}

export function buildModifyPrompt(
  current: WorkflowContent,
  currentWhy: Record<string, string>,
  instruction: string,
  operatorCatalog: OperatorMetadata | null,
  dataProfile: DataProfile | undefined
): string {
  const merged = { ...current, whyExplanations: currentWhy };
  return `You are a Texera workflow editor. The user wants to modify the following existing workflow.

## Current Workflow
\`\`\`json
${JSON.stringify(merged, null, 2)}
\`\`\`

## Data Profile (REAL column names — use these, do NOT guess)
${dataProfileText(dataProfile)}

## User Instruction
${instruction}

## Editing Rules
- Apply the user instruction precisely. Do not refactor unrelated parts.
- Preserve operatorIDs of operators that are NOT being changed.
- If you add new operators, give them fresh IDs in the format "{operatorType}-operator-{shortuuid}".
- Update operatorPositions and links to keep the workflow valid and connected.
- Update or extend whyExplanations to cover any new or changed operators, and explain WHY this edit was made.

## Available Operators
${operatorCatalogText(operatorCatalog)}

${PORT_AND_FORMAT_RULES}

Return the FULL updated workflow JSON (not a diff).`;
}

export function buildRetryPrompt(
  originalPrompt: string,
  prevWorkflow: WorkflowContent,
  validation: ValidationResult
): string {
  const errorList = validation.errors.map(e => `- ${e.field}: ${e.message}`).join("\n");
  return `${originalPrompt}

## PREVIOUS ATTEMPT FAILED VALIDATION
Your previous response was:
\`\`\`json
${JSON.stringify(prevWorkflow, null, 2)}
\`\`\`

The validator reported these errors that MUST be fixed:
${errorList}

Carefully re-read the operator catalog and the Port ID Convention. Produce a corrected workflow JSON that fixes every error above. Do not introduce new errors.`;
}
