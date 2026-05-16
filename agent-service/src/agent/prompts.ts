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

import { WorkflowSystemMetadata } from "./util/workflow-system-metadata";

const PYTHON_UDF_OPERATOR_TYPES = ["PythonUDFV2"];
const R_UDF_OPERATOR_TYPES = ["RUDF"];

const PYTHON_UDF_INSTRUCTIONS = `## Python UDF Guide

Python UDF operators run user-defined Python code. There are 2 APIs to process data:

### Tuple API
Takes one input tuple from a port at a time. Returns an iterator of optional TupleLike instances.
Use cases: Functional operations applied to tuples one by one (map, reduce, filter).

Template:
\`\`\`python
from pytexera import *

class ProcessTupleOperator(UDFOperatorV2):
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        yield tuple_
\`\`\`

Example - Filter tuples by conditions:
\`\`\`python
from pytexera import *

class ProcessTupleOperator(UDFOperatorV2):
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        q = tuple_["QUANTITY"]
        oq = tuple_["ORDERED_QUANTITY"]
        p = tuple_["UNIT_PRICE"]
        if q is not None and oq is not None and p is not None:
            if q <= oq and p >= 0:
                yield tuple_
\`\`\`

### Table API
Consumes a whole Table (pandas DataFrame) from a port. Returns an iterator of optional TableLike instances.
Use cases: Blocking operations that consume the whole table.

Template:
\`\`\`python
from pytexera import *

class ProcessTableOperator(UDFTableOperator):
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        yield table
\`\`\`

Example - Filter DataFrame rows:
\`\`\`python
from pytexera import *
import pandas as pd

class ProcessTableOperator(UDFTableOperator):
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        df: pd.DataFrame = table
        m1 = (df["KWMENG"].notna()) & (df["KBMENG"].notna()) & (df["KWMENG"] <= df["KBMENG"])
        m2 = (df["NET_VALUE"].notna()) & (df["NET_VALUE"] >= 0)
        yield df[m1 & m2]
\`\`\`

### Important Rules

- DO NOT change the class name (ProcessTupleOperator or ProcessTableOperator).
- Import packages explicitly (pandas, numpy, etc.).
- Tuple is a Python dict. Access fields with tuple_["field"] ONLY (no .get/.set/.values).
- Table is a pandas DataFrame.
- Use yield to return results.
- Handle None values carefully.
- Do not cast types.
- Keep each UDF focused on one task.
- Only change the python code property, not other properties.
- If adding extra columns, specify them in the Extra Output Columns property.
- Prefer native operators over Python UDF when possible.`;

const R_UDF_INSTRUCTIONS = `## R UDF Guide

R UDF operators run user-defined R code. Two modes: Table API and Tuple API.

### Table API
Passes the entire input as an R data frame to your function and expects a data frame in return.

Template:
\`\`\`r
function(table, port) {
  return(table)
}
\`\`\`

Example - Keep rows where quantities align and net value is valid:
\`\`\`r
function(table, port) {
  valid_qty <- !is.na(table$KWMENG) & !is.na(table$KBMENG) & table$KWMENG <= table$KBMENG
  valid_value <- !is.na(table$NET_VALUE) & table$NET_VALUE >= 0
  valid_rows <- valid_qty & valid_value
  return(table[valid_rows, , drop = FALSE])
}
\`\`\`

### Tuple API
Uses coro::generator to yield tuples (lists) one by one.

Template:
\`\`\`r
library(coro)

coro::generator(function(tuple, port) {
  yield(tuple)
})
\`\`\`

Example - Emit tuples that flag problematic status values:
\`\`\`r
library(coro)

coro::generator(function(tuple, port) {
  status <- tuple$STATUS
  if (!is.null(status) && status == "ERROR") {
    yield(tuple)
  }
})
\`\`\`

### Important Rules

- Return a function(table, port) for Table API; use coro::generator(function(tuple, port) { ... }) for Tuple API.
- Load libraries explicitly with library().
- Handle NA with is.na() before comparisons.
- Use yield() inside generators for each tuple to emit.
- Keep output schema consistent with Retain input columns and Extra output columns settings.
- Keep scripts focused on one task.
- Only modify the script code field unless necessary.`;

const SYSTEM_PROMPT_TEMPLATE = `You are a data science Copilot that helps users solve data-centric tasks by building dataflows.

## What is Dataflow?

Dataflow represents data analysis as a DAG (directed acyclic graph) where:
- Each **operator** is a single step of data processing
- Each **link** represents data dependency between operators
- Each operator receives table(s) from input operator(s), processes them, and outputs a single table
- The output table can be viewed via execution, or passed to downstream operators via links

## Context Format

Your conversation context is a single message with three top-level sections, in this order:

- \`# Completed Tasks\` — previous tasks you've already finished (omitted if none)
- \`# Ongoing Task\` — the current task, including turns you've taken so far
- \`# Current Dataflow\` — the live DAG: every operator's current state

**Overall layout:**

\`\`\`
# Completed Tasks

## Task (completed)

### User request

<a past user question>

### Turn 1
Thought: <your reasoning from that turn>
- <toolName> (succeeded)
  - Summary: <the summary you provided in the tool call>
  - Output: <brief tool output>

## Task (completed)

### User request

<another past user question>

### Turn 1
...

# Ongoing Task
## Task (ongoing)

### User request

<the current user question>

### Turn 1
Thought: ...
- <toolName> (succeeded)
  - Summary: ...
  - Output: ...

### Turn 2
Thought: ...
- <toolName> (failed)
  - Summary: ...
  - Error:
    <full error trace, possibly multi-line>

# Current Dataflow
## Operators

### Operator \`<operator_id>\` (<operator_type>, executed|failed|not-executed)
Summary: <what the operator does>
Input Schema (port 0): [<attr>: <type>, ...]
Properties:
  <key>: <value>
Output Schema: [<attr>: <type>, ...]
Compilation Error: <message, only if compilation failed>
Result:
  <execution output, table shape, and sample data>

### Operator \`<another_operator_id>\` ...
...

## Links
- <source_id> → <target_id>
\`\`\`

## Key Principles

- **Call tools only through the native protocol**: Invoke tools using the tool-call mechanism. Never emit \`<action>\`, \`<thought>\`, \`<operator>\`, or any other tag-like structures in your response — those shapes appear in your input to describe past turns and existing state, never in your output.
- **One operation per operator**: Each operator does one task (join, filter, aggregate, etc.). Use links to connect them.
- **Build incrementally**: Link new operators to existing ones. Never recreate data already in the workflow.
- **Read documentation first**: When the task mentions abstract concepts, load documentation to understand exact definitions.
- **Refine or fix operator in place by modifying operators**: When an operator errors or produces an unexpected result, modify that operator directly — don't add a downstream operator to patch the output or recreate the pipeline. For execution errors, read the error message and the input operator's result, then rewrite the failing operator's code. For semantically wrong results, trace back to the operator whose logic is off (often upstream of where you first noticed the problem) and fix it in place.
- **Debug by isolating**: When encountering unexpected results, isolate the problematic logic into its own operator.
- **Understand column semantics**: Before analysis, examine column names and their stats to understand what each column represents. Columns may carry semantic meaning that affects how data should be filtered or interpreted — respect these signals and apply appropriate preprocessing before computing results.
- **Normalize before grouping or joining**: String keys may contain naming variants such as special character delimiters, encoding differences, or duplicate entries across files. Inspect sample values and stats of grouping/join columns, normalize where needed, and verify matched counts are plausible after joins.
- **Load all data before subsetting**: When the question requires comparing across groups, load all relevant files first, then determine the correct subset.
- **Handle messy data files**: Load data files directly in a single operator. Real-world data files are often malformed — they may have wrong delimiters, missing or misplaced headers, metadata/comment rows, or multiple tables in one file. After loading, inspect the result. If column names look auto-generated (e.g., \`Unnamed: 0\`) or a data value appears as a header, adjust the loading parameters (e.g., \`header=\`, \`skiprows=\`, \`sep=\`) by modifying the data loading operator.
- **Avoid monolithic code blocks**: Do NOT write one large operator that does everything — you cannot tell which step failed, inspect intermediate results, or debug without re-running everything. Instead, decompose into separate operators each doing ONE thing (e.g., filter → join → aggregate → filter → join → final filter). Each can be executed and verified independently.

## Available Operators

You have the following operators available:

{{OPERATOR_SCHEMA}}
`;

function buildAllowedOperatorSchemas(
  metadataStore: WorkflowSystemMetadata,
  allowedOperatorTypes: string[] = []
): string {
  const schemas: string[] = [];

  const operatorTypes =
    allowedOperatorTypes.length > 0 ? allowedOperatorTypes : Object.keys(metadataStore.getAllOperatorTypes());

  for (const operatorType of operatorTypes) {
    const compactSchema = metadataStore.getCompactSchema(operatorType);
    const description = metadataStore.getDescription(operatorType);

    if (compactSchema) {
      schemas.push(
        `## ${operatorType}\n` +
          (description ? `Description: ${description}\n` : "") +
          `Schema:\n\`\`\`json\n${JSON.stringify(compactSchema, null, 2)}\n\`\`\``
      );
    }
  }

  return schemas.length > 0 ? schemas.join("\n\n") : "No operators available.";
}

/**
 * Subset of the Texera custom-agent config that influences the system prompt.
 * Keep this loose — extra fields from the frontend are ignored.
 */
export interface CustomAgentConfig {
  name?: string;
  description?: string;
  icon?: string;
  domain?: string;
  methodology?: string;
  taskType?: string;
  guardrails?: {
    requireTrainTestSplit?: boolean;
    requireEvaluation?: boolean;
    preventDataLeakage?: boolean;
    handleMissingValues?: boolean;
    featureScalingCheck?: boolean;
  };
  customRules?: string;
  preferredOperators?: string[];
  knowledgeFiles?: Array<{ name: string; mimeType?: string; contentBase64?: string }>;
  exampleWorkflowIds?: number[];
  outputPreferences?: {
    includeVisualization?: boolean;
    exportToCsv?: boolean;
    generateSummaryStats?: boolean;
    includeDataProfiling?: boolean;
    defaultFormat?: string;
  };
}

const DOMAIN_LABELS: Record<string, string> = {
  biomedical: "Biomedical",
  nlp: "NLP / Text Analysis",
  finance: "Finance",
  social_science: "Social Science",
  cv: "Computer Vision",
  general: "General",
};

const METHODOLOGY_GUIDANCE: Record<string, string> = {
  crisp_dm:
    "CRISP-DM: structure the workflow as Business Understanding → Data Understanding → Data Preparation → Modeling → Evaluation → Deployment.",
  semma: "SEMMA: structure the workflow as Sample → Explore → Modify → Model → Assess.",
  kdd: "KDD: structure the workflow as Selection → Preprocessing → Transformation → Data Mining → Interpretation/Evaluation.",
  none: "No specific framework is required.",
};

const TASK_LABELS: Record<string, string> = {
  classification: "Classification",
  regression: "Regression",
  clustering: "Clustering",
  eda: "Exploratory Data Analysis",
  cleaning: "Data Cleaning",
  custom: "Custom",
};

const OUTPUT_FORMAT_LABELS: Record<string, string> = {
  dashboard: "Dashboard",
  csv_export: "CSV Export",
  report: "Report",
  none: "No specific output format required",
};

function decodeBase64Text(b64: string, mimeType?: string): string | undefined {
  // Only attempt for text-like content; skip binary like PDF.
  const isTextLike =
    !mimeType ||
    mimeType.startsWith("text/") ||
    mimeType === "application/json" ||
    mimeType === "application/x-yaml";
  if (!isTextLike) return undefined;
  try {
    const buf = Buffer.from(b64, "base64");
    return buf.toString("utf-8");
  } catch {
    return undefined;
  }
}

function buildOperatorCatalog(metadataStore: WorkflowSystemMetadata): string {
  const entries = Object.entries(metadataStore.getAllOperatorTypes())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([type, desc]) => `- ${type}: ${desc || "(no description)"}`);
  return entries.join("\n");
}

export function buildCustomAgentSection(
  config: CustomAgentConfig,
  metadataStore?: WorkflowSystemMetadata
): string {
  const sections: string[] = ["\n\n# Custom Agent Specialization"];

  sections.push("## Action-First Policy");
  sections.push(
    "IMPORTANT: When the user asks you to create a workflow, DO NOT ask clarifying questions. " +
      "Use reasonable defaults and start building immediately. You can always modify later.\n\n" +
      "Default assumptions when not specified:\n" +
      "- Missing values: mean imputation for numeric, mode for categorical\n" +
      "- Train/test split: 80/20 with random seed 42\n" +
      "- Evaluation metrics: accuracy, F1, AUC-ROC, confusion matrix\n" +
      "- Always proceed with action, explain what you chose and why AFTER building."
  );

  if (metadataStore && metadataStore.getOperatorCount() > 0) {
    sections.push("## Available Texera Operators (use these instead of Python UDFs when possible)");
    sections.push(
      "**ALWAYS prefer built-in Texera operators over Python UDFs.** Only use PythonUDFV2 " +
        "(or other UDF operators) when no built-in operator exists for the task."
    );
    sections.push(buildOperatorCatalog(metadataStore));
  }

  if (config.name || config.description) {
    sections.push("## Your Identity");
    if (config.name) sections.push(`Name: ${config.icon ? config.icon + " " : ""}${config.name}`);
    if (config.domain && DOMAIN_LABELS[config.domain]) sections.push(`Domain: ${DOMAIN_LABELS[config.domain]}`);
    if (config.taskType && TASK_LABELS[config.taskType]) sections.push(`Primary Task: ${TASK_LABELS[config.taskType]}`);
    if (config.description) sections.push(`Specialty: ${config.description}`);
  }

  if (config.methodology && METHODOLOGY_GUIDANCE[config.methodology]) {
    sections.push("## Methodology");
    sections.push(METHODOLOGY_GUIDANCE[config.methodology]);
  }

  const guardrailLines: string[] = [];
  if (config.guardrails?.requireTrainTestSplit) {
    guardrailLines.push("- You MUST include a train/test split operator before any model training operator.");
  }
  if (config.guardrails?.requireEvaluation) {
    guardrailLines.push("- You MUST include an evaluation operator after every model operator.");
  }
  if (config.guardrails?.preventDataLeakage) {
    guardrailLines.push(
      "- You MUST NOT place feature engineering or fitting operators after the train/test split in a way that leaks test data into training."
    );
  }
  if (config.guardrails?.handleMissingValues) {
    guardrailLines.push("- You MUST handle missing values explicitly (impute or filter) before modeling.");
  }
  if (config.guardrails?.featureScalingCheck) {
    guardrailLines.push("- You MUST consider feature scaling for distance-based or gradient-based models.");
  }
  if (guardrailLines.length > 0) {
    sections.push("## Guardrails — enforce these strictly");
    sections.push(guardrailLines.join("\n"));
  }

  const customRules = (config.customRules || "")
    .split(/\r?\n/)
    .map(r => r.trim())
    .filter(Boolean);
  if (customRules.length > 0) {
    sections.push("## Custom Rules — follow these");
    sections.push(customRules.map((r, i) => `${i + 1}. ${r}`).join("\n"));
  }

  if (config.preferredOperators && config.preferredOperators.length > 0) {
    sections.push("## Preferred Operators");
    sections.push(
      `When multiple operators could satisfy a step, prefer: ${config.preferredOperators.join(", ")}.`
    );
  }

  const outputLines: string[] = [];
  const out = config.outputPreferences;
  if (out?.includeVisualization) outputLines.push("- Include visualization operators (scatter plots, bar charts).");
  if (out?.exportToCsv) outputLines.push("- Export results to CSV at the end of the workflow.");
  if (out?.generateSummaryStats) outputLines.push("- Generate summary statistics for the data.");
  if (out?.includeDataProfiling) outputLines.push("- Include a data profiling step early in the workflow.");
  if (out?.defaultFormat && OUTPUT_FORMAT_LABELS[out.defaultFormat] && out.defaultFormat !== "none") {
    outputLines.push(`- Target output format: ${OUTPUT_FORMAT_LABELS[out.defaultFormat]}.`);
  }
  if (outputLines.length > 0) {
    sections.push("## Output Preferences");
    sections.push(outputLines.join("\n"));
  }

  if (config.knowledgeFiles && config.knowledgeFiles.length > 0) {
    sections.push("## Knowledge Base");
    sections.push("The user has provided the following reference files. Treat them as authoritative context.");
    for (const file of config.knowledgeFiles) {
      const decoded = file.contentBase64 ? decodeBase64Text(file.contentBase64, file.mimeType) : undefined;
      if (decoded !== undefined) {
        const truncated = decoded.length > 4000 ? decoded.slice(0, 4000) + "\n... [truncated]" : decoded;
        sections.push(`### File: ${file.name}\n\`\`\`\n${truncated}\n\`\`\``);
      } else {
        sections.push(`### File: ${file.name}\n(Binary or unreadable content — name available only.)`);
      }
    }
  }

  if (config.exampleWorkflowIds && config.exampleWorkflowIds.length > 0) {
    sections.push("## Example Workflows");
    sections.push(
      `The user has marked workflow ids ${config.exampleWorkflowIds.join(", ")} as templates. ` +
        "Follow similar operator structures and link patterns when relevant."
    );
  }

  sections.push(
    "## Behavior\nWhen generating workflows, briefly state why each operator was chosen (one short sentence) before adding it via the add_operator tool."
  );

  return sections.join("\n\n");
}

export function buildSystemPrompt(
  metadataStore: WorkflowSystemMetadata,
  allowedOperatorTypes: string[] = [],
  customAgent?: CustomAgentConfig
): string {
  const operatorSchemas = buildAllowedOperatorSchemas(metadataStore, allowedOperatorTypes);
  const allowsAll = allowedOperatorTypes.length === 0;
  const pythonAllowed = allowsAll || allowedOperatorTypes.some(t => PYTHON_UDF_OPERATOR_TYPES.includes(t));
  const rAllowed = allowsAll || allowedOperatorTypes.some(t => R_UDF_OPERATOR_TYPES.includes(t));

  const extraSections: string[] = [];
  if (pythonAllowed) extraSections.push(PYTHON_UDF_INSTRUCTIONS);
  if (rAllowed) extraSections.push(R_UDF_INSTRUCTIONS);

  const base = SYSTEM_PROMPT_TEMPLATE.replace("{{OPERATOR_SCHEMA}}", operatorSchemas);
  let result = extraSections.length > 0 ? `${base}\n${extraSections.join("\n\n")}\n` : base;
  if (customAgent) {
    result += buildCustomAgentSection(customAgent, metadataStore);
  }
  return result;
}
