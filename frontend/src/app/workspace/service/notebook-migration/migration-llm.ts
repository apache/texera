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

import { Injectable } from "@angular/core";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { AuthService } from "../../../common/service/user/auth.service";
import { createOpenAI } from "@ai-sdk/openai";
import { generateText, type ModelMessage } from "ai";
import { AppSettings } from "../../../common/app-setting";
import { v4 as uuidv4 } from "uuid";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { OperatorPredicate } from "../../types/workflow-common.interface";
import { WorkflowSettings } from "../../../common/type/workflow";
import {
  TEXERA_OVERVIEW,
  TUPLE_DOCUMENTATION,
  TABLE_DOCUMENTATION,
  OPERATOR_DOCUMENTATION,
  UDF_INPUT_PORT_DOCUMENTATION,
  EXAMPLE_OF_GOOD_CONVERSION,
  VISUALIZER_DOCUMENTATION,
  EXAMPLE_OF_MULTIPLE_UDF_CONVERSION,
  WORKFLOW_PROMPT,
  MAPPING_PROMPT,
  EXAMPLE_OF_MULTIPLE_UDF_CONVERSION_SCRIPT,
  SCRIPT_WORKFLOW_PROMPT,
  SCRIPT_MAPPING_PROMPT,
  EXAMPLE_OF_MULTIPLE_UDF_CONVERSION_FOLDER,
  FOLDER_WORKFLOW_PROMPT,
  FOLDER_MAPPING_PROMPT,
  FOLDER_CODE_PROMPT,
} from "./migration-prompts";
import { DerivedCell, ScriptSegmentation, segmentScript, splitScriptLines } from "./script-segmentation";
import { FolderDocument, resolveEntryPoint, scopeSegmentationToSpan } from "./folder-assembly";

interface Cell {
  cell_type: string;
  metadata: { [key: string]: any };
  // nbformat stores source as either a single string or an array of line strings.
  source: string | string[];
  outputs?: unknown[];
  execution_count?: number | null;
}

export interface Notebook {
  cells: Cell[];
  // Present in any real .ipynb and required by Jupyter's contents API, so the notebook
  // synthesized for a script declares them too.
  metadata?: { [key: string]: any };
  nbformat?: number;
  nbformat_minor?: number;
}

export interface WorkflowJSON {
  operators: OperatorPredicate[];
  operatorPositions: Record<string, { x: number; y: number }>;
  links: any[];
  commentBoxes: any[];
  settings: WorkflowSettings;
}

export interface CombinedMapping {
  operator_to_cell: Record<string, string[]>;
  cell_to_operator: Record<string, string[]>;
}

/**
 * The result of converting an input that arrived without cells. It also yields the notebook it
 * derived, because nothing upstream had one: the caller stores and displays it exactly as it
 * would a user's own .ipynb.
 */
export interface SourceConversion {
  workflowJSON: WorkflowJSON;
  workflowNotebookMapping: CombinedMapping;
  notebook: Notebook;
}

// Prefix each line with its number so the model can report ranges without counting lines itself.
// Uses the segmenter's own line splitting: the reported numbers only mean anything if the side
// that numbers and the side that slices agree on what a line is.
function numberScriptLines(source: string): string {
  const lines = splitScriptLines(source);
  const width = String(lines.length).length;
  return lines.map((line, index) => `${String(index + 1).padStart(width, " ")}| ${line}`).join("\n");
}

// Wrap derived cells as a notebook. The nbformat fields are what make it openable in Jupyter,
// and metadata.uuid is the join key the stored mapping is expressed in, same as for a real .ipynb.
function toDerivedNotebook(cells: DerivedCell[], metadata: Notebook["metadata"]): Notebook {
  return {
    cells: cells.map(cell => ({
      cell_type: "code",
      metadata: { uuid: cell.uuid },
      source: cell.source,
      outputs: [],
      execution_count: null,
    })),
    metadata,
    nbformat: 4,
    nbformat_minor: 4,
  };
}

const PYTHON_NOTEBOOK_DOCUMENTATION: string[] = [
  TEXERA_OVERVIEW,
  TUPLE_DOCUMENTATION,
  TABLE_DOCUMENTATION,
  OPERATOR_DOCUMENTATION,
  EXAMPLE_OF_GOOD_CONVERSION,
  VISUALIZER_DOCUMENTATION,
  UDF_INPUT_PORT_DOCUMENTATION,
  EXAMPLE_OF_MULTIPLE_UDF_CONVERSION,
];

// The script prelude differs in exactly one entry. The notebook worked example is written in
// `# START CELL1` form, and a system-message example of that weight would push the model to
// answer in cell ids for an input that has no cells. The notebook array is left untouched so
// existing conversions see byte-identical context.
const PYTHON_SCRIPT_DOCUMENTATION: string[] = PYTHON_NOTEBOOK_DOCUMENTATION.map(doc =>
  doc === EXAMPLE_OF_MULTIPLE_UDF_CONVERSION ? EXAMPLE_OF_MULTIPLE_UDF_CONVERSION_SCRIPT : doc
);

// Differs from the script prelude in the same single entry: its worked example shows banner
// lines, an entry point calling into other files, and definitions inlined rather than imported.
const PYTHON_FOLDER_DOCUMENTATION: string[] = PYTHON_SCRIPT_DOCUMENTATION.map(doc =>
  doc === EXAMPLE_OF_MULTIPLE_UDF_CONVERSION_SCRIPT ? EXAMPLE_OF_MULTIPLE_UDF_CONVERSION_FOLDER : doc
);

export type MigrationLanguage = "python";

interface MigrationPromptSet {
  notebookDocumentation: string[];
  notebookWorkflowPrompt: string;
  notebookMappingPrompt: string;
  scriptDocumentation: string[];
  scriptWorkflowPrompt: string;
  scriptMappingPrompt: string;
  operatorType: string;
  notebookMetadata: Notebook["metadata"];
}

const PROMPT_SETS: Record<MigrationLanguage, MigrationPromptSet> = {
  python: {
    notebookDocumentation: PYTHON_NOTEBOOK_DOCUMENTATION,
    notebookWorkflowPrompt: WORKFLOW_PROMPT,
    notebookMappingPrompt: MAPPING_PROMPT,
    scriptDocumentation: PYTHON_SCRIPT_DOCUMENTATION,
    scriptWorkflowPrompt: SCRIPT_WORKFLOW_PROMPT,
    scriptMappingPrompt: SCRIPT_MAPPING_PROMPT,
    operatorType: "PythonUDFV2",
    notebookMetadata: {
      kernelspec: { display_name: "Python 3", language: "python", name: "python3" },
      language_info: { name: "python" },
    },
  },
};

/**
 * Wraps a single LLM chat session that converts a Jupyter notebook or a Python script into
 * a Texera workflow plus a cell<->operator mapping.
 *
 * Lifecycle: `initialize()` -> `verifyConnection()` (optional) ->
 * `convertNotebookToWorkflow()` or `convertScriptToWorkflow()` -> `close()`. The session keeps
 * a running `messages` history shared by the prompts within one conversion. Each conversion
 * resets that history to its documentation prelude at its start, so the same instance can run
 * several conversions, in either mode, without leaking one conversion's context into the next.
 *
 * The modes differ only in framing. A notebook arrives split into cells and the model maps UDFs
 * onto those cell ids; a script and a folder have none, so they are sent with line numbers and
 * the cells are derived from the ranges the model reports. A folder is one document by the time
 * it reaches here, so everything downstream of the reply is shared.
 *
 * Output column types: intermediate UDFs declare their output columns as `binary` so rich
 * Python objects (DataFrames, arrays, models) round-trip between operators via pickle.
 * Terminal UDFs (no outgoing edge) declare their outputs as `string` so the result panel
 * renders viewable values rather than opaque binary blobs.
 */
export const DEFAULT_LLM_REQUEST_TIMEOUT_MINUTES = 10;

// A conversion returns the input as JSON-escaped UDFs, which runs larger than the input: each UDF
// repeats the operator boilerplate, and shared definitions are inlined into every UDF using them.
// Without an explicit budget the proxy substitutes its own, well below any shipped model's
// ceiling. Set under claude-haiku-4.5's 64k output ceiling so it cannot be rejected outright.
export const MAX_CONVERSION_OUTPUT_TOKENS = 48_000;

// Thrown when the model stopped because it hit the output budget. The reply is truncated, so it
// would otherwise surface as a JSON parse error with nothing saying why.
export class LlmResponseTruncatedError extends Error {
  constructor() {
    super("The model's reply was cut off before it finished. Try again with a smaller input.");
    this.name = "LlmResponseTruncatedError";
    Object.setPrototypeOf(this, LlmResponseTruncatedError.prototype);
  }
}

// Thrown when a model request exceeds the configured timeout, so callers can tell a slow-but-timed-out
// request apart from a genuine transport error and message the user accordingly.
export class LlmRequestTimeoutError extends Error {
  constructor(public readonly minutes: number) {
    super(`LLM request timed out after ${minutes} minutes`);
    this.name = "LlmRequestTimeoutError";
    // Keep instanceof correct regardless of the compile target.
    Object.setPrototypeOf(this, LlmRequestTimeoutError.prototype);
  }
}

@Injectable()
export class NotebookMigrationLLM {
  private model: any;
  private messages: ModelMessage[] = [];
  private initialized = false;

  constructor(
    private config: GuiConfigService,
    private workflowUtilService: WorkflowUtilService
  ) {}

  private get enabled(): boolean {
    return this.config.env.pythonNotebookMigrationEnabled;
  }

  private assertEnabled(): void {
    if (!this.enabled) {
      throw new Error("Notebook migration feature is disabled");
    }
  }

  /**
   * Seed the conversation with a Texera documentation prelude, discarding any prior
   * conversation. Used by initialize() and at the start of each conversion, which is where
   * the input-specific variant is chosen.
   */
  private seedDocumentation(documentation: string[] = PYTHON_NOTEBOOK_DOCUMENTATION): void {
    this.messages = documentation.map(
      (doc): ModelMessage => ({
        role: "system",
        content: doc,
      })
    );
  }

  private parseJsonResponse(raw: string, context: string): any {
    let text = raw.trim();

    // Prefer the contents of a fenced code block if present (```json ... ``` or ``` ... ```),
    // even when wrapped in prose. Otherwise fall back to the outermost {...} object.
    const fenced = text.match(/```(?:[a-zA-Z]+)?\s*([\s\S]*?)```/);
    if (fenced) {
      text = fenced[1].trim();
    } else {
      const firstBrace = text.indexOf("{");
      const lastBrace = text.lastIndexOf("}");
      if (firstBrace !== -1 && lastBrace > firstBrace) {
        text = text.slice(firstBrace, lastBrace + 1);
      }
    }

    try {
      return JSON.parse(text);
    } catch (err) {
      throw new Error(`Failed to parse LLM ${context} response as JSON: ${(err as Error).message}`);
    }
  }

  /**
   * Initialize a new LLM session with Texera documentation
   */
  public initialize(modelType: string = "gpt-5-mini", accessToken: string = AuthService.getAccessToken() ?? ""): void {
    this.assertEnabled();
    this.model = createOpenAI({
      baseURL: new URL(`${AppSettings.getApiEndpoint()}`, document.baseURI).toString(),
      // The /api/chat/* LiteLLM proxy authenticates the caller with the Texera JWT. The AI SDK
      // sends this value verbatim as `Authorization: Bearer <token>`, so we pass the user's
      // access token; the backend validates it, then substitutes the LiteLLM master key upstream.
      apiKey: accessToken,
    }).chat(modelType);

    this.seedDocumentation();

    this.initialized = true;
  }

  /**
   * Verify the connection to the LLM using the current access token
   */
  public async verifyConnection(): Promise<boolean> {
    if (!this.enabled) return false;
    if (!this.initialized) {
      throw new Error("LLM session not initialized");
    }

    try {
      await this.callModelWithTimeout([{ role: "user", content: "ping" }], 10);

      return true;
    } catch (err) {
      console.error("API key verification failed:", err);
      return false;
    }
  }

  // Seam over the `ai` transport. Specs spy this method rather than mocking the "ai" module:
  // a module mock leaks across specs that share the "ai" import and hangs on a real network call.
  protected callModel(
    messages: ModelMessage[],
    maxOutputTokens?: number,
    abortSignal?: AbortSignal
  ): Promise<{ text: string; finishReason?: string }> {
    return generateText({ model: this.model, messages, maxOutputTokens, abortSignal });
  }

  // Deployment-configurable bound (in minutes), falling back to the default when unset or non-positive.
  private get timeoutMinutes(): number {
    const configured = this.config.env.pythonNotebookMigrationTimeoutMinutes;
    return configured > 0 ? configured : DEFAULT_LLM_REQUEST_TIMEOUT_MINUTES;
  }

  // Wraps callModel with a hard timeout so a stalled request cannot hang forever. The abort
  // cancels the underlying request when the transport honors it; the race guarantees rejection
  // even if it does not, so the caller's error path always runs.
  private callModelWithTimeout(
    messages: ModelMessage[],
    maxOutputTokens?: number
  ): Promise<{ text: string; finishReason?: string }> {
    const minutes = this.timeoutMinutes;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(
        () => {
          controller.abort();
          reject(new LlmRequestTimeoutError(minutes));
        },
        minutes * 60 * 1000
      );
    });
    return Promise.race([this.callModel(messages, maxOutputTokens, controller.signal), timeout]).finally(() =>
      clearTimeout(timer)
    );
  }

  /**
   * Send a prompt and receive a response.
   * All prior documentation and conversation is preserved.
   */
  private async sendPrompt(prompt: string): Promise<string> {
    if (!this.initialized) {
      throw new Error("LLM session not initialized");
    }

    this.messages.push({
      role: "user",
      content: prompt,
    });

    const result = await this.callModelWithTimeout(this.messages, MAX_CONVERSION_OUTPUT_TOKENS);
    // "length" means the budget ran out mid-reply, so the JSON is incomplete. Reported here
    // rather than left to the parser, which can only say the JSON was malformed.
    if (result.finishReason === "length") {
      throw new LlmResponseTruncatedError();
    }

    this.messages.push({
      role: "assistant",
      content: result.text,
    });

    return result.text;
  }

  /**
   * Send a Jupyter Notebook to be converted into a workflow and mapping.
   */
  public async convertNotebookToWorkflow(notebook: Notebook, language: MigrationLanguage = "python"): Promise<string> {
    this.assertEnabled();
    if (!this.initialized) {
      throw new Error("LLM session not initialized");
    }

    const prompts = PROMPT_SETS[language];
    // Reset to the documentation prelude so a prior conversion's prompts/responses
    // don't leak into this one. The two sendPrompt calls below still share history.
    this.seedDocumentation(prompts.notebookDocumentation);

    const codeCells = notebook.cells.filter(cell => cell.cell_type === "code");

    // Every code cell must carry a unique metadata.uuid; it is the join key for the
    // cell<->operator mapping. Without it, untagged cells collide on the "undefined" marker.
    const untagged = codeCells.find(cell => cell.metadata?.uuid == null || String(cell.metadata.uuid).trim() === "");
    if (untagged) {
      throw new Error("Notebook code cells must each have a metadata.uuid before conversion");
    }

    const notebookString = codeCells
      .map(cell => {
        const uuid = String(cell.metadata.uuid);
        // nbformat line arrays already include trailing newlines, so join with "".
        const source = Array.isArray(cell.source) ? cell.source.join("") : cell.source;
        return `# START ${uuid}\n${source}\n# END ${uuid}`;
      })
      .join("\n\n");

    const workflow = await this.sendPrompt(`${prompts.notebookWorkflowPrompt}\n${notebookString}`);
    const mapping = await this.sendPrompt(prompts.notebookMappingPrompt);

    // Remove ```json blocks and parse
    const udfLLMResponse = this.parseJsonResponse(workflow, "workflow");
    const { workflowJSON, udfIdToOperatorId } = this.buildWorkflow(udfLLMResponse, prompts.operatorType);

    // The notebook path keys its mapping on the cell uuids embedded in the prompt.
    const parsedMapping: Record<string, string[]> = this.parseJsonResponse(mapping, "mapping");
    const workflowNotebookMapping = this.buildCombinedMapping(parsedMapping, udfIdToOperatorId);

    return JSON.stringify({ workflowJSON, workflowNotebookMapping });
  }

  /**
   * Send a Python script to be converted into a workflow, a mapping, and the notebook the
   * mapping is expressed against.
   *
   * Differs from the notebook path in two places only. The script is sent with line numbers
   * rather than cell markers, and the model is asked which line ranges became which UDF; the
   * cells are then derived from that answer instead of arriving with the input.
   */
  public async convertScriptToWorkflow(
    source: string,
    language: MigrationLanguage = "python"
  ): Promise<SourceConversion> {
    this.assertEnabled();
    if (!this.initialized) {
      throw new Error("LLM session not initialized");
    }

    const prompts = PROMPT_SETS[language];
    const { workflowJSON, udfIdToOperatorId, reportedRanges } = await this.requestConversion(
      prompts.scriptDocumentation,
      prompts.scriptWorkflowPrompt,
      prompts.scriptMappingPrompt,
      prompts.operatorType,
      source
    );

    // segmentScript reconciles whatever the model reported, so a malformed range degrades the
    // mapping rather than discarding a workflow that already cost a full conversion.
    return this.finishConversion(
      workflowJSON,
      udfIdToOperatorId,
      segmentScript(source, reportedRanges),
      prompts.notebookMetadata
    );
  }

  /**
   * Send a folder, already assembled into one document, to be converted into a workflow, a
   * mapping, and the notebook the mapping is expressed against.
   *
   * Every file is converted, but the notebook holds the entry point alone, since the rest are
   * function definitions and a notebook of all of them is a wall of code. The mapping the model
   * reports is in that file's lines, so clicking an operator highlights the call that runs it.
   * Still keyed on cell uuids, so nothing downstream learns the input was a folder.
   */
  public async convertFolderToWorkflow(document: FolderDocument): Promise<SourceConversion> {
    this.assertEnabled();
    if (!this.initialized) {
      throw new Error("LLM session not initialized");
    }

    const prompts = PROMPT_SETS.python;
    const { workflowJSON, udfIdToOperatorId, workflowResponse, reportedRanges } = await this.requestConversion(
      PYTHON_FOLDER_DOCUMENTATION,
      FOLDER_WORKFLOW_PROMPT,
      FOLDER_MAPPING_PROMPT,
      prompts.operatorType,
      document.source,
      // The layout is prompt text, never part of the numbered document: numbering it would shift
      // every line the model reports, and the segmenter would emit it as a cell of directory
      // listing in the derived notebook.
      `${document.tree}\n\n${FOLDER_CODE_PROMPT}`
    );

    const segmentation = segmentScript(document.source, reportedRanges, {
      forcedBoundaries: document.forcedBoundaries,
    });

    // Resolved against the files actually assembled; an unusable answer leaves the notebook
    // covering the whole folder, which is worse to read but never empty.
    const entryPoint = resolveEntryPoint(document.files, workflowResponse?.entry_point);
    return this.finishConversion(
      workflowJSON,
      udfIdToOperatorId,
      entryPoint ? scopeSegmentationToSpan(segmentation, entryPoint) : segmentation,
      prompts.notebookMetadata
    );
  }

  /**
   * The two model calls every cell-less input makes, and the parsing of both replies. Stops short
   * of segmenting, which is where the inputs differ, so this has no folder concept and the untyped
   * reply stays with the caller that understands it.
   */
  private async requestConversion(
    documentation: string[],
    workflowPrompt: string,
    mappingPrompt: string,
    operatorType: string,
    source: string,
    preamble?: string
  ): Promise<{
    workflowJSON: WorkflowJSON;
    udfIdToOperatorId: Record<string, string>;
    workflowResponse: any;
    reportedRanges: any;
  }> {
    this.seedDocumentation(documentation);

    const request = [workflowPrompt, preamble, numberScriptLines(source)].filter(part => part).join("\n");
    const workflow = await this.sendPrompt(request);
    const mapping = await this.sendPrompt(mappingPrompt);

    const workflowResponse = this.parseJsonResponse(workflow, "workflow");
    const { workflowJSON, udfIdToOperatorId } = this.buildWorkflow(workflowResponse, operatorType);

    return {
      workflowJSON,
      udfIdToOperatorId,
      workflowResponse,
      reportedRanges: this.parseJsonResponse(mapping, "mapping"),
    };
  }

  /** Assembles the stored pair: the mapping, and the notebook it is expressed against. */
  private finishConversion(
    workflowJSON: WorkflowJSON,
    udfIdToOperatorId: Record<string, string>,
    segmentation: ScriptSegmentation,
    notebookMetadata: Notebook["metadata"]
  ): SourceConversion {
    return {
      workflowJSON,
      workflowNotebookMapping: this.buildCombinedMapping(segmentation.udfToCellUuids, udfIdToOperatorId),
      notebook: toDerivedNotebook(segmentation.cells, notebookMetadata),
    };
  }

  /**
   * Assemble the workflow from the model's `code`, `edges` and `outputs` response.
   *
   * Input-agnostic: the notebook and script paths differ in how they prompt and in what their
   * mapping is keyed on, not in how the generated UDFs become operators.
   *
   * Returns the workflow together with the UDF id -> operatorID index the mapping is built from.
   */
  private buildWorkflow(
    udfLLMResponse: any,
    operatorType: string
  ): {
    workflowJSON: WorkflowJSON;
    udfIdToOperatorId: Record<string, string>;
  } {
    const workflowJSON: WorkflowJSON = {
      operators: [],
      operatorPositions: {},
      links: [],
      commentBoxes: [],
      settings: {
        dataTransferBatchSize: this.config.env.defaultDataTransferBatchSize,
        executionMode: this.config.env.defaultExecutionMode,
      },
    };

    const udfIdToOperatorId: Record<string, string> = {};

    // UDFs that are never the source of an edge are terminal (result-facing). Their outputs
    // default to "string" so the result panel renders typed values; intermediate UDFs keep
    // "binary" so rich objects (DataFrames, arrays, models) round-trip between operators via pickle.
    const edgeSources = new Set<string>((udfLLMResponse.edges || []).map(([source]: [string, string]) => source));

    Object.entries(udfLLMResponse.code).forEach(([udfId, udfCode], i) => {
      let udfOutputColumns: { attributeName: string; attributeType: string }[] = [];
      if (udfLLMResponse.outputs && udfLLMResponse.outputs[udfId]) {
        const attributeType = edgeSources.has(udfId) ? "binary" : "string";
        udfOutputColumns = udfLLMResponse.outputs[udfId].map((attr: string) => ({
          attributeName: attr,
          attributeType,
        }));
      }

      // Build the operator from the live schema so the operatorVersion, ports, and property
      // defaults track the backend definition, then overlay the generated code/outputs.
      const base = this.workflowUtilService.getNewOperatorPredicate(operatorType, udfId);
      const operator: OperatorPredicate = {
        ...base,
        operatorProperties: {
          ...base.operatorProperties,
          code: udfCode,
          retainInputColumns: false,
          outputColumns: udfOutputColumns,
        },
      };

      udfIdToOperatorId[udfId] = operator.operatorID;
      workflowJSON.operators.push(operator);
      workflowJSON.operatorPositions[operator.operatorID] = { x: 140 * (i + 1), y: 0 };
    });

    const knownUdfIds = new Set(Object.keys(udfIdToOperatorId));

    // Add links/edges. Skip (with a warning) any edge that references a UDF id the LLM
    // never defined in `code`, rather than emitting a link with an undefined endpoint.
    (udfLLMResponse.edges || []).forEach(([source, target]: [string, string]) => {
      if (!knownUdfIds.has(source) || !knownUdfIds.has(target)) {
        console.warn(`Skipping edge with unknown UDF id: ${source} -> ${target}`);
        return;
      }
      workflowJSON.links.push({
        linkID: `link-${uuidv4()}`,
        source: {
          operatorID: udfIdToOperatorId[source],
          portID: "output-0",
        },
        target: {
          operatorID: udfIdToOperatorId[target],
          portID: "input-0",
        },
      });
    });

    return { workflowJSON, udfIdToOperatorId };
  }

  /**
   * Invert a UDF id -> cell ids mapping into the stored operator<->cell form, skipping (with a
   * warning) any UDF the model never defined in `code`. Shared by both input paths: they differ
   * only in where the cell ids came from.
   */
  private buildCombinedMapping(
    udfToCells: Record<string, string[]>,
    udfIdToOperatorId: Record<string, string>
  ): CombinedMapping {
    const operatorToCell: Record<string, string[]> = {};
    const cellToOperator: Record<string, string[]> = {};

    Object.entries(udfToCells).forEach(([udfId, cells]) => {
      const operatorId = udfIdToOperatorId[udfId];
      if (!operatorId) {
        console.warn(`Skipping mapping entry with unknown UDF id: ${udfId}`);
        return;
      }
      // The notebook path's cell ids come straight from an unvalidated model reply. Without this
      // a non-array would throw from forEach below and discard a conversion that already cost two
      // model calls; skipping degrades the mapping instead, as the script path already does.
      if (!Array.isArray(cells)) {
        console.warn(`Skipping mapping entry whose cell list is not an array, for UDF id: ${udfId}`);
        return;
      }
      operatorToCell[operatorId] = cells;
      cells.forEach(cell => {
        if (!cellToOperator[cell]) {
          cellToOperator[cell] = [operatorId];
        } else {
          cellToOperator[cell].push(operatorId);
        }
      });
    });

    return { operator_to_cell: operatorToCell, cell_to_operator: cellToOperator };
  }

  /**
   * Closes the session.
   * Clears all context and releases references.
   */
  public close(): void {
    this.messages = [];
    this.model = null;
    this.initialized = false;
  }
}
