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
import { BehaviorSubject, Observable, firstValueFrom } from "rxjs";
import { filter, timeout } from "rxjs/operators";
import { createOpenAI } from "@ai-sdk/openai";
import { generateText, type ModelMessage } from "ai";
import { AppSettings } from "../../../../common/app-setting";
import { AuthService } from "../../../../common/service/user/auth.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { WarehouseService } from "../../../../common/service/warehouse/warehouse.service";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { PortSchema } from "../../../types/workflow-compiling.interface";
import { buildFixPrompt, withoutSecrets } from "./ai-fix-prompt";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";

export type FixErrorType = "missing_column" | "type_error" | "null_error" | "model_not_found" | "unsupported";
export type FixConfidence = "high" | "medium" | "low";

export interface SuggestedFix {
  type: "code_change" | "property_change";
  original: string;
  suggested: string;
  explanation: string;
  // Rendered as the confidence badge; `fieldName` names the patched property
  // (property_change only) so applyFix knows which field to write.
  confidence: FixConfidence;
  fieldName?: string;
}

export interface FixState {
  operatorId: string;
  errorMessage: string;
  errorType: FixErrorType;
  originalCode?: string;
  originalProperties?: object;
  suggestedFix?: SuggestedFix;
  // `applied_without_run`: the fix was written but the deployment refused to start a
  // run (a warehouse is required and none is selected), so the panel must not claim one.
  status: "idle" | "analyzing" | "ready" | "applying" | "applied" | "applied_without_run" | "error";
}

// Model id as LiteLLM exposes it (bin/single-node/litellm-config.yaml), not the provider's
// id. A deployment whose proxy publishes a different alias has to change this: there is no
// GuiConfig field for it today, and adding one is a backend change.
export const AI_FIXER_MODEL = "claude-haiku-4.5";
export const AI_FIXER_TIMEOUT_MS = 120_000;
export const UNLOCK_TIMEOUT_MS = 15_000;
export const UNSUPPORTED_MESSAGE = "This error type is not yet supported for automatic fixing.";

export const IDLE_STATE: FixState = { operatorId: "", errorMessage: "", errorType: "unsupported", status: "idle" };

// Matched against the exception line only, never the frame lines above it.
const PATTERNS: ReadonlyArray<[RegExp, FixErrorType]> = [
  [/KeyError:\s*['"][^'"]+['"]/, "missing_column"],
  [/TypeError:\s*unsupported operand/i, "type_error"],
  [/ValueError:[^\n]*\bNaN\b|NullPointerException/i, "null_error"],
  [/ModelNotFound|\bmodel[^\n]*\b404\b|\b404\b[^\n]*\bmodel/i, "model_not_found"],
];

/** Writes the model's string back in the type the field already held. */
export function coerceToFieldType(currentValue: unknown, suggested: string): unknown {
  if (typeof currentValue === "number") {
    const parsed = Number(suggested);
    if (Number.isNaN(parsed)) {
      throw new Error(`${suggested} is not a number`);
    }
    return parsed;
  }
  if (typeof currentValue === "boolean") {
    if (suggested !== "true" && suggested !== "false") {
      throw new Error(`${suggested} is not a boolean`);
    }
    return suggested === "true";
  }
  return suggested;
}

/**
 * The exception a traceback ends on.
 *
 * Matching the whole message classified on whatever appeared first, which is wrong twice
 * over: the frame lines carry paths and line numbers that read like patterns -- a path
 * containing "model" above a `line 404` entry looks like a model 404 -- and a chained
 * traceback reports the handled exception before the one that actually stopped the worker.
 * Python puts the real exception last.
 */
export function finalExceptionLine(errorMessage: string): string {
  const lines = errorMessage
    .split("\n")
    .map(line => line.trim())
    .filter(line => line.length > 0);
  // Frame lines are the `File "...", line N` entries and the source line under them; the
  // exception is the last line that is neither that nor the traceback header.
  for (let i = lines.length - 1; i >= 0; i--) {
    if (!/^File\s+"/.test(lines[i]) && !/^Traceback \(/.test(lines[i])) {
      return lines[i];
    }
  }
  return errorMessage;
}

export function classifyError(errorMessage: string): FixErrorType {
  return PATTERNS.find(([pattern]) => pattern.test(finalExceptionLine(errorMessage)))?.[1] ?? "unsupported";
}

/**
 * Holds the AI-fix loop for one failed operator: classify -> ask the LLM -> apply -> re-run.
 *
 * The state lives here rather than in the frame component because
 * ResultPanelComponent.clearResultPanel() destroys and rebuilds the frames on
 * every re-render, which would otherwise discard an in-flight suggestion.
 */
@Injectable({ providedIn: "root" })
export class AiWorkflowFixerService {
  private readonly stateSubject = new BehaviorSubject<FixState>(IDLE_STATE);

  constructor(
    private workflowActionService: WorkflowActionService,
    private executeWorkflowService: ExecuteWorkflowService,
    private warehouseService: WarehouseService,
    private config: GuiConfigService,
    private operatorMetadataService: OperatorMetadataService
  ) {}

  public getState$(): Observable<FixState> {
    return this.stateSubject.asObservable();
  }

  // Numbers the analyses so a slow reply cannot overwrite a newer one. A frame whose
  // operator does not own the state renders idle, so its Analyze button stays live while
  // another operator is mid-analysis -- two requests can be in flight at once.
  private analysisSeq = 0;

  public async analyzeError(
    operatorId: string,
    errorMessage: string,
    schema: PortSchema | undefined,
    code: string | undefined,
    properties: Readonly<Record<string, unknown>>
  ): Promise<void> {
    const errorType = classifyError(errorMessage);
    const base: FixState = {
      operatorId,
      errorMessage,
      errorType,
      originalCode: code,
      originalProperties: properties,
      status: "analyzing",
    };

    // Out of scope: report it without spending a model call.
    if (errorType === "unsupported") {
      this.stateSubject.next({ ...base, status: "ready" });
      return;
    }

    const seq = ++this.analysisSeq;
    this.stateSubject.next(base);
    try {
      const { text } = await this.callModelWithTimeout(
        buildFixPrompt(errorMessage, code, schema, withoutSecrets(properties, this.schemaProperties(operatorId)))
      );
      if (seq !== this.analysisSeq) {
        return;
      }
      this.stateSubject.next({ ...base, status: "ready", suggestedFix: this.toFix(text, properties) });
    } catch (err) {
      console.error("AI workflow fixer: analysis failed", err);
      if (seq !== this.analysisSeq) {
        return;
      }
      this.stateSubject.next({ ...base, status: "error" });
    }
  }

  /**
   * Applies the suggestion to the live operator and re-runs the workflow.
   * Properties are re-read from the graph so a concurrent edit is not clobbered.
   */
  public async applyFix(): Promise<void> {
    const current = this.stateSubject.getValue();
    const fix = current.suggestedFix;
    if (!fix || current.status !== "ready") {
      return;
    }
    this.stateSubject.next({ ...current, status: "applying" });
    try {
      await this.ensureModifiable();
      const properties: Record<string, unknown> = {
        ...this.workflowActionService.getTexeraGraph().getOperator(current.operatorId).operatorProperties,
      };
      if (fix.type === "code_change") {
        const code = String(properties.code ?? "");
        if (!code.includes(fix.original)) {
          throw new Error("the code changed since the suggestion was generated");
        }
        // split/join replaces every occurrence and keeps the match literal; replace() with a
        // string argument would patch only the first one and leave the rest failing.
        properties.code = code.split(fix.original).join(fix.suggested);
      } else {
        const field = String(fix.fieldName);
        // The field name comes from the model, so it is checked against the operator before
        // anything is written. An invented name would add a dead key and leave the real
        // setting wrong, and "code" would hand the whole UDF source to a property change.
        if (field === "code" || !Object.prototype.hasOwnProperty.call(properties, field)) {
          throw new Error(`${field} is not a configuration field of this operator`);
        }
        // Same staleness guard as the code branch: re-reading the properties keeps the
        // unrelated fields, but the field being replaced can itself have moved on since
        // the suggestion was generated, and overwriting it would discard that newer value.
        if (String(properties[field] ?? "") !== fix.original) {
          throw new Error(`${field} changed since the suggestion was generated`);
        }
        // The model always replies with a string; writing it raw would turn a numeric or
        // boolean setting into one, which the operator then rejects at validation.
        properties[field] = coerceToFieldType(properties[field], fix.suggested);
      }
      this.workflowActionService.setOperatorProperty(current.operatorId, properties);
      if (!this.canStartRun()) {
        // ExecuteWorkflowService refuses the run and shows its own toast; reporting
        // "re-running" here would contradict it.
        this.stateSubject.next({ ...current, status: "applied_without_run" });
        return;
      }
      this.stateSubject.next({ ...current, status: "applied" });
      // Same empty execution name the operator menu uses for an ad-hoc run.
      this.executeWorkflowService.executeWorkflow("");
    } catch (err) {
      console.error("AI workflow fixer: apply failed", err);
      this.stateSubject.next({ ...current, status: "error" });
    }
  }

  /**
   * Mirrors ExecuteWorkflowService's own entry guard: while the deployment requires a
   * warehouse and none is selected, a run request is refused before it starts.
   */
  private canStartRun(): boolean {
    return !this.config.env.warehouseEnabled || this.warehouseService.getSelectedWarehouseIdValue() !== undefined;
  }

  public discardFix(): void {
    this.stateSubject.next(IDLE_STATE);
  }

  // Seam over the `ai` transport: specs spy this instead of mocking the "ai" module,
  // which leaks across specs sharing the import and hangs on a real network call.
  protected callModel(messages: ModelMessage[], abortSignal?: AbortSignal): Promise<{ text: string }> {
    // Built per call so a refreshed access token is used: caching the provider on this root
    // singleton would pin the JWT captured on the first Analyze, and a later call after a
    // token rotation would 401. The /api/chat/* LiteLLM proxy authenticates with the Texera
    // JWT and swaps in the master key upstream, so that token is the only credential sent.
    const model = createOpenAI({
      baseURL: new URL(`${AppSettings.getApiEndpoint()}`, document.baseURI).toString(),
      apiKey: AuthService.getAccessToken() ?? "",
    }).chat(AI_FIXER_MODEL);
    return generateText({ model, messages, abortSignal });
  }

  private callModelWithTimeout(prompt: string): Promise<{ text: string }> {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    const timeout = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => {
        controller.abort();
        reject(new Error("AI fix request timed out"));
      }, AI_FIXER_TIMEOUT_MS);
    });
    return Promise.race([this.callModel([{ role: "user", content: prompt }], controller.signal), timeout]).finally(() =>
      clearTimeout(timer)
    );
  }

  /**
   * Clears the workflow-modification lock before the fix is written.
   *
   * The traceback this panel exists for arrives while the execution is still `Running` --
   * the worker pauses, it does not fail -- and Texera locks modification for as long as a
   * run is live, which every other editing feature checks before writing. Killing the run
   * both lifts the lock and releases the computing unit the paused execution still holds.
   */
  private async ensureModifiable(): Promise<void> {
    if (this.workflowActionService.checkWorkflowModificationEnabled()) {
      return;
    }
    this.executeWorkflowService.killWorkflow();
    await firstValueFrom(
      this.workflowActionService.getWorkflowModificationEnabledStream().pipe(
        filter(enabled => enabled),
        timeout(UNLOCK_TIMEOUT_MS)
      )
    );
  }

  /**
   * The operator's own JSON schema, used to spot the fields it renders as password widgets.
   * Returns nothing when the operator or its schema cannot be resolved -- `withoutSecrets`
   * still drops credential-looking names, so a miss here narrows the filter, never removes it.
   */
  private schemaProperties(operatorId: string): Record<string, unknown> | undefined {
    try {
      const operatorType = this.workflowActionService.getTexeraGraph().getOperator(operatorId).operatorType;
      return this.operatorMetadataService.getOperatorSchema(operatorType).jsonSchema.properties as Record<
        string,
        unknown
      >;
    } catch {
      return undefined;
    }
  }

  /** Maps the model's JSON contract onto SuggestedFix, rejecting incomplete replies. */
  private toFix(raw: string, properties: Readonly<Record<string, unknown>>): SuggestedFix {
    let text = raw.trim();
    const fenced = text.match(/```(?:[a-zA-Z]+)?\s*([\s\S]*?)```/);
    if (fenced) {
      text = fenced[1].trim();
    }
    let parsed: any;
    try {
      parsed = JSON.parse(text);
    } catch (err) {
      throw new Error(`model did not return JSON: ${(err as Error).message}`);
    }
    const { explanation, fix_type, original_snippet, suggested_snippet, confidence } = parsed ?? {};
    if (!explanation || !original_snippet || !suggested_snippet) {
      throw new Error("model response is missing a required field");
    }
    const isProperty = fix_type === "property_change";
    return {
      type: isProperty ? "property_change" : "code_change",
      // For a property change the model returns the field name, so show its current value.
      original: isProperty ? String(properties[original_snippet] ?? "") : original_snippet,
      suggested: suggested_snippet,
      explanation,
      confidence: confidence === "high" || confidence === "low" ? confidence : "medium",
      fieldName: isProperty ? original_snippet : undefined,
    };
  }
}
