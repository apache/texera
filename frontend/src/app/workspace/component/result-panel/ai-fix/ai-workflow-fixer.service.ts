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
import { BehaviorSubject, Observable } from "rxjs";
import { createOpenAI } from "@ai-sdk/openai";
import { generateText, type ModelMessage } from "ai";
import { AppSettings } from "../../../../common/app-setting";
import { AuthService } from "../../../../common/service/user/auth.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { WarehouseService } from "../../../../common/service/warehouse/warehouse.service";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { PortSchema } from "../../../types/workflow-compiling.interface";
import { buildFixPrompt } from "./ai-fix-prompt";

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

// Model id as LiteLLM exposes it (bin/single-node/litellm-config.yaml), not the provider's id.
export const AI_FIXER_MODEL = "claude-haiku-4.5";
export const AI_FIXER_TIMEOUT_MS = 120_000;
export const UNSUPPORTED_MESSAGE = "This error type is not yet supported for automatic fixing.";

const IDLE_STATE: FixState = { operatorId: "", errorMessage: "", errorType: "unsupported", status: "idle" };

// Each pattern matches a line anywhere inside a traceback, not the whole message.
const PATTERNS: ReadonlyArray<[RegExp, FixErrorType]> = [
  [/KeyError:\s*['"][^'"]+['"]/, "missing_column"],
  [/TypeError:\s*unsupported operand/i, "type_error"],
  [/ValueError:[^\n]*\bNaN\b|NullPointerException/i, "null_error"],
  [/ModelNotFound|\b404\b/i, "model_not_found"],
];

export function classifyError(errorMessage: string): FixErrorType {
  return PATTERNS.find(([pattern]) => pattern.test(errorMessage))?.[1] ?? "unsupported";
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
  private model: any;

  constructor(
    private workflowActionService: WorkflowActionService,
    private executeWorkflowService: ExecuteWorkflowService,
    private warehouseService: WarehouseService,
    private config: GuiConfigService
  ) {}

  public getState$(): Observable<FixState> {
    return this.stateSubject.asObservable();
  }

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

    this.stateSubject.next(base);
    try {
      const { text } = await this.callModelWithTimeout(buildFixPrompt(errorMessage, code, schema, properties));
      this.stateSubject.next({ ...base, status: "ready", suggestedFix: this.toFix(text, properties) });
    } catch (err) {
      console.error("AI workflow fixer: analysis failed", err);
      this.stateSubject.next({ ...base, status: "error" });
    }
  }

  /**
   * Applies the suggestion to the live operator and re-runs the workflow.
   * Properties are re-read from the graph so a concurrent edit is not clobbered.
   */
  public applyFix(): void {
    const current = this.stateSubject.getValue();
    const fix = current.suggestedFix;
    if (!fix || current.status !== "ready") {
      return;
    }
    this.stateSubject.next({ ...current, status: "applying" });
    try {
      const properties: Record<string, unknown> = {
        ...this.workflowActionService.getTexeraGraph().getOperator(current.operatorId).operatorProperties,
      };
      if (fix.type === "code_change") {
        const code = String(properties.code ?? "");
        if (!code.includes(fix.original)) {
          throw new Error("the code changed since the suggestion was generated");
        }
        properties.code = code.replace(fix.original, fix.suggested);
      } else {
        const field = String(fix.fieldName);
        // Same staleness guard as the code branch: re-reading the properties keeps the
        // unrelated fields, but the field being replaced can itself have moved on since
        // the suggestion was generated, and overwriting it would discard that newer value.
        if (String(properties[field] ?? "") !== fix.original) {
          throw new Error(`${field} changed since the suggestion was generated`);
        }
        properties[field] = fix.suggested;
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
    if (!this.model) {
      // The /api/chat/* LiteLLM proxy authenticates with the Texera JWT and swaps in
      // the master key upstream, so the user's access token is the only credential sent.
      this.model = createOpenAI({
        baseURL: new URL(`${AppSettings.getApiEndpoint()}`, document.baseURI).toString(),
        apiKey: AuthService.getAccessToken() ?? "",
      }).chat(AI_FIXER_MODEL);
    }
    return generateText({ model: this.model, messages, abortSignal });
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
