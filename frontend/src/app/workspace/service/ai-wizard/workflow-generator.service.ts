/**
 * AI Wizard workflow generator. Calls Texera's existing LiteLLM proxy at
 * /api/chat/completions (see AccessControlResource.LiteLLMProxyResource).
 * The proxy uses llm.conf credentials server-side — no LLM key ships to
 * the browser, no /aiassistant/openai config-gated path is touched
 * (design-doc §3.5: don't ship LLM keys, reuse working backend).
 */

import { HttpClient } from "@angular/common/http";
import { Injectable, inject } from "@angular/core";
import { firstValueFrom } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { WorkflowContent } from "../../../common/type/workflow";
import { OperatorMetadata } from "../../types/operator-schema.interface";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { WorkflowValidatorService } from "./workflow-validator.service";
import { buildGeneratePrompt, buildModifyPrompt, buildRetryPrompt } from "./prompt-builders";
import { AttemptLog, DataProfile, ValidationResult, WizardState } from "./types";

interface OpenAIResponse {
  choices: { message: { content: string } }[];
}

const MAX_ATTEMPTS = 3;

// Texera's LiteLLM proxy is mounted by AccessControlResource at
// /api/chat/completions. It uses llm.conf for credentials, no extra config
// needed. Default model is claude-haiku-4.5 (fast + cheap); override via
// VITE/ng env if other models become available.
const DEFAULT_MODEL = "claude-haiku-4.5";

export interface GeneratedWorkflow {
  workflow: WorkflowContent;
  whyExplanations: Record<string, string>;
  attempts: AttemptLog[];
}

@Injectable({ providedIn: "root" })
export class WorkflowGeneratorService {
  private readonly http = inject(HttpClient);
  private readonly operatorMetadataService = inject(OperatorMetadataService);
  private readonly validator = inject(WorkflowValidatorService);

  private operatorCatalog: OperatorMetadata | null = null;
  private readonly chatUrl = `${AppSettings.getApiEndpoint()}/chat/completions`;

  constructor() {
    // Cache the operator catalog once; refresh subscriptions still fire on changes.
    this.operatorMetadataService.getOperatorMetadata().subscribe(md => {
      this.operatorCatalog = md;
    });
  }

  public async generate(state: WizardState, model: string = DEFAULT_MODEL): Promise<GeneratedWorkflow> {
    const prompt = buildGeneratePrompt(state, this.operatorCatalog);
    return this.runWithRetry(prompt, model);
  }

  public async modify(
    current: WorkflowContent,
    currentWhy: Record<string, string>,
    instruction: string,
    dataProfile: DataProfile | undefined,
    model: string = DEFAULT_MODEL
  ): Promise<GeneratedWorkflow> {
    const prompt = buildModifyPrompt(current, currentWhy, instruction, this.operatorCatalog, dataProfile);
    return this.runWithRetry(prompt, model);
  }

  private async runWithRetry(basePrompt: string, model: string): Promise<GeneratedWorkflow> {
    const attempts: AttemptLog[] = [];
    let lastParse: { workflow: WorkflowContent; whyExplanations: Record<string, string> } | null = null;
    let lastValidation: ValidationResult | null = null;

    for (let i = 1; i <= MAX_ATTEMPTS; i++) {
      const prompt = i === 1 ? basePrompt : buildRetryPrompt(basePrompt, lastParse!.workflow, lastValidation!);

      let responseText: string;
      try {
        const body = {
          model,
          messages: [
            {
              role: "system",
              content:
                "You are a Texera workflow generation expert. Your response MUST be a single raw JSON object — no markdown fences, no commentary before or after. Start with { and end with }. " +
                "For every operator's operatorProperties, fill ALL required keys AND every required sub-key inside arrays/objects. Empty arrays, empty strings, and missing fields are NOT acceptable. " +
                "When a property has an enum (shown as 'enum: a | b | c' in the catalog), use one of those exact values. " +
                "When a property is a column name (e.g. Aggregate.aggregations[].attribute, Filter.predicates[].attribute, Aggregate.groupByKeys, BarChart axes, sklearn target columns, TablesPlot.'add attribute'[].attributeName), use a real column name from the Data Profile — match capitalization exactly. " +
                "NEVER use 'dummyProperty', 'dummyValue', or 'dummyPropertyList' — these are Texera scaffolding placeholders and have NO place in a real workflow. Always use the real property names shown in the operator catalog and few-shot examples (e.g. 'aggregations' / 'predicates' / 'add attribute', not 'dummyPropertyList').",
            },
            { role: "user", content: prompt },
          ],
          max_tokens: 8192,
          temperature: 0.2,
        };
        const resp = await firstValueFrom(this.http.post<OpenAIResponse>(this.chatUrl, body));
        responseText = resp?.choices?.[0]?.message?.content?.trim() ?? "";
      } catch (httpErr: any) {
        const status = httpErr?.status;
        const detail = httpErr?.error
          ? typeof httpErr.error === "string"
            ? httpErr.error
            : JSON.stringify(httpErr.error)
          : httpErr?.message;
        if (status === 401 || status === 403) {
          throw new Error(
            `LLM call failed: HTTP ${status}. You must be logged in to Texera, and copilotEnabled must be true.`
          );
        }
        throw new Error(`LLM call failed: HTTP ${status ?? "unknown"} — ${detail ?? "no detail"}`);
      }
      if (!responseText) {
        throw new Error(`LLM returned an empty response from ${this.chatUrl}. Check backend logs.`);
      }
      // Sanitize: drop Texera's dummyPropertyList placeholders if the LLM
      // ignored the instructions. Done client-side so a generation is rescued
      // even if the model emits them; the next attempt won't fight us.
      responseText = stripDummyProperties(responseText);
      const parsed = this.extractWorkflowJson(responseText);
      const validation = this.validator.validate(parsed.workflow, this.operatorCatalog);

      attempts.push({
        attempt: i,
        errorCount: validation.errors.length,
        errors: validation.errors.map(e => `${e.field}: ${e.message}`),
      });

      if (validation.isValid) {
        return { ...parsed, attempts };
      }
      lastParse = parsed;
      lastValidation = validation;
    }

    const final = lastValidation!.errors.map(e => `${e.field}: ${e.message}`).join("\n");
    const err = new Error(`Workflow failed validation after ${MAX_ATTEMPTS} attempts:\n${final}`);
    (err as any).attempts = attempts;
    throw err;
  }

  private extractWorkflowJson(text: string): {
    workflow: WorkflowContent;
    whyExplanations: Record<string, string>;
  } {
    const json = this.tryExtractJsonString(text);
    let parsed: any;
    try {
      parsed = JSON.parse(json);
    } catch (e) {
      console.error("Failed to parse LLM response as JSON.", { error: e, rawResponse: text, attempted: json });
      const preview = text.length > 400 ? `${text.slice(0, 400)}…` : text;
      throw new Error(
        `Failed to parse workflow JSON from LLM response. ` +
          `The LLM returned text that wasn't valid JSON. First 400 chars: ${preview}`
      );
    }

    const whyExplanations: Record<string, string> =
      parsed.whyExplanations && typeof parsed.whyExplanations === "object" ? parsed.whyExplanations : {};
    const { whyExplanations: _drop, ...workflowOnly } = parsed;
    return { workflow: workflowOnly as WorkflowContent, whyExplanations };
  }

  /**
   * Pull the JSON block out of an LLM response that may include markdown
   * fences, prose before/after, or a "Here is the workflow:" preamble.
   * Falls back to the original text if no clear block boundaries are found.
   */
  // (stripDummyProperties is a free function, see end of file)
  private tryExtractJsonString(raw: string): string {
    let s = raw.trim();
    // Strip markdown fences if present.
    const fenceMatch = s.match(/```(?:json)?\s*([\s\S]*?)```/);
    if (fenceMatch) s = fenceMatch[1].trim();
    // Slice from first { to matching last } (handles trailing prose).
    const first = s.indexOf("{");
    const last = s.lastIndexOf("}");
    if (first >= 0 && last > first) {
      return s.slice(first, last + 1);
    }
    return s;
  }
}

/**
 * Strip Texera-scaffolding "dummy*" property keys from an LLM-generated workflow
 * JSON string. Done as a post-processing safety net so the user never sees a
 * generated Aggregate / TablesPlot filled with dummyProperty / dummyValue.
 */
function stripDummyProperties(rawJson: string): string {
  try {
    const obj = JSON.parse(rawJson);
    walk(obj);
    return JSON.stringify(obj);
  } catch {
    return rawJson;
  }
}

function walk(node: any): void {
  if (Array.isArray(node)) {
    node.forEach(walk);
    return;
  }
  if (node && typeof node === "object") {
    for (const key of Object.keys(node)) {
      if (/^dummy/i.test(key)) {
        delete node[key];
        continue;
      }
      walk(node[key]);
    }
  }
}
