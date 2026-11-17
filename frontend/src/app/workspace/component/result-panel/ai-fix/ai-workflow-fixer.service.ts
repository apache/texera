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
import { OperatorPortSchemaMap } from "../../../types/workflow-compiling.interface";
import { ExecutionState } from "../../../types/execute-workflow.interface";
import { buildFixPrompt } from "./ai-fix-prompt";
import { WorkflowConsoleService } from "../../../service/workflow-console/workflow-console.service";
import { GuiConfigService } from "../../../../common/service/gui-config.service";

export type FixErrorType = "missing_column" | "type_error" | "null_error" | "unsupported";
/**
 * One operator error, kept as the two parts its source already distinguishes.
 *
 * Classification reads `header` and the prompt gets `full`: collapsing them would put the
 * burden of finding the exception inside the joined text back on this service.
 */
export interface OperatorError {
  /** The exception line on its own -- a console message's `title`, a fatal error's `message`. */
  header: string;
  /** Exception and stack together, which is what the model is shown. */
  full: string;
}

export type FixConfidence = "high" | "medium" | "low";

export interface SuggestedFix {
  /** How many places Apply will touch; the diff only ever shows one -/+ pair. */
  occurrences?: number;
  /** Whole lines quoted from the operator's code, and what to put in their place. */
  original: string;
  suggested: string;
  explanation: string;
  /** Rendered as the confidence badge. */
  confidence: FixConfidence;
}

export interface FixState {
  operatorId: string;
  /**
   * Identifies the ERROR the suggestion was made for, so a newer one on the same operator
   * can clear it. A terminal execution state is not enough on its own: the case this panel
   * exists for leaves the run `Running` -- the UDF raises and the worker pauses -- so a
   * re-run that fails again never reaches one, and the error key is the only signal left.
   */
  errorKey?: string;
  /**
   * The workflow the analysis belongs to. The operator id alone does not identify it: two
   * imports of the same JSON carry the same ids, so a reply that arrived after the user
   * switched workflows could be applied to the wrong one.
   */
  workflowId?: number;
  suggestedFix?: SuggestedFix;
  // `unsupported`: the error is outside the three shapes this panel handles, reported without
  // spending a model call. `applied_without_run`: the fix was written but the deployment
  // refused to start a run (a warehouse is required and none is selected), so the panel must
  // not claim one. `apply_failed` keeps the suggestion on screen: the analysis was fine, the
  // write was refused, and re-analysing would spend another model call for the same fix.
  status:
    | "idle"
    | "analyzing"
    | "unsupported"
    | "ready"
    | "applying"
    | "applied"
    | "applied_without_run"
    | "error"
    | "apply_failed";
  /** Why the write was refused, shown as-is when the status is `apply_failed`. */
  applyError?: string;
}

export const AI_FIXER_TIMEOUT_MS = 120_000;
export const UNLOCK_TIMEOUT_MS = 15_000;

// A run in one of these is live enough to be worth stopping; a lock coming from anything
// else is not something killing an execution can clear.
const LIVE_STATES: ReadonlySet<ExecutionState> = new Set([
  ExecutionState.Initializing,
  ExecutionState.Running,
  ExecutionState.Pausing,
  ExecutionState.Paused,
  ExecutionState.Resuming,
  ExecutionState.Recovering,
]);

// A run that reached one of these is over, so anything the panel still shows predates it.
// `Uninitialized` is deliberately absent even though updateWorkflowActionLock treats it as
// over: a kill lands there, and a kill is how Apply itself clears the lock, so resetting on
// it would throw away a suggestion that is still the right one for the error on screen.
const TERMINAL_STATES: ReadonlySet<ExecutionState> = new Set([
  ExecutionState.Completed,
  ExecutionState.Failed,
  ExecutionState.Killed,
  ExecutionState.Terminated,
]);

export const UNSUPPORTED_MESSAGE = "This error type is not yet supported for automatic fixing.";

export const IDLE_STATE: FixState = { operatorId: "", status: "idle" };

// Matched against the exception line only, never the frame lines above it.
const PATTERNS: ReadonlyArray<[RegExp, FixErrorType]> = [
  [/KeyError:\s*['"][^'"]+['"]/, "missing_column"],
  [/TypeError:\s*unsupported operand/i, "type_error"],
  [/ValueError:[^\n]*\bNaN\b/i, "null_error"],
];

/**
 * Replaces every occurrence of a snippet, and reports how many there were.
 *
 * No indentation work: the prompt requires whole lines carrying the indentation they should
 * have, and `matchesWholeLines` turns away a reply that does not, so each line of the
 * suggestion already sits at the right column. Re-indenting continuation lines here instead
 * meant guessing which convention the model had used -- absolute or relative -- and the
 * prompt never stated one, so either guess doubled the indentation for the other.
 */
export function replaceAll(code: string, original: string, suggested: string): { code: string; count: number } {
  const parts = code.split(original);
  return { code: parts.join(suggested), count: parts.length - 1 };
}

/**
 * Whether every occurrence of the snippet covers whole lines.
 *
 * Both ends matter, for different reasons. Starting a line is what makes the indentation
 * unambiguous: the snippet carries its own, so the suggestion carries the indentation it
 * wants and nothing is inferred. Ending a line is what keeps the replacement honest -- a
 * snippet that begins a line but stops inside it still replaces cleanly, so `        n = 1`
 * matches `        n = 10` once, shows no occurrence badge, and leaves the trailing `0`
 * behind: the diff offers `n = 1` -> `n = 2` and the operator is left saying `n = 20`.
 */
export function matchesWholeLines(code: string, original: string): boolean {
  let from = 0;
  for (;;) {
    const at = code.indexOf(original, from);
    if (at === -1) {
      return true;
    }
    const end = at + original.length;
    const startsLine = at === 0 || code[at - 1] === "\n";
    const endsLine = end === code.length || code[end] === "\n";
    if (!startsLine || !endsLine) {
      return false;
    }
    from = end;
  }
}

/**
 * Classifies on the exception alone, never on the stack around it.
 *
 * Both sources already separate the exception from its frames, so nothing here has to find
 * it: a console message carries `err.toString` / the traceback's last line in `title`, and a
 * fatal error carries it in `message`. Deducing it from the joined text instead meant
 * guessing at frame shapes, and the JDK's own are unguessable -- on Java 9+ they carry a
 * module prefix (`java.base/java.lang.Thread.run(Thread.java:840)`) and native ones read
 * `(Native Method)`, so a real worker trace ended on a frame no pattern recognised and
 * `NullPointerException` never classified. Python's `title` is the LAST line of the formatted
 * traceback, so a chained one arrives already reduced to the exception that stopped the
 * worker.
 */
export function classifyError(exceptionHeader: string): FixErrorType {
  return PATTERNS.find(([pattern]) => pattern.test(exceptionHeader))?.[1] ?? "unsupported";
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
    private config: GuiConfigService,
    private workflowActionService: WorkflowActionService,
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowConsoleService: WorkflowConsoleService
  ) {
    // The panel holds whatever it last showed until something clears it, and after Apply
    // that is the green "re-running..." message. Resetting when the run ends -- rather than
    // when the next one starts -- keeps that message truthful for exactly as long as the run
    // it describes, so a run that fails again does not reopen the tab on a finished claim.
    this.executeWorkflowService
      .getExecutionStateStream()
      .pipe(
        filter(event => TERMINAL_STATES.has(event.current.state)),
        // applyFix kills a live execution before it writes; that kill must not wipe the
        // state the apply is in the middle of publishing.
        filter(() => this.stateSubject.getValue().status !== "applying")
      )
      .subscribe(() => this.reset());

    // A re-run that fails again never reaches a terminal state, so the subscription above
    // never fires for it. The operator reporting a different error is the signal that
    // whatever the panel is showing belongs to a run that has already been superseded.
    this.workflowConsoleService.getConsoleMessageUpdateStream().subscribe(() => {
      const current = this.stateSubject.getValue();
      if (current.status === "idle" || current.status === "analyzing" || current.status === "applying") {
        return;
      }
      const latest = this.latestErrorKey(current.operatorId);
      if (latest && latest !== current.errorKey) {
        this.reset();
      }
    });
  }

  public getState$(): Observable<FixState> {
    return this.stateSubject.asObservable();
  }

  // Numbers the analyses so a slow reply cannot overwrite a newer one. A frame whose
  // operator does not own the state renders idle, so its Analyze button stays live while
  // another operator is mid-analysis -- two requests can be in flight at once.
  private analysisSeq = 0;

  public async analyzeError(
    operatorId: string,
    error: OperatorError,
    inputSchemas: OperatorPortSchemaMap | undefined,
    code: string | undefined
  ): Promise<void> {
    const errorType = classifyError(error.header);
    const workflowId = this.workflowActionService.getOpenWorkflowId();
    const base: FixState = {
      operatorId,
      workflowId,
      errorKey: this.latestErrorKey(operatorId),
      status: "analyzing",
    };

    // Numbered before the early return as well: this still supersedes whatever is in flight,
    // and without the bump a slower analysis for another operator would land on top of it.
    const seq = ++this.analysisSeq;

    // Out of scope: report it without spending a model call.
    if (errorType === "unsupported") {
      this.stateSubject.next({ ...base, status: "unsupported" });
      return;
    }

    this.stateSubject.next(base);
    try {
      const { text } = await this.callModelWithTimeout(buildFixPrompt(error.full, code, inputSchemas));
      if (seq !== this.analysisSeq || workflowId !== this.workflowActionService.getOpenWorkflowId()) {
        return;
      }
      const suggestedFix = this.toFix(text);
      // The prompt asks for whole lines with their indentation; a snippet starting mid-line
      // means the model ignored that, and applying it would put the lines after the first
      // at a column nothing here can work out.
      if (!matchesWholeLines(code ?? "", suggestedFix.original)) {
        throw new Error("the model did not quote whole lines, so the fix cannot be placed");
      }
      // Counted here so the panel can say it before Apply: a short snippet matched as plain
      // text can appear in lines the user never saw in the diff.
      const occurrences = replaceAll(code ?? "", suggestedFix.original, suggestedFix.suggested).count;
      // Zero means the model quoted something the operator does not contain. The panel
      // only surfaces this count when it is above one, so such a suggestion is
      // indistinguishable from any other, and Apply turns it away as a stale snippet --
      // blaming an edit that never happened.
      if (occurrences === 0) {
        throw new Error(`the code does not contain ${JSON.stringify(suggestedFix.original)}`);
      }
      suggestedFix.occurrences = occurrences;
      this.stateSubject.next({ ...base, status: "ready", suggestedFix });
    } catch (err) {
      console.error("AI workflow fixer: analysis failed", err);
      if (seq !== this.analysisSeq || workflowId !== this.workflowActionService.getOpenWorkflowId()) {
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
    // `apply_failed` is retryable: the panel keeps the suggestion and its Apply button, so
    // refusing here left the user clicking something that silently did nothing. Everything
    // below re-reads the operator and re-runs every check, so a retry is not a shortcut.
    if (!fix || (current.status !== "ready" && current.status !== "apply_failed")) {
      return;
    }
    // Read, not bumped. The unlock wait below can take seconds and an analysis started in
    // the meantime owns the panel, so this apply must not report over it. (Bumping would be
    // equivalent today -- an analysis in flight has already left the status at `analyzing`,
    // which the guard above turns away -- but reading says what is meant.)
    const seq = this.analysisSeq;
    this.stateSubject.next({ ...current, status: "applying" });
    try {
      this.assertSameWorkflow(current);
      // Run before the kill as well as after it. ensureModifiable stops the paused execution
      // to clear the modification lock, so a fix that was going to be refused anyway cost the
      // user their run -- and then the unlock wait before the refusal even reached the panel.
      this.patchedProperties(current, fix);
      await this.ensureModifiable();
      // Checked again: ensureModifiable can wait seconds for a kill to clear the lock, and
      // the user is free to open another workflow while it does.
      this.assertSameWorkflow(current);
      // Re-read and re-validated rather than reusing the pre-flight result, which describes
      // the operator as it was before that wait.
      const properties = this.patchedProperties(current, fix);
      this.workflowActionService.setOperatorProperty(current.operatorId, properties);
      // Asked rather than predicted: mirroring ExecuteWorkflowService's refusal conditions
      // here means every one it gains has to be repeated, and missing one reports
      // "re-running" over a toast saying the opposite. The empty execution name is the one
      // the operator menu uses for an ad-hoc run.
      const started = this.executeWorkflowService.executeWorkflow("");
      if (seq !== this.analysisSeq) {
        return;
      }
      this.stateSubject.next({ ...current, status: started ? "applied" : "applied_without_run" });
    } catch (err) {
      console.error("AI workflow fixer: apply failed", err);
      // Not the same as a failed analysis: the suggestion is still good, the operator moved
      // under it. Reporting "could not suggest a fix" and hiding the diff sent the user back
      // for another model call to get the fix they already had.
      if (seq !== this.analysisSeq) {
        return;
      }
      this.stateSubject.next({
        ...current,
        status: "apply_failed",
        applyError: (err as Error).message,
      });
    }
  }

  /**
   * The operator's properties with the fix written into them, or a refusal.
   *
   * Reads only the graph and returns a fresh object, so it is safe to run twice: once as a
   * pre-flight before ensureModifiable, so a refusal does not cost the user the execution
   * that call kills, and once after it, because the operator can move during the wait.
   */
  private patchedProperties(current: FixState, fix: SuggestedFix): Record<string, unknown> {
    const properties: Record<string, unknown> = {
      ...this.workflowActionService.getTexeraGraph().getOperator(current.operatorId).operatorProperties,
    };
    const code = String(properties.code ?? "");
    const patched = replaceAll(code, fix.original, fix.suggested);
    if (patched.count === 0) {
      throw new Error("the code changed since the suggestion was generated");
    }
    // Counted, not just found. The count behind the "changes N places" badge was taken at
    // the analysis, and this replaces every match -- so a copy added or removed in between
    // means the write touches a different number of lines than the panel promised. A kill
    // lifts the modification lock without resetting the panel, so the user can edit the
    // UDF with the suggestion still on screen.
    if (fix.occurrences !== undefined && patched.count !== fix.occurrences) {
      const places = (n: number) => `${n} place${n === 1 ? "" : "s"}`;
      throw new Error(
        `the snippet is now in ${places(patched.count)} instead of ${places(fix.occurrences)}, ` +
          "so the fix was not applied"
      );
    }
    properties.code = patched.code;
    return properties;
  }

  public discardFix(): void {
    this.reset();
  }

  /**
   * Clears the panel and supersedes whatever analysis is in flight.
   *
   * Publishing IDLE is not enough on its own: a reply still in flight matches the sequence
   * it started with and repaints a suggestion over the cleared panel. Reachable by analysing
   * a failed operator and then stopping the run before the model answers.
   */
  private reset(): void {
    this.analysisSeq++;
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
      // Named as the LiteLLM proxy exposes it, not as the provider does, and read from the
      // deployment's own config: a proxy publishing a different alias would otherwise need a
      // rebuild to be reachable.
    }).chat(this.config.env.aiFixModel);
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

  /** The operator's most recent ERROR, as a value that changes when a new one arrives. */
  private latestErrorKey(operatorId: string): string | undefined {
    const last = (this.workflowConsoleService.getConsoleMessages(operatorId) ?? [])
      .filter(message => message.msgType?.name === "ERROR")
      .pop();
    return last ? `${last.timestamp.seconds}.${last.timestamp.nanos}:${last.title}` : undefined;
  }

  /**
   * Refuses to touch a graph the suggestion was not made against.
   *
   * The service outlives the workspace, so a suggestion survives a switch to another
   * workflow -- and two imports of the same JSON share operator ids, so the id alone would
   * let the write land on the wrong graph.
   */
  private assertSameWorkflow(state: FixState): void {
    if (state.workflowId !== this.workflowActionService.getOpenWorkflowId()) {
      throw new Error("the workflow changed since the suggestion was generated");
    }
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
    // enableWorkflowModification() is a no-op while the workflow is read-only, so the unlock
    // waited for below never arrives. Running only needs write access to the computing unit,
    // so a read-only user can reach a paused run: refused here rather than below, or their
    // run is killed on their behalf and they wait fifteen seconds to hear it would not stop.
    if (this.workflowActionService.getWorkflowMetadata().readonly) {
      throw new Error("this workflow is read-only, so the fix cannot be written");
    }
    // The lock has other causes -- loading, a version preview -- and killing then both fails
    // and reports the wrong reason.
    if (!LIVE_STATES.has(this.executeWorkflowService.getExecutionState().state)) {
      throw new Error("the workflow is locked by something other than a running execution");
    }
    try {
      this.executeWorkflowService.killWorkflow();
    } catch (err) {
      throw new Error(`the running execution could not be stopped: ${(err as Error).message}`);
    }
    try {
      await firstValueFrom(
        this.workflowActionService.getWorkflowModificationEnabledStream().pipe(
          filter(enabled => enabled),
          timeout(UNLOCK_TIMEOUT_MS)
        )
      );
    } catch {
      // rxjs reports this as "Timeout has occurred", which tells the user nothing about
      // what the panel was waiting for.
      throw new Error("the running execution did not stop, so the workflow stayed locked");
    }
  }

  /** Maps the model's JSON contract onto SuggestedFix, rejecting incomplete replies. */
  private toFix(raw: string): SuggestedFix {
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
    const { explanation, original_snippet, suggested_snippet, confidence } = parsed ?? {};
    if (!explanation || !original_snippet || !suggested_snippet) {
      throw new Error("model response is missing a required field");
    }
    // Checked as well as present: the template splits these into diff lines, and a reply
    // that sends an array or an object would throw inside change detection, which leaves the
    // panel stuck on `ready` with nothing rendered and no way back.
    for (const [name, value] of [
      ["explanation", explanation],
      ["original_snippet", original_snippet],
      ["suggested_snippet", suggested_snippet],
    ] as const) {
      if (typeof value !== "string") {
        throw new Error(`model returned ${typeof value} for ${name}, expected a string`);
      }
    }
    // One trailing newline off each, independently. The prompt asks for whole lines and does
    // not settle whether the final newline is part of the quote, so both forms arrive. Kept,
    // it makes the verdict depend on where the line sits: `code[end]` is the next line's
    // first character everywhere except the last line of the file, which passes. Dropped
    // from the replacement too, so the line's own newline stays put and nothing is merged.
    const withoutTrailingNewline = (snippet: string) => (snippet.endsWith("\n") ? snippet.slice(0, -1) : snippet);
    return {
      original: withoutTrailingNewline(original_snippet),
      suggested: withoutTrailingNewline(suggested_snippet),
      explanation,
      confidence: confidence === "high" || confidence === "low" ? confidence : "medium",
    };
  }
}
