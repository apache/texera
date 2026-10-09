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

import { TestBed } from "@angular/core/testing";
import { firstValueFrom, Subject } from "rxjs";
import {
  AiWorkflowFixerService,
  classifyError,
  coerceToFieldType,
  FixState,
  replaceAllIndented,
} from "./ai-workflow-fixer.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { OperatorPortSchemaMap } from "../../../types/workflow-compiling.interface";

const OP = "PythonUDFV2-op-1";
const SCHEMA: OperatorPortSchemaMap = {
  "0_false": [
    { attributeName: "user_email", attributeType: "string" },
    { attributeName: "follower_count", attributeType: "integer" },
  ],
};
const CODE =
  "class ProcessTupleOperator(UDFOperatorV2):\n    def process_tuple(self, t, port):\n        yield t['email']\n";

// A UDF traceback as the Python worker sends it: the whole stack in one string,
// so the regexes must match a line inside it rather than the start of the message.
const KEY_ERROR =
  "Traceback (most recent call last):\n  File \"udf.py\", line 3\n    yield t['email']\nKeyError: 'email'";

describe("AiWorkflowFixerService", () => {
  let service: AiWorkflowFixerService;
  let setOperatorProperty: ReturnType<typeof vi.fn>;
  let executeWorkflow: ReturnType<typeof vi.fn>;
  let killWorkflow: ReturnType<typeof vi.fn>;
  let executionState$: Subject<{ previous: any; current: any }>;
  let operatorProperties: Record<string, unknown>;
  let modificationEnabled: boolean;
  let modificationEnabled$: Subject<boolean>;

  // Stub the transport at the class seam (callModel), as migration-llm.spec.ts does:
  // mocking the "ai" module leaks across specs sharing the import and hits the network.
  const stubModel = (raw: string) => vi.spyOn(service as any, "callModel").mockResolvedValue({ text: raw });
  const state = () => (service as any).stateSubject.getValue() as FixState;

  const suggestion = (over: object = {}) =>
    JSON.stringify({
      explanation: "the column is user_email",
      fix_type: "code_change",
      original_snippet: "yield t['email']",
      suggested_snippet: "yield t['user_email']",
      confidence: "high",
      ...over,
    });

  beforeEach(() => {
    setOperatorProperty = vi.fn();
    executeWorkflow = vi.fn().mockReturnValue(true);
    killWorkflow = vi.fn();
    executionState$ = new Subject<{ previous: any; current: any }>();
    operatorProperties = { code: CODE, workers: 1 };
    modificationEnabled = true;
    modificationEnabled$ = new Subject<boolean>();

    TestBed.configureTestingModule({
      providers: [
        AiWorkflowFixerService,
        {
          provide: WorkflowActionService,
          useValue: {
            setOperatorProperty,
            getTexeraGraph: () => ({ getOperator: () => ({ operatorProperties, operatorType: "PythonUDFV2" }) }),
            // Unlocked by default; the locked path has its own test.
            checkWorkflowModificationEnabled: () => modificationEnabled,
            getWorkflowModificationEnabledStream: () => modificationEnabled$.asObservable(),
          },
        },
        {
          provide: ExecuteWorkflowService,
          useValue: {
            executeWorkflow,
            killWorkflow,
            getExecutionStateStream: () => executionState$.asObservable(),
          },
        },
        ...commonTestProviders,
      ],
    });
    service = TestBed.inject(AiWorkflowFixerService);
  });

  describe("classifyError", () => {
    it("detects each supported pattern", () => {
      expect(classifyError(KEY_ERROR)).toEqual("missing_column");
      expect(classifyError("TypeError: unsupported operand type(s) for *: 'str' and 'int'")).toEqual("type_error");
      expect(classifyError("ValueError: input contains NaN")).toEqual("null_error");
      expect(classifyError("java.lang.NullPointerException: boom")).toEqual("null_error");
      expect(classifyError("ModelNotFound: no such model")).toEqual("model_not_found");
      expect(classifyError("Error code: 404 - model does not exist")).toEqual("model_not_found");
      expect(classifyError("404 Client Error: Not Found for url: .../models/Qwen")).toEqual("model_not_found");
      // A bare 404 is a line number, an unrelated HTTP status, or data -- not a model lookup.
      expect(classifyError('  File "udf.py", line 404, in process_tuple')).toEqual("unsupported");
      expect(classifyError("requests.HTTPError: 404 for https://example.com/report")).toEqual("unsupported");
    });

    it("returns unsupported for out-of-scope, empty, and near-miss messages", () => {
      expect(classifyError("ZeroDivisionError: division by zero")).toEqual("unsupported");
      expect(classifyError("")).toEqual("unsupported");
      expect(classifyError("TypeError: 'int' object is not iterable")).toEqual("unsupported");
    });
  });

  describe("analyzeError", () => {
    it("moves idle -> analyzing -> ready and maps the LLM response into the fix", async () => {
      stubModel(suggestion());
      const seen: string[] = [];
      service.getState$().subscribe(s => seen.push(s.status));

      await service.analyzeError(OP, KEY_ERROR, SCHEMA, CODE, operatorProperties);

      expect(seen).toEqual(["idle", "analyzing", "ready"]);
      expect(state().errorType).toEqual("missing_column");
      expect(state().suggestedFix).toEqual({
        type: "code_change",
        original: "yield t['email']",
        suggested: "yield t['user_email']",
        explanation: "the column is user_email",
        confidence: "high",
        fieldName: undefined,
        // CODE holds the snippet once, and the panel only shows this when it is more.
        occurrences: 1,
      });
    });

    it("tolerates a fenced JSON response even though the prompt forbids markdown", async () => {
      stubModel("```json\n" + suggestion() + "\n```");
      await service.analyzeError(OP, KEY_ERROR, SCHEMA, CODE, operatorProperties);
      expect(state().status).toEqual("ready");
    });

    it("shows the current field value as the original for a property change", async () => {
      operatorProperties = { model: "gpt-4-turb" };
      stubModel(
        suggestion({ fix_type: "property_change", original_snippet: "model", suggested_snippet: "gpt-4-turbo" })
      );

      await service.analyzeError(OP, "Error code: 404 - model not found", SCHEMA, undefined, operatorProperties);

      expect(state().suggestedFix?.fieldName).toEqual("model");
      expect(state().suggestedFix?.original).toEqual("gpt-4-turb");
      expect(state().suggestedFix?.suggested).toEqual("gpt-4-turbo");
    });

    it("never calls the model for an out-of-scope error", async () => {
      const callModel = vi.spyOn(service as any, "callModel");
      await service.analyzeError(OP, "ZeroDivisionError: division by zero", SCHEMA, CODE, operatorProperties);

      expect(callModel).not.toHaveBeenCalled();
      expect(state().errorType).toEqual("unsupported");
      expect(state().status).toEqual("ready");
      expect(state().suggestedFix).toBeUndefined();
    });

    it("ends in error on transport failure, malformed JSON, or a missing field", async () => {
      vi.spyOn(service as any, "callModel").mockRejectedValue(new Error("502"));
      await service.analyzeError(OP, KEY_ERROR, SCHEMA, CODE, operatorProperties);
      expect(state().status).toEqual("error");

      stubModel("I cannot help with that.");
      await service.analyzeError(OP, KEY_ERROR, SCHEMA, CODE, operatorProperties);
      expect(state().status).toEqual("error");

      stubModel(JSON.stringify({ explanation: "e", fix_type: "code_change", confidence: "low" }));
      await service.analyzeError(OP, KEY_ERROR, SCHEMA, CODE, operatorProperties);
      expect(state().status).toEqual("error");
    });
  });

  it("ignores a slow analysis that a newer one has superseded", async () => {
    // A frame whose operator does not own the state renders idle, so its Analyze button
    // stays live while another operator is mid-analysis and two requests can overlap.
    // The first one landing last must not replace the newer result.
    let resolveFirst: (value: { text: string }) => void = () => {};
    const slowFirst = new Promise<{ text: string }>(resolve => (resolveFirst = resolve));
    vi.spyOn(service as any, "callModel")
      .mockReturnValueOnce(slowFirst)
      .mockResolvedValueOnce({ text: suggestion({ explanation: "for the second operator" }) });

    const first = service.analyzeError("op-first", KEY_ERROR, SCHEMA, CODE, operatorProperties);
    await service.analyzeError("op-second", KEY_ERROR, SCHEMA, CODE, operatorProperties);
    expect(state().operatorId).toEqual("op-second");

    resolveFirst({ text: suggestion({ explanation: "for the first operator" }) });
    await first;

    expect(state().operatorId).toEqual("op-second");
    expect(state().suggestedFix?.explanation).toEqual("for the second operator");
  });

  it("never sends a credential property to the model", async () => {
    // Operators keep secrets beside ordinary settings. This asserts the wiring, not the
    // filter: ai-fix-prompt.spec covers withoutSecrets itself, and the bug worth catching
    // here is the service forgetting to call it.
    operatorProperties = { hfApiToken: "hf_live_abc", password: "hunter2", modelId: "gpt-4-turb" };
    const callModel = vi.spyOn(service as any, "callModel").mockResolvedValue({ text: suggestion() });

    await service.analyzeError(OP, "404 model not found", SCHEMA, undefined, operatorProperties);

    const prompt = JSON.stringify(callModel.mock.calls[0][0]);
    expect(prompt).not.toContain("hf_live_abc");
    expect(prompt).not.toContain("hunter2");
    expect(prompt).toContain("gpt-4-turb");
  });

  describe("classifying a real traceback", () => {
    it("reads the exception at the end, not the frames above it", () => {
      // The frame lines carry paths and line numbers that look like patterns on their own.
      const traceback = [
        "Traceback (most recent call last):",
        '  File "/usr/lib/python3/sklearn/model_selection/_split.py", line 404, in split',
        "    n = len(indices) // 0",
        "ZeroDivisionError: integer division or modulo by zero",
      ].join("\n");

      expect(classifyError(traceback)).toEqual("unsupported");
    });

    it("classifies a chained traceback on the exception that stopped the worker", () => {
      const chained = [
        "Traceback (most recent call last):",
        "KeyError: " + String.fromCharCode(39) + "email" + String.fromCharCode(39),
        "",
        "During handling of the above exception, another exception occurred:",
        "",
        "Traceback (most recent call last):",
        "ZeroDivisionError: integer division or modulo by zero",
      ].join("\n");

      expect(classifyError(chained)).toEqual("unsupported");
    });

    it("still classifies the plain case the panel is built for", () => {
      expect(classifyError(KEY_ERROR)).toEqual("missing_column");
    });
  });

  describe("coerceToFieldType", () => {
    it("keeps a numeric field numeric", () => {
      expect(coerceToFieldType(0, "4")).toEqual(4);
      expect(() => coerceToFieldType(0, "four")).toThrow();
    });

    it("keeps a boolean field boolean", () => {
      expect(coerceToFieldType(false, "true")).toEqual(true);
      expect(() => coerceToFieldType(false, "yes")).toThrow();
    });

    it("leaves a string field alone", () => {
      expect(coerceToFieldType("gpt-4-turb", "gpt-4-turbo")).toEqual("gpt-4-turbo");
    });
  });

  it("lets an unsupported error supersede an analysis already in flight", async () => {
    // The unsupported branch answers without calling the model, but it still replaces the
    // panel state, so a slower analysis started earlier must not land on top of it.
    let resolveFirst: (value: { text: string }) => void = () => {};
    const slowFirst = new Promise<{ text: string }>(resolve => (resolveFirst = resolve));
    vi.spyOn(service as any, "callModel").mockReturnValueOnce(slowFirst);

    const first = service.analyzeError("op-first", KEY_ERROR, SCHEMA, CODE, operatorProperties);
    await service.analyzeError("op-second", "ZeroDivisionError: division by zero", SCHEMA, CODE, operatorProperties);
    expect(state().operatorId).toEqual("op-second");
    expect(state().errorType).toEqual("unsupported");

    resolveFirst({ text: suggestion() });
    await first;

    expect(state().operatorId).toEqual("op-second");
    expect(state().errorType).toEqual("unsupported");
  });

  it("rejects a reply whose snippets are not strings", async () => {
    // The template splits these into diff lines; an array would throw inside change
    // detection, leaving the panel on `ready` with nothing rendered and no way out.
    stubModel(
      JSON.stringify({
        explanation: "the column is user_email",
        fix_type: "code_change",
        original_snippet: ["yield t['email']"],
        suggested_snippet: "yield t['user_email']",
        confidence: "high",
      })
    );

    await service.analyzeError(OP, KEY_ERROR, SCHEMA, CODE, operatorProperties);

    expect(state().status).toEqual("error");
    expect(state().suggestedFix).toBeUndefined();
  });

  describe("a finished run clears the panel", () => {
    const ended = (state: string) => executionState$.next({ previous: {}, current: { state } });

    it("drops the applied message once the re-run is over", async () => {
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR, SCHEMA, CODE, operatorProperties);
      await service.applyFix();
      expect(state().status).toEqual("applied");

      ended("Completed");

      // Otherwise a run that failed again brought the tab back still claiming a re-run.
      expect(state().status).toEqual("idle");
    });

    it("drops a suggestion the user never applied", async () => {
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR, SCHEMA, CODE, operatorProperties);
      expect(state().status).toEqual("ready");

      ended("Failed");

      expect(state().status).toEqual("idle");
    });

    it("never flashes the panel back to idle during an apply", async () => {
      // applyFix kills a live execution before it writes, and that Killed state reaches
      // this very subscription. The end state is "applied" either way because the apply
      // publishes last, so what the guard protects is the moment in between: without it
      // the panel drops to idle and offers Analyze while the apply is still running.
      modificationEnabled = false;
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR, SCHEMA, CODE, operatorProperties);
      const seen: string[] = [];
      const watching = service.getState$().subscribe(current => seen.push(current.status));

      const applying = service.applyFix();
      ended("Killed");
      modificationEnabled = true;
      modificationEnabled$.next(true);
      await applying;
      watching.unsubscribe();

      expect(seen).toEqual(["ready", "applying", "applied"]);
      expect(setOperatorProperty).toHaveBeenCalled();
    });
  });

  describe("replaceAllIndented", () => {
    it("indents the continuation lines to the line the match was found on", () => {
      // Without this the first line lands correctly and the rest start at column 0, which
      // is an IndentationError the moment the fix is applied.
      const code = "def f():\n    x = read()\n";

      const out = replaceAllIndented(code, "x = read()", "x = read()\nx = x.strip()").code;

      expect(out).toEqual("def f():\n    x = read()\n    x = x.strip()\n");
    });

    it("keeps the relative indentation the suggestion itself has", () => {
      const code = "    y = 0\n";

      const out = replaceAllIndented(code, "y = 0", "if True:\n    y = 0").code;

      expect(out).toEqual("    if True:\n        y = 0\n");
    });

    it("indents each occurrence at its own depth", () => {
      const code = "  a()\n      a()\n";

      const out = replaceAllIndented(code, "a()", "a()\nb()").code;

      expect(out).toEqual("  a()\n  b()\n      a()\n      b()\n");
    });

    it("counts the places it changed, and leaves a single-line fix alone", () => {
      const out = replaceAllIndented("p()\np()\n", "p()", "q()");

      expect(out.code).toEqual("q()\nq()\n");
      expect(out.count).toEqual(2);
    });
  });

  describe("applyFix", () => {
    it("replaces only the snippet, keeps other properties, and re-runs", async () => {
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR, SCHEMA, CODE, operatorProperties);

      await service.applyFix();

      expect(setOperatorProperty).toHaveBeenCalledWith(OP, {
        code: CODE.replace("yield t['email']", "yield t['user_email']"),
        workers: 1,
      });
      expect(executeWorkflow).toHaveBeenCalledWith("");
      expect(state().status).toEqual("applied");
    });

    it("replaces every occurrence of the snippet, not just the first", async () => {
      // A UDF that reads the same wrong column twice must come back fixed in both places,
      // or the re-run fails again on the second read.
      const twice = CODE + "        yield t['email']\n";
      operatorProperties = { code: twice, workers: 1 };
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR, SCHEMA, twice, operatorProperties);

      await service.applyFix();

      const patched = setOperatorProperty.mock.calls[0][1].code;
      expect(patched).not.toContain("t['email']");
      expect(patched.match(/t\['user_email'\]/g)).toHaveLength(2);
    });

    it("patches the named field for a property change", async () => {
      operatorProperties = { model: "gpt-4-turb", temperature: 0 };
      stubModel(
        suggestion({ fix_type: "property_change", original_snippet: "model", suggested_snippet: "gpt-4-turbo" })
      );
      await service.analyzeError(OP, "404 model not found", SCHEMA, undefined, operatorProperties);

      await service.applyFix();

      expect(setOperatorProperty).toHaveBeenCalledWith(OP, { model: "gpt-4-turbo", temperature: 0 });
      expect(executeWorkflow).toHaveBeenCalledWith("");
      expect(state().status).toEqual("applied");
    });

    it("reports an apply failure, without re-running, when the target field changed since the analysis", async () => {
      operatorProperties = { model: "gpt-4-turb", temperature: 0 };
      stubModel(
        suggestion({ fix_type: "property_change", original_snippet: "model", suggested_snippet: "gpt-4-turbo" })
      );
      await service.analyzeError(OP, "404 model not found", SCHEMA, undefined, operatorProperties);

      // A coeditor picks a different model after the suggestion was generated: applying the
      // stale suggestion would silently discard their choice and re-run on it.
      operatorProperties = { model: "claude-haiku-4.5", temperature: 0 };

      await service.applyFix();

      expect(setOperatorProperty).not.toHaveBeenCalled();
      expect(executeWorkflow).not.toHaveBeenCalled();
      expect(state().status).toEqual("apply_failed");
    });

    it("does nothing without a ready suggestion", async () => {
      await service.applyFix();
      expect(setOperatorProperty).not.toHaveBeenCalled();
      expect(executeWorkflow).not.toHaveBeenCalled();
      expect(state().status).toEqual("idle");
    });

    it("reports an apply failure, without re-running, when the snippet is no longer in the code", async () => {
      stubModel(suggestion({ original_snippet: "a line the user already deleted" }));
      await service.analyzeError(OP, KEY_ERROR, SCHEMA, CODE, operatorProperties);

      await service.applyFix();

      expect(setOperatorProperty).not.toHaveBeenCalled();
      expect(executeWorkflow).not.toHaveBeenCalled();
      expect(state().status).toEqual("apply_failed");
    });

    it("writes the fix but claims no re-run when the deployment refuses one", async () => {
      // Whatever the reason -- no warehouse, a computing unit shutting down -- the panel
      // asks instead of predicting, so it cannot report "re-running" over the toast that
      // says the run was refused.
      executeWorkflow.mockReturnValue(false);
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR, SCHEMA, CODE, operatorProperties);

      await service.applyFix();

      expect(setOperatorProperty).toHaveBeenCalled();
      expect(state().status).toEqual("applied_without_run");
    });

    it("reports the re-run once the deployment accepts it", async () => {
      executeWorkflow.mockReturnValue(true);
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR, SCHEMA, CODE, operatorProperties);

      await service.applyFix();

      expect(executeWorkflow).toHaveBeenCalledWith("");
      expect(state().status).toEqual("applied");
    });

    it("kills the live execution before writing, since modification is locked while it runs", async () => {
      // The traceback this panel exists for arrives while the execution is still Running --
      // the worker pauses, it does not fail -- and Texera locks workflow modification for as
      // long as a run is live. Writing through the lock is what every other editing feature
      // avoids by checking first.
      modificationEnabled = false;
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR, SCHEMA, CODE, operatorProperties);

      const applying = service.applyFix();
      expect(killWorkflow).toHaveBeenCalled();
      expect(setOperatorProperty).not.toHaveBeenCalled();

      modificationEnabled = true;
      modificationEnabled$.next(true);
      await applying;

      expect(setOperatorProperty).toHaveBeenCalled();
      expect(executeWorkflow).toHaveBeenCalledWith("");
    });

    it("explains a lock that never clears, instead of reporting a bare timeout", async () => {
      // rxjs reports this as "Timeout has occurred", which says nothing about what the
      // panel was waiting for.
      vi.useFakeTimers();
      modificationEnabled = false;
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR, SCHEMA, CODE, operatorProperties);

      const applying = service.applyFix();
      await vi.advanceTimersByTimeAsync(20_000);
      await applying;

      expect(setOperatorProperty).not.toHaveBeenCalled();
      expect(state().status).toEqual("apply_failed");
      expect(state().applyError).toContain("stayed locked");
      vi.useRealTimers();
    });

    it("refuses a field the operator does not have", async () => {
      // The staleness guard does not catch this one: an absent field reads as "" and the
      // captured original is "" too, so without the check a dead key is written and the
      // real setting stays wrong.
      operatorProperties = { modelId: "gpt-4-turb", workers: 1 };
      stubModel(suggestion({ fix_type: "property_change", original_snippet: "modelName", suggested_snippet: "Qwen" }));
      await service.analyzeError(OP, "404 model not found", SCHEMA, undefined, operatorProperties);

      await service.applyFix();

      expect(setOperatorProperty).not.toHaveBeenCalled();
      expect(state().status).toEqual("apply_failed");
    });

    it("never rewrites the UDF source through a property change", async () => {
      // Naming "code" would hand the whole operator source to the property branch, which
      // shows a one-line diff and has no snippet guard.
      operatorProperties = { code: "print(1)", workers: 1 };
      stubModel(suggestion({ fix_type: "property_change", original_snippet: "code", suggested_snippet: "print(2)" }));
      await service.analyzeError(OP, "404 model not found", SCHEMA, undefined, operatorProperties);

      await service.applyFix();

      expect(setOperatorProperty).not.toHaveBeenCalled();
      expect(state().status).toEqual("apply_failed");
    });

    it("keeps a numeric setting numeric", async () => {
      // original_snippet carries the field name for a property change; toFix reads the
      // current value from the operator itself.
      operatorProperties = { temperature: 0, workers: 1 };
      stubModel(suggestion({ fix_type: "property_change", original_snippet: "temperature", suggested_snippet: "1" }));
      await service.analyzeError(OP, "404 model not found", SCHEMA, undefined, operatorProperties);

      await service.applyFix();

      expect(setOperatorProperty.mock.calls[0][1].temperature).toEqual(1);
    });

    it("reports an apply failure when the graph rejects the change", async () => {
      setOperatorProperty.mockImplementation(() => {
        throw new Error("read-only workflow");
      });
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR, SCHEMA, CODE, operatorProperties);

      await service.applyFix();

      expect(state().status).toEqual("apply_failed");
      expect(executeWorkflow).not.toHaveBeenCalled();
    });
  });

  it("discards to idle and keeps state across re-subscription", async () => {
    stubModel(suggestion());
    await service.analyzeError(OP, KEY_ERROR, SCHEMA, CODE, operatorProperties);

    // A frame rebuilt by clearResultPanel() re-subscribes and must still see the fix.
    expect((await firstValueFrom(service.getState$())).status).toEqual("ready");

    service.discardFix();
    const after = await firstValueFrom(service.getState$());
    expect(after).toEqual({ operatorId: "", errorType: "unsupported", status: "idle" });
  });
});
