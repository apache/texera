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
  FixState,
  replaceAll,
  matchesWholeLines,
  OperatorError,
} from "./ai-workflow-fixer.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { WorkflowConsoleService } from "../../../service/workflow-console/workflow-console.service";
import { OperatorPortSchemaMap } from "../../../types/workflow-compiling.interface";
import { ExecutionState } from "../../../types/execute-workflow.interface";

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

const QUOTE = String.fromCharCode(39);
const NL = String.fromCharCode(10);

// Both sources hand the exception over on its own, so a test supplies it the same way:
// `header` is what classification reads, `full` is what the model is shown.
const err = (header: string, full: string = header): OperatorError => ({ header, full });
const KEY_ERROR_EVENT = err("KeyError: " + QUOTE + "email" + QUOTE, KEY_ERROR);

describe("AiWorkflowFixerService", () => {
  let service: AiWorkflowFixerService;
  let setOperatorProperty: ReturnType<typeof vi.fn>;
  let executeWorkflow: ReturnType<typeof vi.fn>;
  let killWorkflow: ReturnType<typeof vi.fn>;
  let executionState$: Subject<{ previous: any; current: any }>;
  let operatorProperties: Record<string, unknown>;
  let modificationEnabled: boolean;
  let modificationEnabled$: Subject<boolean>;
  let executionState: ExecutionState;
  let openWorkflowId: number | undefined;
  let workflowReadonly: boolean;
  let operatorSchemaProperties: Record<string, unknown>;
  let consoleMessages: Array<{
    msgType: { name: string };
    timestamp: { seconds: number; nanos: number };
    title: string;
  }>;
  let consoleUpdates$: Subject<void>;

  // Stub the transport at the class seam (callModel), as migration-llm.spec.ts does:
  // mocking the "ai" module leaks across specs sharing the import and hits the network.
  const stubModel = (raw: string) => vi.spyOn(service as any, "callModel").mockResolvedValue({ text: raw });
  const state = () => (service as any).stateSubject.getValue() as FixState;

  const suggestion = (over: object = {}) =>
    JSON.stringify({
      explanation: "the column is user_email",
      fix_type: "code_change",
      // Whole lines with their indentation, which is what the prompt requires.
      original_snippet: "        yield t['email']",
      suggested_snippet: "        yield t['user_email']",
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
    // A live run is what holds the modification lock in production, so every test that
    // sets modificationEnabled = false needs one for the lock to be the kind ensureModifiable
    // is willing to break.
    executionState = ExecutionState.Running;
    openWorkflowId = 11;
    workflowReadonly = false;
    operatorSchemaProperties = {};
    consoleMessages = [];
    consoleUpdates$ = new Subject<void>();

    TestBed.configureTestingModule({
      providers: [
        AiWorkflowFixerService,
        {
          provide: WorkflowActionService,
          useValue: {
            setOperatorProperty,
            getTexeraGraph: () => ({ getOperator: () => ({ operatorProperties, operatorType: "PythonUDFV2" }) }),
            getOpenWorkflowId: () => openWorkflowId,
            getWorkflowMetadata: () => ({ readonly: workflowReadonly }),
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
            getExecutionState: () => ({ state: executionState }),
            getExecutionStateStream: () => executionState$.asObservable(),
          },
        },
        {
          provide: WorkflowConsoleService,
          useValue: {
            getConsoleMessages: () => consoleMessages,
            getConsoleMessageUpdateStream: () => consoleUpdates$.asObservable(),
          },
        },
        {
          // Stubbed rather than left to the real service, which fetches operator metadata
          // from the constructor: 46 TestBeds meant 46 live requests, and the suite died
          // waiting on them instead of reporting.
          provide: OperatorMetadataService,
          useValue: { getOperatorSchema: () => ({ jsonSchema: { properties: operatorSchemaProperties } }) },
        },
        ...commonTestProviders,
      ],
    });
    service = TestBed.inject(AiWorkflowFixerService);
  });

  describe("classifyError", () => {
    it("detects each supported pattern", () => {
      expect(classifyError(KEY_ERROR_EVENT.header)).toEqual("missing_column");
      expect(classifyError("TypeError: unsupported operand type(s) for *: 'str' and 'int'")).toEqual("type_error");
      expect(classifyError("ValueError: input contains NaN")).toEqual("null_error");
      // A bare 404 is a line number, an unrelated HTTP status, or data -- not a model lookup.
      expect(classifyError('  File "udf.py", line 404, in process_tuple')).toEqual("unsupported");
      expect(classifyError("requests.HTTPError: 404 for https://example.com/report")).toEqual("unsupported");
    });

    it("returns unsupported for out-of-scope, empty, and near-miss messages", () => {
      expect(classifyError("ZeroDivisionError: division by zero")).toEqual("unsupported");
      // Only a JVM operator raises this, and a JVM operator has no code for Apply to patch.
      expect(classifyError("java.lang.NullPointerException: boom")).toEqual("unsupported");
      expect(classifyError("")).toEqual("unsupported");
      expect(classifyError("TypeError: 'int' object is not iterable")).toEqual("unsupported");
    });
  });

  describe("analyzeError", () => {
    it("moves idle -> analyzing -> ready and maps the LLM response into the fix", async () => {
      stubModel(suggestion());
      const seen: string[] = [];
      service.getState$().subscribe(s => seen.push(s.status));

      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

      expect(seen).toEqual(["idle", "analyzing", "ready"]);
      expect(state().suggestedFix).toEqual({
        original: "        yield t['email']",
        suggested: "        yield t['user_email']",
        explanation: "the column is user_email",
        confidence: "high",
        // CODE holds the snippet once, and the panel only shows this when it is more.
        occurrences: 1,
      });
    });

    it("accepts snippets quoted with their trailing newline", async () => {
      // Whether the model includes the final newline is not something the prompt settles,
      // and with it `code[end]` is the first character of the next line -- so the same valid
      // reply was refused mid-file and accepted on the last line. One trailing newline is
      // dropped from each snippet, independently, so neither the check nor the replacement
      // sees it and no lines are merged.
      // A line in the MIDDLE of the file: quoted with its newline, the next character is the
      // start of the following line rather than a newline. The last line of a file passes
      // either way, which is what hid this.
      const code = "def f():" + NL + "    a = read()" + NL + "    yield a" + NL;
      stubModel(suggestion({ original_snippet: "    a = read()" + NL, suggested_snippet: "    a = f()" + NL }));

      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, code);

      expect(state().status).toEqual("ready");
      expect(state().suggestedFix?.original).toEqual("    a = read()");
      expect(state().suggestedFix?.suggested).toEqual("    a = f()");
    });

    it("still refuses a mid-line match quoted with a newline", async () => {
      // Tolerating the newline must not tolerate what the end-of-line check was added for:
      // once it is dropped, `n = 1` is again only the start of `n = 10`.
      const code = "def f():\n        n = 10\n";
      stubModel(suggestion({ original_snippet: "        n = 1\n", suggested_snippet: "        n = 2\n" }));

      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, code);

      expect(state().status).toEqual("error");
    });
    it("refuses a snippet quoted from the middle of a line", async () => {
      // The prompt asks for whole lines carrying their indentation. A mid-line snippet says
      // nothing about the column its continuation lines belong at, and inferring one is the
      // guess that put `yield` at sixteen spaces in an eight-space body.
      stubModel(suggestion({ original_snippet: "yield t['email']", suggested_snippet: "yield t['user_email']" }));

      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

      expect(state().status).toEqual("error");
      expect(state().suggestedFix).toBeUndefined();
    });

    it("reports a snippet that is not in the code instead of offering an Apply that cannot work", async () => {
      // The panel only shows the occurrence count when it is above one, so a snippet the
      // model invented looked like any other suggestion. Apply then failed with "the code
      // changed since the suggestion was generated", which blames an edit that never
      // happened.
      stubModel(suggestion({ original_snippet: "        yield t['address']" }));

      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

      expect(state().status).toEqual("error");
      expect(state().suggestedFix).toBeUndefined();
    });

    it("refuses a reply that names a configuration field instead of quoting code", async () => {
      // Not by reading fix_type -- that field is gone, since one legal value could only ever
      // refuse a good reply. A field name is not in the code, so the occurrence count turns
      // it away on its own.
      stubModel(suggestion({ fix_type: "property_change", original_snippet: "modelId", suggested_snippet: "gpt-4" }));

      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

      expect(state().status).toEqual("error");
      expect(state().suggestedFix).toBeUndefined();
    });

    it("accepts a reply that omits fix_type", async () => {
      // There is one kind of fix, so demanding the field only added a way for a good answer
      // to be thrown away.
      const { fix_type, ...withoutKind } = JSON.parse(suggestion());
      stubModel(JSON.stringify(withoutKind));

      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

      expect(state().status).toEqual("ready");
    });

    it("tolerates a fenced JSON response even though the prompt forbids markdown", async () => {
      stubModel("```json\n" + suggestion() + "\n```");
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);
      expect(state().status).toEqual("ready");
    });

    it("never calls the model for an out-of-scope error", async () => {
      const callModel = vi.spyOn(service as any, "callModel");
      await service.analyzeError(OP, err("ZeroDivisionError: division by zero"), SCHEMA, CODE);

      expect(callModel).not.toHaveBeenCalled();
      expect(state().status).toEqual("unsupported");
      expect(state().suggestedFix).toBeUndefined();
    });

    it("ends in error on transport failure, malformed JSON, or a missing field", async () => {
      vi.spyOn(service as any, "callModel").mockRejectedValue(new Error("502"));
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);
      expect(state().status).toEqual("error");

      stubModel("I cannot help with that.");
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);
      expect(state().status).toEqual("error");

      stubModel(JSON.stringify({ explanation: "e", fix_type: "code_change", confidence: "low" }));
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);
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

    const first = service.analyzeError("op-first", KEY_ERROR_EVENT, SCHEMA, CODE);
    await service.analyzeError("op-second", KEY_ERROR_EVENT, SCHEMA, CODE);
    expect(state().operatorId).toEqual("op-second");

    resolveFirst({ text: suggestion({ explanation: "for the first operator" }) });
    await first;

    expect(state().operatorId).toEqual("op-second");
    expect(state().suggestedFix?.explanation).toEqual("for the second operator");
  });

  it("sends the operator's configuration nowhere, so a credential cannot ride along", async () => {
    // Operators keep secrets beside ordinary settings, and filtering by field name was never
    // enough: a Python UDF's uiParameters is a list of attribute/value pairs, so the name
    // says nothing about the value. None of the three fixes reads configuration, so none is
    // sent -- this asserts the wiring, and ai-fix-prompt.spec covers the prompt itself.
    operatorProperties = {
      code: CODE,
      hfApiToken: "hf_live_abc",
      uiParameters: [{ attribute: { attributeName: "hf_token" }, value: "hf_ui_xyz" }],
    };
    const callModel = vi.spyOn(service as any, "callModel").mockResolvedValue({ text: suggestion() });

    await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

    const prompt = JSON.stringify(callModel.mock.calls[0][0]);
    expect(prompt).not.toContain("hf_live_abc");
    expect(prompt).not.toContain("hf_ui_xyz");
    expect(prompt).not.toContain("Operator configuration");
  });

  describe("classifying a real traceback", () => {
    it("reads a JVM exception from the header rather than the frames below it", () => {
      // mkConsoleMessage puts `err.toString` in `title` and only `getStackTrace.mkString` in
      // `message`, so the frames arrive with no `at ` prefix -- and on Java 9+ the JDK's own
      // carry a module name. Nothing has to find the exception any more, because the header
      // is handed over separately. It classifies as unsupported, which is the honest answer:
      // the fix this panel writes is a change to `code`, and a JVM operator has none.
      expect(classifyError("java.lang.NullPointerException: value is null")).toEqual("unsupported");
      expect(classifyError("java.base/java.lang.Thread.run(Thread.java:840)")).toEqual("unsupported");
    });

    it("classifies a chained traceback on the exception that stopped the worker", () => {
      // Nothing here has to unpick the chain: the Python worker sets `title` from
      // `format_exception[-1]`, which is already the final exception.
      expect(classifyError("ZeroDivisionError: integer division or modulo by zero")).toEqual("unsupported");
      expect(classifyError("KeyError: " + QUOTE + "email" + QUOTE)).toEqual("missing_column");
    });

    it("never sees the frame lines that read like patterns on their own", () => {
      // A path with "model" above a `line 404` entry used to look like a model 404. Those
      // lines live in `full`, which only the prompt is given.
      const event = err(
        "ZeroDivisionError: integer division or modulo by zero",
        [
          "Traceback (most recent call last):",
          '  File "/usr/lib/python3/sklearn/model_selection/_split.py", line 404, in split',
          "    n = len(indices) // 0",
          "ZeroDivisionError: integer division or modulo by zero",
        ].join("\n")
      );

      expect(classifyError(event.header)).toEqual("unsupported");
      expect(event.full).toContain("line 404");
    });

    it("still classifies the plain case the panel is built for", () => {
      expect(classifyError(KEY_ERROR_EVENT.header)).toEqual("missing_column");
    });
  });

  it("lets an unsupported error supersede an analysis already in flight", async () => {
    // The unsupported branch answers without calling the model, but it still replaces the
    // panel state, so a slower analysis started earlier must not land on top of it.
    let resolveFirst: (value: { text: string }) => void = () => {};
    const slowFirst = new Promise<{ text: string }>(resolve => (resolveFirst = resolve));
    vi.spyOn(service as any, "callModel").mockReturnValueOnce(slowFirst);

    const first = service.analyzeError("op-first", KEY_ERROR_EVENT, SCHEMA, CODE);
    await service.analyzeError("op-second", err("ZeroDivisionError: division by zero"), SCHEMA, CODE);
    expect(state().operatorId).toEqual("op-second");
    expect(state().status).toEqual("unsupported");

    resolveFirst({ text: suggestion() });
    await first;

    expect(state().operatorId).toEqual("op-second");
    expect(state().status).toEqual("unsupported");
  });

  it("rejects a reply whose snippets are not strings", async () => {
    // The template splits these into diff lines; an array would throw inside change
    // detection, leaving the panel on `ready` with nothing rendered and no way out.
    stubModel(
      JSON.stringify({
        explanation: "the column is user_email",
        fix_type: "code_change",
        original_snippet: ["        yield t['email']"],
        suggested_snippet: "        yield t['user_email']",
        confidence: "high",
      })
    );

    await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

    expect(state().status).toEqual("error");
    expect(state().suggestedFix).toBeUndefined();
  });

  describe("a finished run clears the panel", () => {
    const ended = (state: string) => executionState$.next({ previous: {}, current: { state } });

    it("drops the applied message once the re-run is over", async () => {
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);
      await service.applyFix();
      expect(state().status).toEqual("applied");

      ended("Completed");

      // Otherwise a run that failed again brought the tab back still claiming a re-run.
      expect(state().status).toEqual("idle");
    });

    it("drops a suggestion the user never applied", async () => {
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);
      expect(state().status).toEqual("ready");

      ended("Failed");

      expect(state().status).toEqual("idle");
    });

    it("does not revive a suggestion the reset already cleared", async () => {
      // Reachable from the toolbar: analyse a failed operator, then stop the run while the
      // model is still answering. The panel resets, and the late reply used to repaint a
      // suggestion for an execution that no longer exists.
      let resolveAnalysis: (value: { text: string }) => void = () => {};
      const slow = new Promise<{ text: string }>(resolve => (resolveAnalysis = resolve));
      vi.spyOn(service as any, "callModel").mockReturnValueOnce(slow);

      const analysing = service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);
      ended("Killed");
      expect(state().status).toEqual("idle");

      resolveAnalysis({ text: suggestion() });
      await analysing;

      expect(state().status).toEqual("idle");
      expect(state().suggestedFix).toBeUndefined();
    });

    it("does not revive a suggestion the user discarded", async () => {
      let resolveAnalysis: (value: { text: string }) => void = () => {};
      const slow = new Promise<{ text: string }>(resolve => (resolveAnalysis = resolve));
      vi.spyOn(service as any, "callModel").mockReturnValueOnce(slow);

      const analysing = service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);
      service.discardFix();

      resolveAnalysis({ text: suggestion() });
      await analysing;

      expect(state().status).toEqual("idle");
    });

    it("never flashes the panel back to idle during an apply", async () => {
      // applyFix kills a live execution before it writes, and that Killed state reaches
      // this very subscription. The end state is "applied" either way because the apply
      // publishes last, so what the guard protects is the moment in between: without it
      // the panel drops to idle and offers Analyze while the apply is still running.
      modificationEnabled = false;
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);
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

  describe("replaceAll", () => {
    it("writes the suggestion verbatim, adding no indentation of its own", () => {
      // The snippet is a whole line carrying its own eight spaces, and so is the
      // replacement. Adding the context indentation on top is what put `yield` at sixteen.
      const code = "def f():\n        a = read()\n        yield a\n";

      const out = replaceAll(code, "        a = read()", "        a = read().strip()").code;

      expect(out).toEqual("def f():\n        a = read().strip()\n        yield a\n");
    });

    it("places a multi-line suggestion at the column each of its lines asks for", () => {
      const code = "def f():\n    x = read()\n";

      const out = replaceAll(code, "    x = read()", "    x = read()\n    x = x.strip()").code;

      expect(out).toEqual("def f():\n    x = read()\n    x = x.strip()\n");
    });

    it("counts the places it changed", () => {
      const out = replaceAll("p()\np()\n", "p()", "q()");

      expect(out.code).toEqual("q()\nq()\n");
      expect(out.count).toEqual(2);
    });
  });

  describe("matchesWholeLines", () => {
    it("accepts a snippet that begins a line, including the first one", () => {
      expect(matchesWholeLines("    a()\n    b()\n", "    a()")).toBe(true);
      expect(matchesWholeLines("a()\n", "a()")).toBe(true);
    });

    it("rejects a snippet quoted from the middle of a line", () => {
      // Without its leading indentation the snippet says nothing about what column the
      // lines after it belong at, which is exactly what used to be guessed.
      expect(matchesWholeLines("    a()\n", "a()")).toBe(false);
    });

    it("rejects when any one occurrence is mid-line, not just the first", () => {
      // Every match is replaced, so one mid-line occurrence is enough to make the result
      // depend on a guess -- and the first match looking fine is what would hide it.
      expect(matchesWholeLines("a()\nif x: a()\n", "a()")).toBe(false);
    });

    it("rejects a whole line that is only the start of a longer one", () => {
      // The dangerous half: the match begins a line, so the indentation is unambiguous and
      // the count is 1, which hides the badge -- but it stops mid-line. Replacing it leaves
      // the rest of the line stuck on the end, so `n = 1` becoming `n = 2` silently rewrites
      // `n = 10` as `n = 20` while the diff only ever showed `n = 1`.
      expect(matchesWholeLines("        n = 10\n", "        n = 1")).toBe(false);
    });

    it("accepts a match that ends at the end of the code, with no trailing newline", () => {
      expect(matchesWholeLines("    a()", "    a()")).toBe(true);
    });

    it("rejects when only one of several occurrences stops mid-line", () => {
      expect(matchesWholeLines("    a()\n    a() + 1\n", "    a()")).toBe(false);
    });
    it("says nothing about a snippet the code does not contain", () => {
      // Absence is a separate refusal, with its own message.
      expect(matchesWholeLines("a()\n", "zzz()")).toBe(true);
    });
  });

  describe("a newer error clears a stale result", () => {
    const errorAt = (seconds: number) => ({
      msgType: { name: "ERROR" },
      timestamp: { seconds, nanos: 0 },
      title: "KeyError: 'lines'",
    });

    it("goes back to idle when the operator reports a newer error", async () => {
      // The main case this panel exists for never reaches a terminal state: a UDF that
      // raises leaves the execution Running, so resetting on Completed/Failed/Killed misses
      // it entirely and the tab came back saying "re-running" over a run stuck on an error.
      consoleMessages = [errorAt(100)];
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);
      await service.applyFix();
      expect(state().status).toEqual("applied");

      consoleMessages = [errorAt(100), errorAt(250)];
      consoleUpdates$.next();

      expect(state().status).toEqual("idle");
    });

    it("leaves the result alone while the error is still the one it was made for", async () => {
      consoleMessages = [errorAt(100)];
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);
      await service.applyFix();

      consoleUpdates$.next();

      expect(state().status).toEqual("applied");
    });
  });

  describe("applyFix", () => {
    it("replaces only the snippet, keeps other properties, and re-runs", async () => {
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

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
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, twice);

      await service.applyFix();

      const patched = setOperatorProperty.mock.calls[0][1].code;
      expect(patched).not.toContain("t['email']");
      expect(patched.match(/t\['user_email'\]/g)).toHaveLength(2);
    });

    it("does nothing without a ready suggestion", async () => {
      await service.applyFix();
      expect(setOperatorProperty).not.toHaveBeenCalled();
      expect(executeWorkflow).not.toHaveBeenCalled();
      expect(state().status).toEqual("idle");
    });

    it("does not stop the user's run for a fix it was going to refuse anyway", async () => {
      // Every refusal used to run after ensureModifiable(), which kills the paused execution
      // to clear the modification lock. So a fix that could never be written cost the user
      // their run: the panel reported the refusal over a workflow it had just stopped.
      modificationEnabled = false;
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

      operatorProperties.code = "class ProcessTupleOperator(UDFOperatorV2):\n    pass\n";

      await service.applyFix();

      expect(killWorkflow).not.toHaveBeenCalled();
      expect(setOperatorProperty).not.toHaveBeenCalled();
      expect(state().status).toEqual("apply_failed");
    });

    it("refuses when the snippet now appears fewer times than the panel promised", async () => {
      // Deliberate, not an oversight in the comparison: the badge said two places, so the
      // user approved an edit to two. Patching the one that is left is a different change
      // from the one on screen, and the diff for it is one model call away.
      const twice = CODE + "        yield t['email']\n";
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, twice);
      expect(state().suggestedFix?.occurrences).toEqual(2);

      operatorProperties.code = CODE;

      await service.applyFix();

      expect(setOperatorProperty).not.toHaveBeenCalled();
      expect(state().applyError).toContain("1 place instead of 2 places");
    });

    it("refuses when the snippet now appears more times than the panel promised", async () => {
      // The occurrence count is captured at the analysis and drives the "changes N places"
      // badge, which stays hidden at one. Apply only checked that the snippet was still
      // THERE, so a copy added in between was patched too -- an edit the user was never
      // warned about. Reachable because a kill lifts the modification lock without
      // resetting the panel: `Uninitialized` is deliberately not a terminal state.
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);
      expect(state().suggestedFix?.occurrences).toEqual(1);

      operatorProperties.code = CODE + "        yield t['email']\n";

      await service.applyFix();

      expect(setOperatorProperty).not.toHaveBeenCalled();
      expect(executeWorkflow).not.toHaveBeenCalled();
      expect(state().status).toEqual("apply_failed");
      expect(state().applyError).toContain("2 places");
    });

    it("reports an apply failure, without re-running, when the snippet is no longer in the code", async () => {
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

      // The user edits the UDF while reading the suggestion -- the snippet it quoted was in
      // the code when it was generated and is gone by the time Apply runs. A snippet that
      // was never there is a different fault, and the analysis refuses that one outright.
      operatorProperties.code = "class ProcessTupleOperator(UDFOperatorV2):\n    pass\n";

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
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

      await service.applyFix();

      expect(setOperatorProperty).toHaveBeenCalled();
      expect(state().status).toEqual("applied_without_run");
    });

    it("reports the re-run once the deployment accepts it", async () => {
      executeWorkflow.mockReturnValue(true);
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

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
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

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
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

      const applying = service.applyFix();
      await vi.advanceTimersByTimeAsync(20_000);
      await applying;

      expect(setOperatorProperty).not.toHaveBeenCalled();
      expect(state().status).toEqual("apply_failed");
      expect(state().applyError).toContain("stayed locked");
      vi.useRealTimers();
    });

    it("retries from a failed apply once the operator is back in shape", async () => {
      // The panel keeps the suggestion and its Apply button after a refused write, so the
      // button has to work: refusing silently because the status is no longer `ready`
      // leaves the user clicking something that does nothing.
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

      operatorProperties.code = "something else entirely";
      await service.applyFix();
      expect(state().status).toEqual("apply_failed");
      expect(setOperatorProperty).not.toHaveBeenCalled();

      operatorProperties.code = CODE;
      await service.applyFix();

      expect(setOperatorProperty).toHaveBeenCalled();
      expect(executeWorkflow).toHaveBeenCalledWith("");
      expect(state().status).toEqual("applied");
    });

    it("refuses to write a suggestion into a different workflow", async () => {
      // Two imports of the same JSON carry the same operator ids, and this service outlives
      // the workspace, so the operator id alone would let the write land on the wrong graph.
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

      openWorkflowId = 12;
      await service.applyFix();

      expect(setOperatorProperty).not.toHaveBeenCalled();
      expect(executeWorkflow).not.toHaveBeenCalled();
      expect(state().applyError).toContain("workflow changed");
    });

    it("refuses when the workflow changes during the unlock wait", async () => {
      // ensureModifiable can wait seconds for a kill to clear the lock, and the user is
      // free to open another workflow while it does.
      modificationEnabled = false;
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

      const applying = service.applyFix();
      openWorkflowId = 12;
      modificationEnabled = true;
      modificationEnabled$.next(true);
      await applying;

      expect(setOperatorProperty).not.toHaveBeenCalled();
      expect(state().applyError).toContain("workflow changed");
    });

    it("drops a reply that lands after the user switched workflows", async () => {
      let resolveAnalysis: (value: { text: string }) => void = () => {};
      const slow = new Promise<{ text: string }>(resolve => (resolveAnalysis = resolve));
      vi.spyOn(service as any, "callModel").mockReturnValueOnce(slow);

      const analysing = service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);
      openWorkflowId = 12;
      resolveAnalysis({ text: suggestion() });
      await analysing;

      expect(state().suggestedFix).toBeUndefined();
    });

    it("does not report an apply over an analysis started while it waited", async () => {
      // Reachable: Apply on one operator, then switch to another whose frame shows idle and
      // click Analyze there. The order is forced here so the newer analysis settles first,
      // leaving the apply to publish last -- which is when it overwrites what the user is
      // now looking at.
      modificationEnabled = false;
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

      const applying = service.applyFix();
      await service.analyzeError("other-op", KEY_ERROR_EVENT, SCHEMA, CODE);
      expect(state().operatorId).toEqual("other-op");

      modificationEnabled = true;
      modificationEnabled$.next(true);
      await applying;

      expect(state().operatorId).toEqual("other-op");
      expect(state().status).toEqual("ready");
    });

    it("refuses on a read-only workflow instead of killing someone else's run", async () => {
      // enableWorkflowModification() is a no-op while the workflow is read-only, so the
      // unlock this waits for never arrives. Running only needs write access to the
      // computing unit, so a read-only user can reach a paused run and used to have it
      // killed, then wait fifteen seconds to be told the execution would not stop.
      workflowReadonly = true;
      modificationEnabled = false;
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

      await service.applyFix();

      expect(killWorkflow).not.toHaveBeenCalled();
      expect(setOperatorProperty).not.toHaveBeenCalled();
      expect(state().applyError).toContain("read-only");
    });

    it("refuses a lock that no running execution explains", async () => {
      // killWorkflow() only clears the lock a live run holds. Any other source -- the
      // workflow being shared without write access, a version being viewed -- survives the
      // kill, so the wait would run out while the user watched a spinner.
      modificationEnabled = false;
      executionState = ExecutionState.Uninitialized;
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

      await service.applyFix();

      expect(killWorkflow).not.toHaveBeenCalled();
      expect(setOperatorProperty).not.toHaveBeenCalled();
      expect(state().applyError).toContain("other than a running execution");
    });

    it("reports an apply failure when the graph rejects the change", async () => {
      setOperatorProperty.mockImplementation(() => {
        throw new Error("read-only workflow");
      });
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

      await service.applyFix();

      expect(state().status).toEqual("apply_failed");
      expect(executeWorkflow).not.toHaveBeenCalled();
    });
  });

  it("discards to idle and keeps state across re-subscription", async () => {
    stubModel(suggestion());
    await service.analyzeError(OP, KEY_ERROR_EVENT, SCHEMA, CODE);

    // A frame rebuilt by clearResultPanel() re-subscribes and must still see the fix.
    expect((await firstValueFrom(service.getState$())).status).toEqual("ready");

    service.discardFix();
    const after = await firstValueFrom(service.getState$());
    expect(after).toEqual({ operatorId: "", status: "idle" });
  });
});
