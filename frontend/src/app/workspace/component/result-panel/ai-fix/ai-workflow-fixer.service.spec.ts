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
import { firstValueFrom } from "rxjs";
import { AiWorkflowFixerService, classifyError, FixState } from "./ai-workflow-fixer.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { PortSchema } from "../../../types/workflow-compiling.interface";

const OP = "PythonUDFV2-op-1";
const SCHEMA: PortSchema = [
  { attributeName: "user_email", attributeType: "string" },
  { attributeName: "follower_count", attributeType: "integer" },
];
const CODE =
  "class ProcessTupleOperator(UDFOperatorV2):\n    def process_tuple(self, t, port):\n        yield t['email']\n";

// A UDF traceback as the Python worker sends it: the whole stack in one string,
// so the regexes must match a line inside it rather than the start of the message.
const KEY_ERROR = `Traceback (most recent call last):\n  File "udf.py", line 3\n    yield t['email']\nKeyError: 'email'`;

describe("AiWorkflowFixerService", () => {
  let service: AiWorkflowFixerService;
  let setOperatorProperty: ReturnType<typeof vi.fn>;
  let executeWorkflow: ReturnType<typeof vi.fn>;
  let operatorProperties: Record<string, unknown>;

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
    executeWorkflow = vi.fn();
    operatorProperties = { code: CODE, workers: 1 };

    TestBed.configureTestingModule({
      providers: [
        AiWorkflowFixerService,
        {
          provide: WorkflowActionService,
          useValue: {
            setOperatorProperty,
            getTexeraGraph: () => ({ getOperator: () => ({ operatorProperties }) }),
          },
        },
        { provide: ExecuteWorkflowService, useValue: { executeWorkflow } },
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

  describe("applyFix", () => {
    it("replaces only the snippet, keeps other properties, and re-runs", async () => {
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR, SCHEMA, CODE, operatorProperties);

      service.applyFix();

      expect(setOperatorProperty).toHaveBeenCalledWith(OP, {
        code: CODE.replace("yield t['email']", "yield t['user_email']"),
        workers: 1,
      });
      expect(executeWorkflow).toHaveBeenCalledWith("");
      expect(state().status).toEqual("applied");
    });

    it("patches the named field for a property change", async () => {
      operatorProperties = { model: "gpt-4-turb", temperature: 0 };
      stubModel(
        suggestion({ fix_type: "property_change", original_snippet: "model", suggested_snippet: "gpt-4-turbo" })
      );
      await service.analyzeError(OP, "404 model not found", SCHEMA, undefined, operatorProperties);

      service.applyFix();

      expect(setOperatorProperty).toHaveBeenCalledWith(OP, { model: "gpt-4-turbo", temperature: 0 });
      expect(executeWorkflow).toHaveBeenCalledWith("");
      expect(state().status).toEqual("applied");
    });

    it("ends in error, without re-running, when the target field changed since the analysis", async () => {
      operatorProperties = { model: "gpt-4-turb", temperature: 0 };
      stubModel(
        suggestion({ fix_type: "property_change", original_snippet: "model", suggested_snippet: "gpt-4-turbo" })
      );
      await service.analyzeError(OP, "404 model not found", SCHEMA, undefined, operatorProperties);

      // A coeditor picks a different model after the suggestion was generated: applying the
      // stale suggestion would silently discard their choice and re-run on it.
      operatorProperties = { model: "claude-haiku-4.5", temperature: 0 };

      service.applyFix();

      expect(setOperatorProperty).not.toHaveBeenCalled();
      expect(executeWorkflow).not.toHaveBeenCalled();
      expect(state().status).toEqual("error");
    });

    it("does nothing without a ready suggestion", () => {
      service.applyFix();
      expect(setOperatorProperty).not.toHaveBeenCalled();
      expect(executeWorkflow).not.toHaveBeenCalled();
      expect(state().status).toEqual("idle");
    });

    it("ends in error, without re-running, when the snippet is no longer in the code", async () => {
      stubModel(suggestion({ original_snippet: "a line the user already deleted" }));
      await service.analyzeError(OP, KEY_ERROR, SCHEMA, CODE, operatorProperties);

      service.applyFix();

      expect(setOperatorProperty).not.toHaveBeenCalled();
      expect(executeWorkflow).not.toHaveBeenCalled();
      expect(state().status).toEqual("error");
    });

    it("ends in error when the graph rejects the change", async () => {
      setOperatorProperty.mockImplementation(() => {
        throw new Error("read-only workflow");
      });
      stubModel(suggestion());
      await service.analyzeError(OP, KEY_ERROR, SCHEMA, CODE, operatorProperties);

      service.applyFix();

      expect(state().status).toEqual("error");
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
    expect(after).toEqual({ operatorId: "", errorMessage: "", errorType: "unsupported", status: "idle" });
  });
});
