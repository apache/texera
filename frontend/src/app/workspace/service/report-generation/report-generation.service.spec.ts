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

import { HttpClientTestingModule } from "@angular/common/http/testing";
import { TestBed } from "@angular/core/testing";
import { of, throwError } from "rxjs";
import { ReportGenerationService } from "./report-generation.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { AiAnalystService } from "../ai-analyst/ai-analyst.service";

describe("ReportGenerationService", () => {
  let service: ReportGenerationService;
  let workflowActionService: { getWorkflowContent: ReturnType<typeof vi.fn> };
  let workflowResultService: {
    getResultService: ReturnType<typeof vi.fn>;
    getPaginatedResultService: ReturnType<typeof vi.fn>;
  };
  let notificationService: { error: ReturnType<typeof vi.fn> };
  let aiAnalystService: {
    isOpenAIEnabled: ReturnType<typeof vi.fn>;
    sendPromptToOpenAI: ReturnType<typeof vi.fn>;
  };

  beforeEach(() => {
    vi.clearAllMocks();
    workflowActionService = {
      getWorkflowContent: vi.fn().mockReturnValue({ operators: [{ operatorID: "op1", operatorType: "CSVFileScan" }] }),
    };
    workflowResultService = {
      getResultService: vi.fn().mockReturnValue(undefined),
      getPaginatedResultService: vi.fn().mockReturnValue(undefined),
    };
    notificationService = { error: vi.fn() };
    aiAnalystService = {
      isOpenAIEnabled: vi.fn().mockReturnValue(of(true)),
      sendPromptToOpenAI: vi.fn().mockReturnValue(of("AI comment")),
    };

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        ReportGenerationService,
        { provide: WorkflowActionService, useValue: workflowActionService },
        { provide: WorkflowResultService, useValue: workflowResultService },
        { provide: NotificationService, useValue: notificationService },
        { provide: AiAnalystService, useValue: aiAnalystService },
      ],
    });
    service = TestBed.inject(ReportGenerationService);
  });

  afterEach(() => {
    const editor = document.querySelector("#workflow-editor");
    editor?.remove();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  describe("generateComment / generateSummaryComment", () => {
    it("generateComment embeds the operator info and delegates to the AI service", () => {
      let result: string | undefined;
      service.generateComment({ foo: "bar" }).subscribe(r => (result = r));

      expect(aiAnalystService.sendPromptToOpenAI).toHaveBeenCalledTimes(1);
      const prompt = aiAnalystService.sendPromptToOpenAI.mock.calls[0][0] as string;
      expect(prompt).toContain('"foo": "bar"');
      expect(prompt).toContain("at least 80 words");
      expect(result).toEqual("AI comment");
    });

    it("generateSummaryComment uses the longer (150-word) summary prompt", () => {
      service.generateSummaryComment({ a: 1 }).subscribe();

      const prompt = aiAnalystService.sendPromptToOpenAI.mock.calls[0][0] as string;
      expect(prompt).toContain("at least 150 words");
      expect(prompt).toContain('"a": 1');
    });
  });

  describe("generateWorkflowSnapshot", () => {
    // The rendering path delegates to html2canvas (unmockable reliably under the
    // shared module registry), so only the pre-render guard is pinned here.
    it("errors when the #workflow-editor element is absent", () => {
      let error: unknown;
      service.generateWorkflowSnapshot("wf").subscribe({ error: (e: unknown) => (error = e) });
      expect(error).toEqual("Workflow editor element not found");
    });
  });

  describe("retrieveOperatorInfoReport", () => {
    it("reports 'No results found' when the operator has neither a result nor paginated service", () => {
      const allResults: { operatorId: string; html: string }[] = [];
      service.retrieveOperatorInfoReport("op1", allResults).subscribe();

      expect(allResults).toHaveLength(1);
      expect(allResults[0].operatorId).toEqual("op1");
      expect(allResults[0].html).toContain("No results found for operator");
    });

    it("reports 'No results found' when the paginated table is empty", () => {
      workflowResultService.getPaginatedResultService.mockReturnValue({
        selectPage: vi.fn().mockReturnValue(of({ table: [] })),
      });

      const allResults: { operatorId: string; html: string }[] = [];
      service.retrieveOperatorInfoReport("op1", allResults).subscribe();

      expect(allResults[0].html).toContain("No results found for operator");
    });

    it("renders an HTML table from a non-empty paginated result", () => {
      workflowResultService.getPaginatedResultService.mockReturnValue({
        selectPage: vi.fn().mockReturnValue(of({ table: [{ name: "a", value: 1 }] })),
      });

      const allResults: { operatorId: string; html: string }[] = [];
      service.retrieveOperatorInfoReport("op1", allResults).subscribe();

      const html = allResults[0].html;
      expect(html).toContain("Operator ID: op1");
      expect(html).toContain("<table");
      expect(html).toContain(">name<");
      expect(html).toContain(">value<");
    });

    it("surfaces a notification and errors when the paginated result service fails", () => {
      workflowResultService.getPaginatedResultService.mockReturnValue({
        selectPage: vi.fn().mockReturnValue(throwError(() => new Error("boom"))),
      });

      const allResults: { operatorId: string; html: string }[] = [];
      let errored = false;
      service.retrieveOperatorInfoReport("op1", allResults).subscribe({ error: () => (errored = true) });

      expect(errored).toBe(true);
      expect(notificationService.error).toHaveBeenCalledWith(expect.stringContaining("boom"));
    });
  });

  describe("getAllOperatorResults", () => {
    it("collects one entry per operator id", () => {
      workflowResultService.getPaginatedResultService.mockReturnValue({
        selectPage: vi.fn().mockReturnValue(of({ table: [{ name: "a" }] })),
      });

      let results: { operatorId: string; html: string }[] = [];
      service.getAllOperatorResults(["op1"]).subscribe(r => (results = r));

      expect(results).toHaveLength(1);
      expect(results[0].operatorId).toEqual("op1");
      expect(results[0].html).toContain("Operator ID: op1");
    });
  });
});
