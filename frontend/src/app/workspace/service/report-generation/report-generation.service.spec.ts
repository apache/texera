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
import { HttpClient } from "@angular/common/http";
import { firstValueFrom, of, Subject, throwError } from "rxjs";
import { ReportGenerationService } from "./report-generation.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { AiAnalystService } from "../ai-analyst/ai-analyst.service";
import { commonTestProviders } from "../../../common/testing/test-utils";

/**
 * The service reaches for five collaborators but only ever calls a handful of their methods, so the
 * suite injects narrow stubs rather than the real service graph. `http` is injected by the service
 * and never used, so an empty object is enough to satisfy the constructor.
 */
function stubs() {
  return {
    workflowActionService: { getWorkflowContent: vi.fn().mockReturnValue({ operators: [] }) },
    workflowResultService: {
      getResultService: vi.fn().mockReturnValue(undefined),
      getPaginatedResultService: vi.fn().mockReturnValue(undefined),
    },
    notificationService: { error: vi.fn() },
    aiAnalystService: {
      isOpenAIEnabled: vi.fn().mockReturnValue(of(true)),
      sendPromptToOpenAI: vi.fn().mockReturnValue(of("GENERATED COMMENT")),
    },
  };
}

/** jsdom's Blob has no `text()`, so the report body is read back through a FileReader. */
function readBlob(blob: Blob): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result as string);
    reader.onerror = () => reject(reader.error);
    reader.readAsText(blob);
  });
}

describe("ReportGenerationService", () => {
  let service: ReportGenerationService;
  let deps: ReturnType<typeof stubs>;

  beforeEach(() => {
    deps = stubs();
    TestBed.configureTestingModule({
      providers: [
        ReportGenerationService,
        { provide: HttpClient, useValue: {} },
        { provide: WorkflowActionService, useValue: deps.workflowActionService },
        { provide: WorkflowResultService, useValue: deps.workflowResultService },
        { provide: NotificationService, useValue: deps.notificationService },
        { provide: AiAnalystService, useValue: deps.aiAnalystService },
        ...commonTestProviders,
      ],
    });
    service = TestBed.inject(ReportGenerationService);
  });

  /** Runs the report for one operator and hands back the HTML it pushed into the accumulator. */
  async function htmlFor(operatorId: string): Promise<string> {
    const collected: { operatorId: string; html: string }[] = [];
    await firstValueFrom(service.retrieveOperatorInfoReport(operatorId, collected));
    return collected[0].html;
  }

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  describe("retrieveOperatorInfoReport", () => {
    it("renders a paginated result as a table of its first page", async () => {
      // A null cell renders as the text "null" rather than an empty cell, so the column stays
      // aligned with its header; pinned here because "tidying" it to a blank would shift the row.
      deps.workflowResultService.getPaginatedResultService.mockReturnValue({
        selectPage: vi.fn().mockReturnValue(of({ table: [{ colA: 1, colB: null }] })),
      });
      deps.workflowActionService.getWorkflowContent.mockReturnValue({
        operators: [{ operatorID: "op-1", operatorType: "CSVFileScan" }],
      });

      const html = await htmlFor("op-1");

      expect(html).toContain("<h3>Operator ID: op-1</h3>");
      expect(html).toContain(">colA</th>");
      expect(html).toContain(">colB</th>");
      expect(html).toContain(">1</td>");
      expect(html).toContain(">null</td>");
      expect(html).toContain("GENERATED COMMENT");
    });

    it("asks for the first page of ten rows", async () => {
      const selectPage = vi.fn().mockReturnValue(of({ table: [{ a: 1 }] }));
      deps.workflowResultService.getPaginatedResultService.mockReturnValue({ selectPage });

      await htmlFor("op-1");

      expect(selectPage).toHaveBeenCalledWith(1, 10);
    });

    it("reports an empty page as no results rather than an empty table", async () => {
      deps.workflowResultService.getPaginatedResultService.mockReturnValue({
        selectPage: vi.fn().mockReturnValue(of({ table: [] })),
      });

      const html = await htmlFor("op-1");

      expect(html).toContain("No results found for operator");
      expect(html).not.toContain("<table");
    });

    it("notifies and fails when the page cannot be fetched", async () => {
      const failure = new Error("page 1 unavailable");
      deps.workflowResultService.getPaginatedResultService.mockReturnValue({
        selectPage: vi.fn().mockReturnValue(throwError(() => failure)),
      });

      await expect(htmlFor("op-1")).rejects.toBe(failure);
      expect(deps.notificationService.error).toHaveBeenCalledWith(
        expect.stringContaining("Error processing results for operator op-1")
      );
      expect(deps.notificationService.error).toHaveBeenCalledWith(expect.stringContaining("page 1 unavailable"));
    });

    it("renders the most recent snapshot of a visualization operator", async () => {
      // Visualizations accumulate snapshots; the report must show the latest, not the first.
      deps.workflowResultService.getResultService.mockReturnValue({
        getCurrentResultSnapshot: () => [
          { "html-content": '<div id="v1">FIRST</div>' },
          { "html-content": '<div id="v2">LAST</div>' },
        ],
      });

      const html = await htmlFor("op-1");

      expect(html).toContain("LAST");
      expect(html).not.toContain("FIRST");
      // The embedded document is resized so the chart fits the report rather than overflowing it.
      expect(html).toContain("height: 100%");
    });

    it("reports a visualization operator with no snapshot as having no data", async () => {
      deps.workflowResultService.getResultService.mockReturnValue({
        getCurrentResultSnapshot: () => undefined,
      });

      const html = await htmlFor("op-1");

      expect(html).toContain("No data found for operator");
    });

    it("reports an operator with neither result service as having no results", async () => {
      const html = await htmlFor("op-1");

      expect(html).toContain("No results found for operator");
    });

    it("embeds the operator's own definition in the collapsible details block", async () => {
      deps.workflowActionService.getWorkflowContent.mockReturnValue({
        operators: [
          { operatorID: "op-1", operatorType: "CSVFileScan" },
          { operatorID: "op-2", operatorType: "PythonUDFV2" },
        ],
      });

      const html = await htmlFor("op-2");

      expect(html).toContain('id="details-op-2"');
      expect(html).toContain("PythonUDFV2");
      expect(html).not.toContain("CSVFileScan");
    });

    it("produces nothing until the AI-enabled check emits", async () => {
      // The whole body is nested inside isOpenAIEnabled().subscribe, so a check that never settles
      // leaves the report silently unfinished rather than failing.
      deps.aiAnalystService.isOpenAIEnabled.mockReturnValue(new Subject<boolean>());
      const collected: { operatorId: string; html: string }[] = [];

      service.retrieveOperatorInfoReport("op-1", collected).subscribe();

      expect(collected).toEqual([]);
    });
  });

  describe("getAllOperatorResults", () => {
    it("returns one entry per operator, in the order asked for", async () => {
      const results = await firstValueFrom(service.getAllOperatorResults(["op-a", "op-b"]));

      expect(results.map(r => r.operatorId)).toEqual(["op-a", "op-b"]);
    });
  });

  describe("prompt construction", () => {
    it("asks for a per-operator comment carrying that operator's JSON", () => {
      service.generateComment({ operatorID: "op-1", operatorType: "CSVFileScan" }).subscribe();

      const prompt = deps.aiAnalystService.sendPromptToOpenAI.mock.calls[0][0] as string;
      expect(prompt).toContain('"operatorType": "CSVFileScan"');
      expect(prompt).toContain("at least 80 words");
    });

    it("asks the summary for a longer answer than the per-operator comment", () => {
      // The two methods are near-identical; only the length and the workflow-level framing differ,
      // so a copy-paste between them would otherwise go unnoticed.
      service.generateSummaryComment({ operators: [] }).subscribe();

      const prompt = deps.aiAnalystService.sendPromptToOpenAI.mock.calls[0][0] as string;
      expect(prompt).toContain("at least 150 words");
      expect(prompt).toContain("UDFs");
    });
  });

  describe("generateReportAsHtml", () => {
    let anchor: HTMLAnchorElement;
    let clickSpy: ReturnType<typeof vi.spyOn>;
    let createdBlob: Blob | undefined;
    let revoked: string[];
    let originalCreate: unknown;
    let originalRevoke: unknown;

    beforeEach(() => {
      // Build the anchor before stubbing createElement, or the stub would intercept its own creation.
      anchor = document.createElement("a");
      clickSpy = vi.spyOn(anchor, "click").mockImplementation(() => {});
      vi.spyOn(document, "createElement").mockReturnValue(anchor as unknown as HTMLElement);

      createdBlob = undefined;
      revoked = [];
      originalCreate = (URL as any).createObjectURL;
      originalRevoke = (URL as any).revokeObjectURL;
      (URL as any).createObjectURL = (blob: Blob) => {
        createdBlob = blob;
        return "blob:report-url";
      };
      (URL as any).revokeObjectURL = (url: string) => revoked.push(url);
    });

    afterEach(() => {
      vi.restoreAllMocks();
      (URL as any).createObjectURL = originalCreate;
      (URL as any).revokeObjectURL = originalRevoke;
    });

    it("downloads a report named after the workflow", () => {
      service.generateReportAsHtml("data:image/png;base64,SNAP", ["<p>R1</p>"], "myflow");

      expect(anchor.download).toBe("myflow-report.html");
      expect(anchor.href).toContain("blob:report-url");
      expect(clickSpy).toHaveBeenCalledTimes(1);
      expect(revoked).toEqual(["blob:report-url"]);
    });

    it("writes the snapshot, every operator result, and the summary into the document", async () => {
      deps.aiAnalystService.sendPromptToOpenAI.mockReturnValue(of("OVERALL SUMMARY"));

      service.generateReportAsHtml("data:image/png;base64,SNAP", ["<p>R1</p>", "<p>R2</p>"], "myflow");

      const text = await readBlob(createdBlob!);
      expect(text).toContain("data:image/png;base64,SNAP");
      expect(text).toContain("<p>R1</p>");
      expect(text).toContain("<p>R2</p>");
      expect(text).toContain("OVERALL SUMMARY");
      // The in-report download button names the file after the workflow too.
      expect(text).toContain("myflow-workflow.json");
    });
  });

  describe("generateWorkflowSnapshot", () => {
    it("fails when the editor is not on the page", async () => {
      await expect(firstValueFrom(service.generateWorkflowSnapshot("myflow"))).rejects.toBe(
        "Workflow editor element not found"
      );
    });
  });
});
