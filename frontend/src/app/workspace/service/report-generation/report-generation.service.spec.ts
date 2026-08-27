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
import { firstValueFrom, of, Subject, Subscription, throwError } from "rxjs";
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

    it("falls back to a generic reason when the page failure carries no message", async () => {
      deps.workflowResultService.getPaginatedResultService.mockReturnValue({
        selectPage: vi.fn().mockReturnValue(throwError(() => new Error(""))),
      });

      await expect(htmlFor("op-1")).rejects.toBeInstanceOf(Error);
      expect(deps.notificationService.error).toHaveBeenCalledWith(
        "Error processing results for operator op-1: Unknown error"
      );
    });

    it("still renders a visualization snapshot that has no wrapper div to resize", async () => {
      deps.workflowResultService.getResultService.mockReturnValue({
        getCurrentResultSnapshot: () => [{ "html-content": "<p>plain</p>" }],
      });

      const html = await htmlFor("op-1");

      expect(html).toContain("plain");
    });

    it("notifies and fails when building the report throws outright", async () => {
      const failure = new Error("result service unavailable");
      deps.workflowResultService.getResultService.mockImplementation(() => {
        throw failure;
      });

      await expect(htmlFor("op-1")).rejects.toBe(failure);
      expect(deps.notificationService.error).toHaveBeenCalledWith(
        "Unexpected error in retrieveOperatorInfoReport for operator op-1: result service unavailable"
      );
    });

    it("falls back to a generic reason when that failure carries no message", async () => {
      deps.workflowResultService.getResultService.mockImplementation(() => {
        throw new Error("");
      });

      await expect(htmlFor("op-1")).rejects.toBeInstanceOf(Error);
      expect(deps.notificationService.error).toHaveBeenCalledWith(
        "Unexpected error in retrieveOperatorInfoReport for operator op-1: Unknown error"
      );
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
    /**
     * html2canvas clones the whole document — from `documentElement`, not from the element it is
     * pointed at — and the unit-test builder runs spec files with `isolate: false`, so one jsdom
     * document is shared by every spec file in a worker and the renders below drag in whatever DOM
     * the files before this one left behind, in `<head>` as much as in the body. That is what
     * failed the macOS leg: a clone that costs ~60ms against this file's own DOM was measured at
     * 12–37s there, past the 20s test timeout, while ubuntu and windows passed. Park the foreign
     * nodes of both for the duration of the file and put them back after, so the render's cost
     * depends only on what these tests build. The renders started here outlive the tests that
     * start them, so this has to span the file rather than each test.
     */
    let parkedNodes: [ParentNode, ChildNode][];

    beforeAll(() => {
      parkedNodes = [document.head, document.body].flatMap(parent =>
        Array.from(parent.childNodes).map((node): [ParentNode, ChildNode] => [parent, node])
      );
      parkedNodes.forEach(([, node]) => node.remove());
    });

    afterAll(() => {
      parkedNodes.forEach(([parent, node]) => parent.appendChild(node));
    });

    it("fails when the editor is not on the page", async () => {
      await expect(firstValueFrom(service.generateWorkflowSnapshot("myflow"))).rejects.toBe(
        "Workflow editor element not found"
      );
    });

    /**
     * Before the editor can be rendered, every <image> in it is refetched and inlined as
     * base64 so the snapshot does not depend on URLs the renderer cannot resolve. Both async
     * sources are replaced with fakes that settle synchronously (XHR) or on a microtask
     * (FileReader), so nothing here depends on the network or on real timing.
     *
     * The html2canvas render that follows is left alone — it needs a real canvas — so these
     * assert on what the inlining step did, not on the observable's outcome.
     */
    describe("inlining the editor's images", () => {
      const XLINK_HREF = "xlink:href";
      const BASE64 = "data:image/png;base64,AAAA";

      let realXhr: typeof globalThis.XMLHttpRequest;
      let realFileReader: typeof globalThis.FileReader;
      let editor: HTMLElement;
      let started: Subscription[];
      let xhrOutcome: "load" | "error";
      let readerOutcome: "loadend" | "error";
      let sentUrls: string[];

      class FakeXhr {
        public response: unknown = "blob-stand-in";
        public responseType = "";
        public onload: (() => void) | null = null;
        public onerror: (() => void) | null = null;
        private url = "";
        open(_method: string, url: string): void {
          this.url = url;
        }
        send(): void {
          sentUrls.push(this.url);
          if (xhrOutcome === "load") {
            this.onload?.();
          } else {
            this.onerror?.();
          }
        }
      }

      class FakeFileReader {
        public result: string | null = null;
        public onloadend: (() => void) | null = null;
        public onerror: (() => void) | null = null;
        readAsDataURL(): void {
          queueMicrotask(() => {
            if (readerOutcome === "loadend") {
              this.result = BASE64;
              this.onloadend?.();
            } else {
              this.onerror?.();
            }
          });
        }
      }

      /** Adds an SVG <image> to the editor, optionally with a source attribute. */
      function addImage(src?: string): SVGElement {
        const image = document.createElementNS("http://www.w3.org/2000/svg", "image");
        if (src !== undefined) {
          image.setAttribute(XLINK_HREF, src);
        }
        editor.appendChild(image);
        return image;
      }

      /**
       * Starts the snapshot and returns immediately. Waiting for the observable to settle
       * would mean waiting for html2canvas, which under jsdom takes an unbounded amount of
       * time — on a slow runner long enough to blow the test timeout. These tests are about
       * the inlining step that runs first, so each waits for that step's own effect instead.
       * The subscriber swallows both outcomes so the render's failure is not an unhandled
       * error.
       */
      function startSnapshot(): void {
        started.push(
          service.generateWorkflowSnapshot("myflow").subscribe({
            next: () => {},
            error: () => {},
          })
        );
      }

      beforeEach(() => {
        started = [];
        sentUrls = [];
        xhrOutcome = "load";
        readerOutcome = "loadend";
        realXhr = globalThis.XMLHttpRequest;
        realFileReader = globalThis.FileReader;
        (globalThis as unknown as { XMLHttpRequest: unknown }).XMLHttpRequest = FakeXhr;
        (globalThis as unknown as { FileReader: unknown }).FileReader = FakeFileReader;
        editor = document.createElement("div");
        editor.id = "workflow-editor";
        document.body.appendChild(editor);
        // The render these tests start is left to fail on its own, but jsdom announces its
        // missing 2D context on the virtual console, so an otherwise clean run carries a stack
        // trace per test. Hand back what jsdom hands back after complaining, minus the complaint.
        vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockReturnValue(null as never);
      });

      afterEach(() => {
        (globalThis as unknown as { XMLHttpRequest: unknown }).XMLHttpRequest = realXhr;
        (globalThis as unknown as { FileReader: unknown }).FileReader = realFileReader;
        // The render this started cannot be cancelled, but nothing it emits should reach a
        // test that has already finished.
        started.forEach(subscription => subscription.unsubscribe());
        // Two of these tests spy on console.error; without this the spy would outlive them.
        vi.restoreAllMocks();
        editor.remove();
      });

      it("rewrites an image's source to the fetched base64 data", async () => {
        const image = addImage("/assets/icon.png");

        startSnapshot();

        await vi.waitFor(() => expect(image.getAttribute("href")).toBe(BASE64));
        expect(sentUrls).toEqual(["/assets/icon.png"]);
      });

      it("leaves an image with no source alone and fetches nothing for it", () => {
        const image = addImage();

        // The request, if there were one, is issued synchronously by the fake XHR, so an
        // empty list right after starting is already the answer.
        startSnapshot();

        expect(sentUrls).toEqual([]);
        expect(image.getAttribute("href")).toBeNull();
      });

      it("reports an image whose bytes cannot be converted, and leaves its source alone", async () => {
        const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
        readerOutcome = "error";
        const image = addImage("/assets/icon.png");

        startSnapshot();

        await vi.waitFor(() =>
          expect(consoleSpy).toHaveBeenCalledWith(
            "Failed to load image: /assets/icon.png",
            "Failed to convert image to Base64"
          )
        );
        expect(image.getAttribute("href")).toBeNull();
      });

      it("reports an image that cannot be fetched at all", async () => {
        const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
        xhrOutcome = "error";
        addImage("/assets/missing.png");

        startSnapshot();

        await vi.waitFor(() =>
          expect(consoleSpy).toHaveBeenCalledWith(
            "Failed to load image: /assets/missing.png",
            "Failed to load image from /assets/missing.png"
          )
        );
      });
    });

    /**
     * Once the images are inlined, the editor is handed to html2canvas and whatever canvas comes
     * back is encoded as a PNG. Substituting the renderer with `vi.mock("html2canvas")` does not
     * work here: @angular/build's unit-test runner runs spec files with `isolate: false`, so they
     * share one module registry and MenuComponent's spec — which pulls this service in
     * transitively — can pin the real html2canvas before this file's mock is ever registered.
     * The real renderer is used instead, with jsdom given the three pieces it lacks: a 2D
     * context, an <img> that reports a data-URL source as loaded, and a PNG encoder.
     */
    describe("rendering the editor to a PNG", () => {
      const RENDERED_PNG = "data:image/png;base64,RENDERED";

      let realImage: typeof globalThis.Image;
      let toDataUrl: ReturnType<typeof vi.spyOn>;
      let editor: HTMLElement;

      /**
       * html2canvas draws the cloned editor onto a canvas; none of those calls affect what the
       * service does with the result, so a context that accepts every call stands in for one.
       */
      function permissiveContext(): CanvasRenderingContext2D {
        return new Proxy({}, { get: () => () => undefined }) as CanvasRenderingContext2D;
      }

      /** html2canvas rasterizes the clone through an <img> pointed at a serialized SVG. */
      class InstantImage {
        public onload: (() => void) | null = null;
        public onerror: (() => void) | null = null;
        private source = "";
        get src(): string {
          return this.source;
        }
        set src(value: string) {
          this.source = value;
          queueMicrotask(() => this.onload?.());
        }
      }

      beforeEach(() => {
        vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockImplementation(
          () => permissiveContext() as unknown as never
        );
        toDataUrl = vi.spyOn(HTMLCanvasElement.prototype, "toDataURL").mockReturnValue(RENDERED_PNG);
        realImage = globalThis.Image;
        (globalThis as unknown as { Image: unknown }).Image = InstantImage;
        editor = document.createElement("div");
        editor.id = "workflow-editor";
        document.body.appendChild(editor);
      });

      afterEach(() => {
        (globalThis as unknown as { Image: unknown }).Image = realImage;
        vi.restoreAllMocks();
        editor.remove();
      });

      it("emits the rendered editor as a PNG data URL and then completes", async () => {
        const emitted: string[] = [];
        let completed = false;

        await new Promise<void>((resolve, reject) => {
          service.generateWorkflowSnapshot("myflow").subscribe({
            next: value => emitted.push(value),
            error: reject,
            complete: () => {
              completed = true;
              resolve();
            },
          });
        });

        expect(emitted).toEqual([RENDERED_PNG]);
        expect(completed).toBe(true);
        // PNG specifically: the report embeds the snapshot in an <img>, so the format is not
        // the encoder's default choice to make.
        expect(toDataUrl).toHaveBeenCalledWith("image/png");
      });

      it("fails when the editor cannot be rendered", async () => {
        const failure = new Error("canvas unavailable");
        toDataUrl.mockImplementation(() => {
          throw failure;
        });

        await expect(firstValueFrom(service.generateWorkflowSnapshot("myflow"))).rejects.toBe(failure);
      });
    });
  });
});
