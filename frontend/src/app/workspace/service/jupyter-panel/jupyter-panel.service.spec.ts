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
import { JupyterPanelService } from "./jupyter-panel.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { NotebookMigrationService } from "../notebook-migration/notebook-migration.service";
import { GuiConfigService } from "src/app/common/service/gui-config.service";
import { firstValueFrom, of, throwError } from "rxjs";

/**
 * Regression coverage for the Jupyter notebook panel service: the per-workflow
 * notebook fetch, the cell-to-operator highlight index, the iframe message
 * bridge, and the feature-flag gate.
 *
 * Breakage this catches: reporting a notebook as present when Jupyter refused
 * to load it (the toolbar would offer to expand a panel that has nothing in
 * it); caching a failed Jupyter-origin lookup, which leaves the bridge dead for
 * the rest of the session; dropping the guards that keep an unsaved workflow
 * (no wid) or a workflow with no stored mapping from being looked up or posted
 * to; leaving a previous cell's highlights on the canvas when an unmapped cell
 * is clicked; and losing the feature-flag early-return in the window message
 * listener, which is installed unconditionally in the constructor.
 */
describe("JupyterPanelService", () => {
  let service: JupyterPanelService;
  let httpMock: HttpTestingController;

  let mockWorkflow: any;
  let mockNotification: any;
  let mockNotebook: any;
  // Mutable so individual describe blocks can flip the flag mid-spec; the
  // service stores a reference, so mutations are observed on the next read.
  let mockGuiConfig: { env: { pythonNotebookMigrationEnabled: boolean } };

  beforeEach(() => {
    mockWorkflow = {
      workflowMetaDataChanged: vi.fn().mockReturnValue(of({ wid: 1 })),
      getWorkflow: vi.fn().mockReturnValue({ wid: 1 }),
      getTexeraGraph: vi.fn().mockReturnValue({
        getAllLinks: () => [
          {
            linkID: "L1",
            source: { operatorID: "A" },
            target: { operatorID: "B" },
          },
        ],
        getAllOperators: () => [{ operatorID: "A" }, { operatorID: "B" }],
      }),
      highlightOperators: vi.fn(),
      highlightLinks: vi.fn(),
      unhighlightOperators: vi.fn(),
      unhighlightLinks: vi.fn(),
    };

    mockNotification = {
      warning: vi.fn(),
      error: vi.fn(),
    };

    mockNotebook = {
      hasMapping: vi.fn().mockReturnValue(true),
      getMapping: vi.fn().mockReturnValue({
        cell_to_operator: {
          cell1: ["A", "B"],
        },
        operator_to_cell: {},
      }),
      deleteMapping: vi.fn(),
      setMapping: vi.fn(),
      getJupyterURL: vi.fn().mockResolvedValue("http://jupyter"),
      deleteNotebookAndMapping: vi.fn().mockReturnValue(of({ success: true, deleted: 1 })),
    };

    mockGuiConfig = { env: { pythonNotebookMigrationEnabled: true } };

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        JupyterPanelService,
        { provide: WorkflowActionService, useValue: mockWorkflow },
        { provide: NotificationService, useValue: mockNotification },
        { provide: NotebookMigrationService, useValue: mockNotebook },
        { provide: GuiConfigService, useValue: mockGuiConfig },
      ],
    });

    service = TestBed.inject(JupyterPanelService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
    // Several specs below silence console.error/warn to keep the failure paths
    // quiet; console is a shared global, so put the originals back.
    vi.restoreAllMocks();
  });

  // Panel visibility
  it("should open panel and hide it after deleting the notebook", () => {
    let state: boolean | null = null;

    service.jupyterNotebookPanelVisible$.subscribe(v => (state = v));

    service.openPanel("JupyterNotebookPanel");
    expect(state).toBe(true);

    service.deleteJupyterNotebook();
    expect(mockNotebook.deleteNotebookAndMapping).toHaveBeenCalledWith(1);
    expect(state).toBe(false);
  });

  it("deleteJupyterNotebook clears local state and unhighlights on success", () => {
    let visible: boolean | null = null;
    let exists: boolean | null = null;
    service.jupyterNotebookPanelVisible$.subscribe(v => (visible = v));
    service.jupyterNotebookExists$.subscribe(v => (exists = v));
    (service as any).jupyterNotebookPanelVisible.next(true);
    (service as any).jupyterNotebookExists.next(true);

    service.deleteJupyterNotebook();

    expect(mockNotebook.deleteNotebookAndMapping).toHaveBeenCalledWith(1);
    expect(mockNotebook.deleteMapping).toHaveBeenCalledWith("mapping_wid_1");
    expect(visible).toBe(false);
    expect(exists).toBe(false);
    expect(mockWorkflow.unhighlightOperators).toHaveBeenCalled();
    expect(mockWorkflow.unhighlightLinks).toHaveBeenCalled();
  });

  it("deleteJupyterNotebook keeps the panel open and notifies on failure", () => {
    mockNotebook.deleteNotebookAndMapping.mockReturnValueOnce(throwError(() => new Error("boom")));
    let visible: boolean | null = null;
    service.jupyterNotebookPanelVisible$.subscribe(v => (visible = v));
    (service as any).jupyterNotebookPanelVisible.next(true);

    service.deleteJupyterNotebook();

    expect(mockNotification.error).toHaveBeenCalled();
    expect(visible).toBe(true);
    expect(mockNotebook.deleteMapping).not.toHaveBeenCalled();
  });

  it("deleteJupyterNotebook only resets local state for the default wid 0 (no backend call)", () => {
    mockWorkflow.getWorkflow.mockReturnValue({ wid: 0 });
    let visible: boolean | null = null;
    let exists: boolean | null = null;
    service.jupyterNotebookPanelVisible$.subscribe(v => (visible = v));
    service.jupyterNotebookExists$.subscribe(v => (exists = v));
    (service as any).jupyterNotebookPanelVisible.next(true);
    (service as any).jupyterNotebookExists.next(true);

    service.deleteJupyterNotebook();

    // wid 0 is the unsaved default workflow, so no backend delete should fire.
    expect(mockNotebook.deleteNotebookAndMapping).not.toHaveBeenCalled();
    expect(visible).toBe(false);
    expect(exists).toBe(false);
  });

  it("should minimize panel", () => {
    let state: boolean | null = true;

    service.jupyterNotebookPanelVisible$.subscribe(v => (state = v));

    service.minimizeJupyterNotebookPanel();

    expect(state).toBe(false);
  });

  // openJupyterNotebookPanel
  it("should warn if no mapping exists", () => {
    mockNotebook.hasMapping.mockReturnValue(false);

    service.openJupyterNotebookPanel();

    expect(mockNotification.warning).toHaveBeenCalled();
  });

  it("should open panel if mapping exists", () => {
    mockNotebook.hasMapping.mockReturnValue(true);

    let state: boolean | null = false;

    service.jupyterNotebookPanelVisible$.subscribe(v => (state = v));

    service.openJupyterNotebookPanel();

    expect(state).toBe(true);
  });

  // openPanel
  it("should open panel only for correct name", () => {
    let state: boolean | null = false;

    service.jupyterNotebookPanelVisible$.subscribe(v => (state = v));

    service.openPanel("WrongPanel");
    expect(state).toBe(false);

    service.openPanel("JupyterNotebookPanel");
    expect(state).toBe(true);
  });

  it("openPanel flags jupyterNotebookExists$ so the toolbar expand button appears after an in-place import", () => {
    const states: boolean[] = [];
    service.jupyterNotebookExists$.subscribe(v => states.push(v));
    expect(states.at(-1)).toBe(false);

    // Wrong panel name does not flip the flag.
    service.openPanel("WrongPanel");
    expect(states.at(-1)).toBe(false);

    // Opening the jupyter panel records that the workflow now has a notebook.
    service.openPanel("JupyterNotebookPanel");
    expect(states.at(-1)).toBe(true);
  });

  // HTTP fetchNotebookAndMapping
  it("should return 0 when exists=false", async () => {
    const resultPromise = firstValueFrom((service as any).fetchNotebookAndMapping(1, 1));

    const req = httpMock.expectOne(r => r.url.includes("/notebook-migration/fetch-notebook-and-mapping"));
    req.flush({ exists: false });

    expect(await resultPromise).toBe(0);
  });

  // The fetch pipeline has two distinct ways to yield 0 — Jupyter refusing the
  // notebook, and the request itself failing — and the second one is a
  // catchError that swallows *any* throw inside the switchMap. Pinning them
  // apart (via sendNotebookToJupyter and the console.error the catchError
  // emits) is what keeps a broken happy path from masquerading as "send
  // failed": the base mock has no sendNotebookToJupyter, so a spec that forgets
  // to define it gets a TypeError converted into the very same 0.
  it("returns 0 when Jupyter rejects the notebook, without going through the error path", async () => {
    mockNotebook.sendNotebookToJupyter = vi.fn().mockResolvedValue(0);
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    const mapping = { cell_to_operator: { cell1: ["A"] }, operator_to_cell: {} };
    const notebook = { cells: [] };

    const resultPromise = firstValueFrom((service as any).fetchNotebookAndMapping(1, 1));
    httpMock
      .expectOne(r => r.url.includes("/notebook-migration/fetch-notebook-and-mapping"))
      .flush({ exists: true, mapping, notebook });

    expect(await resultPromise).toBe(0);
    // The mapping is stored before the notebook is handed to Jupyter, ...
    expect(mockNotebook.setMapping).toHaveBeenCalledWith("mapping_wid_1", mapping);
    // ... and the 0 came from Jupyter's own answer, not from a thrown error.
    // Upload uses the wid-derived filename.
    expect(mockNotebook.sendNotebookToJupyter).toHaveBeenCalledWith(notebook, "notebook_1.ipynb");
    expect(consoleError).not.toHaveBeenCalled();
  });

  it("returns 0 and logs when the fetch request fails", async () => {
    mockNotebook.sendNotebookToJupyter = vi.fn().mockResolvedValue(1);
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});

    const resultPromise = firstValueFrom((service as any).fetchNotebookAndMapping(1, 1));
    httpMock
      .expectOne(r => r.url.includes("/notebook-migration/fetch-notebook-and-mapping"))
      .flush("migration service down", { status: 500, statusText: "Server Error" });

    expect(await resultPromise).toBe(0);
    expect(consoleError).toHaveBeenCalled();
    expect(mockNotebook.sendNotebookToJupyter).not.toHaveBeenCalled();
  });

  it("uploads under the fetched workflow's filename even if the current workflow changed", async () => {
    // Stale-fetch guard: a fetch for wid 2 that resolves after the user switched to wid 1
    // must still upload as notebook_2.ipynb, not overwrite wid 1's file.
    mockNotebook.sendNotebookToJupyter = vi.fn().mockResolvedValue(1);
    mockWorkflow.getWorkflow.mockReturnValue({ wid: 1 });
    const mapping = { cell_to_operator: {}, operator_to_cell: {} };
    const notebook = { cells: [] };

    const resultPromise = firstValueFrom((service as any).fetchNotebookAndMapping(2, 1));
    httpMock
      .expectOne(r => r.url.includes("/notebook-migration/fetch-notebook-and-mapping"))
      .flush({ exists: true, mapping, notebook });

    expect(await resultPromise).toBe(1);
    expect(mockNotebook.sendNotebookToJupyter).toHaveBeenCalledWith(notebook, "notebook_2.ipynb");
  });

  // Iframe URL must use the same wid-derived filename as the upload.
  it("getJupyterIframeURLForWorkflow requests the current workflow's per-workflow filename", async () => {
    mockNotebook.getJupyterIframeURL = vi.fn().mockResolvedValue("http://iframe");

    const url = await service.getJupyterIframeURLForWorkflow();

    expect(url).toBe("http://iframe");
    expect(mockNotebook.getJupyterIframeURL).toHaveBeenCalledWith("notebook_1.ipynb");
  });

  // jupyterNotebookExists$ starts false and flips true once init()'s fetch finds
  // a notebook for the workflow; the toolbar's expand button binds to this.
  it("sets jupyterNotebookExists$ true after a workflow's notebook is fetched", async () => {
    mockNotebook.sendNotebookToJupyter = vi.fn().mockResolvedValue(1);
    const states: boolean[] = [];
    service.jupyterNotebookExists$.subscribe(v => states.push(v));

    service.init();
    httpMock
      .expectOne(r => r.url.includes("/notebook-migration/fetch-notebook-and-mapping"))
      .flush({ exists: true, mapping: { cell_to_operator: {}, operator_to_cell: {} }, notebook: {} });
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(states[0]).toBe(false); // starts false
    expect(states.at(-1)).toBe(true); // true once the notebook is found
  });

  // The stored notebook exists but Jupyter refuses it: the toolbar must not
  // advertise a notebook the panel cannot actually show.
  it("leaves the panel closed when the stored notebook cannot be loaded into Jupyter", async () => {
    mockNotebook.sendNotebookToJupyter = vi.fn().mockResolvedValue(0);
    const exists: boolean[] = [];
    const visible: boolean[] = [];
    service.jupyterNotebookExists$.subscribe(v => exists.push(v));
    service.jupyterNotebookPanelVisible$.subscribe(v => visible.push(v));

    service.init();
    httpMock
      .expectOne(r => r.url.includes("/notebook-migration/fetch-notebook-and-mapping"))
      .flush({ exists: true, mapping: { cell_to_operator: {}, operator_to_cell: {} }, notebook: {} });
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(mockNotebook.sendNotebookToJupyter).toHaveBeenCalled();
    expect(exists.at(-1)).toBe(false);
    expect(visible.at(-1)).toBe(false);
  });

  // init(): subscribes to workflow changes, drops the stale mapping for the
  // current workflow, and fetches the incoming workflow's notebook + mapping.
  it("init subscribes, drops the stale mapping, and fetches for the new workflow", () => {
    service.init();

    expect(mockWorkflow.workflowMetaDataChanged).toHaveBeenCalled();
    expect(mockNotebook.deleteMapping).toHaveBeenCalledWith("mapping_wid_1");
    // Data-loss guard: switching workflows must never delete a notebook from the
    // backend. It only drops the in-memory mapping.
    expect(mockNotebook.deleteNotebookAndMapping).not.toHaveBeenCalled();

    const req = httpMock.expectOne(r => r.url.includes("/notebook-migration/fetch-notebook-and-mapping"));
    req.flush({ exists: false });
  });

  // Switching workflows must clear the highlight index even when the incoming
  // workflow has no stored notebook (fetch returns exists=false), otherwise the
  // previous workflow's highlights stay active.
  it("init clears the highlight index on every workflow change", () => {
    (service as any).cellToHighlightMapping = { stale: { components: ["X"], edges: [] } };

    service.init();

    // Cleared synchronously in the subscription, before the fetch resolves.
    expect((service as any).cellToHighlightMapping).toEqual({});

    httpMock.expectOne(r => r.url.includes("/notebook-migration/fetch-notebook-and-mapping")).flush({ exists: false });
  });

  // An unsaved workflow has an undefined wid; init must not POST for it.
  it("init does not fetch for an unsaved workflow (undefined wid)", () => {
    mockWorkflow.workflowMetaDataChanged.mockReturnValue(of({ wid: undefined }));
    mockWorkflow.getWorkflow.mockReturnValue({ wid: undefined });

    service.init();

    httpMock.expectNone(r => r.url.includes("/notebook-migration/fetch-notebook-and-mapping"));
  });

  // iframe ref
  it("should store iframe reference", () => {
    const iframe = document.createElement("iframe");

    service.setIframeRef(iframe);

    expect((service as any).iframeRef).toBe(iframe);
  });

  // highlightFromCell
  it("should highlight operators and links", () => {
    (service as any).cellToHighlightMapping = {
      cell1: {
        components: ["op1", "op2"],
        edges: ["link1"],
      },
    };

    const method = (service as any).highlightFromCell.bind(service);

    method("cell1");

    expect(mockWorkflow.unhighlightOperators).toHaveBeenCalled();
    expect(mockWorkflow.unhighlightLinks).toHaveBeenCalled();
    expect(mockWorkflow.highlightOperators).toHaveBeenCalledWith(true, "op1", "op2");
    expect(mockWorkflow.highlightLinks).toHaveBeenCalledWith(true, "link1");
  });

  // A cell that isn't in the index still has to clear the previous cell's
  // highlights, otherwise clicking an unmapped cell leaves the old selection on
  // the canvas.
  it("clears existing highlights and highlights nothing for an unmapped cell", () => {
    (service as any).cellToHighlightMapping = { cell1: { components: ["op1"], edges: ["link1"] } };

    (service as any).highlightFromCell("unmappedCell");

    expect(mockWorkflow.unhighlightOperators).toHaveBeenCalledWith("A", "B");
    expect(mockWorkflow.unhighlightLinks).toHaveBeenCalledWith("L1");
    expect(mockWorkflow.highlightOperators).not.toHaveBeenCalled();
    expect(mockWorkflow.highlightLinks).not.toHaveBeenCalled();
  });

  // handleNotebookMessage must only act on cellClicked messages that come from
  // our own iframe (event.source) AND carry the Jupyter origin.
  it("handleNotebookMessage highlights only for messages from the iframe at the Jupyter origin", async () => {
    const iframeWindow = {} as Window;
    service.setIframeRef({ contentWindow: iframeWindow } as any);
    const highlightSpy = vi.spyOn(service as any, "highlightFromCell").mockImplementation(() => {});
    const handle = (service as any).handleNotebookMessage;

    // wrong source (some other frame/script): ignored
    await handle({ source: {}, origin: "http://jupyter", data: { action: "cellClicked", cellUUID: "c1" } });
    expect(highlightSpy).not.toHaveBeenCalled();

    // right source, wrong origin: ignored
    await handle({ source: iframeWindow, origin: "http://evil", data: { action: "cellClicked", cellUUID: "c1" } });
    expect(highlightSpy).not.toHaveBeenCalled();

    // right source and origin: highlights
    await handle({ source: iframeWindow, origin: "http://jupyter", data: { action: "cellClicked", cellUUID: "c1" } });
    expect(highlightSpy).toHaveBeenCalledWith("c1");
  });

  // postMessage can deliver a payload-less event; destructuring it must not
  // throw out of the (async, unawaited) listener.
  it("handleNotebookMessage ignores a message that carries no payload", async () => {
    const iframeWindow = {} as Window;
    service.setIframeRef({ contentWindow: iframeWindow } as any);
    const highlightSpy = vi.spyOn(service as any, "highlightFromCell").mockImplementation(() => {});

    await (service as any).handleNotebookMessage({
      source: iframeWindow,
      origin: "http://jupyter",
      data: undefined,
    });

    // The awaited call above is what proves the payload destructuring survived the
    // missing data: drop the `event.data ?? {}` fallback and the async handler rejects,
    // failing this test before either assertion runs. These two then pin that the
    // guards still passed (source + origin are ours) and that no highlight was issued.
    expect(mockNotebook.getJupyterURL).toHaveBeenCalled();
    expect(highlightSpy).not.toHaveBeenCalled();
  });

  // A workflow with operators but no links is valid; precompute must still
  // record each cell's components (with empty edges) so cell clicks highlight.
  it("precomputes component mappings even when the graph has no links", () => {
    mockWorkflow.getTexeraGraph.mockReturnValue({
      getAllLinks: () => [],
      getAllOperators: () => [{ operatorID: "A" }, { operatorID: "B" }],
    });
    mockNotebook.getMapping.mockReturnValue({
      cell_to_operator: { cell1: ["A", "B"] },
      operator_to_cell: {},
    });

    (service as any).precomputeHighlightMapping();

    expect((service as any).cellToHighlightMapping).toEqual({
      cell1: { components: ["A", "B"], edges: [] },
    });
  });

  // Switching workflows re-runs precompute; the map must reflect only the
  // current workflow, not accumulate entries from previously opened ones.
  it("resets the highlight mapping on each precompute", () => {
    mockWorkflow.getTexeraGraph.mockReturnValue({
      getAllLinks: () => [],
      getAllOperators: () => [],
    });

    mockNotebook.getMapping.mockReturnValue({
      cell_to_operator: { cellA: ["A"] },
      operator_to_cell: {},
    });
    (service as any).precomputeHighlightMapping();

    mockNotebook.getMapping.mockReturnValue({
      cell_to_operator: { cellB: ["B"] },
      operator_to_cell: {},
    });
    (service as any).precomputeHighlightMapping();

    expect((service as any).cellToHighlightMapping).toEqual({
      cellB: { components: ["B"], edges: [] },
    });
  });

  // An unsaved workflow has no wid, so there is no mapping key to look up; the
  // precompute must drop the stale index instead of querying "mapping_wid_undefined".
  it("drops the highlight index without a lookup when the workflow has no wid", () => {
    mockWorkflow.getWorkflow.mockReturnValue({ wid: undefined });
    (service as any).cellToHighlightMapping = { stale: { components: ["X"], edges: [] } };
    const consoleWarn = vi.spyOn(console, "warn").mockImplementation(() => {});

    (service as any).precomputeHighlightMapping();

    expect(mockNotebook.getMapping).not.toHaveBeenCalled();
    expect((service as any).cellToHighlightMapping).toEqual({});
    expect(consoleWarn).toHaveBeenCalled();
  });

  // The notebook was found but its mapping was never stored locally: leave the
  // index empty rather than dereferencing the missing mapping.
  it("drops the highlight index when no mapping is stored for the workflow", () => {
    mockNotebook.getMapping.mockReturnValue(undefined);
    (service as any).cellToHighlightMapping = { stale: { components: ["X"], edges: [] } };
    const consoleWarn = vi.spyOn(console, "warn").mockImplementation(() => {});

    (service as any).precomputeHighlightMapping();

    expect(mockNotebook.getMapping).toHaveBeenCalledWith("mapping_wid_1");
    expect((service as any).cellToHighlightMapping).toEqual({});
    expect(consoleWarn).toHaveBeenCalled();
  });

  // A cell mapped to nothing must index as an empty component list; a raw
  // undefined would blow up the later `.length` check in highlightFromCell.
  it("indexes a cell with no mapped operators as an empty selection", () => {
    mockNotebook.getMapping.mockReturnValue({
      cell_to_operator: { emptyCell: null },
      operator_to_cell: {},
    });

    (service as any).precomputeHighlightMapping();

    expect((service as any).cellToHighlightMapping).toEqual({
      emptyCell: { components: [], edges: [] },
    });
  });

  // onWorkflowComponentClick
  it("should postMessage when mapping exists", async () => {
    const mockIframe = {
      contentWindow: {
        postMessage: vi.fn(),
      },
    } as any;

    service.setIframeRef(mockIframe);
    mockNotebook.getMapping.mockReturnValue({
      cell_to_operator: {},
      operator_to_cell: {
        cell1: ["op1", "op2"],
      },
    });

    await service.onWorkflowComponentClick("cell1");

    expect(mockIframe.contentWindow.postMessage).toHaveBeenCalledWith(
      {
        action: "triggerCellClick",
        operators: ["op1", "op2"],
      },
      "http://jupyter"
    );
  });

  it("does not postMessage when the operator maps to no cells", async () => {
    const mockIframe = {
      contentWindow: { postMessage: vi.fn() },
    } as any;
    service.setIframeRef(mockIframe);
    mockNotebook.getMapping.mockReturnValue({
      cell_to_operator: {},
      operator_to_cell: { op1: [] },
    });

    await service.onWorkflowComponentClick("op1");

    expect(mockIframe.contentWindow.postMessage).not.toHaveBeenCalled();
  });

  // The Jupyter origin is process-static, so it must be resolved once and cached
  // rather than re-fetched on every click / incoming message.
  it("resolves the Jupyter URL only once across multiple clicks", async () => {
    const mockIframe = {
      contentWindow: { postMessage: vi.fn() },
    } as any;
    service.setIframeRef(mockIframe);
    mockNotebook.getMapping.mockReturnValue({
      cell_to_operator: {},
      operator_to_cell: { cell1: ["op1"] },
    });

    await service.onWorkflowComponentClick("cell1");
    await service.onWorkflowComponentClick("cell1");
    await service.onWorkflowComponentClick("cell1");

    expect(mockNotebook.getJupyterURL).toHaveBeenCalledTimes(1);
  });

  it("does not postMessage for an unsaved workflow (undefined wid)", async () => {
    const mockIframe = { contentWindow: { postMessage: vi.fn() } } as any;
    service.setIframeRef(mockIframe);
    mockWorkflow.getWorkflow.mockReturnValue({ wid: undefined });
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});

    await service.onWorkflowComponentClick("op1");

    // Bailing before the lookup keeps a "mapping_wid_undefined" key from being read.
    expect(mockNotebook.getMapping).not.toHaveBeenCalled();
    expect(mockIframe.contentWindow.postMessage).not.toHaveBeenCalled();
    expect(consoleError).toHaveBeenCalled();
  });

  it("does not postMessage when the workflow has no stored mapping", async () => {
    const mockIframe = { contentWindow: { postMessage: vi.fn() } } as any;
    service.setIframeRef(mockIframe);
    mockNotebook.getMapping.mockReturnValue(undefined);
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});

    await service.onWorkflowComponentClick("op1");

    expect(mockNotebook.getMapping).toHaveBeenCalledWith("mapping_wid_1");
    expect(mockIframe.contentWindow.postMessage).not.toHaveBeenCalled();
    expect(consoleError).toHaveBeenCalled();
  });

  // The Jupyter pod may not be reachable yet when the first click lands. A
  // failed lookup must NOT be cached, or the panel stays dead for the rest of
  // the session.
  it("retries the origin lookup after an unavailable Jupyter URL", async () => {
    const mockIframe = { contentWindow: { postMessage: vi.fn() } } as any;
    service.setIframeRef(mockIframe);
    mockNotebook.getMapping.mockReturnValue({
      cell_to_operator: {},
      operator_to_cell: { op1: ["cell1"] },
    });
    mockNotebook.getJupyterURL.mockResolvedValueOnce(null);

    await service.onWorkflowComponentClick("op1");
    expect(mockIframe.contentWindow.postMessage).not.toHaveBeenCalled();

    await service.onWorkflowComponentClick("op1");

    expect(mockNotebook.getJupyterURL).toHaveBeenCalledTimes(2);
    expect(mockIframe.contentWindow.postMessage).toHaveBeenCalledWith(
      { action: "triggerCellClick", operators: ["cell1"] },
      "http://jupyter"
    );
  });

  it("treats a malformed Jupyter URL as unavailable and keeps retrying", async () => {
    const mockIframe = { contentWindow: { postMessage: vi.fn() } } as any;
    service.setIframeRef(mockIframe);
    mockNotebook.getMapping.mockReturnValue({
      cell_to_operator: {},
      operator_to_cell: { op1: ["cell1"] },
    });
    mockNotebook.getJupyterURL.mockResolvedValue("://not-a-url");

    await service.onWorkflowComponentClick("op1");
    await service.onWorkflowComponentClick("op1");

    expect(mockIframe.contentWindow.postMessage).not.toHaveBeenCalled();
    expect(mockNotebook.getJupyterURL).toHaveBeenCalledTimes(2);
  });

  // Feature flag gate (defence in depth). With the flag off, init must not
  // subscribe to workflow changes, and onWorkflowComponentClick must not
  // postMessage to the iframe. The window message listener is installed in
  // the constructor unconditionally, but handleNotebookMessage returns early
  // on the flag check.
  describe("when the feature flag is disabled", () => {
    beforeEach(() => {
      mockGuiConfig.env.pythonNotebookMigrationEnabled = false;
    });

    it("init does not subscribe to workflowMetaDataChanged", () => {
      service.init();
      expect(mockWorkflow.workflowMetaDataChanged).not.toHaveBeenCalled();
    });

    it("openPanel does not flip the visibility stream", () => {
      let state: boolean | null = false;
      service.jupyterNotebookPanelVisible$.subscribe(v => (state = v));
      service.openPanel("JupyterNotebookPanel");
      expect(state).toBe(false);
    });

    it("deleteJupyterNotebook does not call the backend or delete the mapping when disabled", () => {
      // When the feature is disabled the method returns early, so neither the
      // backend delete nor the local mapping drop should run.
      service.deleteJupyterNotebook();
      expect(mockNotebook.deleteNotebookAndMapping).not.toHaveBeenCalled();
      expect(mockNotebook.deleteMapping).not.toHaveBeenCalled();
    });

    it("minimizeJupyterNotebookPanel does not flip visibility", () => {
      const visibleSubject = (service as any).jupyterNotebookPanelVisible;
      visibleSubject.next(true);
      service.minimizeJupyterNotebookPanel();
      expect(visibleSubject.value).toBe(true);
    });

    it("openJupyterNotebookPanel does not warn or flip visibility", () => {
      mockNotebook.hasMapping.mockReturnValue(false);
      let state: boolean | null = false;
      service.jupyterNotebookPanelVisible$.subscribe(v => (state = v));
      service.openJupyterNotebookPanel();
      expect(state).toBe(false);
      expect(mockNotification.warning).not.toHaveBeenCalled();
    });

    it("getJupyterIframeURLForWorkflow resolves null without calling the migration service", async () => {
      mockNotebook.getJupyterIframeURL = vi.fn();
      const url = await service.getJupyterIframeURLForWorkflow();
      expect(url).toBeNull();
      expect(mockNotebook.getJupyterIframeURL).not.toHaveBeenCalled();
    });

    it("onWorkflowComponentClick does not postMessage to the iframe", async () => {
      const mockIframe = {
        contentWindow: { postMessage: vi.fn() },
      } as any;
      service.setIframeRef(mockIframe);
      await service.onWorkflowComponentClick("cell1");
      expect(mockIframe.contentWindow.postMessage).not.toHaveBeenCalled();
    });

    it("handleNotebookMessage ignores a cellClicked message from our own iframe", async () => {
      const iframeWindow = {} as Window;
      service.setIframeRef({ contentWindow: iframeWindow } as any);
      const highlightSpy = vi.spyOn(service as any, "highlightFromCell").mockImplementation(() => {});

      await (service as any).handleNotebookMessage({
        source: iframeWindow,
        origin: "http://jupyter",
        data: { action: "cellClicked", cellUUID: "c1" },
      });

      expect(highlightSpy).not.toHaveBeenCalled();
      // The message carried our own source and origin, so it was the flag check
      // that stopped it — the origin was never even resolved.
      expect(mockNotebook.getJupyterURL).not.toHaveBeenCalled();
    });
  });
});
