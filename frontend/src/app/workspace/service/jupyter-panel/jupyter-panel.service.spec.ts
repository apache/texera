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
import { NotebookMigrationService } from "../notebook-migration/notebook-migration.service";
import { GuiConfigService } from "src/app/common/service/gui-config.service";
import { firstValueFrom, of } from "rxjs";

describe("JupyterPanelService", () => {
  let service: JupyterPanelService;
  let httpMock: HttpTestingController;

  let mockWorkflow: any;
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
    };

    mockGuiConfig = { env: { pythonNotebookMigrationEnabled: true } };

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        JupyterPanelService,
        { provide: WorkflowActionService, useValue: mockWorkflow },
        { provide: NotebookMigrationService, useValue: mockNotebook },
        { provide: GuiConfigService, useValue: mockGuiConfig },
      ],
    });

    service = TestBed.inject(JupyterPanelService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  // HTTP fetchNotebookAndMapping
  it("should return 0 when exists=false", async () => {
    const resultPromise = firstValueFrom((service as any).fetchNotebookAndMapping(1, 1));

    const req = httpMock.expectOne(r => r.url.includes("/notebook-migration/fetch-notebook-and-mapping"));
    req.flush({ exists: false });

    expect(await resultPromise).toBe(0);
  });

  // init(): subscribes to workflow changes, drops the stale mapping for the
  // current workflow, and fetches the incoming workflow's notebook + mapping.
  it("init subscribes, drops the stale mapping, and fetches for the new workflow", () => {
    service.init();

    expect(mockWorkflow.workflowMetaDataChanged).toHaveBeenCalled();
    expect(mockNotebook.deleteMapping).toHaveBeenCalledWith("mapping_wid_1");

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

    it("onWorkflowComponentClick does not postMessage to the iframe", async () => {
      const mockIframe = {
        contentWindow: { postMessage: vi.fn() },
      } as any;
      service.setIframeRef(mockIframe);
      await service.onWorkflowComponentClick("cell1");
      expect(mockIframe.contentWindow.postMessage).not.toHaveBeenCalled();
    });
  });
});
