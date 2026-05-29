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
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { firstValueFrom } from "rxjs";
import { take, toArray } from "rxjs/operators";

import { WORKFLOW_VERSIONS_API_BASE_URL, WorkflowVersionService } from "./workflow-version.service";
import { WorkflowActionService } from "../../../../workspace/service/workflow-graph/model/workflow-action.service";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { UndoRedoService } from "../../../../workspace/service/undo-redo/undo-redo.service";
import { AppSettings } from "../../../../common/app-setting";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { ExecutionMode, Workflow, WorkflowContent } from "../../../../common/type/workflow";
import { CommentBox, OperatorLink, OperatorPredicate } from "../../../../workspace/types/workflow-common.interface";
import { WorkflowVersionEntry } from "../../../type/workflow-version-entry";

const API = "api";

function buildOperator(overrides: Partial<OperatorPredicate> = {}): OperatorPredicate {
  return {
    operatorID: "op-1",
    operatorType: "Filter",
    operatorVersion: "v1",
    operatorProperties: {},
    inputPorts: [],
    outputPorts: [],
    showAdvanced: false,
    ...overrides,
  };
}

function buildLink(source: string, target: string): OperatorLink {
  return {
    linkID: `${source}->${target}`,
    source: { operatorID: source, portID: "out-0" },
    target: { operatorID: target, portID: "in-0" },
  };
}

function buildContent(overrides: Partial<WorkflowContent> = {}): WorkflowContent {
  return {
    operators: [],
    operatorPositions: {},
    links: [],
    commentBoxes: [] as CommentBox[],
    settings: { dataTransferBatchSize: 400, executionMode: ExecutionMode.PIPELINED },
    ...overrides,
  };
}

function buildWorkflow(overrides: Partial<Workflow> = {}): Workflow {
  return {
    name: "wf",
    description: undefined,
    wid: 1,
    creationTime: 0,
    lastModifiedTime: 0,
    isPublished: 0,
    readonly: false,
    content: buildContent(),
    ...overrides,
  };
}

describe("WorkflowVersionService", () => {
  let service: WorkflowVersionService;
  let http: HttpTestingController;
  let actionSpy: {
    getJointGraphWrapper: ReturnType<typeof vi.fn>;
    getWorkflow: ReturnType<typeof vi.fn>;
    getWorkflowContent: ReturnType<typeof vi.fn>;
    setTempWorkflow: ReturnType<typeof vi.fn>;
    getTempWorkflow: ReturnType<typeof vi.fn>;
    resetTempWorkflow: ReturnType<typeof vi.fn>;
    reloadWorkflow: ReturnType<typeof vi.fn>;
    enableWorkflowModification: ReturnType<typeof vi.fn>;
    disableWorkflowModification: ReturnType<typeof vi.fn>;
    checkWorkflowModificationEnabled: ReturnType<typeof vi.fn>;
  };
  let persistSpy: { setWorkflowPersistFlag: ReturnType<typeof vi.fn> };
  let undoRedoSpy: {
    clearRedoStack: ReturnType<typeof vi.fn>;
    clearUndoStack: ReturnType<typeof vi.fn>;
    enableWorkFlowModification: ReturnType<typeof vi.fn>;
    disableWorkFlowModification: ReturnType<typeof vi.fn>;
  };
  let highlightedSpy: { getCurrentHighlights: ReturnType<typeof vi.fn>; unhighlightElements: ReturnType<typeof vi.fn> };
  let modelAttr: ReturnType<typeof vi.fn>;
  let paperGetModelById: ReturnType<typeof vi.fn>;
  let getMainJointPaper: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    modelAttr = vi.fn();
    paperGetModelById = vi.fn().mockReturnValue({ attr: modelAttr });
    getMainJointPaper = vi.fn().mockReturnValue({ getModelById: paperGetModelById });
    highlightedSpy = {
      getCurrentHighlights: vi.fn().mockReturnValue(["a", "b"]),
      unhighlightElements: vi.fn(),
    };
    actionSpy = {
      getJointGraphWrapper: vi.fn().mockReturnValue({
        getCurrentHighlights: highlightedSpy.getCurrentHighlights,
        unhighlightElements: highlightedSpy.unhighlightElements,
        getMainJointPaper,
      }),
      getWorkflow: vi.fn().mockReturnValue(buildWorkflow()),
      getWorkflowContent: vi.fn().mockReturnValue(buildContent()),
      setTempWorkflow: vi.fn(),
      getTempWorkflow: vi.fn().mockReturnValue(undefined),
      resetTempWorkflow: vi.fn(),
      reloadWorkflow: vi.fn(),
      enableWorkflowModification: vi.fn(),
      disableWorkflowModification: vi.fn(),
      checkWorkflowModificationEnabled: vi.fn().mockReturnValue(true),
    };
    persistSpy = { setWorkflowPersistFlag: vi.fn() };
    undoRedoSpy = {
      clearRedoStack: vi.fn(),
      clearUndoStack: vi.fn(),
      enableWorkFlowModification: vi.fn(),
      disableWorkFlowModification: vi.fn(),
    };

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        WorkflowVersionService,
        { provide: WorkflowActionService, useValue: actionSpy },
        { provide: WorkflowPersistService, useValue: persistSpy },
        { provide: UndoRedoService, useValue: undoRedoSpy },
        ...commonTestProviders,
      ],
    });
    service = TestBed.inject(WorkflowVersionService);
    http = TestBed.inject(HttpTestingController);
    vi.spyOn(AppSettings, "getApiEndpoint").mockReturnValue(API);
  });

  afterEach(() => {
    http.verify();
  });

  // ─── canRestoreVersion getter ─────────────────────────────────────────────

  describe("canRestoreVersion", () => {
    it("is false when no readonly display has been entered", () => {
      expect(service.canRestoreVersion).toBe(false);
    });

    it("is true after entering readonly display from a modification-enabled state", () => {
      actionSpy.checkWorkflowModificationEnabled.mockReturnValue(true);
      service.displayReadonlyWorkflow(buildWorkflow());
      expect(service.canRestoreVersion).toBe(true);
    });

    it("is false after entering readonly display from an already-disabled state", () => {
      actionSpy.checkWorkflowModificationEnabled.mockReturnValue(false);
      service.displayReadonlyWorkflow(buildWorkflow());
      expect(service.canRestoreVersion).toBe(false);
    });
  });

  // ─── setDisplayParticularVersion + streams ────────────────────────────────

  describe("setDisplayParticularVersion", () => {
    it("publishes both ids and the flag when entering a particular version", async () => {
      const flag = firstValueFrom(service.getDisplayParticularVersionStream().pipe(take(2), toArray()));
      service.setDisplayParticularVersion(true, 7, 9);
      expect(service.selectedVersionId.getValue()).toBe(7);
      expect(service.selectedDisplayedVersionId.getValue()).toBe(9);
      expect(await flag).toEqual([false, true]);
    });

    it("ignores undefined version ids but still flips the flag", () => {
      service.setDisplayParticularVersion(true);
      expect(service.selectedVersionId.getValue()).toBeNull();
      expect(service.selectedDisplayedVersionId.getValue()).toBeNull();
    });

    it("nulls both ids when leaving a particular version", () => {
      service.setDisplayParticularVersion(true, 7, 9);
      service.setDisplayParticularVersion(false);
      expect(service.selectedVersionId.getValue()).toBeNull();
      expect(service.selectedDisplayedVersionId.getValue()).toBeNull();
    });
  });

  // ─── displayWorkflowVersions ──────────────────────────────────────────────

  it("displayWorkflowVersions unhighlights whatever is currently highlighted", () => {
    service.displayWorkflowVersions();
    expect(highlightedSpy.getCurrentHighlights).toHaveBeenCalled();
    expect(highlightedSpy.unhighlightElements).toHaveBeenCalledWith(["a", "b"]);
  });

  // ─── displayReadonlyWorkflow ──────────────────────────────────────────────

  describe("displayReadonlyWorkflow", () => {
    it("snapshots the current workflow, disables persist+undo, then reloads readonly", () => {
      const live = buildWorkflow({ name: "live" });
      const incoming = buildWorkflow({ name: "incoming" });
      actionSpy.getWorkflow.mockReturnValue(live);

      service.displayReadonlyWorkflow(incoming);

      expect(actionSpy.setTempWorkflow).toHaveBeenCalledWith(live);
      expect(persistSpy.setWorkflowPersistFlag).toHaveBeenCalledWith(false);
      expect(undoRedoSpy.disableWorkFlowModification).toHaveBeenCalled();
      expect(actionSpy.reloadWorkflow).toHaveBeenCalledWith(incoming);
      expect(actionSpy.disableWorkflowModification).toHaveBeenCalled();
    });

    it("only captures the modification state once across nested calls", () => {
      actionSpy.checkWorkflowModificationEnabled.mockReturnValue(true);
      service.displayReadonlyWorkflow(buildWorkflow());

      // Second entry must not overwrite the snapshot to `false`.
      actionSpy.checkWorkflowModificationEnabled.mockReturnValue(false);
      service.displayReadonlyWorkflow(buildWorkflow());

      expect(service.canRestoreVersion).toBe(true);
    });
  });

  // ─── displayParticularVersion ─────────────────────────────────────────────

  it("displayParticularVersion diffs, swaps the paper, and emits the flag", () => {
    const liveContent = buildContent({ operators: [buildOperator({ operatorID: "live" })] });
    const versionContent = buildContent({ operators: [buildOperator({ operatorID: "old" })] });
    actionSpy.getWorkflowContent.mockReturnValue(liveContent);
    const versionWorkflow = buildWorkflow({ content: versionContent });

    service.displayParticularVersion(versionWorkflow, 5, 6);

    expect(actionSpy.reloadWorkflow).toHaveBeenCalledWith(versionWorkflow);
    expect(service.selectedVersionId.getValue()).toBe(5);
    expect(service.selectedDisplayedVersionId.getValue()).toBe(6);
  });

  // ─── highlightOpBoundary / highlightOpBracket ─────────────────────────────

  describe("highlight helpers", () => {
    it("highlightOpBoundary writes rect.boundary/fill with the rgba color", () => {
      service.highlightOpBoundary("op-1", "1,2,3,0.5");
      expect(paperGetModelById).toHaveBeenCalledWith("op-1");
      expect(modelAttr).toHaveBeenCalledWith("rect.boundary/fill", "rgba(1,2,3,0.5)");
    });

    it("highlightOpBracket writes the position-prefixed stroke attribute", () => {
      service.highlightOpBracket("op-1", "1,2,3,0.5", "left-");
      expect(modelAttr).toHaveBeenCalledWith("path.left-boundary/stroke", "rgba(1,2,3,0.5)");
    });

    it("is a no-op when the joint paper is not yet bound", () => {
      getMainJointPaper.mockReturnValue(undefined);
      service.highlightOpBoundary("op-1", "1,2,3,0.5");
      expect(paperGetModelById).not.toHaveBeenCalled();
    });
  });

  // ─── highlightOpVersionDiff ───────────────────────────────────────────────

  describe("highlightOpVersionDiff", () => {
    it("colors modified ops orange and added ops green", () => {
      service.highlightOpVersionDiff({ modified: ["m"], added: ["a"], deleted: [] });

      expect(modelAttr).toHaveBeenCalledWith("rect.boundary/fill", "rgba(255,118,20,0.5)");
      expect(modelAttr).toHaveBeenCalledWith("rect.boundary/fill", "rgba(0,255,0,0.5)");
    });

    it("draws left/right red brackets around the neighbors of deleted ops", () => {
      const tempWorkflow = buildWorkflow({
        content: buildContent({
          operators: [buildOperator({ operatorID: "alive-left" }), buildOperator({ operatorID: "alive-right" })],
          links: [buildLink("dead", "alive-right"), buildLink("alive-left", "dead")],
        }),
      });
      actionSpy.getTempWorkflow.mockReturnValue(tempWorkflow);

      service.highlightOpVersionDiff({ modified: [], added: [], deleted: ["dead"] });

      expect(modelAttr).toHaveBeenCalledWith("path.left-boundary/stroke", "rgba(255,0,0,0.5)");
      expect(modelAttr).toHaveBeenCalledWith("path.right-boundary/stroke", "rgba(255,0,0,0.5)");
    });

    it("skips bracket drawing when the temp workflow is missing", () => {
      actionSpy.getTempWorkflow.mockReturnValue(undefined);

      service.highlightOpVersionDiff({ modified: [], added: [], deleted: ["dead"] });

      expect(getMainJointPaper).not.toHaveBeenCalled();
    });
  });

  // ─── unhighlightOpVersionDiff ─────────────────────────────────────────────

  describe("unhighlightOpVersionDiff", () => {
    it("resets the boundary fill of added and modified ops to transparent", () => {
      service.unhighlightOpVersionDiff({ modified: ["m"], added: ["a"], deleted: [] });

      expect(paperGetModelById).toHaveBeenCalledWith("m");
      expect(paperGetModelById).toHaveBeenCalledWith("a");
      expect(modelAttr).toHaveBeenCalledWith("rect.boundary/fill", "rgba(0,0,0,0)");
    });

    it("resets the red brackets drawn around the neighbors of deleted ops", () => {
      const tempWorkflow = buildWorkflow({
        content: buildContent({
          operators: [buildOperator({ operatorID: "alive-left" }), buildOperator({ operatorID: "alive-right" })],
          links: [buildLink("dead", "alive-right"), buildLink("alive-left", "dead")],
        }),
      });
      actionSpy.getTempWorkflow.mockReturnValue(tempWorkflow);

      service.unhighlightOpVersionDiff({ modified: [], added: [], deleted: ["dead"] });

      expect(modelAttr).toHaveBeenCalledWith("path.left-boundary/stroke", "rgba(0,0,0,0)");
      expect(modelAttr).toHaveBeenCalledWith("path.right-boundary/stroke", "rgba(0,0,0,0)");
    });

    it("skips bracket clearing when the temp workflow is missing", () => {
      actionSpy.getTempWorkflow.mockReturnValue(undefined);

      service.unhighlightOpVersionDiff({ modified: [], added: [], deleted: ["dead"] });

      expect(getMainJointPaper).not.toHaveBeenCalled();
    });
  });

  // ─── getWorkflowsDifference / getOperatorsDifference ──────────────────────

  describe("getWorkflowsDifference", () => {
    it("classifies operators into added, deleted, and modified", () => {
      // Forward diff semantics: ids present in arg2 but not arg1 are "added"
      // (to go from arg1 to arg2 you'd add them), ids in arg1 only are
      // "deleted", and ids in both with differing content are "modified".
      const a1 = buildOperator({ operatorID: "stay", operatorProperties: { a: 1 } });
      const a2 = buildOperator({ operatorID: "stay", operatorProperties: { a: 2 } });
      const onlyInArg1 = buildOperator({ operatorID: "in-arg1-only" });
      const onlyInArg2 = buildOperator({ operatorID: "in-arg2-only" });

      const diff = service.getWorkflowsDifference(
        buildContent({ operators: [a1, onlyInArg1] }),
        buildContent({ operators: [a2, onlyInArg2] })
      );

      expect(diff.added.sort()).toEqual(["in-arg2-only"]);
      expect(diff.modified.sort()).toEqual(["stay"]);
      expect(diff.deleted.sort()).toEqual(["in-arg1-only"]);
    });

    it("returns empty diffs when the contents are identical", () => {
      const op = buildOperator();
      const diff = service.getWorkflowsDifference(buildContent({ operators: [op] }), buildContent({ operators: [op] }));
      expect(diff).toEqual({ added: [], modified: [], deleted: [] });
      expect(service.operatorPropertyDiff).toEqual({});
    });

    it("records per-property and version-bump diffs in operatorPropertyDiff", () => {
      const live = buildOperator({
        operatorID: "x",
        operatorVersion: "v2",
        operatorProperties: { foo: "a", bar: 1 },
      });
      const old = buildOperator({
        operatorID: "x",
        operatorVersion: "v1",
        operatorProperties: { foo: "b", bar: 1 },
      });

      service.getWorkflowsDifference(buildContent({ operators: [live] }), buildContent({ operators: [old] }));

      const diffMap = service.operatorPropertyDiff["x"];
      expect(diffMap.get("foo" as unknown as String)).toContain("rgb(255, 118, 20)");
      expect(diffMap.has("bar" as unknown as String)).toBe(false);
      expect(diffMap.has("operatorVersion" as unknown as String)).toBe(true);
    });
  });

  // ─── revertToVersion / closeReadonlyWorkflowDisplay / closeParticular ─────

  describe("close + revert helpers", () => {
    it("revertToVersion clears stacks, re-enables modification, and exits readonly mode", () => {
      service.setDisplayParticularVersion(true, 4, 8);
      service["differentOpIDsList"] = { modified: ["m"], added: ["a"], deleted: [] };

      service.revertToVersion();

      expect(undoRedoSpy.clearRedoStack).toHaveBeenCalled();
      expect(undoRedoSpy.clearUndoStack).toHaveBeenCalled();
      expect(actionSpy.enableWorkflowModification).toHaveBeenCalled();
      expect(actionSpy.resetTempWorkflow).toHaveBeenCalled();
      expect(persistSpy.setWorkflowPersistFlag).toHaveBeenCalledWith(true);
      expect(service.selectedVersionId.getValue()).toBeNull();
      expect(modelAttr).toHaveBeenCalledWith("rect.boundary/fill", "rgba(0,0,0,0)");
    });

    it("closeReadonlyWorkflowDisplay restores the previous workflow and re-enables undo", () => {
      const previous = buildWorkflow({ name: "previous" });
      actionSpy.getTempWorkflow.mockReturnValue(previous);

      service.closeReadonlyWorkflowDisplay();

      expect(actionSpy.enableWorkflowModification).toHaveBeenCalled();
      expect(undoRedoSpy.disableWorkFlowModification).toHaveBeenCalled();
      expect(actionSpy.reloadWorkflow).toHaveBeenCalledWith(previous);
      expect(actionSpy.resetTempWorkflow).toHaveBeenCalled();
      expect(undoRedoSpy.enableWorkFlowModification).toHaveBeenCalled();
      expect(persistSpy.setWorkflowPersistFlag).toHaveBeenCalledWith(true);
    });

    it("closeParticularVersionDisplay unhighlights, restores, and flips the flag", () => {
      service["differentOpIDsList"] = { modified: ["m"], added: [], deleted: [] };
      service.setDisplayParticularVersion(true, 1, 2);

      service.closeParticularVersionDisplay();

      expect(modelAttr).toHaveBeenCalledWith("rect.boundary/fill", "rgba(0,0,0,0)");
      expect(actionSpy.reloadWorkflow).toHaveBeenCalled();
      expect(service.selectedVersionId.getValue()).toBeNull();
    });

    it("restoreModificationState re-disables modification when the snapshot was false", () => {
      actionSpy.checkWorkflowModificationEnabled.mockReturnValue(false);
      service.displayReadonlyWorkflow(buildWorkflow());
      actionSpy.disableWorkflowModification.mockClear();

      service.closeReadonlyWorkflowDisplay();

      expect(actionSpy.disableWorkflowModification).toHaveBeenCalled();
      actionSpy.disableWorkflowModification.mockClear();
      service.closeReadonlyWorkflowDisplay();
      expect(actionSpy.disableWorkflowModification).not.toHaveBeenCalled();
    });
  });

  // ─── HTTP endpoints ───────────────────────────────────────────────────────

  describe("HTTP", () => {
    it("retrieveVersionsOfWorkflow GETs /version/{wid}", async () => {
      const entries: WorkflowVersionEntry[] = [{ vId: 1, creationTime: 0, content: "{}", importance: false }];
      const pending = firstValueFrom(service.retrieveVersionsOfWorkflow(42));
      const req = http.expectOne(`${API}/${WORKFLOW_VERSIONS_API_BASE_URL}/42`);
      expect(req.request.method).toBe("GET");
      req.flush(entries);
      expect(await pending).toEqual(entries);
    });

    it("retrieveWorkflowByVersion parses a string `content` into an object", async () => {
      const pending = firstValueFrom(service.retrieveWorkflowByVersion(42, 7));
      const req = http.expectOne(`${API}/${WORKFLOW_VERSIONS_API_BASE_URL}/42/7`);
      expect(req.request.method).toBe("GET");
      req.flush({
        ...buildWorkflow(),
        content:
          '{"operators":[],"operatorPositions":{},"links":[],"commentBoxes":[],"settings":{"dataTransferBatchSize":400,"executionMode":"PIPELINED"}}',
      });
      const result = await pending;
      expect(typeof result.content).toBe("object");
      expect(result.content.operators).toEqual([]);
    });

    it("retrieveWorkflowByVersion drops null payloads (filter blocks the emission)", () => {
      let nextCalled = false;
      let completed = false;
      service.retrieveWorkflowByVersion(42, 7).subscribe({
        next: () => (nextCalled = true),
        complete: () => (completed = true),
      });
      http.expectOne(`${API}/${WORKFLOW_VERSIONS_API_BASE_URL}/42/7`).flush(null);
      expect(nextCalled).toBe(false);
      expect(completed).toBe(true);
    });

    it("cloneWorkflowVersion POSTs to /version/clone/{vid} with the displayed id", async () => {
      service.setDisplayParticularVersion(true, 11, 12);
      const pending = firstValueFrom(service.cloneWorkflowVersion());
      const req = http.expectOne(`${API}/${WORKFLOW_VERSIONS_API_BASE_URL}/clone/11`);
      expect(req.request.method).toBe("POST");
      expect(req.request.body).toEqual({ displayedVersionId: 12 });
      req.flush(99);
      expect(await pending).toBe(99);
    });
  });
});
