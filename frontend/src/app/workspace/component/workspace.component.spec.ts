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

import { Location } from "@angular/common";
// TODO(coverage): this spec was set up in #5037 to render the workspace with
// stripped child imports + CUSTOM_ELEMENTS_SCHEMA so the @ViewChild on
// #codeEditor resolves while the deep child tree stays out of the bundle.
// Migrating it off NO_ERRORS_SCHEMA / set:{imports:[]} requires providing
// each child's transitive deps; tracking separately.
// eslint-disable-next-line no-restricted-imports
import { CUSTOM_ELEMENTS_SCHEMA, NO_ERRORS_SCHEMA } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { ActivatedRoute, Router } from "@angular/router";
import { NzMessageService } from "ng-zorro-antd/message";
import { EMPTY, of, Subject, throwError } from "rxjs";

import { NotificationService } from "../../common/service/notification/notification.service";
import { UserService } from "../../common/service/user/user.service";
import { WorkflowPersistService } from "../../common/service/workflow-persist/workflow-persist.service";
import { Workflow } from "../../common/type/workflow";
import { CodeEditorService } from "../service/code-editor/code-editor.service";
import { WorkflowCompilingService } from "../service/compile-workflow/workflow-compiling.service";
import { OperatorMetadataService } from "../service/operator-metadata/operator-metadata.service";
import { UndoRedoService } from "../service/undo-redo/undo-redo.service";
import { WorkflowConsoleService } from "../service/workflow-console/workflow-console.service";
import { ExecuteWorkflowService } from "../service/execute-workflow/execute-workflow.service";
import { WorkflowResultService } from "../service/workflow-result/workflow-result.service";
import { WorkflowActionService } from "../service/workflow-graph/model/workflow-action.service";
import { OperatorReuseCacheStatusService } from "../service/workflow-status/operator-reuse-cache-status.service";
import { ComputingUnitStatusService } from "../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { EntityType, HubService } from "../../hub/service/hub.service";
import { commonTestProviders } from "../../common/testing/test-utils";
import { WorkspaceComponent } from "./workspace.component";
import { USER_WORKSPACE } from "../../app-routing.constant";

describe("WorkspaceComponent", () => {
  let component: WorkspaceComponent;
  let fixture: ComponentFixture<WorkspaceComponent>;

  let workflowActionService: any;
  let workflowPersistService: any;
  let operatorMetadataService: any;
  let userService: any;
  let undoRedoService: any;
  let notificationService: any;
  let hubService: any;
  let codeEditorService: any;
  let messageService: any;
  let routerMock: any;
  let locationMock: any;
  let computingUnitStatusService: any;
  let executeWorkflowService: any;
  let workflowConsoleService: any;
  let workflowResultService: any;
  let connectionResetSubject: Subject<void>;
  let metadataChangedSubject: Subject<void>;
  let stubGraph: { triggerCenterEvent: ReturnType<typeof vi.fn>; hasElementWithID: ReturnType<typeof vi.fn> };

  const stubWorkflow: Workflow = {
    wid: 42,
    name: "test",
    creationTime: 0,
    lastModifiedTime: 0,
    content: {
      operators: [],
      operatorPositions: {},
      links: [],
      commentBoxes: [],
      settings: { dataTransferBatchSize: 100 },
    },
  } as unknown as Workflow;

  function configureRoute(params: Record<string, any> = {}, queryParams: Record<string, any> = {}) {
    return {
      snapshot: { params, queryParams, fragment: null as string | null },
    };
  }

  async function createFixture(routeOverride: any = configureRoute()) {
    metadataChangedSubject = new Subject<void>();
    stubGraph = {
      triggerCenterEvent: vi.fn(),
      hasElementWithID: vi.fn().mockReturnValue(false),
    };

    workflowActionService = {
      setHighlightingEnabled: vi.fn(),
      resetAsNewWorkflow: vi.fn(),
      disableWorkflowModification: vi.fn(),
      enableWorkflowModification: vi.fn(),
      reloadWorkflow: vi.fn(),
      setNewSharedModel: vi.fn(),
      setWorkflowMetadata: vi.fn(),
      clearWorkflow: vi.fn(),
      highlightElements: vi.fn(),
      getTexeraGraph: vi.fn().mockReturnValue(stubGraph),
      getWorkflow: vi.fn().mockReturnValue(stubWorkflow),
      getWorkflowMetadata: vi.fn().mockReturnValue({ wid: 42, readonly: false }),
      workflowChanged: vi.fn().mockReturnValue(EMPTY),
      workflowMetaDataChanged: vi.fn().mockReturnValue(metadataChangedSubject.asObservable()),
    };

    workflowPersistService = {
      isWorkflowPersistEnabled: vi.fn().mockReturnValue(true),
      persistWorkflow: vi.fn().mockReturnValue(of(stubWorkflow)),
      retrieveWorkflow: vi.fn().mockReturnValue(of(stubWorkflow)),
    };

    operatorMetadataService = {
      getOperatorMetadata: vi.fn().mockReturnValue(of({})),
    };

    userService = {
      isLogin: vi.fn().mockReturnValue(true),
      getCurrentUser: vi.fn().mockReturnValue({ uid: 7 }),
    };

    undoRedoService = {
      clearUndoStack: vi.fn(),
      clearRedoStack: vi.fn(),
    };

    notificationService = { error: vi.fn() };
    hubService = { postView: vi.fn().mockReturnValue(of(0)) };
    codeEditorService = { vc: undefined };
    messageService = { error: vi.fn() };

    routerMock = { navigate: vi.fn() };
    locationMock = { go: vi.fn() };
    connectionResetSubject = new Subject<void>();
    computingUnitStatusService = {
      disconnect: vi.fn(),
      getConnectionResetStream: () => connectionResetSubject.asObservable(),
    };
    executeWorkflowService = { resetExecutionAndWorkers: vi.fn() };
    workflowConsoleService = { clearConsoleMessages: vi.fn() };
    workflowResultService = { clearResults: vi.fn() };

    // Drop the standalone component's child imports and allow unknown elements via
    // CUSTOM_ELEMENTS_SCHEMA. The template still renders, so `<ng-template #codeEditor>`
    // is wired up and the @ViewChild query resolves to a real ViewContainerRef, while
    // the children's transitive dependencies stay out of the test build.
    // TODO(coverage): rewrite using stub child components via remove/add so the
    // template participates in coverage. See TESTING.md anti-pattern #9.
    /* eslint-disable no-restricted-syntax */
    TestBed.overrideComponent(WorkspaceComponent, {
      set: { imports: [], providers: [], schemas: [CUSTOM_ELEMENTS_SCHEMA] },
    });
    /* eslint-enable no-restricted-syntax */

    await TestBed.configureTestingModule({
      imports: [WorkspaceComponent, HttpClientTestingModule],
      providers: [
        { provide: WorkflowActionService, useValue: workflowActionService },
        { provide: WorkflowPersistService, useValue: workflowPersistService },
        { provide: OperatorMetadataService, useValue: operatorMetadataService },
        { provide: UserService, useValue: userService },
        { provide: UndoRedoService, useValue: undoRedoService },
        { provide: NotificationService, useValue: notificationService },
        { provide: HubService, useValue: hubService },
        { provide: CodeEditorService, useValue: codeEditorService },
        { provide: NzMessageService, useValue: messageService },
        { provide: Router, useValue: routerMock },
        { provide: Location, useValue: locationMock },
        { provide: ActivatedRoute, useValue: routeOverride },
        // The three services listed in the constructor only to force their
        // initialization aren't exercised by any test here; provide stubs.
        { provide: WorkflowCompilingService, useValue: {} },
        { provide: WorkflowConsoleService, useValue: workflowConsoleService },
        { provide: OperatorReuseCacheStatusService, useValue: {} },
        { provide: ComputingUnitStatusService, useValue: computingUnitStatusService },
        { provide: ExecuteWorkflowService, useValue: executeWorkflowService },
        { provide: WorkflowResultService, useValue: workflowResultService },
        ...commonTestProviders,
      ],
      schemas: [NO_ERRORS_SCHEMA],
    }).compileComponents();

    fixture = TestBed.createComponent(WorkspaceComponent);
    component = fixture.componentInstance;
    // ngOnDestroy clears the ViewContainerRef bound to `#codeEditor`. Tests that
    // exercise individual methods skip change detection, so the @ViewChild query
    // is never resolved; assign a stub to keep TestBed teardown from throwing.
    // Tests that exercise `fixture.detectChanges()` will overwrite this with
    // the live ViewContainerRef during ngAfterViewInit.
    component.codeEditorViewRef = { clear: vi.fn() } as any;
  }

  describe("ngOnInit", () => {
    it("parses numeric pid from route query params", async () => {
      await createFixture(configureRoute({}, { pid: "13" }));
      component.ngOnInit();
      expect(component.pid).toBe(13);
    });

    it("treats non-numeric pid as undefined", async () => {
      await createFixture(configureRoute({}, { pid: "not-a-number" }));
      component.ngOnInit();
      expect(component.pid).toBeUndefined();
    });

    it("enables highlighting on the workflow action service", async () => {
      await createFixture();
      component.ngOnInit();
      expect(workflowActionService.setHighlightingEnabled).toHaveBeenCalledWith(true);
    });
  });

  describe("ngAfterViewInit", () => {
    it("cold start (no wid in route): does not flip isLoading and registers metadata listener", async () => {
      await createFixture(configureRoute({}));
      fixture.detectChanges(); // triggers ngOnInit + ngAfterViewInit
      expect(component.isLoading).toBe(false);
      expect(workflowActionService.disableWorkflowModification).not.toHaveBeenCalled();
      expect(operatorMetadataService.getOperatorMetadata).toHaveBeenCalled();
    });

    it("warm start (wid in route): sets isLoading=true and disables modification before load", async () => {
      await createFixture(configureRoute({ id: "42" }));
      // retrieveWorkflow is consumed inside loadWorkflowWithId — keep it pending so
      // we can observe the pre-completion loading state.
      workflowPersistService.retrieveWorkflow.mockReturnValue(new Subject());
      // Drive the lifecycle hooks directly. Going through fixture.detectChanges()
      // would re-render `[nzSpinning]="isLoading"` mid-cycle (isLoading flips from
      // false to true inside ngAfterViewInit) and Angular's dev-mode stability
      // check would throw NG0100.
      component.ngOnInit();
      component.ngAfterViewInit();
      expect(component.isLoading).toBe(true);
      expect(workflowActionService.disableWorkflowModification).toHaveBeenCalled();
    });
  });

  describe("loadWorkflowWithId", () => {
    it("on success: hands the workflow to the action service, clears undo/redo, and turns off loading", async () => {
      await createFixture(configureRoute({ id: "42" }));
      fixture.detectChanges();
      expect(workflowActionService.setNewSharedModel).toHaveBeenCalledWith(42, { uid: 7 });
      expect(workflowActionService.reloadWorkflow).toHaveBeenCalledWith(stubWorkflow);
      expect(undoRedoService.clearUndoStack).toHaveBeenCalled();
      expect(undoRedoService.clearRedoStack).toHaveBeenCalled();
      expect(component.isLoading).toBe(false);
    });

    it("on failure: resets to a new workflow, surfaces an access error, and turns off loading", async () => {
      await createFixture(configureRoute({ id: "42" }));
      workflowPersistService.retrieveWorkflow.mockReturnValue(throwError(() => new Error("403")));
      fixture.detectChanges();
      expect(workflowActionService.resetAsNewWorkflow).toHaveBeenCalled();
      expect(workflowActionService.enableWorkflowModification).toHaveBeenCalled();
      expect(messageService.error).toHaveBeenCalledWith(expect.stringContaining("don't have access"));
      expect(component.isLoading).toBe(false);
    });

    it("flags broken workflows via NotificationService.error but still loads them", async () => {
      const brokenWorkflow = {
        ...stubWorkflow,
        content: {
          ...stubWorkflow.content,
          // link references operator IDs that aren't in `operators: []` → broken.
          links: [{ source: { operatorID: "ghost-a" }, target: { operatorID: "ghost-b" } }],
        },
      } as unknown as Workflow;
      await createFixture(configureRoute({ id: "42" }));
      workflowPersistService.retrieveWorkflow.mockReturnValue(of(brokenWorkflow));
      fixture.detectChanges();
      expect(notificationService.error).toHaveBeenCalledWith(expect.stringContaining("broken"));
      // Workflow still flows through reload — the error is informational, not blocking.
      expect(workflowActionService.reloadWorkflow).toHaveBeenCalledWith(brokenWorkflow);
    });

    it("when URL fragment matches an element in the graph, highlights it", async () => {
      const route = configureRoute({ id: "42" });
      route.snapshot.fragment = "operator-1";
      await createFixture(route);
      stubGraph.hasElementWithID.mockReturnValue(true);
      fixture.detectChanges();
      expect(stubGraph.hasElementWithID).toHaveBeenCalledWith("operator-1");
      expect(workflowActionService.highlightElements).toHaveBeenCalledWith(false, "operator-1");
    });

    it("when URL fragment does not match any element, surfaces an error and clears the fragment", async () => {
      const route = configureRoute({ id: "42" });
      route.snapshot.fragment = "stale-id";
      await createFixture(route);
      // Default mock already returns false, but state explicitly for clarity.
      stubGraph.hasElementWithID.mockReturnValue(false);
      fixture.detectChanges();
      expect(notificationService.error).toHaveBeenCalledWith(expect.stringContaining("stale-id"));
      // Two router.navigate calls: one preserving fragment, one clearing it.
      expect(routerMock.navigate).toHaveBeenLastCalledWith([], { relativeTo: route });
    });
  });

  describe("triggerCenter", () => {
    it("delegates to the texera graph", async () => {
      await createFixture();
      component.triggerCenter();
      expect(stubGraph.triggerCenterEvent).toHaveBeenCalledTimes(1);
    });
  });

  describe("registerAutoPersistWorkflow", () => {
    it("is idempotent — only subscribes to workflowChanged once across repeated calls", async () => {
      await createFixture();
      component.registerAutoPersistWorkflow();
      component.registerAutoPersistWorkflow();
      component.registerAutoPersistWorkflow();
      expect(workflowActionService.workflowChanged).toHaveBeenCalledTimes(1);
    });

    it("updates the URL via location.go to /user/workflow/<wid> (no /dashboard prefix) when the persisted wid differs", async () => {
      vi.useFakeTimers();
      try {
        const workflowChanged$ = new Subject<void>();
        await createFixture();
        workflowActionService.workflowChanged.mockReturnValue(workflowChanged$.asObservable());
        // Persist returns a workflow with a different wid than what's currently
        // on the metadata (wid: 42 in the stub). That mismatch is the trigger
        // for the URL update.
        const persistedWorkflow = { ...stubWorkflow, wid: 99 } as Workflow;
        workflowPersistService.persistWorkflow.mockReturnValue(of(persistedWorkflow));

        component.registerAutoPersistWorkflow();
        workflowChanged$.next();
        // Flush the debounceTime(SAVE_DEBOUNCE_TIME_IN_MS).
        vi.advanceTimersByTime(5000);

        expect(locationMock.go).toHaveBeenCalledWith(`${USER_WORKSPACE}/99`);
        expect(USER_WORKSPACE).toBe("/user/workflow");
      } finally {
        vi.useRealTimers();
      }
    });

    it("skips the URL update when the persisted wid matches the current metadata", async () => {
      vi.useFakeTimers();
      try {
        const workflowChanged$ = new Subject<void>();
        await createFixture();
        workflowActionService.workflowChanged.mockReturnValue(workflowChanged$.asObservable());
        // Metadata wid is 42, persisted wid is also 42 → no URL update.
        workflowPersistService.persistWorkflow.mockReturnValue(of(stubWorkflow));

        component.registerAutoPersistWorkflow();
        workflowChanged$.next();
        vi.advanceTimersByTime(5000);

        expect(locationMock.go).not.toHaveBeenCalled();
        // Metadata is still synced even when the URL doesn't change.
        expect(workflowActionService.setWorkflowMetadata).toHaveBeenCalledWith(stubWorkflow);
      } finally {
        vi.useRealTimers();
      }
    });
  });

  describe("updateViewCount", () => {
    it("posts a view event with the route's wid and the current user's uid", async () => {
      const route = configureRoute({ id: "42" });
      await createFixture(route);
      fixture.detectChanges();
      expect(hubService.postView).toHaveBeenCalledWith("42", 7, EntityType.Workflow);
    });

    it("falls back to uid=0 when no user is signed in", async () => {
      const route = configureRoute({ id: "42" });
      await createFixture(route);
      userService.getCurrentUser.mockReturnValue(undefined);
      // Re-trigger after mutating the mock; createFixture has already wired it.
      component.updateViewCount();
      expect(hubService.postView).toHaveBeenCalledWith("42", 0, EntityType.Workflow);
    });
  });

  describe("onWIDChange", () => {
    it("syncs writeAccess from metadata.readonly each time the metadata changes", async () => {
      await createFixture();
      fixture.detectChanges();
      expect(component.writeAccess).toBe(false); // default before any emission

      workflowActionService.getWorkflowMetadata.mockReturnValue({ wid: 42, readonly: false });
      metadataChangedSubject.next();
      expect(component.writeAccess).toBe(true);

      workflowActionService.getWorkflowMetadata.mockReturnValue({ wid: 42, readonly: true });
      metadataChangedSubject.next();
      expect(component.writeAccess).toBe(false);
    });

    it("ignores metadata emissions that have no wid yet", async () => {
      await createFixture();
      fixture.detectChanges();
      workflowActionService.getWorkflowMetadata.mockReturnValue({ wid: undefined, readonly: false });
      metadataChangedSubject.next();
      // writeAccess stays at its initial false — no metadata.wid means we don't know
      // whether the workflow is editable yet.
      expect(component.writeAccess).toBe(false);
    });
  });

  describe("ngOnDestroy", () => {
    it("persists the workflow on destroy when the user is signed in and persist is enabled", async () => {
      await createFixture();
      fixture.detectChanges();
      component.ngOnDestroy();
      expect(workflowPersistService.persistWorkflow).toHaveBeenCalledWith(stubWorkflow);
      expect(workflowActionService.clearWorkflow).toHaveBeenCalled();
    });

    it("skips the persist call when the user is not signed in", async () => {
      await createFixture();
      fixture.detectChanges();
      userService.isLogin.mockReturnValue(false);
      component.ngOnDestroy();
      expect(workflowPersistService.persistWorkflow).not.toHaveBeenCalled();
      // Cleanup of the workflow state still happens regardless.
      expect(workflowActionService.clearWorkflow).toHaveBeenCalled();
    });

    it("tears down every piece of websocket-derived state when leaving the workspace (issue #3120)", async () => {
      await createFixture();
      fixture.detectChanges();
      component.ngOnDestroy();
      expect(computingUnitStatusService.disconnect).toHaveBeenCalled();
      expect(executeWorkflowService.resetExecutionAndWorkers).toHaveBeenCalled();
      expect(workflowConsoleService.clearConsoleMessages).toHaveBeenCalled();
      expect(workflowResultService.clearResults).toHaveBeenCalled();
    });

    it("clears the workflow session state when the computing unit is switched in-canvas (issue #3120)", async () => {
      await createFixture();
      fixture.detectChanges();
      // Switching to a different unit emits on the connection-reset stream.
      connectionResetSubject.next();
      expect(executeWorkflowService.resetExecutionAndWorkers).toHaveBeenCalled();
      expect(workflowConsoleService.clearConsoleMessages).toHaveBeenCalled();
      expect(workflowResultService.clearResults).toHaveBeenCalled();
    });
  });

  describe("copilotEnabled", () => {
    it("passes through to GuiConfigService.env.copilotEnabled", async () => {
      await createFixture();
      // MockGuiConfigService defaults `copilotEnabled` to false.
      expect(component.copilotEnabled).toBe(false);
    });
  });

  // Exercises the rendered template: the `<ng-template #codeEditor>` outlet is
  // present, so the @ViewChild query resolves to a live ViewContainerRef and
  // ngAfterViewInit can publish it to CodeEditorService.
  describe("child rendering side effects", () => {
    it("publishes the resolved ViewContainerRef to CodeEditorService.vc on view init", async () => {
      codeEditorService.vc = undefined;
      await createFixture();
      fixture.detectChanges();
      // createEmbeddedView is present on a real ViewContainerRef but not on the
      // pre-fixture stub, so checking it distinguishes the resolved query from
      // the placeholder.
      expect(codeEditorService.vc).toBe(component.codeEditorViewRef);
      expect(typeof codeEditorService.vc.createEmbeddedView).toBe("function");
    });
  });
});
