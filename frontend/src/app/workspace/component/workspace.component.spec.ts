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
import { NO_ERRORS_SCHEMA } from "@angular/core";
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
import { WorkflowActionService } from "../service/workflow-graph/model/workflow-action.service";
import { OperatorReuseCacheStatusService } from "../service/workflow-status/operator-reuse-cache-status.service";
import { HubService } from "../../hub/service/hub.service";
import { commonTestProviders } from "../../common/testing/test-utils";
import { WorkspaceComponent } from "./workspace.component";

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
      snapshot: { params, queryParams, fragment: null },
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

    TestBed.overrideComponent(WorkspaceComponent, {
      set: { template: '<div #codeEditor class="stub-host"></div>', imports: [], providers: [] },
    });

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
        { provide: WorkflowConsoleService, useValue: {} },
        { provide: OperatorReuseCacheStatusService, useValue: {} },
        ...commonTestProviders,
      ],
      schemas: [NO_ERRORS_SCHEMA],
    }).compileComponents();

    fixture = TestBed.createComponent(WorkspaceComponent);
    component = fixture.componentInstance;
    // ngOnDestroy clears the ViewContainerRef bound to `#codeEditor`. Tests that
    // exercise individual methods skip change detection, so the @ViewChild query
    // is never resolved; assign a stub to keep TestBed teardown from throwing.
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
      fixture.detectChanges();
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
  });

  describe("copilotEnabled", () => {
    it("passes through to GuiConfigService.env.copilotEnabled", async () => {
      await createFixture();
      // MockGuiConfigService defaults `copilotEnabled` to false.
      expect(component.copilotEnabled).toBe(false);
    });
  });
});
