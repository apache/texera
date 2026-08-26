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

import { DatePipe, Location } from "@angular/common";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { By } from "@angular/platform-browser";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { RouterTestingModule } from "@angular/router/testing";
import { NzModalService, NzModalModule, NzModalRef } from "ng-zorro-antd/modal";
import { BehaviorSubject, of, Subject, throwError } from "rxjs";

import { MenuComponent } from "./menu.component";
import { WorkflowWebsocketService } from "../../service/workflow-websocket/workflow-websocket.service";
import type { ExecutionDurationUpdateEvent } from "../../types/workflow-websocket.interface";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { UserService } from "../../../common/service/user/user.service";
import { StubUserService } from "../../../common/service/user/stub-user.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { ValidationWorkflowService, ValidationOutput } from "../../service/validation/validation-workflow.service";
import { PanelService } from "../../service/panel/panel.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { ComputingUnitState } from "../../../common/type/computing-unit-connection.interface";
import { mockPoint, mockScanPredicate } from "../../service/workflow-graph/model/mock-workflow-data";
import { saveAs } from "file-saver";
import type { ModalOptions } from "ng-zorro-antd/modal";
import type { ComputingUnitSelectionComponent } from "../power-button/computing-unit-selection.component";
import { WorkflowContent } from "../../../common/type/workflow";
import { Router } from "@angular/router";
import { ReportGenerationService } from "../../service/report-generation/report-generation.service";
import { USER_WORKFLOW } from "../../../app-routing.constant";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { MockGuiConfigService } from "../../../common/service/gui-config.service.mock";
import { JupyterPanelService } from "../../service/jupyter-panel/jupyter-panel.service";
import { UserProjectService } from "../../../dashboard/service/user/project/user-project.service";
import { WorkflowCompilingService } from "../../service/compile-workflow/workflow-compiling.service";
import { CompilationState } from "../../types/workflow-compiling.interface";
import type { Mocked } from "vitest";

vi.mock("file-saver", () => ({ saveAs: vi.fn() }));

describe("MenuComponent", () => {
  let component: MenuComponent;
  let fixture: ComponentFixture<MenuComponent>;
  let workflowActionService: WorkflowActionService;
  let executeWorkflowService: ExecuteWorkflowService;
  let validationWorkflowService: ValidationWorkflowService;
  let panelService: PanelService;
  let workflowVersionService: WorkflowVersionService;
  let workflowPersistService: WorkflowPersistService;
  let modalService: NzModalService;
  let notificationService: NotificationService;
  let location: Location;
  let validationStream$: BehaviorSubject<ValidationOutput>;
  let compilationStream$: Subject<CompilationState>;

  beforeEach(async () => {
    compilationStream$ = new Subject<CompilationState>();
    await TestBed.configureTestingModule({
      imports: [MenuComponent, HttpClientTestingModule, RouterTestingModule.withRoutes([]), NzModalModule],
      providers: [
        DatePipe,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        {
          provide: ComputingUnitStatusService,
          useValue: {
            getSelectedComputingUnit: () => of(null),
            getStatus: () => of(ComputingUnitState.NoComputingUnit),
            // Read by ComputingUnitSelectionComponent.ngOnInit when the menu
            // template renders the <texera-computing-unit-selection> child.
            getAllComputingUnits: () => of([]),
          },
        },
        { provide: UserService, useClass: StubUserService },
        {
          // stubbed so the debounced compile request of the real service does not outlive the test injector
          provide: WorkflowCompilingService,
          useValue: { getCompilationStateInfoChangedStream: () => compilationStream$.asObservable() },
        },
        ...commonTestProviders,
      ],
    }).compileComponents();

    workflowActionService = TestBed.inject(WorkflowActionService);
    executeWorkflowService = TestBed.inject(ExecuteWorkflowService);
    validationWorkflowService = TestBed.inject(ValidationWorkflowService);
    panelService = TestBed.inject(PanelService);
    workflowVersionService = TestBed.inject(WorkflowVersionService);
    workflowPersistService = TestBed.inject(WorkflowPersistService);
    modalService = TestBed.inject(NzModalService);
    notificationService = TestBed.inject(NotificationService);
    location = TestBed.inject(Location);

    validationStream$ = new BehaviorSubject<ValidationOutput>({ errors: {}, workflowEmpty: false });
    vi.spyOn(validationWorkflowService, "getWorkflowValidationErrorStream").mockReturnValue(
      validationStream$.asObservable()
    );

    fixture = TestBed.createComponent(MenuComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    vi.mocked(saveAs).mockClear();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("getRunButtonBehavior", () => {
    it("returns 'Invalid Workflow' when the workflow is invalid", () => {
      component.isWorkflowValid = false;
      component.isWorkflowEmpty = false;

      const behavior = component.getRunButtonBehavior();

      expect(behavior.text).toBe("Invalid Workflow");
      expect(behavior.icon).toBe("warning");
      expect(behavior.disable).toBe(true);
    });

    it("returns 'Invalid Workflow' when the workflow does not compile", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.isWorkflowCompilable = false;

      const behavior = component.getRunButtonBehavior();

      expect(behavior.text).toBe("Invalid Workflow");
      expect(behavior.icon).toBe("warning");
      expect(behavior.disable).toBe(true);
    });

    it("returns 'Empty Workflow' when the workflow has no operators", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = true;

      const behavior = component.getRunButtonBehavior();

      expect(behavior.text).toBe("Empty Workflow");
      expect(behavior.icon).toBe("info-circle");
      expect(behavior.disable).toBe(true);
    });

    it("returns 'Connect' when no computing unit is attached", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.computingUnitStatus = ComputingUnitState.NoComputingUnit;

      const behavior = component.getRunButtonBehavior();

      expect(behavior.text).toBe("Connect");
      expect(behavior.icon).toBe("plus-circle");
      expect(behavior.disable).toBe(false);
    });

    it("returns 'Run' when connected and execution is uninitialized", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.computingUnitStatus = ComputingUnitState.Running;
      Object.defineProperty(component.workflowWebsocketService, "isConnected", { get: () => true, configurable: true });
      component.executionState = ExecutionState.Uninitialized;

      const behavior = component.getRunButtonBehavior();

      expect(behavior.text).toBe("Run");
      expect(behavior.icon).toBe("play-circle");
      expect(behavior.disable).toBe(false);
    });

    it("returns 'Pause' while a workflow is running", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.computingUnitStatus = ComputingUnitState.Running;
      Object.defineProperty(component.workflowWebsocketService, "isConnected", { get: () => true, configurable: true });
      component.executionState = ExecutionState.Running;

      const pauseSpy = vi.spyOn(executeWorkflowService, "pauseWorkflow").mockImplementation(() => {});
      const behavior = component.getRunButtonBehavior();
      behavior.onClick();

      expect(behavior.text).toBe("Pause");
      expect(behavior.disable).toBe(false);
      expect(pauseSpy).toHaveBeenCalled();
    });

    it("returns 'Resume' when execution is paused", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.computingUnitStatus = ComputingUnitState.Running;
      Object.defineProperty(component.workflowWebsocketService, "isConnected", { get: () => true, configurable: true });
      component.executionState = ExecutionState.Paused;

      const resumeSpy = vi.spyOn(executeWorkflowService, "resumeWorkflow").mockImplementation(() => {});
      const behavior = component.getRunButtonBehavior();
      behavior.onClick();

      expect(behavior.text).toBe("Resume");
      expect(resumeSpy).toHaveBeenCalled();
    });

    it("returns 'Connecting' when a unit exists but the websocket is not connected", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.computingUnitStatus = ComputingUnitState.Running;
      Object.defineProperty(component.workflowWebsocketService, "isConnected", {
        get: () => false,
        configurable: true,
      });

      const behavior = component.getRunButtonBehavior();

      expect(behavior.text).toBe("Connecting");
      expect(behavior.disable).toBe(true);
    });

    /** Puts the component into the valid, connected state the state switch needs. */
    function connected(): void {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.computingUnitStatus = ComputingUnitState.Running;
      Object.defineProperty(component.workflowWebsocketService, "isConnected", { get: () => true, configurable: true });
    }

    it("wires both the 'Connect' and the 'Run' descriptor to runWorkflow", () => {
      const runSpy = vi.spyOn(component, "runWorkflow").mockImplementation(() => {});

      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.computingUnitStatus = ComputingUnitState.NoComputingUnit;
      component.getRunButtonBehavior().onClick();
      expect(runSpy).toHaveBeenCalledTimes(1);

      connected();
      component.executionState = ExecutionState.Completed;
      const run = component.getRunButtonBehavior();
      run.onClick();

      expect(run.text).toBe("Run");
      expect(runSpy).toHaveBeenCalledTimes(2);
    });

    it("gives the invalid, empty and connecting descriptors an inert click handler", () => {
      const runSpy = vi.spyOn(component, "runWorkflow").mockImplementation(() => {});
      const pauseSpy = vi.spyOn(executeWorkflowService, "pauseWorkflow").mockImplementation(() => {});
      const resumeSpy = vi.spyOn(executeWorkflowService, "resumeWorkflow").mockImplementation(() => {});

      component.isWorkflowValid = false;
      component.getRunButtonBehavior().onClick();

      component.isWorkflowValid = true;
      component.isWorkflowEmpty = true;
      component.getRunButtonBehavior().onClick();

      component.isWorkflowEmpty = false;
      component.computingUnitStatus = ComputingUnitState.Running;
      Object.defineProperty(component.workflowWebsocketService, "isConnected", {
        get: () => false,
        configurable: true,
      });
      component.getRunButtonBehavior().onClick();

      expect(runSpy).not.toHaveBeenCalled();
      expect(pauseSpy).not.toHaveBeenCalled();
      expect(resumeSpy).not.toHaveBeenCalled();
    });

    it("reports every transient execution state as a disabled spinner", () => {
      connected();
      const runSpy = vi.spyOn(component, "runWorkflow").mockImplementation(() => {});
      const cases: [ExecutionState, string][] = [
        [ExecutionState.Initializing, "Submitting"],
        [ExecutionState.Pausing, "Pausing"],
        [ExecutionState.Resuming, "Resuming"],
        [ExecutionState.Recovering, "Recovering"],
      ];

      const seen = cases.map(([state]) => {
        component.executionState = state;
        const behavior = component.getRunButtonBehavior();
        behavior.onClick();
        return [behavior.text, behavior.icon, behavior.disable];
      });

      expect(seen).toEqual(cases.map(([, text]) => [text, "loading", true]));
      expect(runSpy).not.toHaveBeenCalled();
    });

    it("falls back to 'Run' for a state the switch does not list", () => {
      connected();
      const runSpy = vi.spyOn(component, "runWorkflow").mockImplementation(() => {});
      // Defensive default: every ExecutionState member has its own case, so this
      // arm is only reachable with a value from outside the enum.
      component.executionState = "Unrecognized" as ExecutionState;

      const behavior = component.getRunButtonBehavior();
      behavior.onClick();

      expect(behavior.text).toBe("Run");
      expect(behavior.icon).toBe("play-circle");
      expect(behavior.disable).toBe(false);
      expect(runSpy).toHaveBeenCalledTimes(1);
    });
  });

  it("applyRunButtonBehavior copies the behavior onto the bound fields", () => {
    const handler = () => {};
    component.applyRunButtonBehavior({
      text: "Custom",
      icon: "custom-icon",
      disable: true,
      onClick: handler,
    });

    expect(component.runButtonText).toBe("Custom");
    expect(component.runIcon).toBe("custom-icon");
    expect(component.runDisable).toBe(true);
    expect(component.onClickRunHandler).toBe(handler);
  });

  it("re-applies run button behavior when the validation stream reports an empty workflow", () => {
    validationStream$.next({ errors: {}, workflowEmpty: true });

    expect(component.isWorkflowEmpty).toBe(true);
    expect(component.runButtonText).toBe("Empty Workflow");
    expect(component.runDisable).toBe(true);
  });

  describe("hasOperators", () => {
    it("returns false on an empty graph", () => {
      expect(component.hasOperators()).toBe(false);
    });

    it("returns true once an operator is added", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      expect(component.hasOperators()).toBe(true);
    });
  });

  it("onClickAddCommentBox delegates to the workflow action service", () => {
    const addCommentBoxSpy = vi.spyOn(workflowActionService, "addCommentBox");

    component.onClickAddCommentBox();

    expect(addCommentBoxSpy).toHaveBeenCalledTimes(1);
  });

  it("onClickDeleteAllOperators removes every operator from the graph", () => {
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    expect(workflowActionService.getTexeraGraph().getAllOperators().length).toBe(1);

    component.onClickDeleteAllOperators();

    expect(workflowActionService.getTexeraGraph().getAllOperators().length).toBe(0);
  });

  it("onClickAutoLayout is a no-op when there are no operators", () => {
    const autoLayoutSpy = vi.spyOn(workflowActionService, "autoLayoutWorkflow");

    component.onClickAutoLayout();

    expect(autoLayoutSpy).not.toHaveBeenCalled();
  });

  it("onClickAutoLayout invokes auto layout when operators are present", () => {
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    const autoLayoutSpy = vi.spyOn(workflowActionService, "autoLayoutWorkflow").mockImplementation(() => {});

    component.onClickAutoLayout();

    expect(autoLayoutSpy).toHaveBeenCalledTimes(1);
  });

  it("handleKill delegates to executeWorkflowService.killWorkflow", () => {
    const killSpy = vi.spyOn(executeWorkflowService, "killWorkflow").mockImplementation(() => {});

    component.handleKill();

    expect(killSpy).toHaveBeenCalledTimes(1);
  });

  it("handleCheckpoint delegates to executeWorkflowService.takeGlobalCheckpoint", () => {
    const checkpointSpy = vi.spyOn(executeWorkflowService, "takeGlobalCheckpoint").mockImplementation(() => {});

    component.handleCheckpoint();

    expect(checkpointSpy).toHaveBeenCalledTimes(1);
  });

  it("onClickClosePanels and onClickResetPanels delegate to PanelService", () => {
    const closeSpy = vi.spyOn(panelService, "closePanels").mockImplementation(() => {});
    const resetSpy = vi.spyOn(panelService, "resetPanels").mockImplementation(() => {});

    component.onClickClosePanels();
    component.onClickResetPanels();

    expect(closeSpy).toHaveBeenCalledTimes(1);
    expect(resetSpy).toHaveBeenCalledTimes(1);
  });

  describe("runWorkflow", () => {
    beforeEach(() => {
      component.computingUnitSelectionComponent = {
        showAddComputeUnitModalVisible: vi.fn(),
      } as unknown as Mocked<ComputingUnitSelectionComponent>;
    });

    it("does nothing when the workflow is invalid", () => {
      component.isWorkflowValid = false;
      component.isWorkflowEmpty = false;
      const executeSpy = vi.spyOn(executeWorkflowService, "executeWorkflowWithEmailNotification");

      component.runWorkflow();

      expect(executeSpy).not.toHaveBeenCalled();
      expect(component.computingUnitSelectionComponent.showAddComputeUnitModalVisible).not.toHaveBeenCalled();
    });

    it("does nothing when the workflow does not compile", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.isWorkflowCompilable = false;
      component.computingUnitStatus = ComputingUnitState.Running;
      const executeSpy = vi.spyOn(executeWorkflowService, "executeWorkflowWithEmailNotification");

      component.runWorkflow();

      expect(executeSpy).not.toHaveBeenCalled();
      expect(component.computingUnitSelectionComponent.showAddComputeUnitModalVisible).not.toHaveBeenCalled();
    });

    it("does nothing when the workflow is empty", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = true;
      const executeSpy = vi.spyOn(executeWorkflowService, "executeWorkflowWithEmailNotification");

      component.runWorkflow();

      expect(executeSpy).not.toHaveBeenCalled();
    });

    it("opens the add-computing-unit modal when no unit is connected", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.computingUnitStatus = ComputingUnitState.NoComputingUnit;
      component.currentWorkflowName = "wf";
      const executeSpy = vi.spyOn(executeWorkflowService, "executeWorkflowWithEmailNotification");

      component.runWorkflow();

      expect(component.computingUnitSelectionComponent.showAddComputeUnitModalVisible).toHaveBeenCalledWith(
        "wf's Computing Unit"
      );
      expect(component.computingUnitSelectionComponent.showAddComputeUnitModalVisible).toHaveBeenCalledTimes(1);
      expect(executeSpy).not.toHaveBeenCalled();
    });

    it("submits the execution when connected", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.computingUnitStatus = ComputingUnitState.Running;
      component.currentExecutionName = "exec-1";
      const executeSpy = vi
        .spyOn(executeWorkflowService, "executeWorkflowWithEmailNotification")
        .mockImplementation(() => {});

      component.runWorkflow();

      expect(executeSpy).toHaveBeenCalledWith("exec-1", expect.any(Boolean));
    });

    it("names a new computing unit generically when the workflow has no name", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.computingUnitStatus = ComputingUnitState.NoComputingUnit;
      component.currentWorkflowName = "";

      component.runWorkflow();

      expect(component.computingUnitSelectionComponent.showAddComputeUnitModalVisible).toHaveBeenCalledWith(
        "New Computing Unit"
      );
    });

    it("submits under 'Untitled Execution' when no execution name has been chosen", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.computingUnitStatus = ComputingUnitState.Running;
      component.currentExecutionName = "";
      const executeSpy = vi
        .spyOn(executeWorkflowService, "executeWorkflowWithEmailNotification")
        .mockImplementation(() => {});

      component.runWorkflow();

      expect(executeSpy).toHaveBeenCalledWith("Untitled Execution", false);
    });
  });

  it("onWorkflowNameChange forwards the new name to the workflow action service", () => {
    const setNameSpy = vi.spyOn(workflowActionService, "setWorkflowName");
    component.currentWorkflowName = "renamed";

    component.onWorkflowNameChange();

    expect(setNameSpy).toHaveBeenCalledWith("renamed");
  });

  it("onWorkflowNameChange persists the rename only while logged in", () => {
    vi.spyOn(workflowActionService, "setWorkflowName").mockImplementation(() => {});
    const persistSpy = vi.spyOn(component, "persistWorkflow").mockImplementation(() => {});
    const isLogin = vi.spyOn(component.userService, "isLogin").mockReturnValue(true);

    component.onWorkflowNameChange();
    expect(persistSpy).toHaveBeenCalledTimes(1);

    isLogin.mockReturnValue(false);
    component.onWorkflowNameChange();
    expect(persistSpy).toHaveBeenCalledTimes(1);
  });

  // Regression coverage for #6846 (resolved by discussion #6873): the toolbar's
  // import button wiped `wid` before auto-persist and thereby created a spurious
  // duplicate workflow, so it was removed. Creating a workflow from a JSON file
  // is covered by the dashboard workflow-list upload button instead.
  describe("import workflow removal", () => {
    it("does not render an import upload control in the toolbar", () => {
      const element: HTMLElement = fixture.nativeElement;

      expect(element.querySelector("nz-upload")).toBeNull();
      expect(element.querySelector("button[title='import workflow']")).toBeNull();
    });

    it("does not define an onClickImportWorkflow handler", () => {
      expect((component as any).onClickImportWorkflow).toBeUndefined();
    });
  });

  describe("onClickExportWorkflow (save)", () => {
    it("serializes the workflow content as JSON and downloads it under the workflow name", () => {
      const fakeContent = {
        operators: [{ operatorID: "op1" }],
        links: [],
        commentBoxes: [],
        settings: {},
      } as unknown as WorkflowContent;
      vi.spyOn(workflowActionService, "getWorkflowContent").mockReturnValue(fakeContent);
      component.currentWorkflowName = "my-workflow";

      component.onClickExportWorkflow();

      expect(saveAs).toHaveBeenCalledTimes(1);
      const [blobArg, fileNameArg] = vi.mocked(saveAs).mock.calls[0] as [Blob, string];
      expect(fileNameArg).toBe("my-workflow.json");
      expect(blobArg).toBeInstanceOf(Blob);
      expect(blobArg.type).toBe("text/plain;charset=utf-8");
    });
  });

  describe("version history", () => {
    it("onClickGetAllVersions delegates to workflowVersionService.displayWorkflowVersions", () => {
      const displaySpy = vi.spyOn(workflowVersionService, "displayWorkflowVersions").mockImplementation(() => {});

      component.onClickGetAllVersions();

      expect(displaySpy).toHaveBeenCalledTimes(1);
    });

    it("closeParticularVersionDisplay delegates to workflowVersionService", () => {
      const closeSpy = vi.spyOn(workflowVersionService, "closeParticularVersionDisplay").mockImplementation(() => {});

      component.closeParticularVersionDisplay();

      expect(closeSpy).toHaveBeenCalledTimes(1);
    });

    it("revertToVersion reverts and then persists the workflow", () => {
      const revertSpy = vi.spyOn(workflowVersionService, "revertToVersion").mockImplementation(() => {});
      const persistSpy = vi
        .spyOn(workflowPersistService, "persistWorkflow")
        .mockReturnValue(of(workflowActionService.getWorkflow()));

      component.revertToVersion();

      expect(revertSpy).toHaveBeenCalledTimes(1);
      expect(persistSpy).toHaveBeenCalledTimes(1);
    });

    it("cloneVersion notifies success and closes the version panel when cloning succeeds", () => {
      vi.spyOn(workflowVersionService, "cloneWorkflowVersion").mockReturnValue(of(42));
      const successSpy = vi.spyOn(notificationService, "success").mockImplementation(() => {});
      const closeSpy = vi.spyOn(workflowVersionService, "closeParticularVersionDisplay").mockImplementation(() => {});

      component.cloneVersion();

      expect(successSpy).toHaveBeenCalledTimes(1);
      expect(successSpy.mock.calls[0][0]).toContain("42");
      expect(closeSpy).toHaveBeenCalledTimes(1);
    });

    it("cloneVersion shows an error notification and does not close the panel when cloning fails", () => {
      vi.spyOn(workflowVersionService, "cloneWorkflowVersion").mockReturnValue(throwError(() => new Error("boom")));
      const errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});
      const successSpy = vi.spyOn(notificationService, "success").mockImplementation(() => {});
      const closeSpy = vi.spyOn(workflowVersionService, "closeParticularVersionDisplay").mockImplementation(() => {});

      component.cloneVersion();

      expect(errorSpy).toHaveBeenCalledTimes(1);
      expect(successSpy).not.toHaveBeenCalled();
      expect(closeSpy).not.toHaveBeenCalled();
    });
  });

  describe("onClickOpenShareAccess (share)", () => {
    it("looks up workflow owners and opens the share-access modal", async () => {
      vi.spyOn(workflowPersistService, "retrieveOwners").mockReturnValue(of(["alice@example.com"]));
      const fakeModalRef = { afterClose: of(undefined) } as unknown as NzModalRef;
      const createSpy = vi.spyOn(modalService, "create").mockReturnValue(fakeModalRef);
      component.workflowId = 7;
      component.writeAccess = true;

      await component.onClickOpenShareAccess();

      expect(createSpy).toHaveBeenCalledTimes(1);
      const config = createSpy.mock.calls[0][0] as ModalOptions;
      expect(config.nzTitle).toBe("Share this workflow with others");
      expect(config.nzData).toEqual(
        expect.objectContaining({
          writeAccess: true,
          type: "workflow",
          id: 7,
          allOwners: ["alice@example.com"],
          inWorkspace: true,
        })
      );
    });

    it("navigates to /user/workflow (no /dashboard prefix) when the modal reports the owner revoked their own access", async () => {
      vi.spyOn(workflowPersistService, "retrieveOwners").mockReturnValue(of([]));
      const fakeModalRef = { afterClose: of({ userRevokedOwnAccess: true }) } as unknown as NzModalRef;
      vi.spyOn(modalService, "create").mockReturnValue(fakeModalRef);
      const router = TestBed.inject(Router);
      const navigateSpy = vi.spyOn(router, "navigate").mockResolvedValue(true);

      await component.onClickOpenShareAccess();

      expect(navigateSpy).toHaveBeenCalledWith([USER_WORKFLOW]);
      expect(USER_WORKFLOW).toBe("/user/workflow");
    });

    it("does not navigate when the share-access modal closes without revoking own access", async () => {
      vi.spyOn(workflowPersistService, "retrieveOwners").mockReturnValue(of([]));
      const fakeModalRef = { afterClose: of(undefined) } as unknown as NzModalRef;
      vi.spyOn(modalService, "create").mockReturnValue(fakeModalRef);
      const router = TestBed.inject(Router);
      const navigateSpy = vi.spyOn(router, "navigate").mockResolvedValue(true);

      await component.onClickOpenShareAccess();

      expect(navigateSpy).not.toHaveBeenCalled();
    });
  });

  it("onClickCreateNewWorkflow resets the graph and navigates back to root", () => {
    const resetSpy = vi.spyOn(workflowActionService, "resetAsNewWorkflow").mockImplementation(() => {});
    const goSpy = vi.spyOn(location, "go").mockImplementation(() => {});

    component.onClickCreateNewWorkflow();

    expect(resetSpy).toHaveBeenCalledTimes(1);
    expect(goSpy).toHaveBeenCalledWith("/");
  });

  it("onClickRestoreZoomOffsetDefault delegates to the joint graph wrapper", () => {
    const restoreSpy = vi
      .spyOn(workflowActionService.getJointGraphWrapper(), "restoreDefaultZoomAndOffset")
      .mockImplementation(() => {});

    component.onClickRestoreZoomOffsetDefault();

    expect(restoreSpy).toHaveBeenCalledTimes(1);
  });

  it("onClickEditDescription opens the markdown description modal seeded with the current description", () => {
    vi.spyOn(workflowActionService, "getWorkflow").mockReturnValue({
      content: { operators: [], links: [], commentBoxes: [], settings: {} } as unknown as WorkflowContent,
      name: "wf",
      description: "hello world",
      wid: 1,
      creationTime: undefined,
      lastModifiedTime: undefined,
      readonly: false,
      isPublished: 0,
    });
    const fakeModalRef = {
      afterClose: of(undefined),
      getContentComponent: () => ({ descriptionChange: of() }),
      close: vi.fn(),
    } as unknown as NzModalRef;
    const createSpy = vi.spyOn(modalService, "create").mockReturnValue(fakeModalRef);

    component.onClickEditDescription();

    expect(createSpy).toHaveBeenCalledTimes(1);
    const config = createSpy.mock.calls[0][0] as ModalOptions;
    expect(config.nzTitle).toBe("Edit Workflow Description");
    expect(config.nzData).toEqual({ description: "hello world" });
  });

  it("onClickExportExecutionResult opens the result-exportation modal with the current workflow name", () => {
    const fakeModalRef = { afterClose: of(undefined) } as unknown as NzModalRef;
    const createSpy = vi.spyOn(modalService, "create").mockReturnValue(fakeModalRef);
    component.currentWorkflowName = "report-wf";

    component.onClickExportExecutionResult();

    expect(createSpy).toHaveBeenCalledTimes(1);
    const config = createSpy.mock.calls[0][0] as ModalOptions;
    expect(config.nzTitle).toBe("Export All Operators Result");
    expect(config.nzData).toEqual(expect.objectContaining({ workflowName: "report-wf", sourceTriggered: "menu" }));
  });

  describe("canvas display toggles", () => {
    // A fake JointJS element that records `attr(path, value)` calls and answers `get("type")`.
    function fakeElement(type: string) {
      return {
        type,
        attrs: {} as Record<string, unknown>,
        get(key: string) {
          return key === "type" ? this.type : undefined;
        },
        attr: vi.fn(function (this: { attrs: Record<string, unknown> }, path: string, value: unknown) {
          this.attrs[path] = value;
        }),
      };
    }

    // Stubs getJointGraphWrapper() with a paper element + model/graph backed by the given elements.
    function stubWrapper(elements: ReturnType<typeof fakeElement>[]) {
      const el = document.createElement("div");
      const wrapper = {
        mainPaper: { el, model: { getElements: () => elements } },
        jointGraph: { getElements: () => elements },
      };
      vi.spyOn(workflowActionService, "getJointGraphWrapper").mockReturnValue(wrapper as any);
      return el;
    }

    describe("toggleRegion", () => {
      it("publishes the displayed flag to the joint graph wrapper when enabled", () => {
        const setSpy = vi.spyOn(workflowActionService.getJointGraphWrapper(), "setRegionsDisplayed");

        component.showRegion = true;
        component.toggleRegion();

        expect(setSpy).toHaveBeenCalledWith(true);
      });

      it("publishes the displayed flag to the joint graph wrapper when disabled", () => {
        const setSpy = vi.spyOn(workflowActionService.getJointGraphWrapper(), "setRegionsDisplayed");

        component.showRegion = false;
        component.toggleRegion();

        expect(setSpy).toHaveBeenCalledWith(false);
      });
    });

    describe("toggleStatus", () => {
      it("removes hide-operator-status when enabled and repositions the status label", () => {
        const operator = fakeElement("operator");
        const el = stubWrapper([operator]);
        el.classList.add("hide-operator-status");

        component.showStatus = true;
        component.showNumWorkers = false;
        component.toggleStatus();

        expect(el.classList.contains("hide-operator-status")).toBe(false);
        expect(operator.attr).toHaveBeenCalledWith(".texera-operator-state/ref-x", -10);
        expect(operator.attr).toHaveBeenCalledWith(".texera-operator-state/ref-y", -35);
      });

      it("adds hide-operator-status when disabled", () => {
        const operator = fakeElement("operator");
        const el = stubWrapper([operator]);

        component.showStatus = false;
        component.toggleStatus();

        expect(el.classList.contains("hide-operator-status")).toBe(true);
      });

      it("offsets the status label higher when worker counts are shown", () => {
        const operator = fakeElement("operator");
        stubWrapper([operator]);

        component.showNumWorkers = true;
        component.toggleStatus();

        expect(operator.attr).toHaveBeenCalledWith(".texera-operator-state/ref-y", -55);
      });
    });
  });

  // Regression coverage for #5323: the elapsed-time timer was refactored from a
  // manually managed `durationUpdateSubscription` into a declarative `switchMap`
  // pipe terminated by `untilDestroyed`. These tests pin the resulting behavior
  // (base-duration updates, 1s cadence, restart-on-event, stop-when-idle) and,
  // crucially, that the timer is torn down with the component so it cannot keep
  // firing or leak after destroy.
  describe("execution duration timer", () => {
    let durationEvents$: Subject<{ type: "ExecutionDurationUpdateEvent" } & ExecutionDurationUpdateEvent>;
    let timerFixture: ComponentFixture<MenuComponent>;
    let timerComponent: MenuComponent;

    function emitDuration(duration: number, isRunning: boolean): void {
      durationEvents$.next({ type: "ExecutionDurationUpdateEvent", duration, isRunning });
    }

    beforeEach(() => {
      vi.useFakeTimers();
      durationEvents$ = new Subject();
      const websocket = TestBed.inject(WorkflowWebsocketService);
      const original = websocket.subscribeToEvent.bind(websocket);
      // Only intercept the duration event; defer every other event type to the
      // real implementation so unrelated subscriptions keep working.
      vi.spyOn(websocket, "subscribeToEvent").mockImplementation((type: any) =>
        type === "ExecutionDurationUpdateEvent" ? (durationEvents$.asObservable() as any) : original(type)
      );

      timerFixture = TestBed.createComponent(MenuComponent);
      timerComponent = timerFixture.componentInstance;
      timerFixture.detectChanges();
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it("sets executionDuration to the event's base duration on each event", () => {
      emitDuration(5000, false);
      expect(timerComponent.executionDuration).toBe(5000);

      emitDuration(8000, false);
      expect(timerComponent.executionDuration).toBe(8000);
    });

    it("advances the duration by 1s every second while running", () => {
      emitDuration(0, true);
      expect(timerComponent.executionDuration).toBe(0);

      vi.advanceTimersByTime(1000);
      expect(timerComponent.executionDuration).toBe(1000);

      vi.advanceTimersByTime(2000);
      expect(timerComponent.executionDuration).toBe(3000);
    });

    it("does not start a timer when the execution is not running", () => {
      emitDuration(7000, false);

      vi.advanceTimersByTime(5000);

      expect(timerComponent.executionDuration).toBe(7000);
    });

    it("restarts the 1s timer on each new running event, cancelling the previous one", () => {
      emitDuration(0, true);
      vi.advanceTimersByTime(1000);
      expect(timerComponent.executionDuration).toBe(1000);

      // A new event resets the base duration and restarts the cadence; the
      // previous timer must be cancelled (switchMap) so it cannot double-count.
      emitDuration(10000, true);
      expect(timerComponent.executionDuration).toBe(10000);

      vi.advanceTimersByTime(500);
      expect(timerComponent.executionDuration).toBe(10000);

      vi.advanceTimersByTime(500);
      expect(timerComponent.executionDuration).toBe(11000);
    });

    it("stops the timer when a running execution transitions to not running", () => {
      emitDuration(0, true);
      vi.advanceTimersByTime(1000);
      expect(timerComponent.executionDuration).toBe(1000);

      emitDuration(2000, false);
      vi.advanceTimersByTime(5000);
      expect(timerComponent.executionDuration).toBe(2000);
    });

    it("tears down the timer on destroy so the duration stops advancing", () => {
      emitDuration(0, true);
      vi.advanceTimersByTime(1000);
      expect(timerComponent.executionDuration).toBe(1000);

      timerFixture.destroy();

      // The previously running timer must not keep firing after destroy...
      vi.advanceTimersByTime(5000);
      expect(timerComponent.executionDuration).toBe(1000);

      // ...nor should late events revive it (the source subscription is closed).
      emitDuration(9999, true);
      vi.advanceTimersByTime(5000);
      expect(timerComponent.executionDuration).toBe(1000);
    });
  });

  // Regression coverage for #5323: the computing-unit status subscription lost
  // its manual `computingUnitStatusSubscription` aggregator and its
  // `ngOnDestroy` unsubscribe, relying on `untilDestroyed` instead. These tests
  // pin both that status updates still propagate and that they stop on destroy.
  describe("computing unit status subscription", () => {
    let status$: Subject<ComputingUnitState>;
    let cuFixture: ComponentFixture<MenuComponent>;
    let cuComponent: MenuComponent;

    beforeEach(() => {
      status$ = new Subject<ComputingUnitState>();
      const cuService = TestBed.inject(ComputingUnitStatusService);
      vi.spyOn(cuService, "getStatus").mockReturnValue(status$.asObservable());

      cuFixture = TestBed.createComponent(MenuComponent);
      cuComponent = cuFixture.componentInstance;
      cuFixture.detectChanges();
    });

    it("updates computingUnitStatus and re-applies the run button behavior on each status emission", () => {
      const applySpy = vi.spyOn(cuComponent, "applyRunButtonBehavior");

      status$.next(ComputingUnitState.Running);

      expect(cuComponent.computingUnitStatus).toBe(ComputingUnitState.Running);
      expect(applySpy).toHaveBeenCalledTimes(1);
    });

    it("stops updating computingUnitStatus once the component is destroyed", () => {
      status$.next(ComputingUnitState.Running);
      expect(cuComponent.computingUnitStatus).toBe(ComputingUnitState.Running);

      cuFixture.destroy();

      status$.next(ComputingUnitState.NoComputingUnit);
      expect(cuComponent.computingUnitStatus).toBe(ComputingUnitState.Running);
    });
  });

  describe("grid / worker-count toggles, report, and name sizing", () => {
    it("toggleGrid sets the joint paper grid size from the flag", () => {
      const setGridSize = vi.fn();
      vi.spyOn(workflowActionService, "getJointGraphWrapper").mockReturnValue({ mainPaper: { setGridSize } } as any);

      component.showGrid = true;
      component.toggleGrid();
      expect(setGridSize).toHaveBeenCalledWith(2);

      component.showGrid = false;
      component.toggleGrid();
      expect(setGridSize).toHaveBeenCalledWith(1);
    });

    it("toggleNumWorkers toggles the hide-worker-count class from the flag", () => {
      const el = document.createElement("div");
      vi.spyOn(workflowActionService, "getJointGraphWrapper").mockReturnValue({
        mainPaper: { el, model: { getElements: () => [] } },
      } as any);

      component.showNumWorkers = false;
      component.toggleNumWorkers();
      expect(el.classList.contains("hide-worker-count")).toBe(true);

      component.showNumWorkers = true;
      component.toggleNumWorkers();
      expect(el.classList.contains("hide-worker-count")).toBe(false);
    });

    it("onClickGenerateReport shows a blocking notification and builds the report html", () => {
      const reportService = TestBed.inject(ReportGenerationService);
      const blankSpy = vi.spyOn(notificationService, "blank");
      vi.spyOn(reportService, "generateWorkflowSnapshot").mockReturnValue(of("snap-url"));
      vi.spyOn(reportService, "getAllOperatorResults").mockReturnValue(of([]));
      const htmlSpy = vi.spyOn(reportService, "generateReportAsHtml").mockImplementation(() => {});

      component.onClickGenerateReport();

      expect(blankSpy).toHaveBeenCalled();
      expect(htmlSpy).toHaveBeenCalledWith("snap-url", [], expect.any(String));
    });

    it("adjustWorkflowNameWidth is a no-op when the name input is absent", () => {
      component.workflowNameInput = undefined;
      expect(() => component.adjustWorkflowNameWidth()).not.toThrow();
    });

    it("adjustWorkflowNameWidth sizes the input in px to fit its text", () => {
      const input = document.createElement("input");
      input.value = "my workflow";
      component.workflowNameInput = { nativeElement: input } as typeof component.workflowNameInput;

      component.adjustWorkflowNameWidth();

      expect(input.style.width).toMatch(/^\d+px$/);
    });

    it("adjustWorkflowNameWidth measures the placeholder when the input is empty", () => {
      const input = document.createElement("input");
      input.value = "";
      input.placeholder = "Untitled Workflow";
      component.workflowNameInput = { nativeElement: input } as typeof component.workflowNameInput;

      component.adjustWorkflowNameWidth();

      // jsdom reports every element as zero-width, so only the measured text is
      // asserted here: the method still sizes the input and cleans up its probe.
      expect(input.style.width).toMatch(/^\d+px$/);
      expect(document.body.querySelector("span[style*='visibility: hidden']")).toBeNull();
    });
  });

  describe("expand jupyter notebook panel", () => {
    it("onClickExpandJupyterNotebookPanel delegates to JupyterPanelService", () => {
      const openSpy = vi
        .spyOn(TestBed.inject(JupyterPanelService), "openJupyterNotebookPanel")
        .mockImplementation(() => {});

      component.onClickExpandJupyterNotebookPanel();

      expect(openSpy).toHaveBeenCalled();
    });

    it("shows the expand-jupyter button only when the flag is on and a notebook exists", () => {
      const button = () => fixture.nativeElement.querySelector('button[title="expand Jupyter notebook"]');
      // commonTestProviders' MockGuiConfigService defaults the flag to false, and no notebook exists.
      expect(button()).toBeNull();

      (TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService).setConfig({
        pythonNotebookMigrationEnabled: true,
      });
      fixture.detectChanges();
      // Flag on but the current workflow still has no notebook -> hidden.
      expect(button()).toBeNull();

      (TestBed.inject(JupyterPanelService) as any).jupyterNotebookExists$ = of(true);
      fixture.detectChanges();
      // Flag on and a notebook exists -> shown.
      expect(button()).not.toBeNull();
    });

    it("clicking the expand-jupyter button opens the panel", () => {
      const openSpy = vi
        .spyOn(TestBed.inject(JupyterPanelService), "openJupyterNotebookPanel")
        .mockImplementation(() => {});
      (TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService).setConfig({
        pythonNotebookMigrationEnabled: true,
      });
      (TestBed.inject(JupyterPanelService) as any).jupyterNotebookExists$ = of(true);
      fixture.detectChanges();

      const button = fixture.nativeElement.querySelector(
        'button[title="expand Jupyter notebook"]'
      ) as HTMLButtonElement;
      button.click();

      expect(openSpy).toHaveBeenCalled();
    });
  });
  /**
   * The toolbar's buttons and switches are wired in the template, and the suite above calls the
   * handlers directly. That is not the same thing: coverage for `(click)="onClickX()"` lands on the
   * generated listener body, which only runs when the element is really clicked — so a button wired
   * to the wrong handler, or to none, looks perfectly tested today.
   */
  describe("toolbar wiring", () => {
    function host(): HTMLElement {
      return fixture.nativeElement as HTMLElement;
    }

    /** The button carrying the given title attribute. */
    function button(title: string): HTMLButtonElement {
      const found = host().querySelector<HTMLButtonElement>(`button[title="${title}"]`);
      expect(found, `no button titled "${title}"`).not.toBeNull();
      return found!;
    }

    /** Clicks the button and reports whether the spied handler ran exactly once. */
    function clicking(title: string, owner: object, method: string): boolean {
      const spy = vi.spyOn(owner as any, method).mockImplementation(() => {});
      button(title).click();
      const ran = spy.mock.calls.length === 1;
      spy.mockRestore();
      return ran;
    }

    it("routes each toolbar button to its own handler", () => {
      // Table-driven because these buttons are visually near-identical icon buttons; a copy-paste
      // that leaves two of them on the same handler is the realistic defect and is invisible on
      // screen. Each entry is clicked for real, not invoked.
      const wiring: Array<[string, object, string]> = [
        ["close panels", component, "onClickClosePanels"],
        ["reset panels", component, "onClickResetPanels"],
        ["generate report", component, "onClickGenerateReport"],
        ["reset zoom", component, "onClickRestoreZoomOffsetDefault"],
        ["auto layout", component, "onClickAutoLayout"],
        ["add a comment", component, "onClickAddCommentBox"],
      ];

      const results = wiring.map(([title, owner, method]) => `${title}:${clicking(title, owner, method)}`);

      expect(results).toEqual(wiring.map(([title]) => `${title}:true`));
    });

    it("does not fire a neighbour's handler when one button is clicked", () => {
      // The other half of the same concern: clicking auto-layout must not also reset the panels.
      const layout = vi.spyOn(component, "onClickAutoLayout").mockImplementation(() => {});
      const reset = vi.spyOn(component, "onClickResetPanels").mockImplementation(() => {});

      button("auto layout").click();

      expect(layout).toHaveBeenCalledTimes(1);
      expect(reset).not.toHaveBeenCalled();
    });
  });

  /**
   * The suite above calls the handlers directly; these fire them from the rendered markup, so a
   * control that loses its binding fails here. The utilities block is reached through
   * `#expanded-utilities`, the outlet that renders it inline — the dropdown outlet next to it
   * needs a CDK overlay, which jsdom never attaches.
   */
  describe("rendered menu", () => {
    const q = (selector: string) => fixture.debugElement.query(By.css(selector));
    const utility = (title: string) => q(`#expanded-utilities button[title="${title}"]`);
    const toolbar = (title: string) => q(`button[title="${title}"]`);
    /** Undo/redo and export carry no title; they are identified by the icon they render. */
    const buttonWithIcon = (icon: string) => q(`#expanded-utilities i[nztype="${icon}"]`).parent!;

    afterEach(() => {
      fixture.destroy();
      vi.restoreAllMocks();
    });

    describe("version display bar", () => {
      /** Puts the menu into the "viewing an older version" state and renders it. */
      function showVersion(versionId: number | null = 7): void {
        component.displayParticularWorkflowVersion = true;
        workflowVersionService.selectedDisplayedVersionId.next(versionId);
        fixture.detectChanges();
      }

      it("swaps the name input for the version label and a way back", () => {
        const closeSpy = vi.spyOn(component, "closeParticularVersionDisplay").mockImplementation(() => {});
        showVersion();

        expect(q("input.workflow-name")).toBeNull();
        toolbar("back").triggerEventHandler("click", null);

        expect(closeSpy).toHaveBeenCalled();
      });

      it("offers restore only while the version service allows it", () => {
        const revertSpy = vi.spyOn(component, "revertToVersion").mockImplementation(() => {});
        vi.spyOn(workflowVersionService, "canRestoreVersion", "get").mockReturnValue(false);
        showVersion();

        const restore = fixture.debugElement
          .queryAll(By.css("button"))
          .find(b => (b.nativeElement.textContent ?? "").trim() === "Restore this version")!;
        expect(restore.nativeElement.disabled).toBe(true);

        vi.spyOn(workflowVersionService, "canRestoreVersion", "get").mockReturnValue(true);
        fixture.detectChanges();
        expect(restore.nativeElement.disabled).toBe(false);

        restore.triggerEventHandler("click", null);
        expect(revertSpy).toHaveBeenCalled();
      });

      it("offers cloning the displayed version", () => {
        const cloneSpy = vi.spyOn(component, "cloneVersion").mockImplementation(() => {});
        showVersion();

        fixture.debugElement
          .queryAll(By.css("button"))
          .find(b => (b.nativeElement.textContent ?? "").trim() === "Clone this version")!
          .triggerEventHandler("click", null);

        expect(cloneSpy).toHaveBeenCalled();
      });

      it("names the displayed version, and says nothing when there is none", () => {
        showVersion(7);
        expect(q('[title="Current Version"]').nativeElement.textContent).toContain("7");

        showVersion(null);
        expect(q('[title="Current Version"]')).toBeNull();
      });

      it("renders an icon per co-editor", () => {
        component.coeditorPresenceService.coeditors = [
          { clientId: "a", user: { name: "ann" } },
          { clientId: "b", user: { name: "bo" } },
        ] as never;
        fixture.detectChanges();

        expect(fixture.debugElement.queryAll(By.css("texera-coeditor-user-icon")).length).toBe(2);
      });
    });

    describe("workflow name field", () => {
      it("shows the id badge and reports typing and committing the name", () => {
        const widthSpy = vi.spyOn(component, "adjustWorkflowNameWidth").mockImplementation(() => {});
        const changeSpy = vi.spyOn(component, "onWorkflowNameChange").mockImplementation(() => {});
        component.workflowId = 42;
        fixture.detectChanges();

        expect(q("#metadata nz-avatar")).not.toBeNull();
        const nameInput = q("input.workflow-name");
        nameInput.triggerEventHandler("input", { target: nameInput.nativeElement });
        nameInput.triggerEventHandler("change", { target: nameInput.nativeElement });

        expect(widthSpy).toHaveBeenCalled();
        expect(changeSpy).toHaveBeenCalled();
      });

      it("drops the id badge when there is no workflow yet", () => {
        component.workflowId = undefined;
        fixture.detectChanges();

        expect(q("#metadata nz-avatar")).toBeNull();
      });
    });

    describe("execution buttons", () => {
      it("wires share, kill and run", () => {
        const share = vi.spyOn(component, "onClickOpenShareAccess").mockResolvedValue(undefined);
        const kill = vi.spyOn(component, "handleKill").mockImplementation(() => {});
        const run = vi.spyOn(component, "onClickRunHandler").mockImplementation(() => {});
        fixture.detectChanges();

        q("#share-button").triggerEventHandler("click", null);
        q("button[nzdanger]").triggerEventHandler("click", null);
        q("#run-button").triggerEventHandler("click", null);

        expect(share).toHaveBeenCalled();
        expect(run).toHaveBeenCalled();
        expect(kill).toHaveBeenCalled();
      });
    });

    describe("toolbar", () => {
      it("wires each toolbar button to its handler", () => {
        const spies = {
          create: vi.spyOn(component, "onClickCreateNewWorkflow").mockImplementation(() => {}),
          save: vi.spyOn(component, "persistWorkflow").mockImplementation(() => {}),
          deleteAll: vi.spyOn(component, "onClickDeleteAllOperators").mockImplementation(() => {}),
          exportWorkflow: vi.spyOn(component, "onClickExportWorkflow").mockImplementation(() => {}),
          description: vi.spyOn(component, "onClickEditDescription").mockImplementation(() => {}),
        };
        fixture.detectChanges();

        toolbar("create new").triggerEventHandler("click", null);
        toolbar("save").triggerEventHandler("click", null);
        toolbar("delete all").triggerEventHandler("click", null);
        toolbar("export workflow").triggerEventHandler("click", null);
        toolbar("change description").triggerEventHandler("click", null);

        Object.values(spies).forEach(spy => expect(spy).toHaveBeenCalled());
      });
    });

    describe("operator actions", () => {
      it("keeps the operator actions disabled while nothing is selected", () => {
        component.operatorMenu.isDisableOperatorClickable = false;
        component.operatorMenu.isToViewResultClickable = false;
        component.operatorMenu.isReuseResultClickable = false;
        fixture.detectChanges();

        expect(utility("disable operators").nativeElement.disabled).toBe(true);
        expect(utility("view result").nativeElement.disabled).toBe(true);
        expect(utility("reuse result if possible").nativeElement.disabled).toBe(true);
      });

      it("wires each operator action in both of its rendered states", () => {
        const disable = vi.spyOn(component.operatorMenu, "disableHighlightedOperators").mockImplementation(() => {});
        const view = vi.spyOn(component.operatorMenu, "viewResultHighlightedOperators").mockImplementation(() => {});
        const reuse = vi.spyOn(component.operatorMenu, "reuseResultHighlightedOperator").mockImplementation(() => {});
        const menu = component.operatorMenu;
        menu.isDisableOperatorClickable = true;
        menu.isToViewResultClickable = true;
        menu.isReuseResultClickable = true;

        // Each action renders as one of two buttons depending on the state it would move to.
        menu.isDisableOperator = true;
        menu.isToViewResult = true;
        menu.isMarkForReuse = true;
        fixture.detectChanges();
        expect(utility("disable operators").nativeElement.disabled).toBe(false);
        utility("disable operators").triggerEventHandler("click", null);
        utility("view result").triggerEventHandler("click", null);
        // The "reuse result if possible" button is hard-disabled in the template
        // (`[disabled]="true || …"`), so it is asserted, not clicked — firing its handler
        // would claim an interaction the UI cannot perform.
        expect(utility("reuse result if possible").nativeElement.disabled).toBe(true);

        menu.isDisableOperator = false;
        menu.isToViewResult = false;
        menu.isMarkForReuse = false;
        fixture.detectChanges();
        utility("operators disabled, click to re-enable").triggerEventHandler("click", null);
        utility("click to remove view result").triggerEventHandler("click", null);
        utility("remove reusing previous result").triggerEventHandler("click", null);

        expect(disable).toHaveBeenCalledTimes(2);
        expect(view).toHaveBeenCalledTimes(2);
        expect(reuse).toHaveBeenCalledTimes(1);
      });

      it("wires the export-result button", () => {
        const exportSpy = vi.spyOn(component, "onClickExportExecutionResult").mockImplementation(() => {});
        component.isExportDeactivate = false;
        fixture.detectChanges();

        buttonWithIcon("cloud-download").triggerEventHandler("click", null);

        expect(exportSpy).toHaveBeenCalled();
      });

      it("wires undo and redo, and disables them while an older version is displayed", () => {
        const undo = vi.spyOn(component.undoRedoService, "undoAction").mockImplementation(() => {});
        const redo = vi.spyOn(component.undoRedoService, "redoAction").mockImplementation(() => {});
        vi.spyOn(component.undoRedoService, "canUndo").mockReturnValue(true);
        vi.spyOn(component.undoRedoService, "canRedo").mockReturnValue(true);
        fixture.detectChanges();

        buttonWithIcon("undo").triggerEventHandler("click", null);
        buttonWithIcon("redo").triggerEventHandler("click", null);
        expect(undo).toHaveBeenCalled();
        expect(redo).toHaveBeenCalled();

        // Viewing an older version makes the graph read-only, so history is off limits.
        component.displayParticularWorkflowVersion = true;
        fixture.detectChanges();
        expect(buttonWithIcon("undo").nativeElement.disabled).toBe(true);
        expect(buttonWithIcon("redo").nativeElement.disabled).toBe(true);
      });
    });

    describe("checkpoint button", () => {
      it("appears only with time travel on, and is enabled only while paused", () => {
        const checkpoint = vi.spyOn(component, "handleCheckpoint").mockImplementation(() => {});
        const guiConfig = TestBed.inject(GuiConfigService);
        guiConfig.env.timetravelEnabled = false;
        fixture.detectChanges();
        expect(q("#checkpoint-button")).toBeNull();

        guiConfig.env.timetravelEnabled = true;
        component.executionState = ExecutionState.Running;
        fixture.detectChanges();
        expect(q("#checkpoint-button").nativeElement.disabled).toBe(true);

        component.executionState = ExecutionState.Paused;
        fixture.detectChanges();
        expect(q("#checkpoint-button").nativeElement.disabled).toBe(false);

        q("#checkpoint-button").triggerEventHandler("click", null);
        expect(checkpoint).toHaveBeenCalled();
      });
    });
  });

  describe("ngOnInit subscriptions", () => {
    afterEach(() => {
      vi.restoreAllMocks();
    });

    it("re-applies the run button behavior on every execution state event", () => {
      const stateEvents$ = new Subject<{ current: { state: ExecutionState } }>();
      vi.spyOn(executeWorkflowService, "getExecutionStateStream").mockReturnValue(
        stateEvents$.asObservable() as ReturnType<typeof executeWorkflowService.getExecutionStateStream>
      );
      const stateFixture = TestBed.createComponent(MenuComponent);
      const stateComponent = stateFixture.componentInstance;
      stateFixture.detectChanges();
      stateComponent.isWorkflowValid = true;
      stateComponent.isWorkflowEmpty = false;
      stateComponent.computingUnitStatus = ComputingUnitState.Running;
      Object.defineProperty(stateComponent.workflowWebsocketService, "isConnected", {
        get: () => true,
        configurable: true,
      });

      try {
        stateEvents$.next({ current: { state: ExecutionState.Running } });
        expect(stateComponent.executionState).toBe(ExecutionState.Running);
        expect(stateComponent.runButtonText).toBe("Pause");

        stateEvents$.next({ current: { state: ExecutionState.Paused } });
        expect(stateComponent.runButtonText).toBe("Resume");
      } finally {
        stateFixture.destroy();
      }
    });

    it("re-applies the run button behavior on every compilation state event", () => {
      component.isWorkflowValid = true;
      component.isWorkflowEmpty = false;
      component.computingUnitStatus = ComputingUnitState.Running;
      component.executionState = ExecutionState.Uninitialized;
      Object.defineProperty(component.workflowWebsocketService, "isConnected", {
        get: () => true,
        configurable: true,
      });

      compilationStream$.next(CompilationState.Failed);
      expect(component.isWorkflowCompilable).toBe(false);
      expect(component.runButtonText).toBe("Invalid Workflow");
      expect(component.runDisable).toBe(true);

      compilationStream$.next(CompilationState.Succeeded);
      expect(component.isWorkflowCompilable).toBe(true);
      expect(component.runButtonText).toBe("Run");
      expect(component.runDisable).toBe(false);
    });

    it("deactivates the export button unless the feature is on and results exist", () => {
      const guiConfig = TestBed.inject(GuiConfigService);
      const results$ = component.workflowResultExportService.hasResultToExportOnAllOperators;

      // Feature off: deactivated whatever the results say.
      guiConfig.env.exportExecutionResultEnabled = false;
      results$.next(true);
      expect(component.isExportDeactivate).toBe(true);

      // Feature on, but nothing to export.
      guiConfig.env.exportExecutionResultEnabled = true;
      results$.next(false);
      expect(component.isExportDeactivate).toBe(true);

      results$.next(true);
      expect(component.isExportDeactivate).toBe(false);
    });
  });

  describe("onClickGenerateReport", () => {
    let reportService: ReportGenerationService;

    beforeEach(() => {
      reportService = TestBed.inject(ReportGenerationService);
      vi.spyOn(notificationService, "blank");
    });

    afterEach(() => {
      vi.restoreAllMocks();
    });

    it("orders the operator results by operator id and blanks the ones the backend omitted", () => {
      vi.spyOn(workflowActionService, "getWorkflowContent").mockReturnValue({
        operators: [{ operatorID: "op-1" }, { operatorID: "op-2" }],
        links: [],
        commentBoxes: [],
        settings: {},
      } as unknown as WorkflowContent);
      vi.spyOn(reportService, "generateWorkflowSnapshot").mockReturnValue(of("snap-url"));
      vi.spyOn(reportService, "getAllOperatorResults").mockReturnValue(of([{ operatorId: "op-1", html: "<b>x</b>" }]));
      const htmlSpy = vi.spyOn(reportService, "generateReportAsHtml").mockImplementation(() => {});
      const successSpy = vi.spyOn(notificationService, "success").mockImplementation(() => {});
      const removeSpy = vi.spyOn(notificationService, "remove").mockImplementation(() => {});
      const errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});
      component.currentWorkflowName = "wf";

      component.onClickGenerateReport();

      expect(htmlSpy).toHaveBeenCalledWith("snap-url", ["<b>x</b>", ""], "wf");
      expect(removeSpy).toHaveBeenCalledTimes(1);
      expect(successSpy).toHaveBeenCalledWith("Report successfully generated.");
      expect(errorSpy).not.toHaveBeenCalled();
    });

    it("reports a failure to retrieve the operator results and closes the notification", () => {
      vi.spyOn(reportService, "generateWorkflowSnapshot").mockReturnValue(of("snap-url"));
      vi.spyOn(reportService, "getAllOperatorResults").mockReturnValue(throwError(() => new Error("no results")));
      const htmlSpy = vi.spyOn(reportService, "generateReportAsHtml").mockImplementation(() => {});
      const removeSpy = vi.spyOn(notificationService, "remove").mockImplementation(() => {});
      const errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});

      component.onClickGenerateReport();

      expect(errorSpy).toHaveBeenCalledWith("Error in retrieving operator results: no results");
      expect(removeSpy).toHaveBeenCalledTimes(1);
      expect(htmlSpy).not.toHaveBeenCalled();
    });

    it("reports a failure to take the workflow snapshot without asking for results", () => {
      vi.spyOn(reportService, "generateWorkflowSnapshot").mockReturnValue(
        throwError(() => new Error("snapshot failed"))
      );
      const resultsSpy = vi.spyOn(reportService, "getAllOperatorResults");
      const removeSpy = vi.spyOn(notificationService, "remove").mockImplementation(() => {});
      const errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});

      component.onClickGenerateReport();

      expect(errorSpy).toHaveBeenCalledWith("snapshot failed");
      expect(removeSpy).toHaveBeenCalledTimes(1);
      expect(resultsSpy).not.toHaveBeenCalled();
    });
  });

  describe("onClickEditDescription", () => {
    /** Opens the modal over a workflow with `description`, with the editor emitting `edited`. */
    function open(description: string | undefined, edited: string) {
      vi.spyOn(workflowActionService, "getWorkflow").mockReturnValue({
        content: { operators: [], links: [], commentBoxes: [], settings: {} } as unknown as WorkflowContent,
        name: "wf",
        description,
        wid: 1,
        creationTime: undefined,
        lastModifiedTime: undefined,
        readonly: false,
        isPublished: 0,
      });
      const close = vi.fn();
      const createSpy = vi.spyOn(modalService, "create").mockReturnValue({
        afterClose: of(undefined),
        getContentComponent: () => ({ descriptionChange: of(edited) }),
        close,
      } as unknown as NzModalRef);
      return { close, createSpy };
    }

    afterEach(() => {
      vi.restoreAllMocks();
    });

    it("seeds the editor with an empty string when the workflow has no description", () => {
      const { createSpy } = open(undefined, "ignored");

      component.onClickEditDescription();

      expect((createSpy.mock.calls[0][0] as ModalOptions).nzData).toEqual({ description: "" });
    });

    it("stores the edited description, persists it while logged in, and closes the modal", () => {
      const { close } = open("old", "new description");
      const metadataSpy = vi.spyOn(workflowActionService, "setWorkflowMetadata").mockImplementation(() => {});
      const persistSpy = vi.spyOn(component, "persistWorkflow").mockImplementation(() => {});
      vi.spyOn(component.userService, "isLogin").mockReturnValue(true);

      component.onClickEditDescription();

      expect(metadataSpy).toHaveBeenCalledWith(expect.objectContaining({ wid: 1, description: "new description" }));
      expect(persistSpy).toHaveBeenCalledTimes(1);
      expect(close).toHaveBeenCalledTimes(1);
    });

    it("closes the modal without persisting when logged out", () => {
      const { close } = open("old", "new description");
      vi.spyOn(workflowActionService, "setWorkflowMetadata").mockImplementation(() => {});
      const persistSpy = vi.spyOn(component, "persistWorkflow").mockImplementation(() => {});
      vi.spyOn(component.userService, "isLogin").mockReturnValue(false);

      component.onClickEditDescription();

      expect(persistSpy).not.toHaveBeenCalled();
      expect(close).toHaveBeenCalledTimes(1);
    });
  });

  describe("persistWorkflow", () => {
    const saved = (wid?: number) => ({
      content: { operators: [], links: [], commentBoxes: [], settings: {} } as unknown as WorkflowContent,
      name: "wf",
      description: undefined,
      wid,
      creationTime: undefined,
      lastModifiedTime: undefined,
      readonly: false,
      isPublished: 0,
    });

    afterEach(() => {
      vi.restoreAllMocks();
    });

    it("files the saved workflow under the open project when both ids are known", () => {
      const userProjectService = TestBed.inject(UserProjectService);
      vi.spyOn(workflowPersistService, "persistWorkflow").mockReturnValue(of(saved(9)));
      const metadataSpy = vi.spyOn(workflowActionService, "setWorkflowMetadata").mockImplementation(() => {});
      const addSpy = vi.spyOn(userProjectService, "addWorkflowToProject").mockReturnValue(of({} as Response));
      component.pid = 3;

      component.persistWorkflow();

      expect(metadataSpy).toHaveBeenCalledWith(expect.objectContaining({ wid: 9 }));
      expect(addSpy).toHaveBeenCalledWith(3, 9);
      expect(component.isSaving).toBe(false);
    });

    it("leaves the project alone when the saved workflow has no id", () => {
      const userProjectService = TestBed.inject(UserProjectService);
      vi.spyOn(workflowPersistService, "persistWorkflow").mockReturnValue(of(saved(undefined)));
      vi.spyOn(workflowActionService, "setWorkflowMetadata").mockImplementation(() => {});
      const addSpy = vi.spyOn(userProjectService, "addWorkflowToProject");
      component.pid = 3;

      component.persistWorkflow();

      expect(addSpy).not.toHaveBeenCalled();
      expect(component.isSaving).toBe(false);
    });

    it("surfaces a save failure as a notification and stops the saving indicator", () => {
      vi.spyOn(workflowPersistService, "persistWorkflow").mockReturnValue(throwError(() => new Error("save failed")));
      const errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});

      component.persistWorkflow();

      expect(errorSpy).toHaveBeenCalledWith("save failed");
      expect(component.isSaving).toBe(false);
    });
  });

  describe("workflow metadata display", () => {
    const metadata = (overrides: Record<string, unknown>) => ({
      name: "wf",
      description: undefined,
      wid: 4,
      creationTime: undefined,
      lastModifiedTime: undefined,
      isPublished: 0,
      readonly: false,
      ...overrides,
    });

    afterEach(() => {
      vi.useRealTimers();
      vi.restoreAllMocks();
    });

    /** The refresh subscription is debounced by 100ms; the width fix-up is a 0ms macrotask. */
    function refreshWith(overrides: Record<string, unknown>): void {
      workflowActionService.setWorkflowMetadata(
        metadata(overrides) as Parameters<typeof workflowActionService.setWorkflowMetadata>[0]
      );
      vi.advanceTimersByTime(101);
    }

    it("stamps the last save time for a workflow that has been persisted", () => {
      vi.useFakeTimers();
      const widthSpy = vi.spyOn(component, "adjustWorkflowNameWidth").mockImplementation(() => {});

      refreshWith({ name: "saved wf", lastModifiedTime: 1_700_000_000_000 });

      expect(component.currentWorkflowName).toBe("saved wf");
      // The rendered instant depends on the runner's zone, so only the shape is asserted.
      expect(component.autoSaveState).toMatch(/^Saved at \d{2}\/\d{2}\/\d{4} \d{2}:\d{2}:\d{2}$/);
      expect(widthSpy).toHaveBeenCalled();
    });

    it("leaves the stamp empty for a workflow that has never been saved", () => {
      vi.useFakeTimers();
      vi.spyOn(component, "adjustWorkflowNameWidth").mockImplementation(() => {});

      refreshWith({ name: "fresh wf", lastModifiedTime: undefined });

      expect(component.currentWorkflowName).toBe("fresh wf");
      expect(component.autoSaveState).toBe("");
    });

    it("stamps the version date when the displayed workflow has a creation time", () => {
      workflowActionService.setWorkflowMetadata(
        metadata({ creationTime: 1_700_000_000_000 }) as Parameters<typeof workflowActionService.setWorkflowMetadata>[0]
      );

      workflowVersionService.setDisplayParticularVersion(true, 1, 2);

      expect(component.displayParticularWorkflowVersion).toBe(true);
      expect(component.particularVersionDate).toMatch(/^\d{2}\/\d{2}\/\d{4} \d{2}:\d{2}:\d{2}$/);
    });
  });
});
