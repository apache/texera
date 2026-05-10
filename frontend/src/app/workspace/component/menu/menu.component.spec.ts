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
import { NO_ERRORS_SCHEMA } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { RouterTestingModule } from "@angular/router/testing";
import { NzModalService, NzModalModule, NzModalRef } from "ng-zorro-antd/modal";
import { BehaviorSubject, of, throwError } from "rxjs";

import { MenuComponent } from "./menu.component";
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
import { ComputingUnitSelectionComponent } from "../power-button/computing-unit-selection.component";
import { WorkflowContent } from "../../../common/type/workflow";
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

  beforeEach(async () => {
    TestBed.overrideComponent(MenuComponent, {
      set: { template: "" },
    });

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
          },
        },
        { provide: UserService, useClass: StubUserService },
        ...commonTestProviders,
      ],
      schemas: [NO_ERRORS_SCHEMA],
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
        newComputingUnitName: "",
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

      expect(component.computingUnitSelectionComponent.newComputingUnitName).toBe("wf's Computing Unit");
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
  });

  it("onWorkflowNameChange forwards the new name to the workflow action service", () => {
    const setNameSpy = vi.spyOn(workflowActionService, "setWorkflowName");
    component.currentWorkflowName = "renamed";

    component.onWorkflowNameChange();

    expect(setNameSpy).toHaveBeenCalledWith("renamed");
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
});
