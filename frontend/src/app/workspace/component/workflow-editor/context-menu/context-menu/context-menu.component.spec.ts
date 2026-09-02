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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { By } from "@angular/platform-browser";
import { OperatorMetadataService } from "src/app/workspace/service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "src/app/workspace/service/operator-metadata/stub-operator-metadata.service";

import { ContextMenuComponent } from "./context-menu.component";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { WorkflowActionService } from "src/app/workspace/service/workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "src/app/workspace/service/workflow-result/workflow-result.service";
import { WorkflowResultExportService } from "src/app/workspace/service/workflow-result-export/workflow-result-export.service";
import { OperatorMenuService } from "src/app/workspace/service/operator-menu/operator-menu.service";
import { BehaviorSubject, of } from "rxjs";
import { ReactiveFormsModule } from "@angular/forms";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { ValidationWorkflowService } from "src/app/workspace/service/validation/validation-workflow.service";
import { WorkflowCompilingService } from "src/app/workspace/service/compile-workflow/workflow-compiling.service";
import { NzModalModule, NzModalService } from "ng-zorro-antd/modal";
import { commonTestProviders } from "../../../../../common/testing/test-utils"; // Import NzModalModule and NzModalService
import type { Mocked } from "vitest";
import { JointGraphWrapper } from "src/app/workspace/service/workflow-graph/model/joint-graph-wrapper";
import { WorkflowGraph } from "src/app/workspace/service/workflow-graph/model/workflow-graph";
import { ResultExportationComponent } from "../../../result-exportation/result-exportation.component";
import { UndoRedoService } from "src/app/workspace/service/undo-redo/undo-redo.service";
import {
  mockCommentBox,
  mockPoint,
  mockScanPredicate,
  mockScanSentimentLink,
  mockSentimentPredicate,
} from "src/app/workspace/service/workflow-graph/model/mock-workflow-data";
describe("ContextMenuComponent", () => {
  let component: ContextMenuComponent;
  let fixture: ComponentFixture<ContextMenuComponent>;
  let workflowActionService: Mocked<WorkflowActionService>;
  let workflowResultService: Mocked<WorkflowResultService>;
  let workflowResultExportService: Mocked<WorkflowResultExportService>;
  let operatorMenuService: Mocked<OperatorMenuService>;
  let jointGraphWrapperSpy: Mocked<JointGraphWrapper>;
  let validationWorkflowService: Mocked<ValidationWorkflowService>;
  let workflowCompilingService: Mocked<WorkflowCompilingService>;
  let highlightedOperatorsSubject: BehaviorSubject<readonly string[]>;
  let highlightedCommentBoxesSubject: BehaviorSubject<readonly string[]>;

  beforeEach(async () => {
    // Create spies for the services
    jointGraphWrapperSpy = {
      getCurrentHighlightedOperatorIDs: vi.fn(),
      getCurrentHighlightedCommentBoxIDs: vi.fn(),
      getCurrentHighlightedLinkIDs: vi.fn(),
    } as unknown as Mocked<JointGraphWrapper>;

    jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue([]);
    jointGraphWrapperSpy.getCurrentHighlightedCommentBoxIDs.mockReturnValue([]);
    jointGraphWrapperSpy.getCurrentHighlightedLinkIDs.mockReturnValue([]);

    const texeraGraphSpy = {
      isOperatorDisabled: vi.fn(),
      hasLinkWithID: vi.fn(),
      bundleActions: vi.fn(),
      getSubDAG: vi.fn(),
    };

    const workflowActionServiceSpy = {
      getJointGraphWrapper: vi.fn(),
      getWorkflowModificationEnabledStream: vi.fn(),
      deleteOperatorsAndLinks: vi.fn(),
      deleteCommentBox: vi.fn(),
      getWorkflowMetadata: vi.fn(),
      getTexeraGraph: vi.fn(),
      deleteLinkWithID: vi.fn(),
    };
    workflowActionServiceSpy.getJointGraphWrapper.mockReturnValue(jointGraphWrapperSpy);
    workflowActionServiceSpy.getWorkflowModificationEnabledStream.mockReturnValue(of(true));
    workflowActionServiceSpy.getTexeraGraph.mockReturnValue(texeraGraphSpy);
    workflowActionServiceSpy.deleteOperatorsAndLinks.mockReturnValue(undefined);
    workflowActionServiceSpy.deleteCommentBox.mockReturnValue(undefined);
    workflowActionServiceSpy.deleteLinkWithID.mockReturnValue(undefined);
    workflowActionServiceSpy.getWorkflowMetadata.mockReturnValue({ name: "Test Workflow" }); // Mock return value

    // Set up TexeraGraph spy return values
    texeraGraphSpy.hasLinkWithID.mockReturnValue(false);
    texeraGraphSpy.getSubDAG.mockReturnValue({ operators: [], links: [] });
    texeraGraphSpy.bundleActions.mockImplementation((callback: Function) => callback());

    const workflowResultServiceSpy = { getResultService: vi.fn(), hasAnyResult: vi.fn() };
    const workflowResultExportServiceSpy = { exportOperatorsResultAsFile: vi.fn() };

    // Create a mock for OperatorMenuService with necessary properties and methods.
    // The highlight streams are BehaviorSubjects so tests can push new emissions
    // and assert the component's constructor subscriptions stay in sync.
    highlightedOperatorsSubject = new BehaviorSubject<readonly string[]>([]);
    highlightedCommentBoxesSubject = new BehaviorSubject<readonly string[]>([]);
    operatorMenuService = {
      highlightedOperators$: highlightedOperatorsSubject.asObservable(),
      highlightedCommentBoxes$: highlightedCommentBoxesSubject.asObservable(),
      isDisableOperator: false,
      isDisableOperatorClickable: false,
      isToViewResult: false,
      isToViewResultClickable: false,
      isMarkForReuse: false,
      isReuseResultClickable: false,
      saveHighlightedElements: vi.fn(),
      performPasteOperation: vi.fn(),
      disableHighlightedOperators: vi.fn(),
      viewResultHighlightedOperators: vi.fn(),
      reuseResultHighlightedOperator: vi.fn(),
      executeUpToOperator: vi.fn(),
    } as unknown as Mocked<OperatorMenuService>;

    const validationWorkflowServiceSpy = { validateOperator: vi.fn() };

    const workflowCompilingServiceSpy = { getWorkflowCompilationErrors: vi.fn() };
    workflowCompilingServiceSpy.getWorkflowCompilationErrors.mockReturnValue({});

    await TestBed.configureTestingModule({
      providers: [
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: WorkflowActionService, useValue: workflowActionServiceSpy },
        { provide: WorkflowResultService, useValue: workflowResultServiceSpy },
        { provide: WorkflowResultExportService, useValue: workflowResultExportServiceSpy },
        { provide: OperatorMenuService, useValue: operatorMenuService },
        { provide: ValidationWorkflowService, useValue: validationWorkflowServiceSpy },
        { provide: WorkflowCompilingService, useValue: workflowCompilingServiceSpy },
        NzModalService, // Provide NzModalService
        ...commonTestProviders,
      ],
      imports: [
        ContextMenuComponent,
        HttpClientTestingModule,
        ReactiveFormsModule,
        BrowserAnimationsModule,
        NzDropDownModule,
        NzModalModule, // Import NzModalModule
      ],
    }).compileComponents();

    workflowActionService = TestBed.inject(WorkflowActionService) as unknown as Mocked<WorkflowActionService>;
    workflowResultService = TestBed.inject(WorkflowResultService) as unknown as Mocked<WorkflowResultService>;
    workflowResultExportService = TestBed.inject(
      WorkflowResultExportService
    ) as unknown as Mocked<WorkflowResultExportService>;
    // operatorMenuService is already assigned
    validationWorkflowService = TestBed.inject(
      ValidationWorkflowService
    ) as unknown as Mocked<ValidationWorkflowService>;
    workflowCompilingService = TestBed.inject(WorkflowCompilingService) as unknown as Mocked<WorkflowCompilingService>;

    fixture = TestBed.createComponent(ContextMenuComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("isSelectedOperatorValid", () => {
    it("should return false when multiple operators are highlighted", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue(["op1", "op2"]);
      component.isWorkflowModifiable = true;

      expect(component.canExecuteOperator()).toBe(false);
      expect(validationWorkflowService.validateOperator).not.toHaveBeenCalled();
    });

    it("should return false when no operators are highlighted", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue([]);
      component.isWorkflowModifiable = true;

      expect(component.canExecuteOperator()).toBe(false);
      expect(validationWorkflowService.validateOperator).not.toHaveBeenCalled();
    });

    it("should return false when workflow is not modifiable", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue(["op1"]);
      component.isWorkflowModifiable = false;

      expect(component.canExecuteOperator()).toBe(false);
      expect(validationWorkflowService.validateOperator).not.toHaveBeenCalled();
    });

    it("should return true when single operator is highlighted, workflow is modifiable, and operator is valid", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue(["op1"]);
      component.isWorkflowModifiable = true;
      validationWorkflowService.validateOperator.mockReturnValue({ isValid: true });

      expect(component.canExecuteOperator()).toBe(true);
      expect(validationWorkflowService.validateOperator).toHaveBeenCalledWith("op1");
    });

    it("should return false when single operator is highlighted but operator is invalid", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue(["op1"]);
      component.isWorkflowModifiable = true;
      validationWorkflowService.validateOperator.mockReturnValue({ isValid: false, messages: {} });

      expect(component.canExecuteOperator()).toBe(false);
      expect(validationWorkflowService.validateOperator).toHaveBeenCalledWith("op1");
    });
  });

  describe("canExecuteOperator", () => {
    let texeraGraphSpy: Mocked<WorkflowGraph>;

    beforeEach(() => {
      texeraGraphSpy = workflowActionService.getTexeraGraph() as unknown as Mocked<WorkflowGraph>;
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue(["op1"]);
      component.isWorkflowModifiable = true;
      validationWorkflowService.validateOperator.mockReturnValue({ isValid: true });
      texeraGraphSpy.isOperatorDisabled.mockReturnValue(false);
    });

    it("should return false when multiple operators are highlighted", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue(["op1", "op2"]);

      expect(component.canExecuteOperator()).toBe(false);
      expect(validationWorkflowService.validateOperator).not.toHaveBeenCalled();
      expect(texeraGraphSpy.isOperatorDisabled).not.toHaveBeenCalled();
    });

    it("should return false when no operators are highlighted", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue([]);

      expect(component.canExecuteOperator()).toBe(false);
      expect(validationWorkflowService.validateOperator).not.toHaveBeenCalled();
      expect(texeraGraphSpy.isOperatorDisabled).not.toHaveBeenCalled();
    });

    it("should return false when workflow is not modifiable", () => {
      component.isWorkflowModifiable = false;

      expect(component.canExecuteOperator()).toBe(false);
      expect(validationWorkflowService.validateOperator).not.toHaveBeenCalled();
      expect(texeraGraphSpy.isOperatorDisabled).not.toHaveBeenCalled();
    });

    it("should return true when all conditions are met (valid, enabled, modifiable)", () => {
      expect(component.canExecuteOperator()).toBe(true);
      expect(validationWorkflowService.validateOperator).toHaveBeenCalledWith("op1");
      expect(texeraGraphSpy.isOperatorDisabled).toHaveBeenCalledWith("op1");
    });

    it("should return false when operator is invalid and not check disabled status", () => {
      validationWorkflowService.validateOperator.mockReturnValue({ isValid: false, messages: {} });

      expect(component.canExecuteOperator()).toBe(false);
      expect(validationWorkflowService.validateOperator).toHaveBeenCalledWith("op1");
      expect(texeraGraphSpy.isOperatorDisabled).not.toHaveBeenCalled();
    });

    it("should return false when operator is valid but disabled", () => {
      validationWorkflowService.validateOperator.mockReturnValue({ isValid: true });
      texeraGraphSpy.isOperatorDisabled.mockReturnValue(true);

      expect(component.canExecuteOperator()).toBe(false);
      expect(validationWorkflowService.validateOperator).toHaveBeenCalledWith("op1");
      expect(texeraGraphSpy.isOperatorDisabled).toHaveBeenCalledWith("op1");
    });

    it("should return false when the target operator failed to compile", () => {
      texeraGraphSpy.getSubDAG.mockReturnValue({ operators: [{ operatorID: "op1" }], links: [] } as any);
      workflowCompilingService.getWorkflowCompilationErrors.mockReturnValue({ op1: {} as any });

      expect(component.canExecuteOperator()).toBe(false);
      expect(texeraGraphSpy.getSubDAG).toHaveBeenCalledWith("op1");
    });

    it("should return false when an upstream operator failed to compile", () => {
      texeraGraphSpy.getSubDAG.mockReturnValue({
        operators: [{ operatorID: "op1" }, { operatorID: "upstream" }],
        links: [],
      } as any);
      workflowCompilingService.getWorkflowCompilationErrors.mockReturnValue({ upstream: {} as any });

      expect(component.canExecuteOperator()).toBe(false);
    });

    it("should return true when the compilation error is outside the target's sub-DAG", () => {
      texeraGraphSpy.getSubDAG.mockReturnValue({ operators: [{ operatorID: "op1" }], links: [] } as any);
      workflowCompilingService.getWorkflowCompilationErrors.mockReturnValue({ unrelated: {} as any });

      expect(component.canExecuteOperator()).toBe(true);
    });

    it("should check disabled status only for valid operators", () => {
      // First test with invalid operator
      validationWorkflowService.validateOperator.mockReturnValue({ isValid: false, messages: {} });
      component.canExecuteOperator();
      expect(texeraGraphSpy.isOperatorDisabled).not.toHaveBeenCalled();

      // Then test with valid operator
      validationWorkflowService.validateOperator.mockReturnValue({ isValid: true });
      component.canExecuteOperator();
      expect(texeraGraphSpy.isOperatorDisabled).toHaveBeenCalledWith("op1");
    });
  });

  describe("onCopy / onPaste", () => {
    it("should delegate onCopy to OperatorMenuService.saveHighlightedElements", () => {
      component.onCopy();

      expect(operatorMenuService.saveHighlightedElements).toHaveBeenCalledTimes(1);
      expect(operatorMenuService.performPasteOperation).not.toHaveBeenCalled();
    });

    it("should delegate onPaste to OperatorMenuService.performPasteOperation", () => {
      component.onPaste();

      expect(operatorMenuService.performPasteOperation).toHaveBeenCalledTimes(1);
      expect(operatorMenuService.saveHighlightedElements).not.toHaveBeenCalled();
    });
  });

  describe("onCut", () => {
    it("should copy the highlighted elements before deleting them", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue(["op1"]);

      component.onCut();

      expect(operatorMenuService.saveHighlightedElements).toHaveBeenCalledTimes(1);
      expect(workflowActionService.deleteOperatorsAndLinks).toHaveBeenCalledWith(["op1"]);
      // cut is defined as copy followed by delete, so the ordering is behavior
      expect(operatorMenuService.saveHighlightedElements.mock.invocationCallOrder[0]).toBeLessThan(
        workflowActionService.deleteOperatorsAndLinks.mock.invocationCallOrder[0]
      );
    });
  });

  describe("hasHighlightedLinks", () => {
    it("should return false when no links are highlighted", () => {
      jointGraphWrapperSpy.getCurrentHighlightedLinkIDs.mockReturnValue([]);

      expect(component.hasHighlightedLinks()).toBe(false);
    });

    it("should return true when at least one link is highlighted", () => {
      jointGraphWrapperSpy.getCurrentHighlightedLinkIDs.mockReturnValue(["link-1"]);

      expect(component.hasHighlightedLinks()).toBe(true);
    });
  });

  describe("onDelete", () => {
    let texeraGraphSpy: Mocked<WorkflowGraph>;

    beforeEach(() => {
      texeraGraphSpy = workflowActionService.getTexeraGraph() as unknown as Mocked<WorkflowGraph>;
    });

    it("should delete highlighted operators, standalone links, and comment boxes in one bundled action", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue(["op1", "op2"]);
      jointGraphWrapperSpy.getCurrentHighlightedLinkIDs.mockReturnValue(["link-9"]);
      jointGraphWrapperSpy.getCurrentHighlightedCommentBoxIDs.mockReturnValue(["box-1"]);
      texeraGraphSpy.hasLinkWithID.mockReturnValue(true);

      component.onDelete();

      expect(texeraGraphSpy.bundleActions).toHaveBeenCalledTimes(1);
      expect(workflowActionService.deleteOperatorsAndLinks).toHaveBeenCalledWith(["op1", "op2"]);
      expect(workflowActionService.deleteLinkWithID).toHaveBeenCalledWith("link-9");
      expect(workflowActionService.deleteCommentBox).toHaveBeenCalledWith("box-1");
    });

    it("should perform every deletion inside the bundleActions callback", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue(["op1"]);
      jointGraphWrapperSpy.getCurrentHighlightedLinkIDs.mockReturnValue(["link-9"]);
      jointGraphWrapperSpy.getCurrentHighlightedCommentBoxIDs.mockReturnValue(["box-1"]);
      texeraGraphSpy.hasLinkWithID.mockReturnValue(true);
      // if bundleActions never runs the callback, nothing at all may be deleted
      texeraGraphSpy.bundleActions.mockImplementation(() => {});

      component.onDelete();

      expect(workflowActionService.deleteOperatorsAndLinks).not.toHaveBeenCalled();
      expect(workflowActionService.deleteLinkWithID).not.toHaveBeenCalled();
      expect(workflowActionService.deleteCommentBox).not.toHaveBeenCalled();
    });

    it("should skip highlighted links that no longer exist in the graph", () => {
      jointGraphWrapperSpy.getCurrentHighlightedLinkIDs.mockReturnValue(["link-9"]);
      texeraGraphSpy.hasLinkWithID.mockReturnValue(false);

      component.onDelete();

      expect(texeraGraphSpy.hasLinkWithID).toHaveBeenCalledWith("link-9");
      expect(workflowActionService.deleteLinkWithID).not.toHaveBeenCalled();
    });

    it("should snapshot the highlighted IDs before deletion mutates the highlight state", () => {
      const liveOperatorIDs = ["op1"];
      const liveLinkIDs = ["link-9"];
      const liveCommentBoxIDs = ["box-1"];
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue(liveOperatorIDs);
      jointGraphWrapperSpy.getCurrentHighlightedLinkIDs.mockReturnValue(liveLinkIDs);
      jointGraphWrapperSpy.getCurrentHighlightedCommentBoxIDs.mockReturnValue(liveCommentBoxIDs);
      texeraGraphSpy.hasLinkWithID.mockReturnValue(true);
      // deleting operators unhighlights every element, as the real graph would
      workflowActionService.deleteOperatorsAndLinks.mockImplementation(() => {
        liveOperatorIDs.length = 0;
        liveLinkIDs.length = 0;
        liveCommentBoxIDs.length = 0;
      });

      component.onDelete();

      expect(workflowActionService.deleteLinkWithID).toHaveBeenCalledWith("link-9");
      expect(workflowActionService.deleteCommentBox).toHaveBeenCalledWith("box-1");
    });
  });

  describe("onClickExportHighlightedExecutionResult", () => {
    it("should open the result exportation modal with the workflow name and context-menu source", () => {
      const modalService = TestBed.inject(NzModalService);
      const createSpy = vi.spyOn(modalService, "create").mockReturnValue({} as any);

      component.onClickExportHighlightedExecutionResult();

      expect(createSpy).toHaveBeenCalledTimes(1);
      expect(createSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          nzTitle: "Export Highlighted Operators Result",
          nzContent: ResultExportationComponent,
          nzData: {
            workflowName: "Test Workflow",
            sourceTriggered: "context-menu",
          },
          nzFooter: null,
        })
      );
    });
  });

  describe("highlight subscriptions", () => {
    it("should keep highlightedOperatorIds in sync with OperatorMenuService emissions", () => {
      expect(component.highlightedOperatorIds).toEqual([]);

      highlightedOperatorsSubject.next(["op-a", "op-b"]);
      expect(component.highlightedOperatorIds).toEqual(["op-a", "op-b"]);

      highlightedOperatorsSubject.next([]);
      expect(component.highlightedOperatorIds).toEqual([]);
    });

    it("should keep highlightedCommentBoxIds in sync with OperatorMenuService emissions", () => {
      expect(component.highlightedCommentBoxIds).toEqual([]);

      highlightedCommentBoxesSubject.next(["box-a"]);
      expect(component.highlightedCommentBoxIds).toEqual(["box-a"]);

      highlightedCommentBoxesSubject.next([]);
      expect(component.highlightedCommentBoxIds).toEqual([]);
    });
  });
  // ── Template menu-item (click) wiring ──
  // The class methods are covered above; these render the menu and click each
  // item so the template's *ngIf-gated (click) bindings are exercised too.
  describe("menu item click bindings", () => {
    const norm = (s: string | null) => (s ?? "").replace(/\s+/g, " ").trim().toLowerCase();

    /** Click the rendered <li nz-menu-item> whose text matches `label` exactly. */
    function clickItem(label: string): void {
      const items = fixture.debugElement.queryAll(By.css("li[nz-menu-item]"));
      const item = items.find(li => norm(li.nativeElement.textContent) === norm(label));
      if (!item) {
        throw new Error(
          `menu item "${label}" not rendered; present: [${items.map(i => norm(i.nativeElement.textContent)).join(" | ")}]`
        );
      }
      item.triggerEventHandler("click", null);
    }

    it("copy invokes onCopy", () => {
      highlightedOperatorsSubject.next(["op1"]);
      fixture.detectChanges();
      const spy = vi.spyOn(component, "onCopy").mockImplementation(() => {});

      clickItem("copy");

      expect(spy).toHaveBeenCalledTimes(1);
    });

    it("cut invokes onCut", () => {
      highlightedOperatorsSubject.next(["op1"]);
      component.isWorkflowModifiable = true;
      fixture.detectChanges();
      const spy = vi.spyOn(component, "onCut").mockImplementation(() => {});

      clickItem("cut");

      expect(spy).toHaveBeenCalledTimes(1);
    });

    it("paste invokes onPaste", () => {
      highlightedOperatorsSubject.next([]);
      highlightedCommentBoxesSubject.next([]);
      component.isWorkflowModifiable = true;
      fixture.detectChanges();
      const spy = vi.spyOn(component, "onPaste").mockImplementation(() => {});

      clickItem("paste");

      expect(spy).toHaveBeenCalledTimes(1);
    });

    it("disable invokes operatorMenuService.disableHighlightedOperators", () => {
      operatorMenuService.isDisableOperator = true;
      operatorMenuService.isDisableOperatorClickable = true;
      fixture.detectChanges();

      clickItem("disable");

      expect(operatorMenuService.disableHighlightedOperators).toHaveBeenCalledTimes(1);
    });

    it("enable invokes operatorMenuService.disableHighlightedOperators", () => {
      operatorMenuService.isDisableOperator = false;
      operatorMenuService.isDisableOperatorClickable = true;
      fixture.detectChanges();

      clickItem("enable");

      expect(operatorMenuService.disableHighlightedOperators).toHaveBeenCalledTimes(1);
    });

    it("view result invokes operatorMenuService.viewResultHighlightedOperators", () => {
      operatorMenuService.isToViewResult = true;
      operatorMenuService.isToViewResultClickable = true;
      fixture.detectChanges();

      clickItem("view result");

      expect(operatorMenuService.viewResultHighlightedOperators).toHaveBeenCalledTimes(1);
    });

    it("remove view result invokes operatorMenuService.viewResultHighlightedOperators", () => {
      operatorMenuService.isToViewResult = false;
      operatorMenuService.isToViewResultClickable = true;
      fixture.detectChanges();

      clickItem("remove view result");

      expect(operatorMenuService.viewResultHighlightedOperators).toHaveBeenCalledTimes(1);
    });

    it("renders the reuse result item (disabled) when marked for reuse", () => {
      // This entry is hardcoded `nzDisabled`, so it can't be clicked; assert it renders.
      // Its (click) handler is the same as "remove reusing result", covered by the next test.
      operatorMenuService.isMarkForReuse = true;
      operatorMenuService.isReuseResultClickable = true;
      fixture.detectChanges();

      const rendered = fixture.debugElement
        .queryAll(By.css("li[nz-menu-item]"))
        .some(li => norm(li.nativeElement.textContent) === "reuse result");
      expect(rendered).toBe(true);
    });

    it("remove reusing result invokes operatorMenuService.reuseResultHighlightedOperator", () => {
      operatorMenuService.isMarkForReuse = false;
      operatorMenuService.isReuseResultClickable = true;
      fixture.detectChanges();

      clickItem("remove reusing result");

      expect(operatorMenuService.reuseResultHighlightedOperator).toHaveBeenCalledTimes(1);
    });

    it("delete invokes onDelete when operators are highlighted", () => {
      highlightedOperatorsSubject.next(["op1"]);
      component.isWorkflowModifiable = true;
      fixture.detectChanges();
      const spy = vi.spyOn(component, "onDelete").mockImplementation(() => {});

      clickItem("delete");

      expect(spy).toHaveBeenCalledTimes(1);
    });

    it("delete invokes onDelete for a links-only selection", () => {
      highlightedOperatorsSubject.next([]);
      highlightedCommentBoxesSubject.next([]);
      jointGraphWrapperSpy.getCurrentHighlightedLinkIDs.mockReturnValue(["link1"]);
      component.isWorkflowModifiable = true;
      fixture.detectChanges();
      const spy = vi.spyOn(component, "onDelete").mockImplementation(() => {});

      clickItem("delete");

      expect(spy).toHaveBeenCalledTimes(1);
    });

    it("execute to this operator invokes executeUpToOperator", () => {
      highlightedOperatorsSubject.next(["op1"]);
      component.isWorkflowModifiable = true;
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue(["op1"]);
      validationWorkflowService.validateOperator.mockReturnValue({ isValid: true });
      (workflowActionService.getTexeraGraph() as unknown as Mocked<WorkflowGraph>).isOperatorDisabled.mockReturnValue(
        false
      );
      fixture.detectChanges();
      expect(component.canExecuteOperator()).toBe(true); // item is enabled

      clickItem("execute to this operator");

      expect(operatorMenuService.executeUpToOperator).toHaveBeenCalledTimes(1);
    });

    it("Export result invokes onClickExportHighlightedExecutionResult", () => {
      (
        workflowResultExportService as unknown as { hasResultToExportOnHighlightedOperators: boolean }
      ).hasResultToExportOnHighlightedOperators = true;
      (component as unknown as { config: { env: Record<string, unknown> } }).config.env.exportExecutionResultEnabled =
        true;
      fixture.detectChanges();
      const spy = vi.spyOn(component, "onClickExportHighlightedExecutionResult").mockImplementation(() => {});

      clickItem("export result");

      expect(spy).toHaveBeenCalledTimes(1);
    });
  });
});

describe("ContextMenuComponent onDelete with real WorkflowActionService", () => {
  let component: ContextMenuComponent;
  let fixture: ComponentFixture<ContextMenuComponent>;
  let workflowActionService: WorkflowActionService;
  let undoRedoService: UndoRedoService;

  // mockCommentBox reuses operator ID "1" (operators and comment boxes share the
  // joint-graph cell namespace), so give the comment box a distinct ID.
  const commentBox = { ...mockCommentBox, commentBoxID: "comment-box-1" };

  beforeEach(async () => {
    // Only the collaborators onDelete does not touch are stubbed; deletions run
    // through the real WorkflowActionService against a seeded graph.
    const operatorMenuServiceStub = {
      highlightedOperators$: of([] as readonly string[]),
      highlightedCommentBoxes$: of([] as readonly string[]),
      isDisableOperator: false,
      isDisableOperatorClickable: false,
      isToViewResult: false,
      isToViewResultClickable: false,
      isMarkForReuse: false,
      isReuseResultClickable: false,
      saveHighlightedElements: vi.fn(),
      performPasteOperation: vi.fn(),
    } as unknown as OperatorMenuService;

    await TestBed.configureTestingModule({
      providers: [
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: WorkflowResultService, useValue: { getResultService: vi.fn(), hasAnyResult: vi.fn() } },
        { provide: WorkflowResultExportService, useValue: { exportOperatorsResultAsFile: vi.fn() } },
        { provide: OperatorMenuService, useValue: operatorMenuServiceStub },
        { provide: ValidationWorkflowService, useValue: { validateOperator: vi.fn() } },
        NzModalService,
        ...commonTestProviders,
      ],
      imports: [
        ContextMenuComponent,
        HttpClientTestingModule,
        ReactiveFormsModule,
        BrowserAnimationsModule,
        NzDropDownModule,
        NzModalModule,
      ],
    }).compileComponents();

    workflowActionService = TestBed.inject(WorkflowActionService);
    undoRedoService = TestBed.inject(UndoRedoService);

    fixture = TestBed.createComponent(ContextMenuComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  /**
   * Seeds scan -> sentiment connected by mockScanSentimentLink, then clears the
   * auto-highlight state and enables multi-select so tests can highlight exactly
   * the elements they want deleted.
   */
  function seedScanSentimentGraph(): void {
    workflowActionService.addOperatorsAndLinks(
      [
        { op: mockScanPredicate, pos: mockPoint },
        { op: mockSentimentPredicate, pos: mockPoint },
      ],
      [mockScanSentimentLink]
    );
    const wrapper = workflowActionService.getJointGraphWrapper();
    wrapper.unhighlightOperators(...wrapper.getCurrentHighlightedOperatorIDs());
    wrapper.unhighlightLinks(...wrapper.getCurrentHighlightedLinkIDs());
    wrapper.setMultiSelectMode(true);
  }

  it("should delete a highlighted operator with its attached link without double-deleting the link", () => {
    seedScanSentimentGraph();
    const wrapper = workflowActionService.getJointGraphWrapper();
    const texeraGraph = workflowActionService.getTexeraGraph();
    // highlight both the operator and its attached link: deleting the operator
    // removes the link first, so the standalone-link pass must hit the
    // hasLinkWithID guard instead of deleting (and throwing on) a missing link.
    wrapper.highlightOperators(mockScanPredicate.operatorID);
    wrapper.highlightLinks(mockScanSentimentLink.linkID);
    expect(wrapper.getCurrentHighlightedLinkIDs()).toEqual([mockScanSentimentLink.linkID]);

    component.onDelete();

    expect(texeraGraph.hasOperator(mockScanPredicate.operatorID)).toBe(false);
    expect(texeraGraph.hasLinkWithID(mockScanSentimentLink.linkID)).toBe(false);
    expect(texeraGraph.hasOperator(mockSentimentPredicate.operatorID)).toBe(true);
  });

  it("should delete a standalone highlighted link and keep its endpoint operators", () => {
    seedScanSentimentGraph();
    const wrapper = workflowActionService.getJointGraphWrapper();
    const texeraGraph = workflowActionService.getTexeraGraph();
    wrapper.highlightLinks(mockScanSentimentLink.linkID);

    component.onDelete();

    expect(texeraGraph.hasLinkWithID(mockScanSentimentLink.linkID)).toBe(false);
    expect(texeraGraph.hasOperator(mockScanPredicate.operatorID)).toBe(true);
    expect(texeraGraph.hasOperator(mockSentimentPredicate.operatorID)).toBe(true);
  });

  it("should delete highlighted comment boxes", () => {
    workflowActionService.addCommentBox(commentBox);
    const wrapper = workflowActionService.getJointGraphWrapper();
    const texeraGraph = workflowActionService.getTexeraGraph();
    wrapper.highlightCommentBoxes(commentBox.commentBoxID);

    component.onDelete();

    expect(texeraGraph.hasCommentBox(commentBox.commentBoxID)).toBe(false);
  });

  it("should bundle the whole deletion into a single undo step", () => {
    seedScanSentimentGraph();
    workflowActionService.addCommentBox(commentBox);
    const wrapper = workflowActionService.getJointGraphWrapper();
    const texeraGraph = workflowActionService.getTexeraGraph();
    // addCommentBox turns multi-select off again, so re-enable it before
    // highlighting the whole graph.
    wrapper.setMultiSelectMode(true);
    wrapper.highlightOperators(mockScanPredicate.operatorID, mockSentimentPredicate.operatorID);
    wrapper.highlightLinks(mockScanSentimentLink.linkID);
    wrapper.highlightCommentBoxes(commentBox.commentBoxID);

    // the yjs undo manager merges transactions within its capture timeout, so
    // force the deletion to start a fresh undo stack item.
    (texeraGraph as WorkflowGraph).sharedModel.undoManager.stopCapturing();
    const undoLengthBefore = undoRedoService.getUndoLength();

    component.onDelete();

    expect(texeraGraph.getAllOperators()).toEqual([]);
    expect(texeraGraph.getAllLinks()).toEqual([]);
    expect(texeraGraph.getAllCommentBoxes()).toEqual([]);
    expect(undoRedoService.getUndoLength()).toBe(undoLengthBefore + 1);

    // a single undo must restore every deleted element at once
    undoRedoService.undoAction();

    expect(texeraGraph.hasOperator(mockScanPredicate.operatorID)).toBe(true);
    expect(texeraGraph.hasOperator(mockSentimentPredicate.operatorID)).toBe(true);
    expect(texeraGraph.hasLinkWithID(mockScanSentimentLink.linkID)).toBe(true);
    expect(texeraGraph.hasCommentBox(commentBox.commentBoxID)).toBe(true);
  });
});
