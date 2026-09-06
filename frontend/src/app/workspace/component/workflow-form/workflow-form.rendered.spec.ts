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

import { DatePipe } from "@angular/common";
import { Component, Input } from "@angular/core";
import { FormGroup } from "@angular/forms";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { ActivatedRoute, Router } from "@angular/router";
import { FormlyForm, FormlyModule } from "@ngx-formly/core";
import { FormlyJsonschema } from "@ngx-formly/core/json-schema";
import { NZ_ICONS } from "ng-zorro-antd/icon";
import {
  InfoCircleOutline,
  DownOutline,
  PlusCircleOutline,
  CaretRightOutline,
  StopOutline,
  WarningOutline,
  LoadingOutline,
  MinusOutline,
  PlusOutline,
} from "@ant-design/icons-angular/icons";
import { EMPTY, of, Subject } from "rxjs";

import { WorkflowFormComponent } from "./workflow-form.component";
import { UserIconComponent } from "../../../dashboard/component/user/user-icon/user-icon.component";
import { CoeditorUserIconComponent } from "../menu/coeditor-user-icon/coeditor-user-icon.component";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { FormBindingService } from "../../service/form-binding/form-binding.service";
import { DynamicSchemaService } from "../../service/dynamic-schema/dynamic-schema.service";
import { WorkflowCompilingService } from "../../service/compile-workflow/workflow-compiling.service";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { UserService } from "../../../common/service/user/user.service";
import { MarkdownService } from "ngx-markdown";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { WorkflowConsoleService } from "../../service/workflow-console/workflow-console.service";
import { WorkflowWebsocketService } from "../../service/workflow-websocket/workflow-websocket.service";
import { ValidationWorkflowService } from "../../service/validation/validation-workflow.service";
import { ComputingUnitSelectionComponent } from "../power-button/computing-unit-selection.component";
import { PropertyEditorComponent } from "../property-editor/property-editor.component";
import { ResultTableFrameComponent } from "../result-panel/result-table-frame/result-table-frame.component";
import { VisualizationFrameContentComponent } from "../visualization-panel-content/visualization-frame-content.component";
import { PanelResizeService } from "../../service/workflow-result/panel-resize/panel-resize.service";
import { WorkflowComputingUnitManagingService } from "../../../common/service/computing-unit/workflow-computing-unit/workflow-computing-unit-managing.service";
import { WorkflowExecutionsService } from "../../../dashboard/service/user/workflow-executions/workflow-executions.service";
import { ComputingUnitActionsService } from "../../../common/service/computing-unit/computing-unit-actions/computing-unit-actions.service";
import { WorkflowPveService } from "../../service/virtual-environment/virtual-environment.service";
import { NzModalService } from "ng-zorro-antd/modal";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { GuiConfigService } from "../../../common/service/gui-config.service";

/**
 * The direct-construction spec exercises the component's logic without a DOM; this one stands
 * the page's real template up through TestBed so the rendered shell is covered too -- the
 * name/avatar row, the Canvas switch actually firing, the loading/body swap, and the co-editor
 * row -- which is the review's evidence of the rendered page in place of a screenshot.
 */
// A stand-in for the always-mounted property panel. The real one is heavy -- its ngOnInit
// subscribes to the full JointJS highlight-stream set and the panel service -- and it has its
// own spec. This page only needs the panel present (it lives behind [hidden], not *ngIf, so it
// is mounted from the start to catch the highlight that opens it), so swap in a stub carrying the
// two inputs the template binds and nothing else. The swap is on a child of the page, so the
// page's own template still renders as shipped and stays covered.
@Component({ selector: "texera-property-editor", template: "", standalone: true })
class MockPropertyEditorComponent {
  @Input() exposeChoosing = false;
  @Input() persistPlacement = true;
}

describe("WorkflowFormComponent (rendered template)", () => {
  let fixture: ComponentFixture<WorkflowFormComponent>;
  let workflow$: Subject<any>;
  const navigate = vi.fn();

  const configure = async () => {
    workflow$ = new Subject<any>();
    // Blank out ONLY the two child icons: their ng-zorro dropdown/menu needs a host context this
    // page does not set up. The override is on the children, not the page, so the page's own
    // .component.html renders as shipped and stays covered -- which is the point of this spec, and
    // why the no-restricted-syntax guard (aimed at blanking the component under test) does not
    // apply here. The embedded workflow editor / mini-map are never instantiated (they sit behind
    // *ngIf="workflowEverOpened", and these tests never open the strip -- a real JointJS paper
    // needs layout jsdom lacks), so they need no override.
    /* eslint-disable no-restricted-syntax */
    TestBed.overrideComponent(UserIconComponent, { set: { template: "" } });
    TestBed.overrideComponent(CoeditorUserIconComponent, { set: { template: "" } });
    // Blank the formly-form child too: rendering real fields needs the ng-zorro type registry the
    // property panel sets up, which is out of scope here. Blanking the child (not the page) keeps
    // the page's own inputs markup -- the section head, the empty state, the card and the form
    // wrapper -- rendered and covered.
    TestBed.overrideComponent(FormlyForm, { set: { template: "" } });
    // Blank the computing-unit selector's own template (a child, not the page): its real markup
    // needs a modal/executions/PVE service chain out of scope here. Blanking the child -- rather
    // than overriding the page's imports, which would JIT-recompile the page and drop its
    // host-binding coverage -- keeps the run bar around it rendered and the page fully covered.
    TestBed.overrideComponent(ComputingUnitSelectionComponent, { set: { template: "" } });
    // Blank the two result frame children: the real table/visualization need a live result service
    // and (for the chart) an iframe jsdom cannot run. Blanking the children keeps the page's own
    // results markup -- the section, the card, the head, the zoom controls -- rendered and covered.
    TestBed.overrideComponent(ResultTableFrameComponent, { set: { template: "" } });
    TestBed.overrideComponent(VisualizationFrameContentComponent, { set: { template: "" } });
    /* eslint-enable no-restricted-syntax */
    // Swap the real property panel (a heavy child with its own spec) for the stub above. Done by
    // replacing it in the page's imports rather than blanking its template, because the panel's
    // trouble is its ngOnInit -- the highlight-stream and panel-service subscriptions -- which a
    // blanked template still runs; a stub component has neither.
    TestBed.overrideComponent(WorkflowFormComponent, {
      remove: { imports: [PropertyEditorComponent] },
      add: { imports: [MockPropertyEditorComponent] },
    });

    await TestBed.configureTestingModule({
      // forRoot registers the FormlyConfig the form builder needs: the page imports FormlyModule
      // (standalone) but the root config lives with the app; supply it here so the blanked
      // formly-form still builds instead of throwing "missing forRoot()".
      imports: [WorkflowFormComponent, FormlyModule.forRoot()],
      providers: [
        // One co-editor so the collaborator row (the *ngFor) renders and is covered.
        {
          provide: CoeditorPresenceService,
          useValue: { coeditors: [{ clientId: "c1", userName: "co", color: "#888" }] },
        },
        { provide: ActivatedRoute, useValue: { snapshot: { params: { id: "7" } } } },
        { provide: Router, useValue: { navigate } },
        {
          provide: WorkflowActionService,
          useValue: {
            resetAsNewWorkflow: vi.fn(),
            setNewSharedModel: vi.fn(),
            reloadWorkflow: vi.fn(),
            disableWorkflowModification: vi.fn(),
            clearWorkflow: vi.fn(),
            getWorkflowMetadata: () => ({ name: "scGPT", lastModifiedTime: undefined }),
            getWorkflow: () => ({ wid: 7, content: { operators: [], operatorPositions: {} } }),
            setWorkflowName: vi.fn(),
            workflowChanged: () => EMPTY,
            workflowMetaDataChanged: () => EMPTY,
            formBindingChanged$: EMPTY,
            setHighlightingEnabled: vi.fn(),
            getTexeraGraph: () => ({
              triggerCenterEvent: vi.fn(),
              hasOperator: () => false,
              getOperator: () => undefined,
              getAllOperators: () => [],
              getOperatorsToViewResult: () => new Set<string>(),
              updateSharedModelAwareness: vi.fn(),
            }),
            getJointGraphWrapper: () => ({
              getJointOperatorHighlightStream: () => EMPTY,
              getJointOperatorUnhighlightStream: () => EMPTY,
              getCurrentHighlightedOperatorIDs: () => [],
              unhighlightOperators: vi.fn(),
            }),
          },
        },
        {
          provide: WorkflowPersistService,
          useValue: {
            retrieveWorkflow: () => workflow$,
            isWorkflowPersistEnabled: () => false,
            persistWorkflow: () => of({}),
          },
        },
        { provide: OperatorMetadataService, useValue: { getOperatorMetadata: () => of({}) } },
        {
          provide: FormBindingService,
          useValue: {
            // An instruction so the instruction card renders and is covered.
            getConfig: () => ({
              instruction: { title: "How to use this", body: "Fill in the inputs." },
              fields: [],
              resultOperatorIds: [],
            }),
            resolveFields: () => [],
            readValue: () => undefined,
            writeValue: vi.fn(),
          },
        },
        { provide: FormlyJsonschema, useValue: { toFieldConfig: () => ({ fieldGroup: [] }) } },
        { provide: DynamicSchemaService, useValue: { getDynamicSchema: () => ({ jsonSchema: {} }) } },
        {
          provide: WorkflowCompilingService,
          useValue: { getCompilationStateInfoChangedStream: () => EMPTY },
        },
        {
          provide: ExecuteWorkflowService,
          useValue: {
            getExecutionStateStream: () => EMPTY,
            executeWorkflow: vi.fn(),
            killWorkflow: vi.fn(),
            resetExecutionAndWorkers: vi.fn(),
          },
        },
        {
          provide: WorkflowResultService,
          useValue: {
            clearResults: vi.fn(),
            getResultUpdateStream: () => EMPTY,
            hasNonEmptyResult: () => false,
            hasAnyResult: () => false,
            hasPaginatedResult: () => false,
            getResultService: () => undefined,
          },
        },
        { provide: PanelResizeService, useValue: { changePanelSize: vi.fn() } },
        { provide: NotificationService, useValue: { error: vi.fn() } },
        { provide: UserService, useValue: { getCurrentUser: () => undefined, isLogin: () => false } },
        { provide: MarkdownService, useValue: { parse: (s: string) => s } },
        {
          provide: ComputingUnitStatusService,
          useValue: {
            disconnect: vi.fn(),
            getSelectedComputingUnit: () => EMPTY,
            getStatus: () => EMPTY,
            // Read by the (blanked) computing-unit selector's own ngOnInit.
            getAllComputingUnits: () => EMPTY,
          },
        },
        // The blanked computing-unit selector still constructs and runs ngOnInit; give it the few
        // services it reads so it does not throw. It renders nothing (its template is blanked).
        { provide: WorkflowComputingUnitManagingService, useValue: { getComputingUnitLimitOptions: () => EMPTY } },
        { provide: WorkflowExecutionsService, useValue: {} },
        { provide: ComputingUnitActionsService, useValue: {} },
        { provide: WorkflowPveService, useValue: {} },
        { provide: NzModalService, useValue: {} },
        { provide: WorkflowConsoleService, useValue: { clearConsoleMessages: vi.fn() } },
        {
          provide: WorkflowWebsocketService,
          useValue: { subscribeToEvent: () => EMPTY, isConnected: true, getConnectionStatusStream: () => EMPTY },
        },
        { provide: ValidationWorkflowService, useValue: { getWorkflowValidationErrorStream: () => EMPTY } },
        { provide: GuiConfigService, useValue: { env: { formViewEnabled: true } } },
        // Register the icons the run bar and instruction use, so nz-icon renders them inline instead
        // of fetching each SVG over HTTP (an unresolved fetch that would hang fixture.whenStable).
        {
          provide: NZ_ICONS,
          useValue: [
            InfoCircleOutline,
            DownOutline,
            PlusCircleOutline,
            CaretRightOutline,
            StopOutline,
            WarningOutline,
            LoadingOutline,
            MinusOutline,
            PlusOutline,
          ],
        },
        DatePipe,
      ],
    }).compileComponents();
    fixture = TestBed.createComponent(WorkflowFormComponent);
  };

  const el = (sel: string): HTMLElement | null => fixture.nativeElement.querySelector(sel);
  const finishLoad = (workflow: any = { name: "scGPT", content: {} }) => {
    workflow$.next(workflow);
    workflow$.complete();
    fixture.detectChanges();
  };

  beforeEach(configure);

  it("renders the workflow's avatar and name in the title row", async () => {
    fixture.detectChanges(); // ngOnInit -> load()
    finishLoad();
    // ngModel writes the name into the input on a microtask; let it flush before reading.
    await fixture.whenStable();
    fixture.detectChanges();

    expect(el(".pc-topbar")).not.toBeNull();
    expect(el("nz-avatar.wid")).not.toBeNull();
    // The name is an editable input on this slice; its value is the workflow name.
    expect((el("input.wf-name") as HTMLInputElement | null)?.value).toBe("scGPT");
  });

  it("renames the workflow when the name input fires a change", async () => {
    fixture.detectChanges();
    finishLoad();
    await fixture.whenStable();
    const spy = vi.spyOn(fixture.componentInstance, "onRenameWorkflow");
    const input = el("input.wf-name") as HTMLInputElement;

    input.value = "Renamed";
    input.dispatchEvent(new Event("change"));

    expect(spy).toHaveBeenCalled();
  });

  it("switches to the operator canvas when the Canvas control is clicked", () => {
    fixture.detectChanges();
    finishLoad();
    const spy = vi.spyOn(fixture.componentInstance, "openRegularCanvas").mockImplementation(() => {});

    el(".view-switch button")!.click(); // the first button is Canvas

    expect(spy).toHaveBeenCalled();
  });

  it("shows the loading state until the workflow arrives, then swaps to the body", () => {
    fixture.detectChanges(); // load() started; workflow not yet emitted

    expect(el(".pc-loading")?.textContent?.trim()).toBe("Loading…");

    finishLoad();

    expect(el(".pc-loading")).toBeNull();
  });

  it("toggles the workflow preview when its bar is clicked", () => {
    fixture.detectChanges();
    finishLoad();
    // Spied so the click only exercises the template binding, without building the JointJS canvas.
    const spy = vi.spyOn(fixture.componentInstance, "toggleWorkflow").mockImplementation(() => {});

    el(".wf-bar")!.click();

    expect(spy).toHaveBeenCalled();
  });

  it("shows the empty state when there are no inputs to fill in", () => {
    fixture.detectChanges();
    finishLoad();

    expect(el(".pc-section-head .label")?.textContent?.trim()).toBe("Inputs");
    expect(el(".empty")).not.toBeNull();
    expect(el(".params .param")).toBeNull();
  });

  it("renders an exposed input as a card holding its formly field", () => {
    fixture.detectChanges();
    finishLoad();
    const c = fixture.componentInstance;
    // One resolved input with a field; the formly-form child is blanked, so this covers the page's
    // own card + form wrapper markup without standing up the field registry. `parameters` is
    // internal (drives the empty-state getter), reached here through a cast.
    (c as any).parameters = [{ binding: { id: "b1" } }];
    c.rendered = [
      { resolved: { binding: { id: "b1" } }, fields: [{ key: "b1" }], form: new FormGroup({}), model: {} },
    ] as any;
    fixture.detectChanges();

    expect(el(".empty")).toBeNull();
    expect(el(".params .param")).not.toBeNull();
    expect(el(".param .param-form formly-form")).not.toBeNull();
  });

  it("shows the author's help text under an input and locks a read-only viewer's card", () => {
    fixture.detectChanges();
    finishLoad();
    const c = fixture.componentInstance;
    c.canEdit = false;
    (c as any).parameters = [{ binding: { id: "b1" } }];
    c.rendered = [
      {
        resolved: { binding: { id: "b1", helpText: "Pick a small model." } },
        fields: [{ key: "b1" }],
        form: new FormGroup({}),
        model: {},
      },
    ] as any;
    fixture.detectChanges();

    expect(el(".param .param-help-text")?.textContent?.trim()).toBe("Pick a small model.");
    // A read-only viewer's card blocks pointer interaction (covers the extra widget buttons too).
    expect(el(".param.read-only")).not.toBeNull();
  });

  it("renders the author's instruction card and toggles it", async () => {
    fixture.detectChanges();
    finishLoad();
    // renderInstruction resolves the markdown on a microtask.
    await fixture.whenStable();
    fixture.detectChanges();

    expect(el(".card.instr")).not.toBeNull();
    expect(el(".instr .instr-bar h2")?.textContent?.trim()).toBe("How to use this");
    expect(el(".instr .md")?.innerHTML).toContain("Fill in the inputs.");

    (el(".instr-bar") as HTMLButtonElement).click();
    expect(fixture.componentInstance.instructionOpen).toBe(false);
  });

  it("renders the run bar with the run button and the computing-unit selector", () => {
    fixture.detectChanges();
    finishLoad();

    expect(el(".runbar .run")).not.toBeNull();
    // Default state: no unit chosen, so the button reads Connect and is disabled.
    expect(el(".runbar .run")?.textContent?.trim()).toContain("Connect");
    expect((el(".runbar .run") as HTMLButtonElement).disabled).toBe(true);
    expect(el(".runbar texera-computing-unit-selection")).not.toBeNull();
    // At rest there is nothing to count and no run note.
    expect(el(".run-clock")).toBeNull();
    expect(el(".run-note")).toBeNull();
  });

  it("fires onRun when the enabled run button is clicked", () => {
    fixture.detectChanges();
    finishLoad();
    // A running state makes the button "Stop" (enabled); a disabled button would swallow the click.
    fixture.componentInstance.executionState = ExecutionState.Running;
    fixture.detectChanges();
    const run = vi.spyOn(fixture.componentInstance, "onRun").mockImplementation(() => {});

    el(".runbar .run")!.click();

    expect(run).toHaveBeenCalled();
  });

  it("announces a run failure as an alert and a running note as a status", () => {
    fixture.detectChanges();
    finishLoad();
    const c = fixture.componentInstance;

    c.runError = "Run failed: boom";
    fixture.detectChanges();
    expect(el(".run-note")?.getAttribute("role")).toBe("alert");

    c.runError = "";
    c.executionState = ExecutionState.Running;
    fixture.detectChanges();
    expect(el(".run-note")?.getAttribute("role")).toBe("status");
  });

  it("renders the results section: empty state, then a result card for a produced step", () => {
    fixture.detectChanges();
    finishLoad();
    const c = fixture.componentInstance;

    // Before any result: the section shows its quiet empty line, no cards.
    expect(el(".results .label")?.textContent?.trim()).toBe("Results");
    expect(el(".results-empty")).not.toBeNull();
    expect(el(".result")).toBeNull();

    // A chosen step reports a non-empty result: a card appears. Kept in the neutral "no result
    // yet" switch branch (not tabular, no snapshot) so the heavy table/visualization children --
    // which their own specs cover, and which drag in a websocket/status chain jsdom cannot run --
    // are not instantiated here; this test covers the page's own card + head markup.
    const wrs: any = TestBed.inject(WorkflowResultService);
    wrs.hasNonEmptyResult = () => true;
    c.shownResultIds = ["op-1"];
    fixture.detectChanges();

    expect(el(".results-empty")).toBeNull();
    expect(el(".result .result-head")).not.toBeNull();
    expect(el(".result .result-body")).not.toBeNull();
  });

  it("tears the workflow down when the browser unloads (the beforeunload host binding)", () => {
    fixture.detectChanges();
    finishLoad();
    const workflowActionService: any = TestBed.inject(WorkflowActionService);

    window.dispatchEvent(new Event("beforeunload"));

    expect(workflowActionService.clearWorkflow).toHaveBeenCalled();
  });
});
