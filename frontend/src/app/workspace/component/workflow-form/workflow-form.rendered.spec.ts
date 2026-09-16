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
import { CdkDropList } from "@angular/cdk/drag-drop";
import { FormGroup } from "@angular/forms";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { By } from "@angular/platform-browser";
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
  LockOutline,
  MinusOutline,
  PlusOutline,
  UpOutline,
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
    // Blank the always-mounted property panel the same way, and switch off its lifecycle hooks: its
    // ngOnInit subscribes to the highlight streams and the panel service, its ngOnChanges remounts
    // the frame, and it has its own spec for all of that. This page only needs the panel present
    // (it lives behind [hidden], not *ngIf) with the three inputs the template binds readable. The
    // real class stays in the page's imports on purpose: swapping it for a stub means overriding the
    // page's imports, which JIT-recompiles the page and drops every line of its template from the
    // coverage report (the .component.html then reads 0%, as it did while a stub was used here).
    TestBed.overrideComponent(PropertyEditorComponent, { set: { template: "" } });
    /* eslint-enable no-restricted-syntax */
    for (const hook of ["ngOnInit", "ngOnChanges", "ngOnDestroy"] as const) {
      vi.spyOn(PropertyEditorComponent.prototype, hook).mockImplementation(() => {});
    }

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
            getWorkflowModificationEnabledStream: () => EMPTY,
            clearWorkflow: vi.fn(),
            getWorkflowMetadata: () => ({ name: "scGPT", lastModifiedTime: undefined }),
            getWorkflow: () => ({ wid: 7, content: { operators: [], operatorPositions: {} } }),
            setWorkflowName: vi.fn(),
            workflowChanged: () => EMPTY,
            workflowMetaDataChanged: () => EMPTY,
            formBindingChanged$: EMPTY,
            setHighlightingEnabled: vi.fn(),
            unhighlightOperators: vi.fn(),
            getTexeraGraph: () => ({
              triggerCenterEvent: vi.fn(),
              hasOperator: () => false,
              getOperator: () => undefined,
              getAllOperators: () => [],
              getAllEnabledLinks: () => [],
              getOperatorsToViewResult: () => new Set<string>(),
              getViewResultOperatorsChangedStream: () => EMPTY,
              getOperatorAddStream: () => EMPTY,
              getOperatorDeleteStream: () => EMPTY,
              getLinkAddStream: () => EMPTY,
              getLinkDeleteStream: () => EMPTY,
              getDisabledOperatorsChangedStream: () => EMPTY,
              getOperatorDisplayNameChangedStream: () => EMPTY,
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
            }),
            resolveFields: () => [],
            readValue: () => undefined,
            writeValue: vi.fn(),
            // Author-mode writes the rendered controls reach.
            updateConfig: vi.fn(),
            toggleShownResult: vi.fn(),
            removeBinding: vi.fn(),
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
            LockOutline,
            MinusOutline,
            PlusOutline,
            UpOutline,
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
  afterEach(() => vi.restoreAllMocks());

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

    // Reading, the whole header row is one toggle button, wired to the body it opens.
    const toggle = el(".instr-toggle") as HTMLButtonElement;
    expect(toggle.getAttribute("aria-controls")).toBe("instr-body");
    expect(el("#instr-body")).not.toBeNull();
    toggle.click();
    expect(fixture.componentInstance.instructionOpen).toBe(false);
    fixture.detectChanges();
    expect(toggle.getAttribute("aria-expanded")).toBe("false");
  });

  it("keeps the author's title input outside the toggle button", async () => {
    fixture.detectChanges();
    finishLoad();
    const c = fixture.componentInstance;
    c.canEdit = true;
    c.authoring = true;
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();

    // An input nested in a button is invalid interactive content; the author's header is a row with
    // the input as a sibling of a chevron button that does the toggling.
    const input = el(".instr-title-input") as HTMLInputElement;
    expect(input).not.toBeNull();
    expect(input.closest("button")).toBeNull();
    const chevron = el(".instr-chev") as HTMLButtonElement;
    expect(chevron.getAttribute("aria-controls")).toBe("instr-body");
    expect(chevron.getAttribute("aria-expanded")).toBe("true");
    chevron.click();
    expect(c.instructionOpen).toBe(false);
  });

  it("offers Edit to a writer only, and flips it to Done with the lede while authoring", () => {
    fixture.detectChanges();
    // A reader (read-only workflow) has no Edit control at all, not a disabled one.
    finishLoad({ name: "scGPT", content: {}, readonly: true });
    const c = fixture.componentInstance;
    expect(c.canEdit).toBe(false);
    expect(el(".author-toggle")).toBeNull();

    c.canEdit = true;
    fixture.detectChanges();
    // Stubbed: the real toggle opens the workflow strip, whose JointJS paper needs layout jsdom
    // lacks. The wiring from the button is what this test is about.
    const toggle = vi.spyOn(c, "toggleAuthoring").mockImplementation(() => {});
    const button = el(".author-toggle") as HTMLButtonElement;
    expect(button.textContent?.trim()).toBe("Edit");
    expect(el(".lede")).toBeNull();
    button.click();
    expect(toggle).toHaveBeenCalledTimes(1);

    c.authoring = true;
    fixture.detectChanges();
    expect((el(".author-toggle") as HTMLButtonElement).textContent?.trim()).toBe("Done");
    expect(el(".lede")).not.toBeNull();
  });

  it("edits the instruction in place while authoring: Write / Preview tabs, heading and body", async () => {
    fixture.detectChanges();
    finishLoad();
    const c = fixture.componentInstance;
    c.canEdit = true;
    c.authoring = true;
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();
    const changed = vi.spyOn(c, "onInstructionChange");
    const mode = vi.spyOn(c, "setInstructionMode");

    // Write is the default: the markdown box is up and the rendered preview is not.
    expect(el("textarea.md-input")).not.toBeNull();
    expect(el("#instr-body .md")).toBeNull();
    const tabs = Array.from(fixture.nativeElement.querySelectorAll(".tabs button")) as HTMLButtonElement[];
    expect(tabs.map(t => t.textContent?.trim())).toEqual(["Write", "Preview"]);
    expect(tabs[0].getAttribute("aria-current")).toBe("true");

    // Typing into the heading or the body saves through onInstructionChange. Both carry a name of
    // their own: a placeholder is gone as soon as there is text, so it is not one.
    const title = el(".instr-title-input") as HTMLInputElement;
    expect(title.getAttribute("aria-label")).toBe("Instruction heading");
    title.value = "Start here";
    title.dispatchEvent(new Event("input"));
    expect(c.instructionTitle).toBe("Start here");
    const body = el("textarea.md-input") as HTMLTextAreaElement;
    expect(body.getAttribute("aria-label")).toBe("Instruction body");
    body.value = "Pick a file.";
    body.dispatchEvent(new Event("input"));
    expect(c.instructionBody).toBe("Pick a file.");
    expect(changed).toHaveBeenCalledTimes(2);

    // Preview swaps the box for the rendered markdown; Write brings it back.
    tabs[1].click();
    expect(mode).toHaveBeenCalledWith("preview");
    await fixture.whenStable();
    fixture.detectChanges();
    expect(el("textarea.md-input")).toBeNull();
    expect(el("#instr-body .md")).not.toBeNull();
    expect(tabs[1].getAttribute("aria-current")).toBe("true");
    tabs[0].click();
    expect(mode).toHaveBeenCalledWith("write");
    fixture.detectChanges();
    expect(el("textarea.md-input")).not.toBeNull();
  });

  it("shows the result picker to everyone with something to choose, and to an author always", () => {
    fixture.detectChanges();
    finishLoad();
    const c = fixture.componentInstance;
    // A reader with nothing to choose from gets no section.
    expect(el(".respick")).toBeNull();

    // A reader with candidates gets the picker, worded as their own view.
    c.resultChoices = [{ operatorID: "last", label: "Limit", shown: true }];
    fixture.detectChanges();
    expect(el(".respick")).not.toBeNull();
    expect(el(".respick p")?.textContent).toContain("only your view");
    expect(el(".respick .pill")?.textContent?.trim()).toBe("Limit");

    // An author always gets it, with the hint on how to add steps, worded as the default for all.
    c.authoring = true;
    c.resultChoices = [];
    fixture.detectChanges();
    expect(el(".respick")).not.toBeNull();
    expect(el(".respick p")?.textContent).toContain("What everyone sees");
    expect(el(".respick .hint")?.textContent).toContain("No steps to choose from yet");

    c.resultChoices = [
      { operatorID: "mid", label: "Filter", shown: false },
      { operatorID: "viz", label: "Chart", shown: true },
    ];
    fixture.detectChanges();
    const toggle = vi.spyOn(c, "onToggleResult").mockImplementation(() => {});
    const pills = Array.from(fixture.nativeElement.querySelectorAll(".respick .pill")) as HTMLButtonElement[];
    expect(pills.map(p => p.textContent?.trim())).toEqual(["Filter", "Chart"]);
    // The pressed state is exposed to assistive tech as well as styled.
    expect(pills.map(p => p.getAttribute("aria-pressed"))).toEqual(["false", "true"]);
    expect(pills[1].classList.contains("on")).toBe(true);
    expect(el(".respick .hint")).toBeNull();
    pills[0].click();
    expect(toggle).toHaveBeenCalledWith(c.resultChoices[0]);

    // A toggle re-reads the config and rebuilds the choices as new objects. The pill just pressed
    // must be the same element afterwards (tracked by step), or the keyboard focus on it is lost.
    pills[0].focus();
    c.resultChoices = [
      { operatorID: "mid", label: "Filter", shown: true },
      { operatorID: "viz", label: "Chart", shown: true },
    ];
    fixture.detectChanges();
    const after = Array.from(fixture.nativeElement.querySelectorAll(".respick .pill")) as HTMLButtonElement[];
    expect(after[0]).toBe(pills[0]);
    expect(document.activeElement).toBe(pills[0]);
    expect(after[0].getAttribute("aria-pressed")).toBe("true");
  });

  it("offers Move up / Move down on an author's cards, named with the input and inert at the ends", () => {
    fixture.detectChanges();
    finishLoad();
    const c = fixture.componentInstance;
    c.canEdit = true;
    c.authoring = true;
    (c as any).parameters = [{ binding: { id: "b1" } }, { binding: { id: "b2" } }];
    c.rendered = [
      {
        resolved: { binding: { id: "b1", displayName: "File", propertyKey: "fileName" }, operatorLabel: "Scan" },
        fields: [],
        form: new FormGroup({}),
        model: {},
      },
      {
        resolved: { binding: { id: "b2", propertyKey: "predicate" }, operatorLabel: "Filter" },
        fields: [],
        form: new FormGroup({}),
        model: {},
      },
    ] as any;
    const move = vi.spyOn(c, "onMoveBinding");
    fixture.detectChanges();

    const buttons = Array.from(fixture.nativeElement.querySelectorAll(".param .move")) as HTMLButtonElement[];
    // Every card has the same two buttons, so each is named with its input (the author's name, else
    // the property key).
    expect(buttons.map(b => b.getAttribute("aria-label"))).toEqual([
      "Move File up",
      "Move File down",
      "Move predicate up",
      "Move predicate down",
    ]);
    // First card cannot move up, last card cannot move down: marked inert for assistive tech, but
    // NOT disabled, so the button that has the focus after a move to the end keeps it.
    expect(buttons.map(b => b.getAttribute("aria-disabled"))).toEqual(["true", null, null, "true"]);
    expect(buttons.map(b => b.disabled)).toEqual([false, false, false, false]);

    // Each button carries its own direction: the first card's Move down, the second card's Move up.
    buttons[1].click();
    expect(move).toHaveBeenCalledWith(c.rendered[0], 1);
    buttons[2].click();
    expect(move).toHaveBeenCalledWith(c.rendered[1], -1);
    // An inert end button still takes the click; the handler moves nothing off the end.
    buttons[0].focus();
    buttons[0].click();
    expect(move).toHaveBeenCalledWith(c.rendered[0], -1);
    expect(document.activeElement).toBe(buttons[0]);
  });

  it("hands the focus to the Inputs heading when the last card is removed", async () => {
    fixture.detectChanges();
    finishLoad();
    const c = fixture.componentInstance;
    c.canEdit = true;
    c.authoring = true;
    c.rendered = [
      {
        resolved: { binding: { id: "b1", displayName: "File", propertyKey: "fileName" }, operatorLabel: "Scan" },
        fields: [],
        form: new FormGroup({}),
        model: {},
      },
    ] as any;
    fixture.detectChanges();

    const remove = el(".param .remove") as HTMLButtonElement;
    expect(remove.getAttribute("aria-label")).toBe("Remove File");
    expect(remove.getAttribute("data-binding")).toBe("b1");
    remove.focus();
    // The real handler runs: the binding is removed and the re-read (resolveFields -> []) takes the
    // card away, so there is no neighbour to hand the focus to.
    remove.click();
    fixture.detectChanges();
    await new Promise(r => setTimeout(r, 10));

    expect(el(".param")).toBeNull();
    expect(document.activeElement).toBe(el(".pc-section-head .label"));
  });

  it("gives an author's card its provenance, drag handle, help-text box and Remove, and drops the reader's help line", () => {
    fixture.detectChanges();
    finishLoad();
    const c = fixture.componentInstance;
    c.canEdit = true;
    c.authoring = true;
    c.rendered = [
      {
        resolved: { binding: { id: "b1", helpText: "Pick a file", propertyKey: "fileName" }, operatorLabel: "Scan" },
        fields: [],
        form: new FormGroup({}),
        model: {},
      },
    ] as any;
    const help = vi.spyOn(c, "onEditHelpText").mockImplementation(() => {});
    const remove = vi.spyOn(c, "onRemoveBinding").mockImplementation(() => {});
    fixture.detectChanges();

    expect(el(".param .grip")).not.toBeNull();
    expect(el(".param .field-help")?.textContent?.trim()).toBe("From Scan");
    // While authoring, the help text is edited in its own box rather than shown as the reader's line.
    expect(el(".param .param-help-text")).toBeNull();
    const box = el(".param .edit input") as HTMLInputElement;
    expect(box.value).toBe("Pick a file");
    box.value = "Pick a CSV";
    box.dispatchEvent(new Event("input"));
    expect(help).toHaveBeenCalledWith(c.rendered[0].resolved, "Pick a CSV");
    expect(el(".param .edit-foot .hint")?.textContent).toContain("Set on Scan: fileName");
    (el(".param .remove") as HTMLButtonElement).click();
    expect(remove).toHaveBeenCalledWith(c.rendered[0].resolved);
  });

  it("renders a broken input as its reason plus Remove: no field, no help box, no provenance", () => {
    fixture.detectChanges();
    finishLoad();
    const c = fixture.componentInstance;
    c.canEdit = true;
    c.authoring = true;
    c.rendered = [
      {
        resolved: { binding: { id: "gone" }, operatorLabel: "gone-op", brokenReason: "This step was removed." },
        fields: [],
        form: new FormGroup({}),
        model: {},
      },
    ] as any;
    fixture.detectChanges();

    expect(el(".param .broken")?.textContent?.trim()).toBe("This step was removed.");
    expect(el(".param form")).toBeNull();
    expect(el(".param .field-help")).toBeNull();
    expect(el(".param .edit input")).toBeNull();
    expect(el(".param .edit-foot .hint")).toBeNull();
    expect(el(".param .remove")).not.toBeNull();
  });

  it("hands a drop on the card list to onDrop", () => {
    fixture.detectChanges();
    finishLoad();
    const c = fixture.componentInstance;
    const drop = vi.spyOn(c, "onDrop").mockImplementation(() => {});

    fixture.debugElement
      .query(By.directive(CdkDropList))
      .triggerEventHandler("cdkDropListDropped", { previousIndex: 1, currentIndex: 0 });

    expect(drop).toHaveBeenCalledWith({ previousIndex: 1, currentIndex: 0 });
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

  it("mounts the step panel from the start, hidden until a step is selected", () => {
    fixture.detectChanges();
    finishLoad();
    const c = fixture.componentInstance;

    // Mounted before anything is selected: the panel opens by REACTING to the highlight stream, so
    // it has to be subscribed already when the click arrives. [hidden] is what keeps it out of
    // sight, and this is the assertion that would fail if it were swapped back to *ngIf.
    expect(el("texera-property-editor")).not.toBeNull();
    expect((el(".panel") as HTMLElement).hidden).toBe(true);

    c.selectedOperatorId = "op-1";
    fixture.detectChanges();

    expect((el(".panel") as HTMLElement).hidden).toBe(false);
    // The close button is a sibling of the panel rather than inside it, so `inert` cannot swallow
    // the one control that has to stay live.
    expect(el("button.panel-close")).not.toBeNull();
  });

  it("mounts that panel read-only: writes off, placement not persisted, content inert", () => {
    fixture.detectChanges();
    finishLoad();
    fixture.componentInstance.selectedOperatorId = "op-1";
    fixture.detectChanges();

    const panel = fixture.debugElement.query(By.directive(PropertyEditorComponent))
      .componentInstance as PropertyEditorComponent;
    // The three bindings that make this an inspect rather than an editor: no writes to the shared
    // workflow, no tick boxes for choosing what to expose, and no claim on the canvas panel's
    // saved geometry.
    expect(panel.actsAsEditor).toBe(false);
    expect(panel.exposeChoosing).toBe(false);
    expect(panel.persistPlacement).toBe(false);
    // inert blocks pointer, keyboard and focus for the whole subtree, which is what covers the
    // controls a disabled FormGroup does not reach (preset save/apply/delete, array add/remove,
    // drag handles).
    expect(el("texera-property-editor")!.hasAttribute("inert")).toBe(true);
    // The panel itself takes the focus instead, so a keyboard reader can still scroll a long panel.
    expect(el(".panel")!.getAttribute("tabindex")).toBe("0");
    expect(el(".panel")!.getAttribute("aria-label")).toBe("Step settings, read-only");
  });

  it("turns that same panel live in edit mode: writes on, tick boxes on, inert off", () => {
    fixture.detectChanges();
    finishLoad();
    const c = fixture.componentInstance;
    c.selectedOperatorId = "op-1";
    c.authoring = true;
    fixture.detectChanges();

    const panel = fixture.debugElement.query(By.directive(PropertyEditorComponent))
      .componentInstance as PropertyEditorComponent;
    // Authoring is a real edit of the shared graph, the same edit the canvas makes, so the panel
    // acts as an editor; and its tick boxes are how the author picks what the form exposes.
    expect(panel.actsAsEditor).toBe(true);
    expect(panel.exposeChoosing).toBe(true);
    // Still not the docked canvas panel, so it still makes no claim on that panel's geometry.
    expect(panel.persistPlacement).toBe(false);
    expect(el("texera-property-editor")!.hasAttribute("inert")).toBe(false);
    // The container's tab stop existed only because inert content cannot hold focus. With the form
    // focusable again it would just sit in front of it, so it goes away with inert.
    expect(el(".panel")!.hasAttribute("tabindex")).toBe(false);
    expect(el(".panel")!.getAttribute("aria-label")).toBe("Step settings");
  });

  it("keeps that panel read-only while a run is in flight, even in edit mode, and turns it live after", () => {
    // The frame does not consult the lock before its own writes (version sync on mount, the schema
    // defaults ajv fills in, the editing marker), so a step selected mid-run must mount as a viewer
    // although edit mode is on; the run ending is what makes it live.
    fixture.detectChanges();
    finishLoad();
    const c = fixture.componentInstance;
    c.selectedOperatorId = "op-1";
    c.authoring = true;
    c.executionState = ExecutionState.Running;
    fixture.detectChanges();

    const panel = fixture.debugElement.query(By.directive(PropertyEditorComponent))
      .componentInstance as PropertyEditorComponent;
    expect(panel.actsAsEditor).toBe(false);
    expect(panel.exposeChoosing).toBe(false);
    expect(el("texera-property-editor")!.hasAttribute("inert")).toBe(true);
    expect(el(".panel")!.getAttribute("aria-label")).toBe("Step settings, read-only");

    c.executionState = ExecutionState.Completed;
    fixture.detectChanges();
    expect(panel.actsAsEditor).toBe(true);
    expect(panel.exposeChoosing).toBe(true);
    expect(el("texera-property-editor")!.hasAttribute("inert")).toBe(false);
  });

  it("tears the workflow down when the browser unloads (the beforeunload host binding)", () => {
    fixture.detectChanges();
    finishLoad();
    const workflowActionService: any = TestBed.inject(WorkflowActionService);

    window.dispatchEvent(new Event("beforeunload"));

    expect(workflowActionService.clearWorkflow).toHaveBeenCalled();
  });

  // The held rebuild is drained by a real blur: a focusout bubbling up from a control inside the
  // page reaches the host listener. Dispatching the DOM event (not calling the handler) is what
  // would catch the listener being removed or miswired.
  it("runs a held rebuild when a control inside the page loses focus (the focusout host binding)", async () => {
    fixture.detectChanges();
    finishLoad();
    const c = fixture.componentInstance;
    const rebuild = vi.spyOn(c as any, "readConfig");
    (c as any).rebuildDeferred = true;

    el("input.wf-name")!.dispatchEvent(new FocusEvent("focusout", { bubbles: true }));
    await new Promise(r => setTimeout(r, 10));

    expect(rebuild).toHaveBeenCalledTimes(1);
    expect((c as any).rebuildDeferred).toBe(false);
  });
});
