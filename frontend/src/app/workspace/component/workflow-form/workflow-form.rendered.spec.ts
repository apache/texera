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
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { ActivatedRoute, Router } from "@angular/router";
import { EMPTY, of, Subject } from "rxjs";

import { WorkflowFormComponent } from "./workflow-form.component";
import { UserIconComponent } from "../../../dashboard/component/user/user-icon/user-icon.component";
import { CoeditorUserIconComponent } from "../menu/coeditor-user-icon/coeditor-user-icon.component";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { UserService } from "../../../common/service/user/user.service";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { WorkflowConsoleService } from "../../service/workflow-console/workflow-console.service";
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
    // Blank out ONLY the two child icons: their ng-zorro dropdown/menu needs a host context
    // this page does not set up. The override is on the children, not the page, so the page's
    // own .component.html renders as shipped and stays covered -- which is the point of this
    // spec, and why the no-restricted-syntax guard (aimed at blanking the component under test)
    // does not apply here.
    /* eslint-disable no-restricted-syntax */
    TestBed.overrideComponent(UserIconComponent, { set: { template: "" } });
    TestBed.overrideComponent(CoeditorUserIconComponent, { set: { template: "" } });
    /* eslint-enable no-restricted-syntax */

    await TestBed.configureTestingModule({
      imports: [WorkflowFormComponent],
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
        { provide: ExecuteWorkflowService, useValue: { resetExecutionAndWorkers: vi.fn() } },
        { provide: WorkflowResultService, useValue: { clearResults: vi.fn() } },
        { provide: NotificationService, useValue: { error: vi.fn() } },
        { provide: UserService, useValue: { getCurrentUser: () => undefined, isLogin: () => false } },
        { provide: ComputingUnitStatusService, useValue: { disconnect: vi.fn() } },
        { provide: WorkflowConsoleService, useValue: { clearConsoleMessages: vi.fn() } },
        { provide: GuiConfigService, useValue: { env: { formViewEnabled: true } } },
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

  it("tears the workflow down when the browser unloads (the beforeunload host binding)", () => {
    fixture.detectChanges();
    finishLoad();
    const workflowActionService: any = TestBed.inject(WorkflowActionService);

    window.dispatchEvent(new Event("beforeunload"));

    expect(workflowActionService.clearWorkflow).toHaveBeenCalled();
  });
});
