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

import { ComponentFixture, TestBed, fakeAsync, tick } from "@angular/core/testing";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { By } from "@angular/platform-browser";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { FormlyModule } from "@ngx-formly/core";
import { TEXERA_FORMLY_CONFIG } from "../../../../common/formly/formly-config";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { of } from "rxjs";
import { TimeTravelComponent } from "./time-travel.component";
import { ComputingUnitStatusService } from "../../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import {
  WORKFLOW_EXECUTIONS_API_BASE_URL,
  WorkflowExecutionsService,
} from "../../../../dashboard/service/user/workflow-executions/workflow-executions.service";
import { WorkflowVersionService } from "../../../../dashboard/service/user/workflow-version/workflow-version.service";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { WorkflowExecutionsEntry } from "../../../../dashboard/type/workflow-executions-entry";

function makeExecution(eId: number, logLocation: string | undefined): WorkflowExecutionsEntry {
  return { eId, logLocation } as unknown as WorkflowExecutionsEntry;
}

// A row the template can render: it also needs a vId (passed to onInteractionClick)
// and a startingTime (fed to the date pipe).
function makeRow(eId: number, vId: number): WorkflowExecutionsEntry {
  return { eId, vId, logLocation: "s3://log", startingTime: 1700000000000 } as unknown as WorkflowExecutionsEntry;
}

describe("TimeTravelComponent", () => {
  let component: TimeTravelComponent;
  let fixture: ComponentFixture<TimeTravelComponent>;
  let workflowActionService: WorkflowActionService;
  let metadataSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      providers: [
        WorkflowActionService,
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
        ...commonTestProviders,
      ],
      imports: [
        TimeTravelComponent,
        BrowserAnimationsModule,
        FormsModule,
        FormlyModule.forRoot(TEXERA_FORMLY_CONFIG),
        ReactiveFormsModule,
        HttpClientTestingModule,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(TimeTravelComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.inject(WorkflowActionService);
    // Stub before detectChanges so the ngOnInit timer(0, 5000) poller sees no wid
    // (WorkflowActionService otherwise reports wid 0) and never schedules an
    // executions request in any test. Tests that need a wid override this spy.
    metadataSpy = vi.spyOn(workflowActionService, "getWorkflowMetadata").mockReturnValue(undefined as any);
    fixture.detectChanges();
  });

  afterEach(() => {
    // Destroy the component so @UntilDestroy unsubscribes the ngOnInit timer(0, 5000)
    // poller; otherwise it keeps ticking in real time and could fire a stray request
    // in a later test once the metadata stub is restored.
    fixture.destroy();
    vi.restoreAllMocks();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("extended method coverage", () => {
    let executionsService: WorkflowExecutionsService;
    let versionService: WorkflowVersionService;
    let executeService: ExecuteWorkflowService;
    let notificationService: NotificationService;
    let httpMock: HttpTestingController;

    beforeEach(() => {
      executionsService = TestBed.inject(WorkflowExecutionsService);
      versionService = TestBed.inject(WorkflowVersionService);
      executeService = TestBed.inject(ExecuteWorkflowService);
      notificationService = TestBed.inject(NotificationService);
      httpMock = TestBed.inject(HttpTestingController);
      // metadataSpy (installed in the outer beforeEach) already stubs getWorkflowMetadata
      // to no wid; tests that need one override it below.
    });

    describe("getWid", () => {
      it("returns the current workflow id from the metadata", () => {
        metadataSpy.mockReturnValue({ wid: 42 } as any);
        expect(component.getWid()).toBe(42);
      });

      it("returns undefined when there is no workflow metadata", () => {
        metadataSpy.mockReturnValue(undefined as any);
        expect(component.getWid()).toBeUndefined();
      });
    });

    describe("retrieveLoggedExecutions", () => {
      it("keeps only executions that have a non-empty log location", () => {
        vi.spyOn(executionsService, "retrieveWorkflowExecutions").mockReturnValue(
          of([makeExecution(1, "s3://log"), makeExecution(2, ""), makeExecution(3, undefined)])
        );

        let result: WorkflowExecutionsEntry[] = [];
        component.retrieveLoggedExecutions(1).subscribe(r => (result = r));

        expect(executionsService.retrieveWorkflowExecutions).toHaveBeenCalledWith(1);
        expect(result.map(e => e.eId)).toEqual([1]);
      });
    });

    describe("retrieveInteractionHistory", () => {
      it("GETs the interactions endpoint for the given wid/eid", () => {
        let result: string[] = [];
        component.retrieveInteractionHistory(11, 22).subscribe(r => (result = r));

        const req = httpMock.expectOne(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/11/interactions/22`);
        expect(req.request.method).toBe("GET");
        req.flush(["h1", "h2"]);

        expect(result).toEqual(["h1", "h2"]);
      });
    });

    describe("getInteractionHistory", () => {
      it("does nothing when there is no wid", () => {
        const spy = vi.spyOn(component, "retrieveInteractionHistory");
        component.getInteractionHistory(5);
        expect(spy).not.toHaveBeenCalled();
        expect(component.interactionHistories[5]).toBeUndefined();
      });

      it("stores the fetched interaction history under the eid", () => {
        metadataSpy.mockReturnValue({ wid: 1 } as any);
        vi.spyOn(component, "retrieveInteractionHistory").mockReturnValue(of(["click-a", "click-b"]));

        component.getInteractionHistory(5);

        expect(component.retrieveInteractionHistory).toHaveBeenCalledWith(1, 5);
        expect(component.interactionHistories[5]).toEqual(["click-a", "click-b"]);
      });
    });

    describe("displayExecutionWithLogs", () => {
      it("stores the executions and refreshes the interaction history of expanded rows", () => {
        const executions = [makeExecution(1, "s3://log")];
        vi.spyOn(component, "retrieveLoggedExecutions").mockReturnValue(of(executions));
        const interactionSpy = vi.spyOn(component, "getInteractionHistory").mockImplementation(() => {});
        component.expandedRows = new Set([7]);

        component.displayExecutionWithLogs(3);

        expect(component.executionList).toBe(executions);
        expect(interactionSpy).toHaveBeenCalledWith(7);
      });
    });

    describe("onInteractionClick", () => {
      it("does nothing when there is no wid", () => {
        const spy = vi.spyOn(versionService, "retrieveWorkflowByVersion");
        component.onInteractionClick(2, 3, "x");
        expect(spy).not.toHaveBeenCalled();
        expect(component.revertedToInteraction).toBeUndefined();
      });

      it("loads the versioned workflow, records the reverted interaction, and starts the replay", () => {
        const versionedWorkflow = { wid: 1, name: "v1" } as any;
        metadataSpy.mockReturnValue({ wid: 1 } as any);
        vi.spyOn(versionService, "retrieveWorkflowByVersion").mockReturnValue(of(versionedWorkflow));
        const displaySpy = vi.spyOn(versionService, "displayReadonlyWorkflow").mockImplementation(() => {});
        const infoSpy = vi.spyOn(notificationService, "info").mockImplementation(() => undefined as any);
        const replaySpy = vi.spyOn(executeService, "executeWorkflowWithReplay").mockImplementation(() => {});
        // This test sets revertedToInteraction, so ngOnDestroy (via fixture.destroy in
        // afterEach) would run the real replay-cleanup; keep those calls inert.
        vi.spyOn(versionService, "closeReadonlyWorkflowDisplay").mockImplementation(() => {});
        vi.spyOn(executeService, "killWorkflow").mockImplementation(() => {});

        component.onInteractionClick(2, 3, "click-x");

        expect(versionService.retrieveWorkflowByVersion).toHaveBeenCalledWith(1, 2);
        expect(displaySpy).toHaveBeenCalledWith(versionedWorkflow);
        expect(component.revertedToInteraction).toEqual({ eid: 3, interaction: "click-x" });
        expect(infoSpy).toHaveBeenCalledWith("start replay to interaction click-x at execution 3");
        expect(replaySpy).toHaveBeenCalledWith({ eid: 3, interaction: "click-x" });
      });
    });

    describe("toggleRow", () => {
      it("expands a collapsed row and loads its interaction history", () => {
        const spy = vi.spyOn(component, "getInteractionHistory").mockImplementation(() => {});
        component.toggleRow(9);
        expect(component.expandedRows.has(9)).toBe(true);
        expect(spy).toHaveBeenCalledWith(9);
      });

      it("collapses an expanded row without reloading", () => {
        const spy = vi.spyOn(component, "getInteractionHistory").mockImplementation(() => {});
        component.toggleRow(9); // expand (loads once)
        component.toggleRow(9); // collapse
        expect(component.expandedRows.has(9)).toBe(false);
        expect(spy).toHaveBeenCalledTimes(1);
      });
    });
  });

  /**
   * The panel keeps itself up to date from a `timer(0, 5000)` poller started in ngOnInit.
   * The fixture built by the outer beforeEach cannot be used to drive it: `src/test-zone-setup.ts`
   * installs the ProxyZone that fakeAsync patches around the `it` body only, so anything
   * scheduled from a beforeEach — including the poller that beforeEach's detectChanges starts
   * (inert there, since the metadata stub reports no wid) — lives in the real zone, out of
   * tick()'s reach. These tests therefore re-run ngOnInit from inside the fakeAsync body,
   * which is where timer(0, 5000) has to be created for tick() to drive it, and destroy the
   * fixture at the end so @UntilDestroy unsubscribes the periodic timer.
   */
  describe("ngOnInit polling", () => {
    it("skips the refresh while the workflow has no id", fakeAsync(() => {
      metadataSpy.mockReturnValue(undefined as any);
      const widSpy = vi.spyOn(component, "getWid");
      const displaySpy = vi.spyOn(component, "displayExecutionWithLogs").mockImplementation(() => {});

      component.ngOnInit();
      tick(0); // the first emission of timer(0, 5000) is asynchronous

      // getWid pins that the poller actually ran: without it the negative assertion
      // below would also pass with the poller never firing at all.
      expect(widSpy).toHaveBeenCalledTimes(1);
      expect(displaySpy).not.toHaveBeenCalled();

      component.ngOnDestroy();
    }));

    it("refreshes the execution list immediately and then every five seconds", fakeAsync(() => {
      metadataSpy.mockReturnValue({ wid: 7 } as any);
      const displaySpy = vi.spyOn(component, "displayExecutionWithLogs").mockImplementation(() => {});

      component.ngOnInit();

      tick(0);
      expect(displaySpy).toHaveBeenCalledTimes(1);
      expect(displaySpy).toHaveBeenCalledWith(7);

      // The second advance brackets the period instead of merely clearing it: asserting only
      // "two calls by t=5000" holds for every period <= 5000, so a shortened interval — which
      // multiplies the panel's load on /api/executions — would pass unnoticed.
      tick(4999);
      expect(displaySpy).toHaveBeenCalledTimes(1);
      tick(1);
      expect(displaySpy).toHaveBeenCalledTimes(2);

      fixture.destroy();
    }));

    it("stops polling once the panel is destroyed", fakeAsync(() => {
      metadataSpy.mockReturnValue({ wid: 7 } as any);
      const displaySpy = vi.spyOn(component, "displayExecutionWithLogs").mockImplementation(() => {});

      component.ngOnInit();
      tick(0);
      expect(displaySpy).toHaveBeenCalledTimes(1);

      fixture.destroy();
      tick(5000);

      expect(displaySpy).toHaveBeenCalledTimes(1);
    }));
  });

  describe("template rendering", () => {
    // Query, assert the element is present, then dispatch — a missing selector fails
    // with a clear message instead of a null dereference.
    const fire = (css: string, index: number, event: string, payload: unknown): void => {
      const els = fixture.debugElement.queryAll(By.css(css));
      expect(els.length).toBeGreaterThan(index);
      els[index].triggerEventHandler(event, payload);
    };

    // Rows are only rendered by the *ngFor; the ngOnInit poller stays inert because
    // the outer beforeEach stubs the workflow metadata to no wid.
    const seedRows = (...rows: WorkflowExecutionsEntry[]): void => {
      component.executionList = rows;
      fixture.detectChanges();
    };

    // An execution row is the one with both data cells (eid + starting time). The
    // nz-table empty placeholder and the expanded detail row each use a single
    // colspan cell, so this counts execution rows without depending on nz classes.
    const executionRows = () =>
      fixture.debugElement.queryAll(By.css("tbody tr")).filter(r => r.queryAll(By.css("td")).length === 2);

    it("renders one row per execution with its eid and a starting-time cell", () => {
      seedRows(makeRow(1, 10), makeRow(2, 20));

      const rows = executionRows();
      expect(rows.length).toBe(2);

      const firstCells = rows[0].queryAll(By.css("td"));
      expect(firstCells.length).toBe(2);
      expect(firstCells[0].nativeElement.textContent.trim()).toBe("1");
      // The second cell runs `startingTime | date:'short'`; its exact text is
      // timezone-dependent, so only assert that the pipe rendered something.
      expect(firstCells[1].nativeElement.textContent.trim().length).toBeGreaterThan(0);
    });

    it("renders no body rows when the execution list is empty", () => {
      seedRows();
      expect(executionRows().length).toBe(0);
    });

    it("expands the detail row on a row click and collapses it on a second click", () => {
      seedRows(makeRow(1, 10));

      fire("tbody tr", 0, "click", new MouseEvent("click"));
      fixture.detectChanges();
      expect(component.expandedRows.has(1)).toBe(true);
      // the detail row is a second <tr> rendered under the same *ngFor
      expect(fixture.debugElement.query(By.css(".interaction-container"))).toBeTruthy();

      fire("tbody tr", 0, "click", new MouseEvent("click"));
      fixture.detectChanges();
      expect(component.expandedRows.has(1)).toBe(false);
      expect(executionRows().length).toBe(1);
      expect(fixture.debugElement.query(By.css(".interaction-container"))).toBeNull();
    });

    it("renders one button per interaction in the expanded row", () => {
      component.interactionHistories = { 1: ["click-a", "click-b"] };
      seedRows(makeRow(1, 10));

      fire("tbody tr", 0, "click", new MouseEvent("click"));
      fixture.detectChanges();

      const buttons = fixture.debugElement.queryAll(By.css(".interaction-item"));
      expect(buttons.map(b => b.nativeElement.textContent.trim())).toEqual(["click-a", "click-b"]);
    });

    it("passes the row's vid/eid and the interaction to the click handler", () => {
      component.interactionHistories = { 1: ["click-a", "click-b"] };
      seedRows(makeRow(1, 10));
      fire("tbody tr", 0, "click", new MouseEvent("click"));
      fixture.detectChanges();

      // Spying the handler is what isolates the template binding itself — the
      // handler's own behavior is covered by the onInteractionClick tests above.
      const clickSpy = vi.spyOn(component, "onInteractionClick").mockImplementation(() => {});
      fire(".interaction-item", 1, "click", new MouseEvent("click"));

      expect(clickSpy).toHaveBeenCalledWith(10, 1, "click-b");
    });

    it("disables only the interaction the panel has already reverted to", () => {
      component.interactionHistories = { 1: ["click-a", "click-b"] };
      component.revertedToInteraction = { eid: 1, interaction: "click-b" };
      // revertedToInteraction makes ngOnDestroy (run by fixture.destroy in afterEach)
      // take the replay-cleanup path; keep those service calls inert.
      vi.spyOn(TestBed.inject(WorkflowVersionService), "closeReadonlyWorkflowDisplay").mockImplementation(() => {});
      vi.spyOn(TestBed.inject(ExecuteWorkflowService), "killWorkflow").mockImplementation(() => {});
      seedRows(makeRow(1, 10));
      fire("tbody tr", 0, "click", new MouseEvent("click"));
      fixture.detectChanges();

      const buttons = fixture.debugElement.queryAll(By.css(".interaction-item"));
      expect((buttons[0].nativeElement as HTMLButtonElement).disabled).toBe(false);
      expect((buttons[1].nativeElement as HTMLButtonElement).disabled).toBe(true);
    });
  });
});
