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

import { FormArray, FormControl, FormGroup, Validators } from "@angular/forms";
import { Router } from "@angular/router";
import { of, Subject, throwError } from "rxjs";
import { Workflow } from "../../../common/type/workflow";

import { WorkflowFormComponent } from "./workflow-form.component";
import { setupHarness, formViewWorkflow, resolved } from "./workflow-form.spec-harness";
import { USER_WORKFLOW, USER_WORKSPACE } from "../../../app-routing.constant";
import { DefaultView } from "../../../dashboard/type/workflow-metadata.interface";
import { FORM_DEBOUNCE_TIME_MS } from "../../service/execute-workflow/execute-workflow.service";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { ComputingUnitState } from "../../../common/type/computing-unit-connection.interface";

/**
 * These exercise the page's own decisions -- what a reader is shown, where an ordinary
 * workflow is sent, and how the title bar renames and saves -- without standing up the JointJS
 * canvas. The component is built directly (not through TestBed) with the shared spec harness's
 * mocks; the read-only preview, inputs, running and results are added, with their own tests, by
 * later PRs.
 */
describe("WorkflowFormComponent", () => {
  let component: WorkflowFormComponent;
  let h: ReturnType<typeof setupHarness>;
  let router: { navigate: ReturnType<typeof vi.fn> };
  let workflowActionService: any;
  let workflowPersistService: any;
  let formBindingService: any;

  const build = (workflow: any) => {
    h.useWorkflow(workflow);
    component = new WorkflowFormComponent(
      h.coeditorPresenceService as any,
      h.route as any,
      h.router as unknown as Router,
      h.workflowActionService as any,
      h.workflowPersistService as any,
      h.operatorMetadataService as any,
      h.formBindingService as any,
      h.executeWorkflowService as any,
      h.workflowResultService as any,
      h.notificationService as any,
      h.userService as any,
      h.markdownService as any,
      h.formlyJsonschema as any,
      h.cdr as any,
      h.dynamicSchemaService as any,
      h.workflowCompilingService as any,
      h.computingUnitStatusService as any,
      h.workflowConsoleService as any,
      h.workflowWebsocketService as any,
      h.host as any,
      h.datePipe as any,
      h.panelResizeService as any,
      h.validationWorkflowService as any,
      h.config as any
    );
    return component;
  };

  beforeEach(() => {
    h = setupHarness();
    router = h.router;
    workflowActionService = h.workflowActionService;
    workflowPersistService = h.workflowPersistService;
    formBindingService = h.formBindingService;
  });

  describe("who this page is for", () => {
    it("opens the form for a workflow that opens in it", () => {
      build(formViewWorkflow).ngOnInit();

      expect(component.wid).toBe(7);
      expect(component.workflowName).toBe("scGPT");
      expect(component.loading).toBe(false);
      expect(router.navigate).not.toHaveBeenCalled();
    });

    // A bad URL id should not try to load anything.
    it("goes back to the workflow list when the URL carries no valid id", () => {
      h.route.snapshot.params.id = "not-a-number";

      build(formViewWorkflow).ngOnInit();

      expect(router.navigate).toHaveBeenCalledWith([USER_WORKFLOW]);
      expect(workflowActionService.reloadWorkflow).not.toHaveBeenCalled();
    });

    // The flag, not the workflow, gates the form: with it on, the form renders for any
    // workflow -- default_view only picks the landing view (settled on #8011), so a
    // canvas-default workflow opens here too rather than being bounced to the canvas.
    it("renders the form for any workflow while the flag is on, whatever its default view", () => {
      build({ ...formViewWorkflow, defaultView: DefaultView.CANVAS }).ngOnInit();

      expect(router.navigate).not.toHaveBeenCalled();
      expect(workflowActionService.reloadWorkflow).toHaveBeenCalled();
      expect(component.loading).toBe(false);
    });

    // With the feature turned off, the form does not exist at all -- even for a form-default
    // workflow, the page hands over to the canvas without loading anything, so a failing
    // request cannot strand the visitor on an error instead.
    it("hands over to the canvas when the feature flag is off, without loading", () => {
      h.config.env.formViewEnabled = false;

      build(formViewWorkflow).ngOnInit();

      expect(router.navigate).toHaveBeenCalledWith([USER_WORKSPACE, "7"], { replaceUrl: true });
      expect(workflowPersistService.retrieveWorkflow).not.toHaveBeenCalled();
      expect(workflowActionService.resetAsNewWorkflow).not.toHaveBeenCalled();
    });

    it("shows the workflow read-only, since editing belongs to the other view", () => {
      build(formViewWorkflow).ngOnInit();

      expect(workflowActionService.disableWorkflowModification).toHaveBeenCalled();
      expect(workflowActionService.enableWorkflowModification).not.toHaveBeenCalled();
      expect(workflowActionService.setNewSharedModel).toHaveBeenCalled();
      expect(workflowActionService.reloadWorkflow).toHaveBeenCalled();
    });

    /** The clamp runs a microtask after the unlock emission (observeOn asap); let that microtask run. */
    const afterTheUnlockingCall = async () => {
      await Promise.resolve();
      await Promise.resolve();
    };

    it("puts the lock back whenever something else unlocks the graph while the page is not in edit mode", async () => {
      // The lock is a root-level flag with writers that know nothing of this page: the execute service
      // unlocks it when a run ends, the computing-unit selector when it finds no run on the chosen
      // unit. A writer merely viewing must stay locked, or the preview's view-result command would
      // write the shared graph without them ever entering edit mode.
      build(formViewWorkflow).ngOnInit();
      const before = workflowActionService.disableWorkflowModification.mock.calls.length;

      h.modificationEnabled.next(true);

      // Not inside the unlocking call: enableWorkflowModification enables undo/redo after it emits and
      // the stream still has other subscribers to reach, so a nested disable would leave them on the
      // stale "true". The clamp waits until that call has finished, then has the last word.
      expect(workflowActionService.disableWorkflowModification.mock.calls.length).toBe(before);
      await afterTheUnlockingCall();
      expect(workflowActionService.disableWorkflowModification.mock.calls.length).toBe(before + 1);
      expect(workflowActionService.enableWorkflowModification).not.toHaveBeenCalled();
    });

    it("ends up unlocked in edit mode once the run has ended, in the execute service's real order", async () => {
      // In edit mode the canvas rule applies unchanged: the graph is unlocked once the run ends. The
      // execute service flips the lock BEFORE it emits the new state; by the time the clamp looks, the
      // state handler has re-applied the rule with the final state, so the unlock stands.
      build(formViewWorkflow).ngOnInit();
      component.authoring = true;
      h.executionStateStream.next({ current: { state: ExecutionState.Running } });
      workflowActionService.enableWorkflowModification.mockClear();
      const before = workflowActionService.disableWorkflowModification.mock.calls.length;

      h.modificationEnabled.next(true); // the execute service unlocks first...
      h.executionStateStream.next({ current: { state: ExecutionState.Completed } }); // ...then emits
      await afterTheUnlockingCall();

      expect(workflowActionService.enableWorkflowModification).toHaveBeenCalled();
      expect(workflowActionService.disableWorkflowModification.mock.calls.length).toBe(before);
    });

    it("keeps the graph locked when edit mode is entered mid-run, until the run ends", async () => {
      // The canvas locks the graph while a run is in flight; entering edit mode must not undo that
      // (the live panel and the view-result command would otherwise edit a running workflow). An
      // unlock arriving mid-run is clamped; once the run ends the page unlocks.
      build(formViewWorkflow).ngOnInit();
      h.executionStateStream.next({ current: { state: ExecutionState.Running } });

      component.toggleAuthoring();

      expect(component.authoring).toBe(true);
      expect(workflowActionService.enableWorkflowModification).not.toHaveBeenCalled();
      const before = workflowActionService.disableWorkflowModification.mock.calls.length;
      h.modificationEnabled.next(true); // something unlocks while the run is still in flight
      await afterTheUnlockingCall();
      expect(workflowActionService.disableWorkflowModification.mock.calls.length).toBe(before + 1);
      expect(workflowActionService.enableWorkflowModification).not.toHaveBeenCalled();
      h.executionStateStream.next({ current: { state: ExecutionState.Completed } });
      expect(workflowActionService.enableWorkflowModification).toHaveBeenCalledTimes(1);
    });

    it("closes the step panel on Done while the frame is still an editor, so the editing marker is cleared", () => {
      // The property editor clears the "currently editing" marker only while it acts as an editor;
      // remounting it as a viewer clears nothing. So Done dismisses the panel first, then leaves
      // edit mode, and co-editors stop seeing this session as editing the step.
      build(formViewWorkflow).ngOnInit();
      component.toggleAuthoring();
      component.selectedOperatorId = "op-1";
      h.highlightedIds.push("op-1");
      let authoringWhenDismissed: boolean | undefined;
      workflowActionService.unhighlightOperators.mockImplementationOnce(() => {
        authoringWhenDismissed = component.authoring;
      });

      component.toggleAuthoring();

      expect(workflowActionService.unhighlightOperators).toHaveBeenCalledWith("op-1");
      expect(authoringWhenDismissed).toBe(true);
      expect(component.selectedOperatorId).toBeUndefined();
      expect(component.authoring).toBe(false);
    });

    it("goes back to the list when the workflow cannot be opened", () => {
      build(formViewWorkflow);
      workflowPersistService.retrieveWorkflow.mockReturnValue(throwError(() => new Error("denied")));

      component.ngOnInit();

      expect(h.notificationService.error).toHaveBeenCalled();
      expect(router.navigate).toHaveBeenCalledWith([USER_WORKFLOW]);
    });

    // Write access decides whether a filled-in value writes back and whether the page saves.
    it("has write access for a writable workflow and none for a read-only one", () => {
      build(formViewWorkflow).ngOnInit();
      expect(component.canEdit).toBe(true);

      build({ ...formViewWorkflow, readonly: true }).ngOnInit();
      expect(component.canEdit).toBe(false);
    });
  });

  describe("leaving the page", () => {
    // Both views drive the same singleton services, so the page must release them on the way
    // out or they follow the user to the next page.
    it("releases the shared services on destroy", () => {
      build(formViewWorkflow).ngOnInit();

      component.ngOnDestroy();

      expect(workflowActionService.clearWorkflow).toHaveBeenCalled();
      expect(h.computingUnitStatusService.disconnect).toHaveBeenCalled();
      expect(h.executeWorkflowService.resetExecutionAndWorkers).toHaveBeenCalled();
      expect(h.workflowConsoleService.clearConsoleMessages).toHaveBeenCalled();
      expect(h.workflowResultService.clearResults).toHaveBeenCalled();
    });
  });

  describe("title bar and saving", () => {
    const enableSave = () => {
      h.userService.isLogin.mockReturnValue(true);
      h.workflowPersistService.isWorkflowPersistEnabled.mockReturnValue(true);
    };

    it("shows the last-saved time from the workflow's metadata", () => {
      build(formViewWorkflow).ngOnInit();

      expect(component.autoSaveState).toBe("Saved at 01/01/2026 00:00:00");
    });

    it("shows no saved state when the workflow has never been saved", () => {
      workflowActionService.getWorkflowMetadata = () => ({ name: "x", lastModifiedTime: undefined });

      build(formViewWorkflow).ngOnInit();

      expect(component.autoSaveState).toBe("");
    });

    it("renames through the workflow action service and saves", () => {
      enableSave();
      build(formViewWorkflow).ngOnInit();
      component.workflowName = "New name";

      component.onRenameWorkflow();

      expect(workflowActionService.setWorkflowName).toHaveBeenCalledWith("New name");
      expect(workflowPersistService.persistWorkflow).toHaveBeenCalled();
    });

    // The title bar is refreshed from one place: a rename or save -- here or by a co-editor --
    // updates the shown name and the saved-at state, so the two views never drift apart. This
    // is also where onRenameWorkflow's normalised name is read back.
    it("follows the workflow metadata: refreshes the name and saved state when it changes", () => {
      vi.useFakeTimers();
      build(formViewWorkflow).ngOnInit();
      component.workflowName = "stale";
      workflowActionService.getWorkflowMetadata = () => ({ name: "Renamed", lastModifiedTime: 1767225600000 });

      h.workflowMetaDataChangedStream.next(undefined);
      vi.runAllTimers();

      expect(component.workflowName).toBe("Renamed");
      expect(component.autoSaveState).toBe("Saved at 01/01/2026 00:00:00");
      vi.useRealTimers();
    });

    it("persists the workflow, filling in a position for every operator", () => {
      enableSave();
      workflowActionService.getWorkflow.mockReturnValue({
        wid: 7,
        content: {
          operators: [{ operatorID: "op-1" }, { operatorID: "op-2" }],
          operatorPositions: { "op-1": { x: 5, y: 6 } },
        },
      });
      build(formViewWorkflow).ngOnInit();

      (component as any).save();

      const saved = workflowPersistService.persistWorkflow.mock.calls.at(-1)[0];
      expect(saved.content.operatorPositions).toEqual({ "op-1": { x: 5, y: 6 }, "op-2": { x: 0, y: 0 } });
    });

    // The graph is read-only here, but a co-editor can still move operators on the canvas; a save
    // must carry those live positions, not revert them to where they sat when this page opened.
    it("saves the live positions, not the load-time snapshot", () => {
      enableSave();
      build({ ...formViewWorkflow, content: { operatorPositions: { "op-1": { x: 1, y: 1 } } } }).ngOnInit();
      // a co-editor has since dragged op-1; the shared graph reflects the new spot
      workflowActionService.getWorkflow.mockReturnValue({
        wid: 7,
        content: { operators: [{ operatorID: "op-1" }], operatorPositions: { "op-1": { x: 9, y: 9 } } },
      });

      (component as any).save();

      const saved = workflowPersistService.persistWorkflow.mock.calls.at(-1)[0];
      expect(saved.content.operatorPositions).toEqual({ "op-1": { x: 9, y: 9 } });
    });

    // The canvas advances "Saved at ..." by feeding the persist response back into the metadata;
    // the form must do the same, or the saved-at state never moves past the moment it opened.
    it("feeds the persist response back into the workflow metadata", () => {
      enableSave();
      build(formViewWorkflow).ngOnInit();
      const updated = { wid: 7, name: "scGPT", lastModifiedTime: 999, content: {} };
      workflowPersistService.persistWorkflow.mockReturnValue(of(updated));

      (component as any).save();

      expect(workflowActionService.setWorkflowMetadata).toHaveBeenCalledWith(updated);
    });

    it("does not save when the user is not logged in", () => {
      build(formViewWorkflow).ngOnInit();
      workflowPersistService.persistWorkflow.mockClear();

      (component as any).save();

      expect(workflowPersistService.persistWorkflow).not.toHaveBeenCalled();
    });

    it("does not save when persistence is disabled", () => {
      h.userService.isLogin.mockReturnValue(true);
      build(formViewWorkflow).ngOnInit();
      workflowPersistService.persistWorkflow.mockClear();

      (component as any).save();

      expect(workflowPersistService.persistWorkflow).not.toHaveBeenCalled();
    });

    it("does not save when the viewer only has read access", () => {
      h.userService.isLogin.mockReturnValue(true);
      h.workflowPersistService.isWorkflowPersistEnabled.mockReturnValue(true);
      build({ ...formViewWorkflow, readonly: true }).ngOnInit();
      workflowPersistService.persistWorkflow.mockClear();

      (component as any).save();

      expect(workflowPersistService.persistWorkflow).not.toHaveBeenCalled();
    });

    it("does not save a workflow that is not the one this page opened", () => {
      enableSave();
      workflowActionService.getWorkflow.mockReturnValue({ wid: 99, content: { operators: [], operatorPositions: {} } });
      build(formViewWorkflow).ngOnInit();
      workflowPersistService.persistWorkflow.mockClear();

      (component as any).save();

      expect(workflowPersistService.persistWorkflow).not.toHaveBeenCalled();
    });

    it("reports a failed save so a lost edit is not silent", () => {
      enableSave();
      build(formViewWorkflow).ngOnInit();
      // set after build(): build()'s useWorkflow() resets the persist mock
      workflowPersistService.persistWorkflow.mockReturnValue(throwError(() => new Error("no")));

      (component as any).save();

      expect(h.notificationService.error).toHaveBeenCalled();
    });

    it("saves on any workflow change, debounced", () => {
      vi.useFakeTimers();
      enableSave();
      build(formViewWorkflow).ngOnInit();
      workflowPersistService.persistWorkflow.mockClear();

      h.workflowChangedStream.next(undefined);
      vi.runAllTimers();

      expect(workflowPersistService.persistWorkflow).toHaveBeenCalled();
      vi.useRealTimers();
    });

    it("saves, then hands over to the operator canvas only once the save has completed", () => {
      enableSave();
      build(formViewWorkflow).ngOnInit();
      workflowPersistService.persistWorkflow.mockClear();
      const navigate = vi.spyOn(component as any, "openCanvasPage").mockImplementation(() => {});

      component.openRegularCanvas();

      // The full-page load aborts a request still in flight, so the navigation waits for the save
      // to complete (the persist mock completes synchronously here).
      expect(workflowPersistService.persistWorkflow).toHaveBeenCalled();
      expect(navigate).toHaveBeenCalledTimes(1);
    });

    it("sends the switch's save only after an autosave already in flight, and navigates after both", () => {
      // Two persists in flight at once can land out of order and the older content would win. The
      // queue holds the switch's save until the autosave has completed, snapshots the workflow then,
      // and hands over only once that later save has completed too.
      vi.useFakeTimers();
      enableSave();
      build(formViewWorkflow).ngOnInit();
      workflowPersistService.persistWorkflow.mockClear();
      const autosave$ = new Subject<Workflow>();
      const switchSave$ = new Subject<Workflow>();
      workflowPersistService.persistWorkflow.mockReturnValueOnce(autosave$).mockReturnValueOnce(switchSave$);
      const navigate = vi.spyOn(component as any, "openCanvasPage").mockImplementation(() => {});

      h.workflowChangedStream.next(undefined);
      vi.runAllTimers();
      expect(workflowPersistService.persistWorkflow).toHaveBeenCalledTimes(1); // the autosave, in flight

      component.openRegularCanvas();
      expect(workflowPersistService.persistWorkflow).toHaveBeenCalledTimes(1); // the switch's save waits
      expect(navigate).not.toHaveBeenCalled();

      autosave$.complete();
      expect(workflowPersistService.persistWorkflow).toHaveBeenCalledTimes(2); // now it goes out
      expect(navigate).not.toHaveBeenCalled();

      switchSave$.complete();
      expect(navigate).toHaveBeenCalledTimes(1);
      vi.useRealTimers();
    });

    it("does not let an older save's response undo a rename made while it was in flight", () => {
      // Save A carries the old name. The author renames to B (B's own save is queued behind A). When
      // A returns, its echoed name must not be written back over B, or an autosave in that window
      // would carry the old name and the rename would be lost. The server-owned timestamp is kept.
      vi.useFakeTimers();
      enableSave();
      build(formViewWorkflow).ngOnInit();
      workflowPersistService.persistWorkflow.mockClear();
      const saveA$ = new Subject<Workflow>();
      workflowPersistService.persistWorkflow.mockReturnValueOnce(saveA$);
      h.workflowChangedStream.next(undefined);
      vi.runAllTimers();
      expect(workflowPersistService.persistWorkflow).toHaveBeenCalledTimes(1);

      // The rename lands in the shared metadata while A is still out.
      workflowActionService.getWorkflowMetadata = () => ({ name: "B", lastModifiedTime: 1 });
      saveA$.next({ ...formViewWorkflow, wid: 7, name: "scGPT", lastModifiedTime: 42 } as any);
      saveA$.complete();

      expect(workflowActionService.setWorkflowMetadata).toHaveBeenCalledTimes(1);
      const fedBack = workflowActionService.setWorkflowMetadata.mock.calls[0][0];
      expect(fedBack.name).toBe("B");
      expect(fedBack.lastModifiedTime).toBe(42);
      vi.useRealTimers();
    });

    it("hands over only once a save queued behind the switch's has completed too", () => {
      // The page stays interactive while the switch's save is in flight, so an edit made then gets its
      // own autosave queued behind it. Navigating on the switch's save alone would abort that newer
      // save with the full-page load; the hand-over waits for the queue to drain.
      vi.useFakeTimers();
      enableSave();
      build(formViewWorkflow).ngOnInit();
      workflowPersistService.persistWorkflow.mockClear();
      const switchSave$ = new Subject<Workflow>();
      const laterSave$ = new Subject<Workflow>();
      workflowPersistService.persistWorkflow.mockReturnValueOnce(switchSave$).mockReturnValueOnce(laterSave$);
      const navigate = vi.spyOn(component as any, "openCanvasPage").mockImplementation(() => {});

      component.openRegularCanvas();
      h.workflowChangedStream.next(undefined); // an edit while the switch's save is in flight
      vi.runAllTimers();
      switchSave$.complete();
      expect(workflowPersistService.persistWorkflow).toHaveBeenCalledTimes(2);
      expect(navigate).not.toHaveBeenCalled(); // the later save is still out

      laterSave$.complete();
      expect(navigate).toHaveBeenCalledTimes(1);
      vi.useRealTimers();
    });

    it("flushes an edit still waiting in the autosave debounce before handing over", () => {
      // An edit made after the switch click enters the debounce, not the queue: when the switch's
      // save completes the queue is empty, and navigating then would kill the debounce with the
      // full-page load and lose the edit. The drain flushes it as one more save first.
      vi.useFakeTimers();
      enableSave();
      build(formViewWorkflow).ngOnInit();
      workflowPersistService.persistWorkflow.mockClear();
      const switchSave$ = new Subject<Workflow>();
      workflowPersistService.persistWorkflow.mockReturnValueOnce(switchSave$).mockReturnValue(of(formViewWorkflow));
      const navigate = vi.spyOn(component as any, "openCanvasPage").mockImplementation(() => {});

      component.openRegularCanvas();
      h.workflowChangedStream.next(undefined); // an edit after the click; its debounce has NOT elapsed
      switchSave$.complete(); // the queue drains while that edit still sits in the debounce

      // The flush went out at once (no 5-second wait), and only its completion handed over.
      expect(workflowPersistService.persistWorkflow).toHaveBeenCalledTimes(2);
      expect(navigate).toHaveBeenCalledTimes(1);
      vi.useRealTimers();
    });

    it("stays on the form when a save queued behind the switch's fails", () => {
      vi.useFakeTimers();
      enableSave();
      build(formViewWorkflow).ngOnInit();
      workflowPersistService.persistWorkflow.mockClear();
      const laterSave$ = new Subject<Workflow>();
      workflowPersistService.persistWorkflow.mockReturnValueOnce(of(formViewWorkflow)).mockReturnValueOnce(laterSave$);
      const navigate = vi.spyOn(component as any, "openCanvasPage").mockImplementation(() => {});

      // The switch's save completes at once, but by then an edit's autosave is already queued: without
      // the queued edit the switch would have navigated here.
      h.workflowChangedStream.next(undefined);
      vi.runAllTimers();
      component.openRegularCanvas();
      // (the autosave, first in the queue, was the synchronous one; the switch's save is the later$)
      laterSave$.error(new Error("nope"));

      expect(navigate).not.toHaveBeenCalled();
      expect(h.notificationService.error).toHaveBeenCalledWith(
        "Could not save. Your latest changes are not stored yet."
      );
      vi.useRealTimers();
    });

    it("keeps saving after a failed save: the queue does not stop, and only that save reports", () => {
      enableSave();
      build(formViewWorkflow).ngOnInit();
      workflowPersistService.persistWorkflow.mockClear();
      workflowPersistService.persistWorkflow
        .mockReturnValueOnce(throwError(() => new Error("nope")))
        .mockReturnValueOnce(of(formViewWorkflow));
      const navigate = vi.spyOn(component as any, "openCanvasPage").mockImplementation(() => {});

      component.onRenameWorkflow(); // a save that fails
      component.openRegularCanvas(); // the next one still goes out, and completes

      expect(workflowPersistService.persistWorkflow).toHaveBeenCalledTimes(2);
      expect(h.notificationService.error).toHaveBeenCalledTimes(1);
      expect(navigate).toHaveBeenCalledTimes(1);
    });

    it("stays on the form and reports it when the save before the switch fails", () => {
      enableSave();
      build(formViewWorkflow).ngOnInit();
      workflowPersistService.persistWorkflow.mockReturnValue(throwError(() => new Error("nope")));
      const navigate = vi.spyOn(component as any, "openCanvasPage").mockImplementation(() => {});

      component.openRegularCanvas();

      expect(navigate).not.toHaveBeenCalled();
      expect(h.notificationService.error).toHaveBeenCalledWith(
        "Could not save. Your latest changes are not stored yet."
      );
    });

    it("hands a reader with nothing to save straight over to the canvas", () => {
      build({ ...formViewWorkflow, readonly: true }).ngOnInit();
      workflowPersistService.persistWorkflow.mockClear();
      const navigate = vi.spyOn(component as any, "openCanvasPage").mockImplementation(() => {});

      component.openRegularCanvas();

      expect(workflowPersistService.persistWorkflow).not.toHaveBeenCalled();
      expect(navigate).toHaveBeenCalledTimes(1);
    });

    it("saves once more on the way out", () => {
      enableSave();
      build(formViewWorkflow).ngOnInit();
      workflowPersistService.persistWorkflow.mockClear();

      component.ngOnDestroy();

      expect(workflowPersistService.persistWorkflow).toHaveBeenCalled();
    });

    it("sends the final save behind an autosave still in flight, after the page is gone", () => {
      // The queue outlives the component: the save on the way out waits for the autosave already on
      // its way, so the older snapshot can never commit after the final one. The snapshot is taken
      // when the save is asked for, before ngOnDestroy clears the graph.
      vi.useFakeTimers();
      enableSave();
      build(formViewWorkflow).ngOnInit();
      workflowPersistService.persistWorkflow.mockClear();
      const autosave$ = new Subject<Workflow>();
      workflowPersistService.persistWorkflow.mockReturnValueOnce(autosave$).mockReturnValueOnce(of(formViewWorkflow));
      h.workflowChangedStream.next(undefined);
      vi.runAllTimers();
      expect(workflowPersistService.persistWorkflow).toHaveBeenCalledTimes(1); // the autosave, in flight

      // What the graph holds as the page goes: ngOnDestroy clears it right after asking for the final
      // save, so the save must carry the snapshot taken before that, not what the graph holds later.
      const content = { operators: [], operatorPositions: {} };
      workflowActionService.getWorkflow.mockReturnValue({ wid: 7, name: "as left", content });
      workflowActionService.clearWorkflow.mockImplementation(() =>
        workflowActionService.getWorkflow.mockReturnValue({ wid: 7, name: "cleared", content })
      );
      component.ngOnDestroy();
      expect(workflowPersistService.persistWorkflow).toHaveBeenCalledTimes(1); // the final save waits
      workflowActionService.setWorkflowMetadata.mockClear();

      autosave$.next({ ...formViewWorkflow, wid: 7 } as any);
      autosave$.complete();
      expect(workflowPersistService.persistWorkflow).toHaveBeenCalledTimes(2); // now it goes out
      expect(workflowPersistService.persistWorkflow.mock.calls.at(-1)[0].name).toBe("as left");
      // Responses landing after the page is gone repaint nothing and do not refill the cleared graph.
      expect(workflowActionService.setWorkflowMetadata).not.toHaveBeenCalled();
      vi.useRealTimers();
    });

    it("reports a failed save on the way out instead of throwing", () => {
      // The final save goes through the queue like any other, so its failure is reported the same
      // way: the notification fires, and nothing is thrown out of ngOnDestroy.
      enableSave();
      build(formViewWorkflow).ngOnInit();
      workflowPersistService.persistWorkflow.mockReturnValue(throwError(() => new Error("nope")));

      expect(() => component.ngOnDestroy()).not.toThrow();

      expect(h.notificationService.error).toHaveBeenCalledWith(
        "Could not save. Your latest changes are not stored yet."
      );
    });

    it("measures the name field after load, and no-ops when it is not in the DOM", () => {
      vi.useFakeTimers();
      const query = vi.spyOn(h.host.nativeElement, "querySelector");
      build(formViewWorkflow).ngOnInit();

      vi.runAllTimers();

      expect(query).toHaveBeenCalledWith("input.wf-name");
      vi.useRealTimers();
    });

    it("stops a deferred name measurement once the page is gone", () => {
      vi.useFakeTimers();
      build(formViewWorkflow).ngOnInit();
      const query = vi.spyOn(h.host.nativeElement, "querySelector");
      component.ngOnDestroy();

      vi.runAllTimers();

      expect(query).not.toHaveBeenCalled();
      vi.useRealTimers();
    });
  });

  // JointJS measures the paper once, when the editor is created. Creating it in the same pass
  // that uncollapses the strip races the browser's layout, and losing that race draws links up
  // and over the boxes -- so the strip opens first, and the canvas is built a frame later.
  describe("the workflow preview", () => {
    const frame = () => new Promise(r => requestAnimationFrame(() => r(null)));

    it("opens the strip but does not build the canvas in the same pass", () => {
      build(formViewWorkflow).ngOnInit();

      component.toggleWorkflow();

      expect(component.workflowOpen).toBe(true);
      expect(component.workflowEverOpened).toBe(false);
    });

    it("builds the canvas a frame after the strip opens, then centres it", async () => {
      build(formViewWorkflow).ngOnInit();

      component.toggleWorkflow();
      await frame();
      expect(component.workflowEverOpened).toBe(true);

      await frame();
      expect(h.triggerCenterEvent).toHaveBeenCalled();
    });

    it("closes the strip again without rebuilding the canvas", () => {
      build(formViewWorkflow).ngOnInit();
      component.toggleWorkflow();

      component.toggleWorkflow();

      expect(component.workflowOpen).toBe(false);
    });

    // Opening then immediately collapsing must not build the children into a hidden (0-sized)
    // strip -- the mini-map has no resize observer and would be stuck blank on the next open.
    it("does not build the canvas if the strip is collapsed again before the frame", async () => {
      build(formViewWorkflow).ngOnInit();

      component.toggleWorkflow(); // open -> schedules the deferred build
      component.toggleWorkflow(); // collapse again in the same tick, before the frame
      await frame();

      expect(component.workflowEverOpened).toBe(false);
    });

    // Leaving for the dashboard is an ordinary in-app navigation, so a reader can walk out in the
    // frame between opening the strip and the canvas being built; that deferred build must not run
    // on a page that is gone (detectChanges would throw on a destroyed view).
    it("does not build the canvas for a page that has been left", async () => {
      build(formViewWorkflow).ngOnInit();

      component.toggleWorkflow();
      component.ngOnDestroy();
      await frame();

      expect(component.workflowEverOpened).toBe(false);
    });
  });

  // The heart of this slice: turn each exposed binding into its operator's own formly field, and
  // write a filled-in value straight back to the operator.
  describe("the exposed inputs", () => {
    // Put op-1 on the graph and expose one of its properties, then read the config.
    const renderOne = (id: string, extra: any = {}) => {
      h.hasOperatorIds.add("op-1");
      formBindingService.resolveFields.mockReturnValue([resolved(id, id, extra)]);
      (component as any).readConfig();
    };

    it("renders a healthy input as a real formly field keyed by its binding id", () => {
      build(formViewWorkflow).ngOnInit();

      renderOne("n_hvg");

      expect(component.rendered).toHaveLength(1);
      expect(component.rendered[0].fields[0].key).toBe(component.rendered[0].resolved.binding.id);
    });

    it("renders nothing for an input whose operator is no longer on the graph", () => {
      build(formViewWorkflow).ngOnInit();
      // op-1 deliberately not added to the graph.
      formBindingService.resolveFields.mockReturnValue([resolved("n_hvg", "Genes")]);

      (component as any).readConfig();

      expect(component.rendered).toHaveLength(0);
    });

    it("skips an exposed property that has no matching schema field", () => {
      build(formViewWorkflow).ngOnInit();

      renderOne("nonesuch");

      expect(component.rendered).toHaveLength(0);
    });

    it("leaves broken inputs out of what a reader sees", () => {
      build(formViewWorkflow).ngOnInit();
      h.hasOperatorIds.add("op-1");
      formBindingService.resolveFields.mockReturnValue([
        resolved("n_hvg", "Genes"),
        resolved("gone", "Gone", { brokenReason: "the step it belonged to was removed" }),
      ]);

      (component as any).readConfig();

      expect(component.visibleFields).toHaveLength(1);
      expect(component.rendered).toHaveLength(1);
    });

    it("gives an exposed property its custom widget instead of a text box", () => {
      build(formViewWorkflow).ngOnInit();

      renderOne("datasetVersionPath");

      expect(component.rendered[0].fields[0].type).toBe("datasetversionselector");
    });

    it("uses the operator type to pick a widget (the HuggingFace model picker)", () => {
      build(formViewWorkflow).ngOnInit();
      h.graphOperators.push({ operatorID: "op-1", operatorType: "HuggingFace" });

      renderOne("modelId");

      expect(component.rendered[0].fields[0].type).toBe("huggingface");
    });

    it("renders a file property through its own picker type", () => {
      build(formViewWorkflow).ngOnInit();

      renderOne("fileName");

      expect(component.rendered[0].fields[0].type).toBe("inputautocomplete");
    });

    it("seeds the field model with the operator's other properties as read-only context", () => {
      build(formViewWorkflow).ngOnInit();
      h.hasOperatorIds.add("op-1");
      // A HuggingFace operator whose model picker (modelId) needs the sibling `task` to work.
      h.graphOperators.push({
        operatorID: "op-1",
        operatorType: "HuggingFace",
        operatorProperties: { task: "image-classification", modelId: "seed" },
      });
      formBindingService.resolveFields.mockReturnValue([resolved("modelId", "Model")]);

      (component as any).readConfig();

      const card = component.rendered[0];
      // The sibling context is present (so the widget reads the right task) ...
      expect(card.model.task).toBe("image-classification");
      // ... alongside this input's own value, keyed by the binding id, which is what writes back.
      expect(card.model[card.resolved.binding.id]).toBe("seed");
    });

    it("prefers the per-instance schema, falling back to the static one when it is unavailable", () => {
      build(formViewWorkflow).ngOnInit();
      h.graphOperators.push({ operatorID: "op-1", operatorType: "X" });
      (component as any).dynamicSchemaService = {
        getDynamicSchema: () => {
          throw new Error("no dynamic schema");
        },
      };
      (component as any).operatorMetadataService = {
        getOperatorSchema: () => ({ jsonSchema: { properties: { n_hvg: {} } } }),
      };

      renderOne("n_hvg");

      expect(component.rendered).toHaveLength(1);
    });

    it("renders nothing when neither the per-instance nor the static schema is available", () => {
      build(formViewWorkflow).ngOnInit();
      h.graphOperators.push({ operatorID: "op-1", operatorType: "X" });
      (component as any).dynamicSchemaService = {
        getDynamicSchema: () => {
          throw new Error("no dynamic schema");
        },
      };
      (component as any).operatorMetadataService = {
        getOperatorSchema: () => {
          throw new Error("no static schema");
        },
      };

      renderOne("n_hvg");

      expect(component.rendered).toHaveLength(0);
    });

    it("identifies a rendered card by its binding id", () => {
      build(formViewWorkflow);

      const key = component.trackByRendered(0, { resolved: { binding: { id: "b-1" } } } as any);

      expect(key).toBe("b-1");
    });

    it("locks the inputs for a read-only viewer", () => {
      build({ ...formViewWorkflow, readonly: true }).ngOnInit();

      renderOne("n_hvg");

      expect(component.canEdit).toBe(false);
      // The field carries props.disabled, which is what actually disables the control formly builds
      // (a form.disable() on the still-empty group does not, and does not persist). It cascades to
      // a nested property's sub-fields.
      expect((component.rendered[0].fields[0].props as any).disabled).toBe(true);
    });

    it("writes a dirtied value back to the operator", () => {
      build(formViewWorkflow).ngOnInit();
      renderOne("n_hvg");
      const card = component.rendered[0];
      const key = card.resolved.binding.id;
      vi.useFakeTimers();

      card.model[key] = "typed";
      card.form.addControl(key, new FormControl("typed"));
      card.form.markAsDirty();
      vi.advanceTimersByTime(FORM_DEBOUNCE_TIME_MS + 50);
      vi.useRealTimers();

      expect(formBindingService.writeValue).toHaveBeenCalled();
    });

    it("ignores an unchanged form emission", () => {
      build(formViewWorkflow).ngOnInit();
      formBindingService.readValue.mockReturnValue("seed");
      renderOne("n_hvg");
      const card = component.rendered[0];
      const key = card.resolved.binding.id;
      vi.useFakeTimers();

      card.model[key] = "seed";
      card.form.addControl(key, new FormControl("seed"));
      vi.advanceTimersByTime(FORM_DEBOUNCE_TIME_MS + 50);
      vi.useRealTimers();

      expect(formBindingService.writeValue).not.toHaveBeenCalled();
    });

    it("keeps a still-set value when formly emits a blank before an edit", () => {
      build(formViewWorkflow).ngOnInit();
      formBindingService.readValue.mockReturnValue("seed");
      renderOne("n_hvg");
      const card = component.rendered[0];
      const key = card.resolved.binding.id;
      vi.useFakeTimers();

      card.model[key] = "";
      card.form.addControl(key, new FormControl(""));
      vi.advanceTimersByTime(FORM_DEBOUNCE_TIME_MS + 50);
      vi.useRealTimers();

      expect(formBindingService.writeValue).not.toHaveBeenCalled();
    });

    it("refreshes the card's snapshot after a write-back", () => {
      build(formViewWorkflow).ngOnInit();
      renderOne("n_hvg");
      const card = component.rendered[0];
      const key = card.resolved.binding.id;
      // The re-read after a write returns the new value on the same binding.
      formBindingService.resolveFields.mockReturnValue([resolved("n_hvg", "n_hvg", { value: "typed" })]);
      vi.useFakeTimers();

      card.model[key] = "typed";
      card.form.addControl(key, new FormControl("typed"));
      card.form.markAsDirty();
      vi.advanceTimersByTime(FORM_DEBOUNCE_TIME_MS + 50);
      vi.useRealTimers();

      expect(component.rendered[0].resolved.value).toBe("typed");
    });

    it("leaves the card unchanged when the re-read no longer carries the binding", () => {
      build(formViewWorkflow).ngOnInit();
      renderOne("n_hvg");
      const card = component.rendered[0];
      const before = card.resolved;
      const key = card.resolved.binding.id;
      // The write succeeds, but the following resolve returns nothing for this binding.
      formBindingService.resolveFields.mockReturnValue([]);
      vi.useFakeTimers();

      card.model[key] = "typed";
      card.form.addControl(key, new FormControl("typed"));
      card.form.markAsDirty();
      vi.advanceTimersByTime(FORM_DEBOUNCE_TIME_MS + 50);
      vi.useRealTimers();

      expect(formBindingService.writeValue).toHaveBeenCalled();
      expect(component.rendered[0].resolved).toBe(before);
    });

    it("labels an unnamed input by its schema title, not the raw key", () => {
      build(formViewWorkflow).ngOnInit();
      h.hasOperatorIds.add("op-1");
      formBindingService.resolveFields.mockReturnValue([
        resolved("n_hvg", "", {
          binding: { id: "b", operatorID: "op-1", propertyKey: "n_hvg", displayName: "" } as any,
        }),
      ]);

      (component as any).readConfig();

      // The schema's own title ("N"), not "n_hvg".
      expect((component.rendered[0].fields[0].props as any).label).toBe("N");
    });
  });

  // A nested (object) or repeated (array) property carries sub-fields; the author can rename and
  // hide each one, and the schema's own per-field notes are dropped so only the author's help text
  // guides a reader. Overrides are keyed by field path, array indices dropped.
  describe("nested and array sub-fields", () => {
    // Expose one property of op-1 with the given binding, then read the config.
    const expose = (bindingExtra: any) => {
      h.hasOperatorIds.add("op-1");
      formBindingService.resolveFields.mockReturnValue([resolved("x", "x", { binding: bindingExtra })]);
      (component as any).readConfig();
      return component.rendered[0].fields[0] as any;
    };

    // The shared array widget prints its label at the bottom beside its add button, so a repeated
    // input's title would sit above the rows in edit mode and jump below them on Done. A reader gets
    // the same static title above instead, and the widget's own label is blanked.
    it("gives a repeated input its title above in reader mode, not the array widget's bottom label", () => {
      build(formViewWorkflow).ngOnInit();
      h.formlyJsonschema.toFieldConfig = () => ({
        fieldGroup: [
          {
            key: "predicates",
            type: "array",
            props: { label: "Predicates" },
            fieldArray: () => ({ fieldGroup: [{ key: "alias", props: { label: "Alias" } }] }),
          },
        ],
      });

      const field = expose({ id: "p", operatorID: "op-1", propertyKey: "predicates", displayName: "Predicate" });

      expect(field.wrappers).toContain("editable-label-wrapper");
      expect(field.props.authoring).toBe(false);
      expect(field.props.authorName).toBe("Predicate");
      expect(field.props.schemaLabel).toBe("Predicates");
      expect(field.props.label).toBe("");
    });

    it("leaves a scalar input's label to formly in reader mode", () => {
      build(formViewWorkflow).ngOnInit();

      const field = expose({ id: "n", operatorID: "op-1", propertyKey: "n_hvg", displayName: "How many" });

      // Above the control already, with formly's required marker; nothing to wrap.
      expect(field.wrappers ?? []).not.toContain("editable-label-wrapper");
      expect(field.props.label).toBe("How many");
    });

    it("renames and hides an overridden sub-field of an object property", () => {
      build(formViewWorkflow).ngOnInit();

      const field = expose({
        id: "n",
        operatorID: "op-1",
        propertyKey: "nested",
        displayName: "Nested",
        overrides: { sub: { displayName: "Renamed sub", hidden: true } },
      });

      const sub = field.fieldGroup[0];
      expect(sub.key).toBe("sub");
      expect(sub.props.label).toBe("Renamed sub");
      expect(sub.hide).toBe(true);
      // Hidden must not strip the value: formly's resetFieldOnHide default would otherwise clear it
      // from the model on render, and the card writes the whole nested object back -- deleting the
      // author's pinned value. resetOnHide=false keeps it.
      expect(sub.resetOnHide).toBe(false);
    });

    it("renames and hides an overridden sub-field of a repeated section, per row", () => {
      build(formViewWorkflow).ngOnInit();

      const field = expose({
        id: "p",
        operatorID: "op-1",
        propertyKey: "predicates",
        displayName: "Predicates",
        overrides: { alias: { displayName: "Renamed", hidden: true } },
      });

      // Formly builds a repeated section's rows on demand; invoke the wrapped builder so the walk
      // decorates the row's sub-fields (every row formly ever makes comes out decorated).
      const row = field.fieldArray({});
      const alias = row.fieldGroup[0];
      expect(alias.key).toBe("alias");
      expect(alias.props.label).toBe("Renamed");
      expect(alias.hide).toBe(true);
      expect(alias.resetOnHide).toBe(false);
    });

    it("while authoring, gives a repeated section's controls to its first row only; later rows follow", () => {
      // Every row shares one override, so a name box and an eye on each row would be that many copies
      // of one control, none following the others. The first row carries the controls; later rows
      // show the same name and hidden state statically and follow the first row's edits at once.
      build(formViewWorkflow).ngOnInit();
      component.authoring = true;
      h.formlyJsonschema.toFieldConfig = () => ({
        fieldGroup: [
          {
            key: "predicates",
            type: "array",
            props: { label: "Predicates" },
            fieldArray: () => ({ fieldGroup: [{ key: "alias", props: { label: "Alias" } }] }),
          },
        ],
      });

      const field = expose({
        id: "p",
        operatorID: "op-1",
        propertyKey: "predicates",
        displayName: "Predicates",
        overrides: { alias: { displayName: "Renamed", hidden: true } },
      });
      const first = field.fieldArray({}).fieldGroup[0];
      const second = field.fieldArray({}).fieldGroup[0];

      expect(first.props.authoring).toBe(true);
      expect(first.props.renameField).toBeTypeOf("function");
      expect(first.props.setFieldHidden).toBeTypeOf("function");
      expect(second.props.authoring).toBe(false); // static: no box, no eye
      expect(second.props.renameField).toBeUndefined();
      expect(second.props.authorName).toBe("Renamed");
      expect(second.props.authorHidden).toBe(true);

      first.props.renameField("New alias");
      first.props.setFieldHidden(false);
      expect(second.props.authorName).toBe("New alias");
      expect(second.props.authorHidden).toBe(false);

      // A rebuild replaces the rows: the old followers go with them and the new rows register anew,
      // so an edit on the rebuilt first row reaches the rebuilt rows, not the stale ones.
      const rebuilt = expose({
        id: "p",
        operatorID: "op-1",
        propertyKey: "predicates",
        displayName: "Predicates",
        overrides: { alias: { displayName: "Renamed", hidden: true } },
      });
      const rebuiltFirst = rebuilt.fieldArray({}).fieldGroup[0];
      const rebuiltSecond = rebuilt.fieldArray({}).fieldGroup[0];
      rebuiltFirst.props.renameField("Again");
      expect(rebuiltSecond.props.authorName).toBe("Again");
      expect(second.props.authorName).toBe("New alias");
    });

    it("keeps the schema's own label as the name box's fallback when the sub-field is already renamed", () => {
      // The placeholder and the "Empty keeps ..." tooltip promise what clearing the box yields; an
      // empty name deletes the override, so that is the schema label, not the old override.
      build(formViewWorkflow).ngOnInit();
      component.authoring = true;

      const field = expose({
        id: "n",
        operatorID: "op-1",
        propertyKey: "nested",
        displayName: "Nested",
        overrides: { sub: { displayName: "Renamed sub" } },
      });

      const sub = field.fieldGroup[0];
      expect(sub.props.authorName).toBe("Renamed sub");
      expect(sub.props.schemaLabel).toBe("Sub");
    });

    it("walks a scalar array's rows as rows, so the input's title box appears once, above them", () => {
      build(formViewWorkflow).ngOnInit();
      component.authoring = true;
      h.formlyJsonschema.toFieldConfig = () => ({
        fieldGroup: [
          {
            key: "tags",
            type: "array",
            props: { label: "Tags" },
            fieldArray: () => ({ type: "input", props: { label: "Tag" } }),
          },
        ],
      });

      const field = expose({ id: "t", operatorID: "op-1", propertyKey: "tags", displayName: "Tags" });
      const row = field.fieldArray({});

      expect(field.props.authoring).toBe(true); // the input itself carries the one title box...
      expect(field.props.labelsGroup).toBe(true); // ...naming the rows as a group
      expect(row.wrappers ?? []).not.toContain("editable-label-wrapper"); // no second title box per row
      expect(row.props.description).toBe("");
    });

    it("does not rebuild the form on its own presentation writes, though each is announced; a structural change still does", () => {
      // Every config write announces on formBindingChanged$ (the harness emits like the real service).
      // A name, a hide flag or help text is already shown by the control that took it, and the eye is
      // a button (the typing hold does not cover it), so a rebuild would replace the control mid-click
      // and drop the focus. Expose/remove/reorder rebuild as before.
      build(formViewWorkflow).ngOnInit();
      component.authoring = true;
      const field = expose({ id: "n", operatorID: "op-1", propertyKey: "nested", displayName: "Nested" });
      const rebuild = vi.spyOn(component as any, "readConfig");
      const sub = field.fieldGroup[0];

      sub.props.setFieldHidden(true);
      sub.props.renameField("Other");
      field.props.renameField("Whole input");
      component.onEditHelpText(resolved("n", "N", {}), "help");

      expect(h.formBindingService.setFieldOverride).toHaveBeenCalledTimes(2);
      expect(h.formBindingService.updateBinding).toHaveBeenCalledTimes(2);
      expect(rebuild).not.toHaveBeenCalled();

      h.formBindingChanged.next(undefined); // e.g. the panel exposing a property
      expect(rebuild).toHaveBeenCalledTimes(1);
    });

    it("drops the schema's own descriptions on the field and its sub-fields", () => {
      build(formViewWorkflow).ngOnInit();

      const field = expose({ id: "n", operatorID: "op-1", propertyKey: "nested", displayName: "Nested" });

      expect(field.props.description).toBe("");
      expect(field.fieldGroup[0].props.description).toBe("");
    });

    it("leaves a sub-field untouched when the author set no override for it", () => {
      build(formViewWorkflow).ngOnInit();

      const field = expose({ id: "n", operatorID: "op-1", propertyKey: "nested", displayName: "Nested" });

      const sub = field.fieldGroup[0];
      // No override: keeps the schema label and stays visible.
      expect(sub.props.label).toBe("Sub");
      expect(sub.hide).toBeUndefined();
      // A visible field is never opted out of reset-on-hide -- the switch rides with the hide.
      expect(sub.resetOnHide).toBeUndefined();
    });

    it("drops the description on a scalar array's row template", () => {
      build(formViewWorkflow).ngOnInit();

      const field = expose({ id: "t", operatorID: "op-1", propertyKey: "tags", displayName: "Tags" });

      // The row template is a leaf (no sub-fields); its schema description is dropped like the rest.
      expect(field.fieldArray.props.description).toBe("");
    });

    it("drops the description on a builder-backed scalar array's leaf row", () => {
      build(formViewWorkflow).ngOnInit();

      const field = expose({ id: "tf", operatorID: "op-1", propertyKey: "tagsFn", displayName: "Tags" });
      // Invoke the wrapped builder: it returns a leaf row (no fieldGroup), which the walk decorates.
      const row = field.fieldArray({});

      expect(row.props.description).toBe("");
    });

    it("drops the description on a builder-backed object row without reprinting its title", () => {
      build(formViewWorkflow).ngOnInit();

      const field = expose({ id: "p", operatorID: "op-1", propertyKey: "predicates", displayName: "Predicates" });
      // An object row (fieldGroup): its container is not walked as a root (that would reprint the
      // array's title), but its own items.description would still render once per row, so it is
      // dropped; the row's sub-field is walked as before.
      const row = field.fieldArray({});

      expect(row.props.description).toBe("");
      expect(row.fieldGroup[0].props.description).toBe("");
    });

    it("drops the description on a static object-array's row template", () => {
      build(formViewWorkflow).ngOnInit();

      const field = expose({ id: "r", operatorID: "op-1", propertyKey: "rules", displayName: "Rules" });

      // The template container (fieldArray with a fieldGroup) carries items.description; it is
      // dropped, and its sub-fields are still walked (their descriptions dropped too).
      expect(field.fieldArray.props.description).toBe("");
      expect(field.fieldArray.fieldGroup[0].props.description).toBe("");
    });
  });

  describe("keeping the inputs in step with the workflow", () => {
    it("rebuilds the inputs when compilation reports a new state", async () => {
      build(formViewWorkflow).ngOnInit();
      const rebuild = vi.spyOn(component as any, "readConfig");

      h.compilationChanged.next("Succeeded");
      await new Promise(r => setTimeout(r, FORM_DEBOUNCE_TIME_MS + 50));

      expect(rebuild).toHaveBeenCalled();
    });

    it("rebuilds the inputs when a step is renamed, so each card's attribution follows", () => {
      // A rename in the live panel (or a co-editor's) reaches no other stream: no compilation, no
      // config change. Without this the "From ..." line on the step's cards would keep the old name.
      build(formViewWorkflow).ngOnInit();
      const rebuild = vi.spyOn(component as any, "readConfig");

      h.displayNameChanged.next({});

      expect(rebuild).toHaveBeenCalledTimes(1);
    });

    it("holds a rebuild while someone is typing and runs it once the focus leaves", async () => {
      build(formViewWorkflow).ngOnInit();
      const typing = vi.spyOn(component as any, "isTypingInTheForm").mockReturnValue(true);
      const rebuild = vi.spyOn(component as any, "readConfig");

      h.compilationChanged.next("Succeeded");
      await new Promise(r => setTimeout(r, FORM_DEBOUNCE_TIME_MS + 50));
      expect(rebuild).not.toHaveBeenCalled();

      // The cursor leaves the field: the held rebuild runs, once. Held rather than dropped, or the
      // compiled schema would never reach the cards until something else rebuilt them.
      typing.mockReturnValue(false);
      component.onFocusOut();
      await new Promise(r => setTimeout(r, 10));

      expect(rebuild).toHaveBeenCalledTimes(1);
    });

    it("keeps a held rebuild held when the focus only moves to another text field", async () => {
      build(formViewWorkflow).ngOnInit();
      vi.spyOn(component as any, "isTypingInTheForm").mockReturnValue(true);
      const rebuild = vi.spyOn(component as any, "readConfig");
      h.formBindingChanged.next(undefined);

      component.onFocusOut(); // tabbed to the next input: still typing when the check runs
      await new Promise(r => setTimeout(r, 10));

      expect(rebuild).not.toHaveBeenCalled();
    });

    it("rebuilds nothing on a focusout with no rebuild held", async () => {
      build(formViewWorkflow).ngOnInit();
      const rebuild = vi.spyOn(component as any, "readConfig");

      component.onFocusOut();
      await new Promise(r => setTimeout(r, 10));

      expect(rebuild).not.toHaveBeenCalled();
    });

    // Leaving the text field by clicking a tick box: focusout queues the held rebuild, then the tick
    // box's own change rebuilds at once and clears the hold. The queued callback must notice and
    // not rebuild the same cards a second time.
    it("does not rebuild twice when the control that took the focus already rebuilt", async () => {
      build(formViewWorkflow).ngOnInit();
      const typing = vi.spyOn(component as any, "isTypingInTheForm").mockReturnValue(true);
      const rebuild = vi.spyOn(component as any, "readConfig");
      h.formBindingChanged.next(undefined); // held
      component.onFocusOut(); // queued

      typing.mockReturnValue(false);
      h.formBindingChanged.next(undefined); // the tick box's own change: rebuilds now
      expect(rebuild).toHaveBeenCalledTimes(1);
      await new Promise(r => setTimeout(r, 10)); // the queued callback fires

      expect(rebuild).toHaveBeenCalledTimes(1);
    });

    it("re-reads the config when a property is exposed or un-exposed", () => {
      build(formViewWorkflow).ngOnInit();
      const before = formBindingService.resolveFields.mock.calls.length;

      h.formBindingChanged.next(undefined);

      expect(formBindingService.resolveFields.mock.calls.length).toBeGreaterThan(before);
    });

    // Once #8351 makes this stream fire for a co-editor's change, a rebuild under the cursor would
    // discard a half-entered value -- so the binding path holds it while typing, like the
    // compilation path, and runs it when the focus leaves.
    it("holds a binding-change rebuild while the reader is typing, then runs it on focusout", async () => {
      build(formViewWorkflow).ngOnInit();
      const typing = vi.spyOn(component as any, "isTypingInTheForm").mockReturnValue(true);
      const rebuild = vi.spyOn(component as any, "readConfig");

      h.formBindingChanged.next(undefined);
      expect(rebuild).not.toHaveBeenCalled();

      typing.mockReturnValue(false);
      component.onFocusOut();
      await new Promise(r => setTimeout(r, 10));

      expect(rebuild).toHaveBeenCalledTimes(1);
    });

    // The bug this guards against: ticking a property in the step panel focuses the tick box, an
    // <input type="checkbox"> inside this page. Counted as typing, the rebuild that should add the
    // card was held back, so the tick looked like it did nothing until something else rebuilt.
    it("does not count a focused tick box as typing", () => {
      build(formViewWorkflow).ngOnInit();
      const box = document.createElement("input");
      box.type = "checkbox";
      document.body.appendChild(box);
      (component as any).host = { nativeElement: { contains: () => true, querySelector: () => null } };
      box.focus();

      expect((component as any).isTypingInTheForm()).toBe(false);

      document.body.removeChild(box);
    });

    it("reports typing when a form field inside the page is focused", () => {
      build(formViewWorkflow).ngOnInit();
      const input = document.createElement("input");
      document.body.appendChild(input);
      (component as any).host = { nativeElement: { contains: () => true, querySelector: () => null } };
      input.focus();

      expect((component as any).isTypingInTheForm()).toBe(true);

      document.body.removeChild(input);
    });

    it("reports no typing when the focus is outside the page", () => {
      build(formViewWorkflow).ngOnInit();
      (component as any).host = { nativeElement: { contains: () => false, querySelector: () => null } };

      expect((component as any).isTypingInTheForm()).toBe(false);
    });

    it("reports typing when a content-editable element inside the page is focused", () => {
      build(formViewWorkflow).ngOnInit();
      const editable = document.createElement("div");
      editable.tabIndex = 0;
      // jsdom does not derive isContentEditable from the attribute; set it directly.
      Object.defineProperty(editable, "isContentEditable", { value: true });
      document.body.appendChild(editable);
      (component as any).host = { nativeElement: { contains: () => true, querySelector: () => null } };
      editable.focus();

      expect((component as any).isTypingInTheForm()).toBe(true);

      document.body.removeChild(editable);
    });
  });

  describe("the author's instruction", () => {
    it("shows the instruction as rendered markdown when there is one", async () => {
      formBindingService.getConfig.mockReturnValue({
        instruction: { title: "Read me", body: "**bold**" },
        fields: [],
      });
      build(formViewWorkflow).ngOnInit();
      // renderInstruction resolves the parsed markdown on a microtask; let it settle.
      await Promise.resolve();

      expect(component.hasInstruction).toBe(true);
      expect(component.instructionTitle).toBe("Read me");
      // The markdown mock returns its input; the point is renderInstruction populated the html.
      expect(component.instructionPreviewHtml).toBe("**bold**");
    });

    it("has no instruction when the body is blank", async () => {
      formBindingService.getConfig.mockReturnValue({
        instruction: { title: "T", body: "   " },
        fields: [],
      });
      build(formViewWorkflow).ngOnInit();
      await Promise.resolve();

      expect(component.hasInstruction).toBe(false);
      expect(component.instructionPreviewHtml).toBe("");
    });

    it("discards a stale instruction render when the body changed while parsing", async () => {
      build(formViewWorkflow).ngOnInit();
      (component as any).instructionBody = "first";
      const pending = (component as any).renderInstruction();
      // A newer readConfig sets a different body before the parse microtask resolves.
      (component as any).instructionBody = "second";
      await pending;

      // The stale "first" result is dropped rather than overwriting the newer body's render.
      expect(component.instructionPreviewHtml).not.toBe("first");
    });

    it("toggles the instruction open and closed", () => {
      build(formViewWorkflow).ngOnInit();
      expect(component.instructionOpen).toBe(true);

      component.toggleInstruction();

      expect(component.instructionOpen).toBe(false);
    });
  });

  describe("the run button, mirroring the operator canvas", () => {
    // Put the page in a ready-to-run state: a WRITE-access unit is up, the socket is connected, the
    // graph valid.
    const makeReady = () => {
      h.workflowWebsocketService.isConnected = true;
      h.statusStream.next(ComputingUnitState.Running);
      (component as any).selectedUnit = { accessPrivilege: "WRITE" };
      h.validationStream.next({ errors: {}, workflowEmpty: false });
    };

    it("stores the selected unit from the status service so Run can gate on write access", () => {
      build(formViewWorkflow).ngOnInit();

      h.selectedUnitStream.next({ accessPrivilege: "WRITE" });

      expect((component as any).selectedUnit).toEqual({ accessPrivilege: "WRITE" });
    });

    it("offers Connect before a unit is chosen", () => {
      build(formViewWorkflow).ngOnInit();

      expect(component.runButtonState).toEqual({ label: "Connect", icon: "plus-circle", disabled: true });
    });

    it("offers Run once a unit is up and the graph is valid", () => {
      build(formViewWorkflow).ngOnInit();
      makeReady();

      expect(component.runButtonState.label).toBe("Run");
      expect(component.runButtonState.disabled).toBe(false);
    });

    it("shows Stop while running", () => {
      build(formViewWorkflow).ngOnInit();
      h.executionStateStream.next({ current: { state: ExecutionState.Running } });

      expect(component.isRunning).toBe(true);
      expect(component.runButtonState).toEqual({ label: "Stop", icon: "stop", disabled: false });
    });

    it("disables and says Invalid for a broken graph", () => {
      build(formViewWorkflow).ngOnInit();
      makeReady();
      h.validationStream.next({ errors: { op: {} }, workflowEmpty: false });

      expect(component.runButtonState).toEqual({ label: "Invalid", icon: "warning", disabled: true });
    });

    it("disables and says Empty for an empty graph", () => {
      build(formViewWorkflow).ngOnInit();
      makeReady();
      h.validationStream.next({ errors: {}, workflowEmpty: true });

      expect(component.runButtonState).toEqual({ label: "Empty", icon: "info-circle", disabled: true });
    });

    it("disables and says Connecting while the unit's socket comes up", () => {
      build(formViewWorkflow).ngOnInit();
      h.statusStream.next(ComputingUnitState.Running);
      h.validationStream.next({ errors: {}, workflowEmpty: false });
      h.workflowWebsocketService.isConnected = false;

      expect(component.runButtonState).toEqual({ label: "Connecting", icon: "loading", disabled: true });
    });

    it("disables with No access when the chosen unit is shared read-only", () => {
      build(formViewWorkflow).ngOnInit();
      h.workflowWebsocketService.isConnected = true;
      h.statusStream.next(ComputingUnitState.Running);
      h.validationStream.next({ errors: {}, workflowEmpty: false });
      (component as any).selectedUnit = { accessPrivilege: "READ" };

      expect(component.runButtonState).toEqual({ label: "No access", icon: "lock", disabled: true });
    });

    it("does not offer a dead Stop when the socket drops mid-run", () => {
      build(formViewWorkflow).ngOnInit();
      h.statusStream.next(ComputingUnitState.Running); // a unit is selected
      h.executionStateStream.next({ current: { state: ExecutionState.Running } }); // a run is in flight
      h.workflowWebsocketService.isConnected = false; // its socket drops

      // Still "running", but the button must not offer a Stop that would kill through a dead socket.
      expect(component.isRunning).toBe(true);
      expect(component.runButtonState).toEqual({ label: "Connecting", icon: "loading", disabled: true });
    });

    it("repaints when the websocket connection status changes", () => {
      build(formViewWorkflow).ngOnInit();
      h.cdr.markForCheck.mockClear();

      h.connectionStream.next(true);

      expect(h.cdr.markForCheck).toHaveBeenCalled();
    });
  });

  describe("running", () => {
    const makeReady = () => {
      h.workflowWebsocketService.isConnected = true;
      h.statusStream.next(ComputingUnitState.Running);
      (component as any).selectedUnit = { accessPrivilege: "WRITE" };
      h.validationStream.next({ errors: {}, workflowEmpty: false });
    };

    it("runs the workflow with its name and clears any prior error", () => {
      build(formViewWorkflow).ngOnInit();
      makeReady();
      component.runError = "old error";

      component.onRun();

      expect(h.executeWorkflowService.executeWorkflow).toHaveBeenCalledWith("scGPT");
      expect(component.runError).toBe("");
    });

    it("clears a stale failure banner when a new run starts, even a co-editor's", () => {
      build(formViewWorkflow).ngOnInit();
      h.executionStateStream.next({ current: { state: ExecutionState.Failed, errorMessages: [{ message: "boom" }] } });
      expect(component.runError).not.toBe("");

      // A co-editor starts the next run: the shared stream goes in-flight without this page's onRun().
      h.executionStateStream.next({ current: { state: ExecutionState.Running } });

      expect(component.runError).toBe("");
    });

    it("tells apart a never-run form from a completed run that produced nothing", () => {
      build(formViewWorkflow).ngOnInit();
      expect(component.hasRunFinished).toBe(false);

      h.executionStateStream.next({ current: { state: ExecutionState.Completed } });

      expect(component.hasRunFinished).toBe(true);
    });

    it("stops a running workflow instead of starting another", () => {
      build(formViewWorkflow).ngOnInit();
      h.executionStateStream.next({ current: { state: ExecutionState.Running } });

      component.onRun();

      expect(h.executeWorkflowService.killWorkflow).toHaveBeenCalled();
      expect(h.executeWorkflowService.executeWorkflow).not.toHaveBeenCalled();
    });

    it("does nothing when the button is disabled", () => {
      build(formViewWorkflow).ngOnInit();
      // Default state is "Connect" (disabled): no unit chosen.

      component.onRun();

      expect(h.executeWorkflowService.executeWorkflow).not.toHaveBeenCalled();
      expect(h.executeWorkflowService.killWorkflow).not.toHaveBeenCalled();
    });

    it("counts the run clock off the engine's duration event", () => {
      build(formViewWorkflow).ngOnInit();

      h.durationEvents.next({ duration: 5000, isRunning: false });

      expect(component.executionDuration).toBe(5000);
    });

    it("ticks the clock a second at a time while a run is going", () => {
      vi.useFakeTimers();
      build(formViewWorkflow).ngOnInit();

      h.durationEvents.next({ duration: 1000, isRunning: true });
      vi.advanceTimersByTime(1000);
      vi.useRealTimers();

      expect(component.executionDuration).toBe(2000);
    });
  });

  describe("showing the chosen results", () => {
    // Until the author has chosen, the final steps show (the engine always materializes them); once
    // there is a saved list, exactly its steps show, kept to those that still have a result on the
    // canvas. The form never writes the view-result set (display filter, per the settled design).
    const saved = (shownResultIds?: string[]) =>
      formBindingService.getConfig.mockReturnValue({
        instruction: undefined,
        fields: [],
        shownResultIds,
      });

    it("shows every final step until the author has chosen, then exactly the saved list", () => {
      // No list (an untouched form, or one from before the field existed): every final step is on. A
      // list: exactly those, so a step that becomes final after the author chose does not appear by
      // itself, and an empty list means no results at all -- a choice the form can store.
      build(formViewWorkflow).ngOnInit();
      h.graphOperators.push({ operatorID: "last-a", operatorType: "Limit" });
      h.graphOperators.push({ operatorID: "last-b", operatorType: "Limit" });
      h.terminalIds.add("last-a");
      h.terminalIds.add("last-b");

      saved();
      (component as any).readConfig();
      expect(component.shownResultIds).toEqual(["last-a", "last-b"]);

      saved(["last-a"]);
      (component as any).readConfig();
      expect(component.shownResultIds).toEqual(["last-a"]);

      saved([]);
      (component as any).readConfig();
      expect(component.shownResultIds).toEqual([]);
    });

    it("lets a viewer pick their own results for this page, without writing anything", () => {
      build(formViewWorkflow).ngOnInit();
      saved();
      h.graphOperators.push({ operatorID: "mid", operatorType: "Filter" });
      h.graphOperators.push({ operatorID: "last", operatorType: "Limit" });
      h.viewResultIds.add("mid");
      h.terminalIds.add("last");
      (component as any).readConfig();
      expect(component.shownResultIds).toEqual(["last"]);
      const mid = component.resultChoices.find(c => c.operatorID === "mid")!;
      const last = component.resultChoices.find(c => c.operatorID === "last")!;

      // Not in edit mode: the toggle is the viewer's own, so nothing goes to the shared config.
      component.onToggleResult(mid);
      expect(component.shownResultIds).toEqual(["last", "mid"]);
      component.onToggleResult(last);
      expect(component.shownResultIds).toEqual(["mid"]);
      expect(h.formBindingService.toggleShownResult).not.toHaveBeenCalled();
      // The picker's pills follow the viewer's choice.
      expect(component.resultChoices.find(c => c.operatorID === "last")?.shown).toBe(false);
      expect(component.resultChoices.find(c => c.operatorID === "mid")?.shown).toBe(true);

      // Entering edit mode edits the default for everyone, so the viewer's own pick gives way to it.
      component.toggleAuthoring();
      expect(component.shownResultIds).toEqual(["last"]);
    });

    it("shows a chosen result only while its operator still has view-result on the canvas", () => {
      build(formViewWorkflow).ngOnInit();
      saved(["a", "b"]);
      h.viewResultIds.add("a"); // b's eye is off on the canvas

      (component as any).readConfig();

      expect(component.shownResultIds).toEqual(["a"]);
    });

    it("shows a chosen terminal operator with no eye", () => {
      // The engine materializes a terminal operator unconditionally, so its result is always available.
      // The form must show it, or an author who picks the workflow's final operator -- the most natural
      // choice -- would get a card that never appears.
      build(formViewWorkflow).ngOnInit();
      saved(["last"]);
      h.graphOperators.push({ operatorID: "last", operatorType: "Limit" });
      h.terminalIds.add("last"); // no downstream link, and its eye is off

      (component as any).readConfig();

      expect(component.shownResultIds).toEqual(["last"]);
    });

    it("shows the terminal result with nothing chosen", () => {
      // A reader who never curates still sees the workflow's final result: the engine always
      // materializes the terminal operator, so its result is always available to show.
      build(formViewWorkflow).ngOnInit();
      saved();
      h.graphOperators.push({ operatorID: "last", operatorType: "Limit" });
      h.hasOperatorIds.add("last");
      h.terminalIds.add("last"); // terminal, no eye, nothing chosen

      (component as any).readConfig();

      expect(component.shownResultIds).toEqual(["last"]);
    });

    it("drops a shown step the moment the graph's shape makes it unavailable, and takes it off the picker", () => {
      // A co-editor disables a shown final step (or deletes it, or gives it a downstream link): the
      // graph reports that on its own streams, without a result update or a config change, and the
      // card and the pill must go at once rather than wait for the next unrelated event.
      build(formViewWorkflow).ngOnInit();
      saved();
      const last = { operatorID: "last", operatorType: "Limit", isDisabled: false };
      h.graphOperators.push(last);
      h.terminalIds.add("last");
      (component as any).readConfig();
      expect(component.shownResultIds).toEqual(["last"]);
      expect(component.resultChoices.map(c => c.operatorID)).toEqual(["last"]);

      last.isDisabled = true;
      h.graphStructureChanged.next({});

      expect(component.shownResultIds).toEqual([]);
      expect(component.resultChoices).toEqual([]);
    });

    it("renames a step's pill the moment its display name changes on the canvas", () => {
      // A rename in the live panel (or a co-editor's) reaches no other stream: no result update, no
      // compilation, no config change. The pill must follow it at once.
      build(formViewWorkflow).ngOnInit();
      saved();
      const op = { operatorID: "last", operatorType: "Limit", customDisplayName: "Old name" };
      h.graphOperators.push(op);
      h.terminalIds.add("last");
      (component as any).readConfig();
      expect(component.resultChoices.map(c => c.label)).toEqual(["Old name"]);

      op.customDisplayName = "New name";
      h.displayNameChanged.next({});

      expect(component.resultChoices.map(c => c.label)).toEqual(["New name"]);
    });

    it("does not show a chosen non-terminal operator whose eye is off", () => {
      // A mid-graph step with an enabled downstream link is materialized only when its eye is on;
      // without the eye it produces no result, so the form must not show a card that sits forever empty.
      build(formViewWorkflow).ngOnInit();
      saved(["mid"]);
      h.graphOperators.push({ operatorID: "mid", operatorType: "Filter" }); // enabled downstream, no eye

      (component as any).readConfig();

      expect(component.shownResultIds).toEqual([]);
    });

    it("treats an operator whose only downstream link is disabled as terminal", () => {
      // The backend's storage rule is out-degree 0 on the ENABLED plan, so an operator whose downstream
      // link is disabled is terminal and gets materialized. The form must match, reading enabled links.
      build(formViewWorkflow).ngOnInit();
      saved();
      h.graphOperators.push({ operatorID: "a", operatorType: "Filter" });
      h.graphOperators.push({ operatorID: "b", operatorType: "Limit" });
      h.disabledDownstream.add("a"); // a -> b link disabled, so a has no enabled downstream
      h.terminalIds.add("b"); // b is the true end

      (component as any).readConfig();

      expect(component.shownResultIds).toContain("a");
      expect(component.shownResultIds).toContain("b");
    });

    it("does not treat a disabled operator as terminal", () => {
      // A disabled operator is not in the compiled plan, so the engine never materializes it; even with
      // no downstream it must not be shown as a terminal result.
      build(formViewWorkflow).ngOnInit();
      saved();
      h.graphOperators.push({ operatorID: "off", operatorType: "Limit", isDisabled: true });
      h.terminalIds.add("off"); // no downstream, but disabled

      (component as any).readConfig();

      expect(component.shownResultIds).toEqual([]);
    });

    it("does not show a disabled step's result, even with its eye on and picked", () => {
      // The eye and the pick survive disabling the step, but the compiled plan leaves the step out, so
      // there is never a result behind such a card.
      build(formViewWorkflow).ngOnInit();
      saved(["off", "last"]);
      h.graphOperators.push({ operatorID: "off", operatorType: "Filter", isDisabled: true });
      h.graphOperators.push({ operatorID: "last", operatorType: "Limit" });
      h.viewResultIds.add("off");
      h.terminalIds.add("last");

      (component as any).readConfig();

      expect(component.shownResultIds).toEqual(["last"]);
    });

    it("drops a card when the canvas view-result set changes, without a result update", () => {
      build(formViewWorkflow).ngOnInit();
      saved(["a", "b"]);
      h.viewResultIds.add("a");
      h.viewResultIds.add("b");
      (component as any).readConfig();
      expect(component.shownResultIds).toEqual(["a", "b"]);

      // A co-editor turns b's eye off on the canvas. This emits no result-update event, so the
      // filter must react to the view-result set changing directly, or b's card would go stale.
      h.viewResultIds.delete("b");
      h.viewResultChanged.next({});

      expect(component.shownResultIds).toEqual(["a"]);
    });

    it("cards only the chosen, viewed steps that actually produced a result", () => {
      build(formViewWorkflow).ngOnInit();
      saved(["produces", "produces-nothing"]);
      h.viewResultIds.add("produces");
      h.viewResultIds.add("produces-nothing");
      h.anyResultIds.add("produces"); // the other ran but yielded nothing (e.g. a download UDF)

      (component as any).readConfig();

      expect(component.resultIdsToShow).toEqual(["produces"]);
      expect(component.hasResults).toBe(true);
    });

    it("has no results when nothing chosen has produced anything", () => {
      build(formViewWorkflow).ngOnInit();
      saved(["a"]);
      h.viewResultIds.add("a");
      (component as any).readConfig();

      expect(component.hasResults).toBe(false);
    });

    it("calls a paginated result a table, and gates visualisation content on a snapshot", () => {
      build(formViewWorkflow).ngOnInit();
      (component as any).workflowResultService.hasPaginatedResult = (id: string) => id === "tab";

      expect(component.isTabularResult("tab")).toBe(true);
      expect(component.vizHasContent("tab")).toBe(false); // tables take the tabular branch
      // A non-tabular op with a non-empty snapshot has viz content; an empty one does not.
      h.snapshotById.set("viz", [{ a: 1 }]);
      expect(component.vizHasContent("viz")).toBe(true);
      expect(component.vizHasContent("blank")).toBe(false);
    });

    it("labels a result by the operator's friendly name, falling back to the id", () => {
      build(formViewWorkflow).ngOnInit();
      h.graphOperators.push({ operatorID: "op-1", operatorType: "CSVFileScan" });

      expect(component.resultLabel("op-1")).toBe("CSVFileScan");
      expect(component.resultLabel("gone")).toBe("gone");
    });

    it("keeps a result's frame identity stable until its version moves", () => {
      build(formViewWorkflow).ngOnInit();
      const before = component.resultKey("op-1");
      expect(component.resultKey("op-1")).toBe(before);

      (component as any).resultVersion.set("op-1", 1);

      expect(component.resultKey("op-1")).not.toBe(before);
      expect(component.trackByKey(0, "k")).toBe("k");
    });

    it("resizes a result within bounds, per result, and re-fits after", () => {
      vi.useFakeTimers();
      build(formViewWorkflow).ngOnInit();
      const fit = vi.spyOn(component as any, "fitVisualisations").mockImplementation(() => {});
      expect(component.resultZoom("op-1")).toBe(1);

      component.zoomResult("op-1", 1);
      component.zoomResult("op-1", 1);
      expect(component.resultZoom("op-1")).toBe(2); // clamped at 2

      component.zoomResult("op-1", -1);
      component.zoomResult("op-1", -1);
      component.zoomResult("op-1", -1);
      expect(component.resultZoom("op-1")).toBe(0); // clamped at 0
      expect(component.resultZoom("op-2")).toBe(1); // untouched

      // The deferred re-fit runs after the card height lands.
      vi.advanceTimersByTime(60);
      expect(fit).toHaveBeenCalled();
      vi.useRealTimers();
    });

    it("bumps the result version and re-fits on a result update", () => {
      vi.useFakeTimers();
      build(formViewWorkflow).ngOnInit();
      const fit = vi.spyOn(component as any, "fitVisualisations").mockImplementation(() => {});

      h.resultUpdateStream.next({ "op-1": {} });
      expect(component.resultKey("op-1")).toBe("op-1#1");
      vi.advanceTimersByTime(300);
      expect(fit).toHaveBeenCalled();
      vi.useRealTimers();
    });

    it("re-fits the charts once a finished run has results", () => {
      vi.useFakeTimers();
      build(formViewWorkflow).ngOnInit();
      vi.spyOn(component, "hasResults", "get").mockReturnValue(true);
      const fit = vi.spyOn(component as any, "fitVisualisations").mockImplementation(() => {});

      h.executionStateStream.next({ current: { state: ExecutionState.Completed } });
      vi.advanceTimersByTime(400);

      expect(fit).toHaveBeenCalled();
      vi.useRealTimers();
    });

    it("gives the result tables a realistic page height on init", () => {
      build(formViewWorkflow).ngOnInit();
      expect(h.panelResizeService.changePanelSize).toHaveBeenCalled();
    });
  });

  describe("reporting a failed run", () => {
    it("blames empty required inputs when a required field is left empty", () => {
      build(formViewWorkflow).ngOnInit();
      const form = new FormGroup({ v: new FormControl("", Validators.required) });
      component.rendered = [{ form } as any];

      h.executionStateStream.next({ current: { state: ExecutionState.Failed, errorMessages: [{ message: "x" }] } });

      expect(component.runError).toBe("Run failed: please fill in the required fields.");
    });

    it("finds a required error nested inside an array input", () => {
      build(formViewWorkflow).ngOnInit();
      const form = new FormGroup({ arr: new FormArray([new FormControl("", Validators.required)]) });
      component.rendered = [{ form } as any];

      h.executionStateStream.next({ current: { state: ExecutionState.Failed, errorMessages: [{ message: "x" }] } });

      expect(component.runError).toBe("Run failed: please fill in the required fields.");
    });

    it("does not blame required fields for a non-required validation error", () => {
      build(formViewWorkflow).ngOnInit();
      // A pattern failure, not an empty required field: the reader gets the engine message, not
      // "fill in the required fields".
      const form = new FormGroup({ v: new FormControl("abc", Validators.pattern(/^\d+$/)) });
      component.rendered = [{ form } as any];

      h.executionStateStream.next({ current: { state: ExecutionState.Failed, errorMessages: [{ message: "boom" }] } });

      expect(component.runError).toBe("Run failed: boom");
    });

    it("keeps a short human message, dropping the exception prefix", () => {
      build(formViewWorkflow).ngOnInit();

      h.executionStateStream.next({
        current: {
          state: ExecutionState.Failed,
          errorMessages: [{ message: "java.lang.RuntimeException: too many rows" }],
        },
      });

      expect(component.runError).toBe("Run failed: too many rows");
    });

    it("collapses an opaque engine trace to a reload sentence", () => {
      build(formViewWorkflow).ngOnInit();

      h.executionStateStream.next({
        current: {
          state: ExecutionState.Failed,
          errorMessages: [{ message: "org.jooq.DataAccessException: SQL [..]" }],
        },
      });

      expect(component.runError).toBe("Run failed -- please reload and try again.");
    });

    it("collapses an empty error message to the reload sentence too", () => {
      build(formViewWorkflow).ngOnInit();

      h.executionStateStream.next({ current: { state: ExecutionState.Failed, errorMessages: [] } });

      expect(component.runError).toBe("Run failed -- please reload and try again.");
    });

    it("gives a generic tail when the message cleans down to nothing", () => {
      build(formViewWorkflow).ngOnInit();

      h.executionStateStream.next({
        current: { state: ExecutionState.Failed, errorMessages: [{ message: "requirement failed: " }] },
      });

      expect(component.runError).toBe("Run failed: please check your inputs and try again.");
    });
  });

  describe("inspecting a step read-only", () => {
    const withOp = () => {
      h.hasOperatorIds.add("op-1");
      h.graphOperators.push({ operatorID: "op-1", operatorType: "Filter" });
    };

    // Model a highlight the way the real graph does: the stream emits only the newly-highlighted
    // ids (the delta), while getCurrentHighlightedOperatorIDs returns the whole selection. So set
    // the full selection first, then emit the delta.
    const highlight = (full: string[], delta: string[] = full) => {
      h.highlightedIds.length = 0;
      h.highlightedIds.push(...full);
      h.highlightStream.next(delta);
    };

    it("turns highlighting on so a click selects a step", () => {
      build(formViewWorkflow).ngOnInit();
      expect(workflowActionService.setHighlightingEnabled).toHaveBeenCalledWith(true);
    });

    it("opens the read-only panel for the clicked step", () => {
      build(formViewWorkflow).ngOnInit();
      withOp();

      highlight(["op-1"]);

      expect(component.selectedOperatorId).toBe("op-1");
    });

    it("never broadcasts editing itself: silence is delegated to the panel (actsAsEditor=false)", () => {
      build(formViewWorkflow).ngOnInit();
      withOp();

      highlight(["op-1"]);

      // The form component does not touch the co-editor channel at all; the panel is mounted with
      // [actsAsEditor]="false", which suppresses every write at the frame (the only writer).
      // The frame's suppression is covered in operator-property-edit-frame.component.spec.ts.
      expect(h.updateSharedModelAwareness).not.toHaveBeenCalled();
    });

    it("clears the selection when the clicked step is not on the graph", () => {
      build(formViewWorkflow).ngOnInit();
      (component as any).selectedOperatorId = "old";

      highlight(["ghost"]);

      expect(component.selectedOperatorId).toBeUndefined();
    });

    it("closes the panel when the canvas clears its highlight", () => {
      build(formViewWorkflow).ngOnInit();
      withOp();
      highlight(["op-1"]);

      h.highlightedIds.length = 0; // nothing highlighted any more
      h.unhighlightStream.next([]);

      expect(component.selectedOperatorId).toBeUndefined();
    });

    it("opens the panel on the one step left after dropping one of two selected", () => {
      build(formViewWorkflow).ngOnInit();
      withOp();
      h.hasOperatorIds.add("op-2");
      h.graphOperators.push({ operatorID: "op-2", operatorType: "Filter" });

      highlight(["op-1", "op-2"], ["op-2"]);
      expect(component.selectedOperatorId).toBeUndefined(); // two selected: no single step to show

      // Ctrl-clicking op-1 off leaves exactly one selected, which has to OPEN the panel. Only the
      // un-highlight stream fires here -- nothing was newly highlighted -- so that stream has to
      // apply the same rule as the highlight stream, not just test for an empty selection.
      h.highlightedIds.length = 0;
      h.highlightedIds.push("op-2");
      h.unhighlightStream.next(["op-1"]);

      expect(component.selectedOperatorId).toBe("op-2");
    });

    it("keeps the panel closed while more than one step is still highlighted", () => {
      build(formViewWorkflow).ngOnInit();
      withOp();
      highlight(["op-1", "op-2", "op-3"], ["op-2", "op-3"]);

      h.highlightedIds.splice(h.highlightedIds.indexOf("op-3"), 1); // two left
      h.unhighlightStream.next(["op-3"]);

      expect(component.selectedOperatorId).toBeUndefined();
    });

    it("dismisses the panel via the close button, dropping the highlight for co-editors too", () => {
      build(formViewWorkflow).ngOnInit();
      withOp();
      highlight(["op-1"]);

      component.closeOperatorPanel();

      // Through the action service, whose unhighlight also publishes the new selection on the
      // shared awareness channel. Calling the joint wrapper's method directly would drop the ring
      // locally and leave co-editors still seeing it on this reader's behalf.
      expect(h.serviceUnhighlightOperators).toHaveBeenCalledWith("op-1");
      expect(h.updateSharedModelAwareness).toHaveBeenCalledWith("highlighted", []);
      expect(component.selectedOperatorId).toBeUndefined();
    });

    it("ignores a multi-select highlight, closing the panel (no single step to show)", () => {
      build(formViewWorkflow).ngOnInit();
      withOp();
      highlight(["op-1"]);
      expect(component.selectedOperatorId).toBe("op-1");

      // Shift-clicking a second step: the stream emits only the new id, but the full selection is
      // now two, so the panel closes rather than opening whichever was clicked last.
      highlight(["op-1", "op-2"], ["op-2"]);

      expect(component.selectedOperatorId).toBeUndefined();
    });
  });

  describe("author mode", () => {
    it("enters edit mode: opens the workflow, enables modification, re-reads the config", () => {
      build(formViewWorkflow).ngOnInit();
      const read = vi.spyOn(component as any, "readConfig");

      component.toggleAuthoring();

      expect(component.authoring).toBe(true);
      expect(component.workflowOpen).toBe(true);
      expect(h.workflowActionService.enableWorkflowModification).toHaveBeenCalled();
      expect(read).toHaveBeenCalled();
    });

    it("leaves edit mode: collapses the workflow and locks modification back", () => {
      build(formViewWorkflow).ngOnInit();
      component.toggleAuthoring();
      (h.workflowActionService.disableWorkflowModification as any).mockClear();

      component.toggleAuthoring();

      expect(component.authoring).toBe(false);
      expect(component.workflowOpen).toBe(false);
      expect(h.workflowActionService.disableWorkflowModification).toHaveBeenCalled();
    });

    it("refuses to enter edit mode without write access, at the method and not only the button", () => {
      // The Edit button is not rendered for a reader, but every authoring action writes the shared
      // config, so the method itself is the boundary: a reader stays a reader whoever calls it.
      build({ ...formViewWorkflow, readonly: true }).ngOnInit();
      expect(component.canEdit).toBe(false);
      const read = vi.spyOn(component as any, "readConfig");

      component.toggleAuthoring();

      expect(component.authoring).toBe(false);
      expect(read).not.toHaveBeenCalled();
      expect(h.workflowActionService.enableWorkflowModification).not.toHaveBeenCalled();
    });

    it("always allows leaving edit mode, even if write access is gone", () => {
      build(formViewWorkflow).ngOnInit();
      component.toggleAuthoring();
      expect(component.authoring).toBe(true);
      component.canEdit = false;

      component.toggleAuthoring();

      expect(component.authoring).toBe(false);
      expect(h.workflowActionService.disableWorkflowModification).toHaveBeenCalled();
    });

    it("shows broken inputs to an author but never to a reader", () => {
      build(formViewWorkflow).ngOnInit();
      (component as any).parameters = [resolved("b", "B", { brokenReason: "gone" })];

      expect(component.visibleFields).toEqual([]);
      component.authoring = true;
      expect(component.visibleFields.length).toBe(1);
    });

    it("renders a broken input as an empty card carrying its reason", () => {
      build(formViewWorkflow).ngOnInit();
      component.authoring = true;
      h.formBindingService.resolveFields.mockReturnValue([resolved("b", "B", { brokenReason: "gone" })]);

      (component as any).readConfig();

      expect(component.rendered[0].fields).toEqual([]);
      expect(component.rendered[0].resolved.brokenReason).toBe("gone");
    });

    it("keeps an input whose operator is gone, in either mode, for the author to remove explicitly", () => {
      // Re-reading the config must not rewrite it. A broken input reaches the author as a card with
      // its reason (the two tests above) and leaves only through onRemoveBinding; dropping it on the
      // way in would be a silent config write and would hide where the input went.
      build(formViewWorkflow).ngOnInit();
      (component as any).loading = false;
      h.hasOperatorIds.add("op-1");
      h.formBindingService.getConfig.mockReturnValue({
        instruction: undefined,
        fields: [
          { id: "gone", operatorID: "removed" },
          { id: "b", operatorID: "op-1" },
        ],
      });

      (component as any).readConfig();
      component.authoring = true;
      (component as any).readConfig();

      expect(h.formBindingService.setFields).not.toHaveBeenCalled();
      expect(h.formBindingService.removeBinding).not.toHaveBeenCalled();
    });

    it("lists the final steps and the viewed and chosen intermediate steps, with their shown state", () => {
      build(formViewWorkflow).ngOnInit();
      component.authoring = true;
      h.graphOperators.push({ operatorID: "viewed-mid", operatorType: "Filter" });
      h.graphOperators.push({ operatorID: "chosen-mid", operatorType: "Filter" });
      h.graphOperators.push({ operatorID: "stale-pick", operatorType: "Filter" });
      h.graphOperators.push({ operatorID: "plain-mid", operatorType: "Filter" });
      h.graphOperators.push({ operatorID: "last", operatorType: "Limit" });
      h.graphOperators.push({ operatorID: "last-off", operatorType: "Limit" });
      // Disabled steps keep their eye and can even be in the saved picks, but are left out of the run,
      // so neither is offered: featuring one could never show anyone anything.
      h.graphOperators.push({ operatorID: "off-viewed", operatorType: "Filter", isDisabled: true });
      h.graphOperators.push({ operatorID: "off-chosen", operatorType: "Filter", isDisabled: true });
      h.viewResultIds.add("viewed-mid");
      h.viewResultIds.add("chosen-mid");
      h.viewResultIds.add("off-viewed");
      h.terminalIds.add("last");
      h.terminalIds.add("last-off");
      // The author's saved list: last on, last-off left out, two intermediates and a disabled step.
      h.formBindingService.getConfig.mockReturnValue({
        instruction: undefined,
        fields: [],
        shownResultIds: ["chosen-mid", "stale-pick", "off-chosen", "last"],
      });

      (component as any).readConfig();

      const byId = new Map(component.resultChoices.map(c => [c.operatorID, c]));
      expect(byId.get("viewed-mid")).toMatchObject({ shown: false }); // has the eye, not on the list
      expect(byId.get("chosen-mid")).toMatchObject({ shown: true }); // eye and on the list
      // A listed step whose eye was turned off on the canvas stays offered (so it can be taken off the
      // list) but reads as off: the pill shows what is actually displayed, and nothing is produced for
      // it any more.
      expect(byId.get("stale-pick")).toMatchObject({ shown: false });
      expect(byId.has("plain-mid")).toBe(false); // no eye, not listed -> nothing to show, not offered
      // The final steps are always offered, under their plain name like any other step.
      expect(byId.get("last")).toMatchObject({ shown: true, label: "Limit" });
      expect(byId.get("last-off")).toMatchObject({ shown: false }); // left off the list by the author
      expect(byId.has("off-viewed")).toBe(false); // disabled: not in the run, eye or no eye
      expect(byId.has("off-chosen")).toBe(false); // disabled: not in the run, listed or not
    });

    it("adds a step to the picker the moment its eye is turned on, without a re-read", () => {
      build(formViewWorkflow).ngOnInit();
      component.authoring = true;
      h.graphOperators.push({ operatorID: "mid", operatorType: "Filter" });
      h.formBindingService.getConfig.mockReturnValue({ instruction: undefined, fields: [] });
      (component as any).readConfig();
      expect(component.resultChoices.map(c => c.operatorID)).not.toContain("mid");

      // The author gives "mid" the eye on the canvas: the view-result set changes, emitting no result
      // update, so the picker must react to that stream directly (or the option would not appear).
      h.viewResultIds.add("mid");
      h.viewResultChanged.next({});

      expect(component.resultChoices.map(c => c.operatorID)).toContain("mid");
    });

    it("builds the picker for a reader too, so they can choose what to see", () => {
      build({ ...formViewWorkflow, readonly: true }).ngOnInit();
      h.graphOperators.push({ operatorID: "last", operatorType: "Limit" });
      h.terminalIds.add("last");

      (component as any).readConfig();

      expect(component.resultChoices.map(c => c.operatorID)).toEqual(["last"]);
      expect(component.resultChoices[0].shown).toBe(true);
    });

    it("does not offer a reader a saved pick whose eye is off, since it could never turn on", () => {
      // The author's stale pick stays listed in edit mode so it can be un-picked (see above); for a
      // reader nothing is materialised for it, so a pill they can click but never turn on is left out.
      build({ ...formViewWorkflow, readonly: true }).ngOnInit();
      h.graphOperators.push({ operatorID: "stale-pick", operatorType: "Filter" });
      h.graphOperators.push({ operatorID: "chosen-mid", operatorType: "Filter" });
      h.graphOperators.push({ operatorID: "last", operatorType: "Limit" });
      h.viewResultIds.add("chosen-mid");
      h.terminalIds.add("last");
      h.formBindingService.getConfig.mockReturnValue({
        instruction: undefined,
        fields: [],
        shownResultIds: ["chosen-mid", "stale-pick"],
      });

      (component as any).readConfig();

      expect(component.resultChoices.map(c => c.operatorID).sort()).toEqual(["chosen-mid", "last"]);
    });

    it("in edit mode, a toggle sets the default for everyone, starting the saved list from the final steps", () => {
      // The service materialises the list on the first choice from the default handed in, which must
      // be the final steps as they are now (the one terminal rule), so what the author saw is kept.
      build(formViewWorkflow).ngOnInit();
      component.authoring = true;
      h.graphOperators.push({ operatorID: "last", operatorType: "Limit" });
      h.terminalIds.add("last");
      const read = vi.spyOn(component as any, "readConfig");

      component.onToggleResult({ operatorID: "op-1", label: "Filter", shown: false });

      expect(h.formBindingService.toggleShownResult).toHaveBeenCalledWith("op-1", ["last"]);
      expect(read).toHaveBeenCalledTimes(1);
    });

    it("a writer merely viewing changes only their own view, like any reader", () => {
      build(formViewWorkflow).ngOnInit();
      expect(component.canEdit).toBe(true);
      expect(component.authoring).toBe(false);
      const read = vi.spyOn(component as any, "readConfig");

      component.onToggleResult({ operatorID: "op-1", label: "Filter", shown: false });

      expect(h.formBindingService.toggleShownResult).not.toHaveBeenCalled();
      expect(read).not.toHaveBeenCalled();
    });

    it("routes a removal through the binding service and re-reads", () => {
      build(formViewWorkflow).ngOnInit();
      const read = vi.spyOn(component as any, "readConfig");

      component.onRemoveBinding(resolved("n", "N", {}));

      expect(h.formBindingService.removeBinding).toHaveBeenCalledWith("n");
      expect(read).toHaveBeenCalledTimes(1);
    });

    it("saves help text without rebuilding the form (no readConfig on every keystroke)", () => {
      build(formViewWorkflow).ngOnInit();
      const read = vi.spyOn(component as any, "readConfig");

      component.onEditHelpText(resolved("n", "N", {}), "help");

      expect(h.formBindingService.updateBinding).toHaveBeenCalledWith("n", { helpText: "help" });
      expect(read).not.toHaveBeenCalled();
    });

    it("reorders the saved field the dragged card names, not the raw rendered index", () => {
      build(formViewWorkflow).ngOnInit();
      // rendered is shorter than the saved fields: 'b' rendered no card (its schema was unavailable).
      component.rendered = [{ resolved: { binding: { id: "a" } } }, { resolved: { binding: { id: "c" } } }] as any;
      h.formBindingService.getConfig.mockReturnValue({
        instruction: undefined,
        fields: [{ id: "a" }, { id: "b" }, { id: "c" }],
      });

      // Drag rendered[1] ("c", saved index 2) to the top (onto rendered[0] "a", saved index 0).
      component.onDrop({ previousIndex: 1, currentIndex: 0 } as any);

      expect(h.formBindingService.reorder).toHaveBeenCalledWith(2, 0);
    });

    it("moves a card one place from the keyboard through the same reorder as the drag", () => {
      build(formViewWorkflow).ngOnInit();
      // Three cards, but 'b' is not among the saved fields' neighbours in the same order (a saved
      // field that rendered no card sits between), so the move has to resolve by id, as the drag does.
      const cards = [
        { resolved: { binding: { id: "a" } } },
        { resolved: { binding: { id: "b" } } },
        { resolved: { binding: { id: "c" } } },
      ] as any;
      component.rendered = cards;
      h.formBindingService.getConfig.mockReturnValue({
        instruction: undefined,
        fields: [{ id: "a" }, { id: "hidden" }, { id: "b" }, { id: "c" }],
      });

      component.onMoveBinding(cards[1], -1); // 'b' (saved 2) up onto 'a' (saved 0)
      expect(h.formBindingService.reorder).toHaveBeenCalledWith(2, 0);

      // The move re-reads the config, which rebuilds `rendered` from the (mocked, empty) resolved
      // fields; put the cards back to move again.
      component.rendered = cards;
      component.onMoveBinding(cards[1], 1); // 'b' (saved 2) down onto 'c' (saved 3)
      expect(h.formBindingService.reorder).toHaveBeenCalledWith(2, 3);
    });

    it("moves nothing off either end", () => {
      build(formViewWorkflow).ngOnInit();
      const cards = [{ resolved: { binding: { id: "a" } } }, { resolved: { binding: { id: "b" } } }] as any;
      component.rendered = cards;
      h.formBindingService.getConfig.mockReturnValue({
        instruction: undefined,
        fields: [{ id: "a" }, { id: "b" }],
      });

      component.onMoveBinding(cards[0], -1);
      component.onMoveBinding(cards[1], 1);

      expect(h.formBindingService.reorder).not.toHaveBeenCalled();
    });

    it("drops a reorder whose card names a field the config no longer holds", () => {
      build(formViewWorkflow).ngOnInit();
      // A card left over from a config that has since changed: its binding is gone from the saved
      // fields, so neither end of the drag resolves. Reordering on those -1s would move the wrong
      // field, so the drag is dropped instead.
      component.rendered = [{ resolved: { binding: { id: "gone" } } }, { resolved: { binding: { id: "a" } } }] as any;
      h.formBindingService.getConfig.mockReturnValue({
        instruction: undefined,
        fields: [{ id: "a" }],
      });
      const read = vi.spyOn(component as any, "readConfig");

      component.onDrop({ previousIndex: 0, currentIndex: 1 } as any);

      expect(h.formBindingService.reorder).not.toHaveBeenCalled();
      expect(read).not.toHaveBeenCalled();
    });

    it("saves the instruction as the author types, and previews on demand", () => {
      build(formViewWorkflow).ngOnInit();
      component.instructionTitle = "T";
      component.instructionBody = "B";

      component.onInstructionChange();
      expect(h.formBindingService.updateConfig).toHaveBeenCalledWith({ instruction: { title: "T", body: "B" } });

      const render = vi.spyOn(component as any, "renderInstruction");
      component.setInstructionMode("write");
      expect(render).not.toHaveBeenCalled();
      component.setInstructionMode("preview");
      expect(component.instructionMode).toBe("preview");
      expect(render).toHaveBeenCalled();
    });

    it("wires the editable title to rename the input and a sub-field to rename or hide it", () => {
      build(formViewWorkflow).ngOnInit();
      component.authoring = true;
      h.hasOperatorIds.add("op-1");
      h.formBindingService.resolveFields.mockReturnValue([
        resolved("n", "N", {
          binding: { id: "n", operatorID: "op-1", propertyKey: "nested", displayName: "N", overrides: {} },
        }),
      ]);

      (component as any).readConfig();
      const root = component.rendered[0].fields[0] as any;
      const read = vi.spyOn(component as any, "readConfig");

      // The input's own title renames the whole binding.
      root.props.renameField("New name");
      expect(h.formBindingService.updateBinding).toHaveBeenCalledWith("n", { displayName: "New name" });

      // A sub-field's editable label renames it and its eye hides it, both keyed by path.
      const sub = root.fieldGroup[0];
      sub.props.renameField("Sub name");
      expect(h.formBindingService.setFieldOverride).toHaveBeenCalledWith("n", "sub", { displayName: "Sub name" });
      sub.props.setFieldHidden(true);
      expect(h.formBindingService.setFieldOverride).toHaveBeenCalledWith("n", "sub", { hidden: true });

      // Presentation only, shown by the wrapper itself: no rebuild, so the name box or the eye the
      // author is on is not replaced under their focus.
      expect(read).not.toHaveBeenCalled();
    });

    it("hands the focus to the next card's Remove after a removal, else the previous one's", async () => {
      build(formViewWorkflow).ngOnInit();
      const focused: string[] = [];
      // Only the focus targets are answered; the name-width measuring the page also runs off the host
      // gets null, as the harness gives it.
      (component as any).host = {
        nativeElement: {
          contains: () => true,
          querySelector: (selector: string) =>
            selector.startsWith(".remove[") ? { focus: () => focused.push(selector) } : null,
        },
      };
      const cards = [
        { resolved: { binding: { id: "a" } } },
        { resolved: { binding: { id: "b" } } },
        { resolved: { binding: { id: "c" } } },
      ] as any;

      component.rendered = cards;
      component.onRemoveBinding(cards[1].resolved);
      await new Promise(r => setTimeout(r, 10));
      expect(h.formBindingService.removeBinding).toHaveBeenCalledWith("b");
      expect(focused).toEqual(['.remove[data-binding="c"]']);

      // The last card has no next: its predecessor takes the focus.
      component.rendered = cards;
      component.onRemoveBinding(cards[2].resolved);
      await new Promise(r => setTimeout(r, 10));
      expect(focused[1]).toBe('.remove[data-binding="b"]');
    });

    it("names a card's controls with the author's name, else the property key", () => {
      build(formViewWorkflow).ngOnInit();

      expect(
        component.cardName({ resolved: { binding: { displayName: "File", propertyKey: "fileName" } } } as any)
      ).toBe("File");
      expect(component.cardName({ resolved: { binding: { displayName: "", propertyKey: "fileName" } } } as any)).toBe(
        "fileName"
      );
    });
  });
});
