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

import { Router } from "@angular/router";
import { of, throwError } from "rxjs";

import { WorkflowFormComponent } from "./workflow-form.component";
import { setupHarness, formViewWorkflow } from "./workflow-form.spec-harness";
import { USER_WORKFLOW, USER_WORKSPACE } from "../../../app-routing.constant";
import { DefaultView } from "../../../dashboard/type/workflow-metadata.interface";

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

  const build = (workflow: any) => {
    h.useWorkflow(workflow);
    component = new WorkflowFormComponent(
      h.coeditorPresenceService as any,
      h.route as any,
      h.router as unknown as Router,
      h.workflowActionService as any,
      h.workflowPersistService as any,
      h.operatorMetadataService as any,
      h.executeWorkflowService as any,
      h.workflowResultService as any,
      h.notificationService as any,
      h.userService as any,
      h.cdr as any,
      h.computingUnitStatusService as any,
      h.workflowConsoleService as any,
      h.host as any,
      h.datePipe as any,
      h.config as any
    );
    return component;
  };

  beforeEach(() => {
    h = setupHarness();
    router = h.router;
    workflowActionService = h.workflowActionService;
    workflowPersistService = h.workflowPersistService;
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

    it("goes back to the list when the workflow cannot be opened", () => {
      build(formViewWorkflow);
      workflowPersistService.retrieveWorkflow.mockReturnValue(throwError(() => new Error("denied")));

      component.ngOnInit();

      expect(h.notificationService.error).toHaveBeenCalled();
      expect(router.navigate).toHaveBeenCalledWith([USER_WORKFLOW]);
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

    it("saves before handing over to the operator canvas", () => {
      enableSave();
      build(formViewWorkflow).ngOnInit();
      workflowPersistService.persistWorkflow.mockClear();

      component.openRegularCanvas();

      expect(workflowPersistService.persistWorkflow).toHaveBeenCalled();
    });

    it("saves once more on the way out", () => {
      enableSave();
      build(formViewWorkflow).ngOnInit();
      workflowPersistService.persistWorkflow.mockClear();

      component.ngOnDestroy();

      expect(workflowPersistService.persistWorkflow).toHaveBeenCalled();
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
});
