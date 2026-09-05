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

import { FormControl } from "@angular/forms";
import { Router } from "@angular/router";
import { of, throwError } from "rxjs";

import { WorkflowFormComponent } from "./workflow-form.component";
import { setupHarness, formViewWorkflow, resolved } from "./workflow-form.spec-harness";
import { USER_WORKFLOW, USER_WORKSPACE } from "../../../app-routing.constant";
import { DefaultView } from "../../../dashboard/type/workflow-metadata.interface";
import { FORM_DEBOUNCE_TIME_MS } from "../../service/execute-workflow/execute-workflow.service";

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
      h.formlyJsonschema as any,
      h.cdr as any,
      h.dynamicSchemaService as any,
      h.workflowCompilingService as any,
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

    it("does not rebuild under the cursor of someone typing", async () => {
      build(formViewWorkflow).ngOnInit();
      vi.spyOn(component as any, "isTypingInTheForm").mockReturnValue(true);
      const rebuild = vi.spyOn(component as any, "readConfig");

      h.compilationChanged.next("Succeeded");
      await new Promise(r => setTimeout(r, FORM_DEBOUNCE_TIME_MS + 50));

      expect(rebuild).not.toHaveBeenCalled();
    });

    it("re-reads the config when a property is exposed or un-exposed", () => {
      build(formViewWorkflow).ngOnInit();
      const before = formBindingService.resolveFields.mock.calls.length;

      workflowActionService.formBindingChanged$.next(undefined);

      expect(formBindingService.resolveFields.mock.calls.length).toBeGreaterThan(before);
    });

    // Once #8351 makes this stream fire for a co-editor's change, a rebuild under the cursor would
    // discard a half-entered value -- so the binding path skips typing, like the compilation path.
    it("does not re-read the config on a binding change while the reader is typing", () => {
      build(formViewWorkflow).ngOnInit();
      vi.spyOn(component as any, "isTypingInTheForm").mockReturnValue(true);
      const rebuild = vi.spyOn(component as any, "readConfig");

      workflowActionService.formBindingChanged$.next(undefined);

      expect(rebuild).not.toHaveBeenCalled();
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
});
