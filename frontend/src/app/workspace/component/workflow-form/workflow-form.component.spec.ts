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
import { throwError } from "rxjs";

import { WorkflowFormComponent } from "./workflow-form.component";
import { setupHarness, formViewWorkflow } from "./workflow-form.spec-harness";
import { USER_WORKFLOW, USER_WORKSPACE } from "../../../app-routing.constant";
import { DefaultView } from "../../../dashboard/type/workflow-metadata.interface";

/**
 * These exercise the shell's own decisions -- what a reader is shown and where an ordinary
 * workflow is sent -- without standing up the JointJS canvas. The component is built directly
 * (not through TestBed) with the shared spec harness's mocks; the title bar, save and preview
 * are added, with their own tests, by later PRs.
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
});
