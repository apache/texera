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

import { ChangeDetectorRef, Component, HostListener, OnDestroy, OnInit } from "@angular/core";
import { CommonModule } from "@angular/common";
import { ActivatedRoute, Router } from "@angular/router";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzAvatarModule } from "ng-zorro-antd/avatar";
import { UserIconComponent } from "../../../dashboard/component/user/user-icon/user-icon.component";
import { forkJoin } from "rxjs";

import { USER_WORKFLOW, USER_WORKSPACE } from "../../../app-routing.constant";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { UserService } from "../../../common/service/user/user.service";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { WorkflowConsoleService } from "../../service/workflow-console/workflow-console.service";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { CoeditorUserIconComponent } from "../menu/coeditor-user-icon/coeditor-user-icon.component";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";

/**
 * The Form View: a second way to use a workflow. This PR lays down the page shell -- behind
 * the feature flag it loads the workflow the URL names, shows it read-only, and hands back to
 * the operator canvas. The title bar's rename/save, the read-only preview, the inputs, running
 * and results are added on top by later PRs. A view, not a new object: it opens the same
 * workflow the canvas does.
 */
@UntilDestroy()
@Component({
  selector: "texera-workflow-form",
  templateUrl: "./workflow-form.component.html",
  styleUrls: ["./workflow-form.component.scss"],
  imports: [CommonModule, NzAvatarModule, UserIconComponent, CoeditorUserIconComponent],
})
export class WorkflowFormComponent implements OnInit, OnDestroy {
  public wid?: number;
  public workflowName = "";
  public loading = true;

  constructor(
    // Public for the template: shows the same live collaborator avatars as the canvas.
    public coeditorPresenceService: CoeditorPresenceService,
    private route: ActivatedRoute,
    private router: Router,
    private workflowActionService: WorkflowActionService,
    private workflowPersistService: WorkflowPersistService,
    private operatorMetadataService: OperatorMetadataService,
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowResultService: WorkflowResultService,
    private notificationService: NotificationService,
    private userService: UserService,
    private cdr: ChangeDetectorRef,
    private computingUnitStatusService: ComputingUnitStatusService,
    private workflowConsoleService: WorkflowConsoleService,
    private config: GuiConfigService
  ) {}

  ngOnInit(): void {
    const wid = Number(this.route.snapshot.params.id);
    if (!Number.isFinite(wid)) {
      void this.router.navigate([USER_WORKFLOW]);
      return;
    }
    this.wid = wid;
    this.load(wid);
  }

  private load(wid: number): void {
    // With the feature off the form does not exist: hand straight to the operator canvas
    // without loading anything, so a request that then fails cannot strand the visitor on
    // an error instead of the page they would have gotten.
    if (!this.config.env.formViewEnabled) {
      void this.router.navigate([USER_WORKSPACE, String(wid)], { replaceUrl: true });
      return;
    }
    this.workflowActionService.resetAsNewWorkflow();
    forkJoin({
      metadata: this.operatorMetadataService.getOperatorMetadata(),
      workflow: this.workflowPersistService.retrieveWorkflow(wid),
    })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: ({ workflow }) => {
          // With the flag on, the form renders for any workflow: default_view only decides
          // which view a workflow lands on by default, not whether the form is reachable
          // (settled on #8011). Gating the form on default_view here would quietly reintroduce
          // a per-workflow switch -- and bounce a later PR's canvas-to-form switch straight
          // back for any canvas-default workflow.
          this.workflowName = workflow.name;
          this.workflowActionService.setNewSharedModel(wid, this.userService.getCurrentUser());
          this.workflowActionService.reloadWorkflow(workflow);
          // The workflow is shown, not edited, from here: dragging operators around or
          // deleting them belongs to the operator canvas.
          this.applyEditability();
          this.loading = false;
          this.cdr.detectChanges();
        },
        // The load can fail for many reasons (no access, a network or server error, the
        // metadata call): a neutral message covers them without claiming it was permissions.
        error: () => {
          this.notificationService.error("Unable to open this workflow.");
          void this.router.navigate([USER_WORKFLOW]);
        },
      });
  }

  /**
   * Show the workflow rather than edit it: the graph shape and its properties are read-only
   * on this page. A later PR's authoring mode makes properties editable with write access.
   */
  private applyEditability(): void {
    this.workflowActionService.disableWorkflowModification();
  }

  /**
   * Switch to the operator canvas with a full page load, not a route. The two views share
   * root-level singletons (the graph, the Yjs shared model, the CU connection); handing
   * over in-process left the old state attached -- undraggable operators, a ghost coeditor
   * of yourself, broken runs. A fresh document is the reliable handover.
   */
  public openRegularCanvas(): void {
    /* v8 ignore start -- full-document navigation; jsdom cannot navigate */
    window.location.href = `${USER_WORKSPACE}/${this.wid}`;
    /* v8 ignore stop */
  }

  /**
   * Tear down exactly what the operator canvas tears down: both views drive the same
   * singleton services, so anything left bound here follows the user to the next page
   * (the symptom was a frozen canvas after a visit -- the old shared model still attached).
   */
  @HostListener("window:beforeunload")
  ngOnDestroy(): void {
    this.workflowActionService.clearWorkflow();
    this.computingUnitStatusService.disconnect();
    this.executeWorkflowService.resetExecutionAndWorkers();
    this.workflowConsoleService.clearConsoleMessages();
    this.workflowResultService.clearResults();
  }
}
