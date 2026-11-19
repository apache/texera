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

import { isMacro } from "../../common/util/workflow-macro";
import { Location, NgIf } from "@angular/common";
import {
  AfterViewInit,
  ChangeDetectorRef,
  Component,
  HostListener,
  Input,
  OnDestroy,
  OnInit,
  ViewChild,
  ViewContainerRef,
} from "@angular/core";
import { ActivatedRoute, Router } from "@angular/router";
import { UserService } from "../../common/service/user/user.service";
import { WorkflowPersistService } from "../../common/service/workflow-persist/workflow-persist.service";
import { Workflow } from "../../common/type/workflow";
import { OperatorMetadataService } from "../service/operator-metadata/operator-metadata.service";
import { UndoRedoService } from "../service/undo-redo/undo-redo.service";
import { WorkflowActionService } from "../service/workflow-graph/model/workflow-action.service";
import { NzMessageService } from "ng-zorro-antd/message";
import { debounceTime, distinctUntilChanged, filter, switchMap, throttleTime, concatMap, map } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { forkJoin, of, from, firstValueFrom } from "rxjs";
import { isDefined } from "../../common/util/predicate";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { WorkflowConsoleService } from "../service/workflow-console/workflow-console.service";
import { OperatorReuseCacheStatusService } from "../service/workflow-status/operator-reuse-cache-status.service";
import { CodeEditorService } from "../service/code-editor/code-editor.service";
import { WorkflowMetadata } from "src/app/dashboard/type/workflow-metadata.interface";
import { EntityType, HubService } from "../../hub/service/hub.service";
import { THROTTLE_TIME_MS } from "../../hub/component/workflow/detail/hub-workflow-detail.component";
import { WorkflowCompilingService } from "../service/compile-workflow/workflow-compiling.service";
import { isLeavingWorkspace, USER_WORKSPACE, macroCanvasUrl, workspaceCanvasUrl } from "../../app-routing.constant";
import { GuiConfigService } from "../../common/service/gui-config.service";
import { ComputingUnitStatusService } from "../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { ExecuteWorkflowService } from "../service/execute-workflow/execute-workflow.service";
import { HeatmapStatsRestoreService } from "../service/heatmap/heatmap-stats-restore.service";
import { WorkflowResultService } from "../service/workflow-result/workflow-result.service";
import { checkIfWorkflowBroken } from "../../common/util/workflow-check";
import { WorkflowVersionService } from "../../dashboard/service/user/workflow-version/workflow-version.service";
import { NzSpinComponent } from "ng-zorro-antd/spin";
import { ResultPanelComponent } from "./result-panel/result-panel.component";
import { WorkflowEditorComponent } from "./workflow-editor/workflow-editor.component";
import { MenuComponent } from "./menu/menu.component";
import { MiniMapComponent } from "./workflow-editor/mini-map/mini-map.component";
import { LeftPanelComponent } from "./left-panel/left-panel.component";
import { AgentPanelComponent } from "./agent/agent-panel/agent-panel.component";
import { PropertyEditorComponent } from "./property-editor/property-editor.component";
import { MacroNavigationService } from "../service/macro/macro-navigation.service";
import { MacroService } from "../service/macro/macro.service";
import { JupyterNotebookPanelComponent } from "./jupyter-notebook-panel/jupyter-notebook-panel.component";

export const SAVE_DEBOUNCE_TIME_IN_MS = 5000;

@UntilDestroy()
@Component({
  selector: "texera-workspace",
  templateUrl: "./workspace.component.html",
  styleUrls: ["./workspace.component.scss"],
  providers: [
    // uncomment this line for manual testing without opening backend server
    // { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
  ],
  imports: [
    NzSpinComponent,
    ResultPanelComponent,
    WorkflowEditorComponent,
    MenuComponent,
    MiniMapComponent,
    LeftPanelComponent,
    NgIf,
    AgentPanelComponent,
    PropertyEditorComponent,
    JupyterNotebookPanelComponent,
  ],
})
export class WorkspaceComponent implements AfterViewInit, OnInit, OnDestroy {
  public writeAccess: boolean = false;
  public isLoading: boolean = false;
  @ViewChild("codeEditor", { read: ViewContainerRef }) codeEditorViewRef!: ViewContainerRef;

  /**
   * Optional agent ID to activate when the workspace loads.
   * When provided (from agent dashboard), the agent panel will open
   * and connect to this agent automatically.
   */
  @Input() agentIdToActivate?: string;

  /**
   * Flag to ensure auto persist is registered only once.  This prevents multiple
   * subscriptions and avoids accidental persistence of an empty workflow
   * before the actual workflow is loaded from backend.
   */
  private autoPersistRegistered = false;

  /**
   * Whether this page opened onto a workflow that was already live, handed over by its Form View
   * rather than loaded from scratch. Decided once, in ngAfterViewInit, before anything can change
   * what it is read from.
   */
  private resumedSession = false;
  private macroNavigationRegistered = false;
  private restoringMacroPath = false;

  constructor(
    private userService: UserService,
    // list additional 3 services in constructor so they are initialized even if no one use them directly
    // TODO: make their lifecycle better
    private workflowCompilingService: WorkflowCompilingService,
    private workflowConsoleService: WorkflowConsoleService,
    private operatorReuseCacheStatusService: OperatorReuseCacheStatusService,
    // end of additional services
    private undoRedoService: UndoRedoService,
    private workflowPersistService: WorkflowPersistService,
    private workflowActionService: WorkflowActionService,
    private location: Location,
    private route: ActivatedRoute,
    private operatorMetadataService: OperatorMetadataService,
    private message: NzMessageService,
    private router: Router,
    private notificationService: NotificationService,
    private hubService: HubService,
    private codeEditorService: CodeEditorService,
    private config: GuiConfigService,
    private changeDetectorRef: ChangeDetectorRef,
    private computingUnitStatusService: ComputingUnitStatusService,
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowResultService: WorkflowResultService,
    private heatmapStatsRestoreService: HeatmapStatsRestoreService,
    public macroNavigation: MacroNavigationService,
    private macros: MacroService,
    private workflowVersionService: WorkflowVersionService
  ) {}

  ngOnInit() {
    this.macroNavigation.changing$.pipe(untilDestroyed(this)).subscribe(() => this.codeEditorViewRef.clear());
    this.macros.inspect$.pipe(untilDestroyed(this)).subscribe(id => {
      this.macroNavigation.open(id).catch(error => this.notificationService.error(error.message));
    });
    this.workflowResultService.setMacroAliases?.(port => this.macroNavigation.resultAliases(port));
    this.workflowActionService.setHighlightingEnabled(true);
    // Clear session state when the user switches computing units in-canvas, so
    // the previous unit's status/console/results don't linger.
    this.computingUnitStatusService
      .getConnectionResetStream()
      .pipe(untilDestroyed(this))
      .subscribe(() => this.resetWorkflowSessionState());
  }

  ngAfterViewInit(): void {
    /**
     * On initialization of the workspace, there could be two cases:
     *
     * 1. Accessed by URL `/`, no workflow is in the URL (Cold Start):
     -    - A new `WorkflowActionService.DEFAULT_WORKFLOW` is created, which is an empty workflow with undefined id.
     *    - After an Auto-persist being triggered by a WorkflowAction event, it will create a new workflow in the database
     *    and update the URL with its new ID from database.
     * 2. Accessed by URL `/workflow/:id` (refresh manually, or redirected from dashboard workflow list):
     *    - It will retrieve the workflow from database with the given ID. Because it has an ID, it will be linked to the database
     *    - Auto-persist will be triggered upon all workspace events.
     *
     * WorkflowActionService is the single source of the workflow representation. WorkflowPersistService reflects
     * changes from WorkflowActionService.
     */
    const widInRoute = this.route.snapshot.params.id;
    // Arriving from the Form View of this same workflow, which kept its session for us (see its
    // ngOnDestroy). The graph is already here and already in its co-editing room, so clearing it
    // and fetching it again would undo exactly what was kept.
    this.resumedSession = widInRoute !== undefined && this.workflowActionService.hasWorkflowOpen(Number(widInRoute));
    if (!this.resumedSession) {
      // clear the current workspace, reset as `WorkflowActionService.DEFAULT_WORKFLOW`
      this.workflowActionService.resetAsNewWorkflow();
      // if a workflow id is present in the route, display loading spinner immediately while loading
      if (widInRoute) {
        this.isLoading = true;
        this.workflowActionService.disableWorkflowModification();
      }
    }
    this.onWIDChange();
    this.updateViewCount();
    this.registerLoadOperatorMetadata();
    this.codeEditorService.vc = this.codeEditorViewRef;
    this.changeDetectorRef.detectChanges();
  }

  /**
   * The browser is leaving this document: save the workflow, and change nothing else.
   *
   * Tearing the session down here was the cause of a page that came back dead. Leaving the
   * document -- a refresh, a closed tab, a URL typed over this one; the Form View switch used to
   * be one, and routes now -- fires this, and the browser may then keep the document in its
   * back/forward cache rather than discarding it. Coming back restores the
   * JavaScript state exactly as it was left, so whatever this method had already destroyed stayed
   * destroyed: an empty graph on a canvas that answered no clicks, and a workflow id reset to the
   * default, which the share dialog then asked the backend about and got an error for. Nothing
   * re-runs on a restore, because the component was never re-created.
   *
   * There is nothing to tear down on the way out anyway. A document that is really discarded takes
   * its websockets and its graph with it, and a document that comes back needs them.
   */
  @HostListener("window:beforeunload")
  onBeforeUnload(): void {
    this.persistBeforeLeaving();
  }

  ngOnDestroy() {
    this.persistBeforeLeaving();
    this.macroNavigation.clear();
    this.codeEditorViewRef.clear();
    // Tear down the connection and all websocket-derived session state so a
    // re-entered workflow starts clean instead of reusing the previous one -- unless the Form
    // View of this same workflow is taking over, in which case the session is handed to it
    // rather than rebuilt, which is what used to make the switch take seconds.
    if (isLeavingWorkspace(this.router, this.workflowActionService.getOpenWorkflowId())) {
      this.workflowActionService.clearWorkflow();
      this.workflowVersionService.discardOpenVersionDisplay();
      this.computingUnitStatusService.disconnect();
      this.resetWorkflowSessionState();
      // The overlay's view lives in the root-provided wrapper; the metrics behind it are cleared
      // just above, so the view goes with them. On a hand-over it stays: the arriving view has
      // already restored the persisted overlay, and the metrics are kept.
      this.workflowActionService.getJointGraphWrapper().setHeatmapView(null);
    }
  }

  private registerMacroNavigation(initialFragment = this.route.snapshot.fragment): void {
    if (this.macroNavigationRegistered) return;
    this.macroNavigationRegistered = true;
    const route = this.route.snapshot.routeConfig?.path;
    if (route === "workflow/:id" || route === "macros/:id") {
      const workflow = this.macroNavigation.rootWorkflow;
      const target = workflow.content.macroInterface
        ? macroCanvasUrl(workflow.wid!)
        : workspaceCanvasUrl(workflow.wid!);
      if (this.router.url.split(/[?#]/)[0] !== target) {
        this.router.navigate([target], {
          replaceUrl: true,
          queryParams: this.route.snapshot.queryParams,
          fragment: initialFragment ?? undefined,
        });
        return;
      }
    }
    const syncURL = (replaceUrl = false) =>
      this.router.navigate([], {
        relativeTo: this.route,
        queryParamsHandling: "merge",
        replaceUrl,
        queryParams: { macro: this.macroNavigation.parents.map(p => p.operatorID) },
        fragment: undefined,
      });
    this.macroNavigation.changed$.pipe(untilDestroyed(this)).subscribe(() => {
      if (!this.macroNavigation.parents.length && !this.workflowActionService.getTempWorkflow())
        this.executeWorkflowService.reapplyExecutionLock();
      if (!this.restoringMacroPath) syncURL();
    });
    this.route.queryParamMap
      .pipe(
        map((params, index) => ({
          path: params.getAll("macro"),
          fragment: index === 0 ? initialFragment : this.route.snapshot.fragment,
        })),
        distinctUntilChanged((a, b) => a.path.length === b.path.length && a.path.every((id, i) => id === b.path[i])),
        concatMap(async ({ path, fragment }) => {
          const current = this.macroNavigation.parents.map(p => p.operatorID);
          let common = 0;
          while (common < path.length && path[common] === current[common]) common++;
          this.restoringMacroPath = true;
          try {
            if (common < current.length) await this.macroNavigation.back(common);
            for (const id of path.slice(common)) await this.macroNavigation.open(id);
          } catch (error) {
            this.notificationService.error(error instanceof Error ? error.message : String(error));
            syncURL(true);
            return;
          } finally {
            this.restoringMacroPath = false;
          }
          this.selectURLFragment(fragment);
        }),
        untilDestroyed(this)
      )
      .subscribe();
  }

  private selectURLFragment(fragment: string | null): void {
    // Loading a graph changes its highlights, so restore the URL after the breadcrumb document is ready.
    this.router.navigate([], {
      relativeTo: this.route,
      fragment: fragment ?? undefined,
      preserveFragment: false,
      queryParamsHandling: "preserve",
    });
    if (!fragment) return;
    if (this.workflowActionService.getTexeraGraph().hasElementWithID(fragment)) {
      this.workflowActionService.highlightElements(false, fragment);
    } else {
      this.notificationService.error(`Element ${fragment} doesn't exist`);
      this.router.navigate([], { relativeTo: this.route, queryParamsHandling: "preserve" });
    }
  }

  public async canLeave(): Promise<boolean> {
    // Restoring the root for an outgoing route must not start another breadcrumb navigation.
    this.restoringMacroPath = true;
    try {
      if (this.macroNavigation.parents.length) await this.macroNavigation.back(0);
      if (this.workflowActionService.getTempWorkflow()) this.workflowVersionService.closeParticularVersionDisplay();
      return true;
    } catch (error) {
      this.notificationService.error(error instanceof Error ? error.message : String(error));
      return false;
    } finally {
      this.restoringMacroPath = false;
    }
  }

  private persistBeforeLeaving(): void {
    if (this.userService.isLogin() && this.workflowPersistService.isWorkflowPersistEnabled()) {
      const workflow = this.workflowActionService.getWorkflow();
      void firstValueFrom(
        this.macroNavigation.parents.length
          ? from(this.macroNavigation.save())
          : this.workflowPersistService.persistWorkflow(workflow)
      ).catch((error: unknown) =>
        this.notificationService.error(error instanceof Error ? error.message : String(error))
      );
    }
  }

  /**
   * Clear websocket-derived session state (execution status, console, results).
   * Shared by workspace teardown and in-canvas unit switches.
   */
  private resetWorkflowSessionState(): void {
    this.executeWorkflowService.resetExecutionAndWorkers();
    this.workflowConsoleService.clearConsoleMessages();
    this.workflowResultService.clearResults();
  }

  registerAutoPersistWorkflow(): void {
    // make sure it is only registered once
    if (this.autoPersistRegistered) {
      return;
    }
    this.autoPersistRegistered = true;

    this.workflowActionService
      .workflowChanged()
      .pipe(
        filter(() => !this.workflowActionService.getJointGraphWrapper().getReloadingWorkflow()),
        debounceTime(SAVE_DEBOUNCE_TIME_IN_MS)
      )
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        if (this.userService.isLogin() && this.workflowPersistService.isWorkflowPersistEnabled()) {
          const wid = this.workflowActionService.getWorkflowMetadata().wid;
          (this.macroNavigation.parents.length
            ? from(this.macroNavigation.save())
            : this.workflowPersistService.persistWorkflow(this.workflowActionService.getWorkflow())
          )
            .pipe(untilDestroyed(this))
            .subscribe({
              next: (updatedWorkflow: Workflow) => {
                if (this.workflowActionService.getWorkflowMetadata().wid !== wid) return;
                if (this.workflowActionService.getWorkflowMetadata().wid !== updatedWorkflow.wid) {
                  this.location.go(`${USER_WORKSPACE}/${updatedWorkflow.wid}`);
                }
                this.workflowActionService.setWorkflowMetadata(updatedWorkflow);
              },
              error: (error: unknown) =>
                this.notificationService.error(error instanceof Error ? error.message : String(error)),
            });
          // to sync up with the updated information, such as workflow.wid
        }
      });
  }

  loadWorkflowWithId(wid: number): void {
    // disable the workspace until the workflow is fetched from the backend
    this.isLoading = true;
    this.workflowActionService.disableWorkflowModification();
    forkJoin({
      operatorMetadata: this.operatorMetadataService.getOperatorMetadata(),
      workflow: this.workflowPersistService.retrieveWorkflow(wid),
    })
      .pipe(untilDestroyed(this))
      .subscribe(
        ({ workflow }) => {
          if (checkIfWorkflowBroken(workflow)) {
            this.notificationService.error(
              "Sorry! The workflow is broken and cannot be persisted. Please contact the system admin."
            );
          }

          this.workflowActionService.setNewSharedModel(wid, this.userService.getCurrentUser());
          // remember URL fragment
          const fragment = this.route.snapshot.fragment;
          // An AI-generated workflow arrives with autolayout=1. Render synchronously
          // (asyncRendering = false) so the operators exist before the one-shot layout runs.
          const shouldAutoLayout = this.route.snapshot.queryParams.autolayout === "1";
          this.workflowActionService.reloadWorkflow(workflow, shouldAutoLayout ? false : undefined);
          this.workflowActionService.enableWorkflowModification();
          // Register before autoLayoutWorkflow(): workflowChanged() streams are hot, so subscribing
          // afterward would drop the layout's position events and the tidied layout would never save.
          this.registerAutoPersistWorkflow();
          if (shouldAutoLayout) {
            this.workflowActionService.autoLayoutWorkflow();
          }
          this.heatmapStatsRestoreService
            .restoreLatestRunStatistics(workflow.content.operators.some(isMacro))
            .pipe(untilDestroyed(this))
            .subscribe({
              error: (err: unknown) => console.error("Failed to restore heat-map statistics:", err),
            });
          // clear stack
          this.undoRedoService.clearUndoStack();
          this.undoRedoService.clearRedoStack();
          this.setLoadingState(false);
          this.triggerCenter();
          this.registerMacroNavigation(fragment);
        },
        () => {
          this.workflowActionService.resetAsNewWorkflow();
          // enable workspace for modification
          this.workflowActionService.enableWorkflowModification();
          // clear stack
          this.undoRedoService.clearUndoStack();
          this.undoRedoService.clearRedoStack();
          this.message.error("You don't have access to this workflow, please log in with an appropriate account");
          this.setLoadingState(false);
        }
      );
  }

  registerLoadOperatorMetadata() {
    const wid = this.route.snapshot.params.id;
    // The Form View handed this workflow over still open: the graph, its co-editing room and the
    // computing unit connection are the ones it was using. Nothing to fetch, nothing to rebuild.
    // The lock the Form View put on the graph is lifted, since editing is what the canvas is for,
    // and the view is centred because this canvas's paper is a new one, at its default offset.
    if (this.resumedSession) {
      // Not an unconditional unlock: a run may still be in flight. The execute service owns the
      // state-to-lock rule and reapplies it only when the state changes, so ask it to apply that
      // rule again rather than restating it here. Unlocking outright left a workflow that was
      // still running editable until its run happened to end. Nothing is announced: the menu
      // reads the current state when it is constructed, and the rest of this page's subscribers
      // want transitions, not a repeat of one that already happened.
      this.executeWorkflowService.reapplyExecutionLock();
      this.registerAutoPersistWorkflow();
      this.triggerCenter();
      // This page and everything on it is new, and the metadata it needs was set by the view that
      // was here before: the stream that carries it does not replay, so say it again now that
      // this page's own subscribers are listening. Without it the menu shows no workflow name
      // and no id, the computing unit picker does not restore the unit this workflow last ran
      // on, and this page believes the user cannot write to the workflow.
      this.workflowActionService.republishWorkflowMetadata();
      this.registerMacroNavigation();
      return;
    }
    // load workflow with wid if presented in the URL
    if (wid) {
      // show loading spinner right away while waiting for workflow to load
      this.isLoading = true;
      // temporarily disable modification to prevent editing an empty workflow before real data is loaded
      this.workflowActionService.disableWorkflowModification();
      this.loadWorkflowWithId(Number(wid));
      return;
    }

    this.operatorMetadataService
      .getOperatorMetadata()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        // no workflow to load; directly register auto persist for brand-new workflow
        this.registerAutoPersistWorkflow();
        this.registerMacroNavigation();
      });
  }

  onWIDChange() {
    this.workflowActionService
      .workflowMetaDataChanged()
      .pipe(
        switchMap(() => of(this.workflowActionService.getWorkflowMetadata())),
        filter((metadata: WorkflowMetadata) => isDefined(metadata.wid)),
        distinctUntilChanged()
      )
      .pipe(untilDestroyed(this))
      .subscribe((metadata: WorkflowMetadata) => {
        this.writeAccess = !metadata.readonly;
      });
  }

  updateViewCount() {
    let wid = this.route.snapshot.params.id;
    let uid = this.userService.getCurrentUser()?.uid;
    this.hubService
      .postView(wid, uid ? uid : 0, EntityType.Workflow)
      .pipe(throttleTime(THROTTLE_TIME_MS))
      .pipe(untilDestroyed(this))
      .subscribe();
  }

  public triggerCenter(): void {
    this.workflowActionService.getTexeraGraph().triggerCenterEvent();
  }

  private setLoadingState(isLoading: boolean): void {
    this.isLoading = isLoading;
    this.changeDetectorRef.detectChanges();
  }

  public get copilotEnabled(): boolean {
    return this.config.env.copilotEnabled;
  }
}
