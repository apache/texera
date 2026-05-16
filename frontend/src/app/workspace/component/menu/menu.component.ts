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

import { DatePipe, Location, NgIf, NgFor, NgTemplateOutlet } from "@angular/common";
import { Component, ElementRef, Input, OnDestroy, OnInit, ViewChild } from "@angular/core";
import { Router, RouterLink } from "@angular/router";
import { UserService } from "../../../common/service/user/user.service";
import {
  DEFAULT_WORKFLOW_NAME,
  WorkflowPersistService,
} from "../../../common/service/workflow-persist/workflow-persist.service";
import { Workflow, WorkflowContent } from "../../../common/type/workflow";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { UndoRedoService } from "../../service/undo-redo/undo-redo.service";
import { ValidationWorkflowService } from "../../service/validation/validation-workflow.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { WorkflowWebsocketService } from "../../service/workflow-websocket/workflow-websocket.service";
import { WorkflowResultExportService } from "../../service/workflow-result-export/workflow-result-export.service";
import { catchError, debounceTime, filter, mergeMap, tap } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowUtilService } from "../../service/workflow-graph/util/workflow-util.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { UserProjectService } from "../../../dashboard/service/user/project/user-project.service";
import { NzUploadFile, NzUploadComponent } from "ng-zorro-antd/upload";
import { saveAs } from "file-saver";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { OperatorMenuService } from "../../service/operator-menu/operator-menu.service";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
import { firstValueFrom, of, Subscription, timer } from "rxjs";
import { isDefined } from "../../../common/util/predicate";
import { NzModalService } from "ng-zorro-antd/modal";
import { ResultExportationComponent } from "../result-exportation/result-exportation.component";
import { ReportGenerationService } from "../../service/report-generation/report-generation.service";
import { ShareAccessComponent } from "src/app/dashboard/component/user/share-access/share-access.component";
import { PanelService } from "../../service/panel/panel.service";
import { DASHBOARD_USER_WORKFLOW } from "../../../app-routing.constant";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { ComputingUnitState } from "../../../common/type/computing-unit-connection.interface";
import { ComputingUnitSelectionComponent } from "../power-button/computing-unit-selection.component";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { DashboardWorkflowComputingUnit } from "../../../common/type/workflow-computing-unit";
import { Privilege } from "../../../dashboard/type/share-access.interface";
import { MarkdownDescriptionComponent } from "../../../dashboard/component/user/markdown-description/markdown-description.component";
import { NzSpaceCompactItemDirective, NzSpaceCompactComponent } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzAvatarComponent } from "ng-zorro-antd/avatar";
import { FormsModule } from "@angular/forms";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { CoeditorUserIconComponent } from "./coeditor-user-icon/coeditor-user-icon.component";
import { UserIconComponent } from "../../../dashboard/component/user/user-icon/user-icon.component";
import { NzDropdownDirective, NzDropdownMenuComponent } from "ng-zorro-antd/dropdown";
import { NzMenuDirective, NzMenuItemComponent } from "ng-zorro-antd/menu";
import { NzCheckboxComponent } from "ng-zorro-antd/checkbox";
import { NzPopoverDirective } from "ng-zorro-antd/popover";
import { NzSwitchComponent } from "ng-zorro-antd/switch";
import { NzBadgeComponent } from "ng-zorro-antd/badge";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzSelectComponent, NzOptionComponent } from "ng-zorro-antd/select";
import { NzSliderComponent } from "ng-zorro-antd/slider";
import { AsyncPipe } from "@angular/common";
import { ProfilerService, ProfilerView } from "../../service/profiler/profiler.service";
import { ProfilerHistoryService } from "../../service/profiler/profiler-history.service";
import { WorkflowExecutionsEntry } from "../../../dashboard/type/workflow-executions-entry";
import {
  buildReport,
  formatFilenameTimestamp,
  slugifyForFilename,
} from "../../service/profiler/profiler-report";
import { parseBaselineReport } from "../../service/profiler/profiler-delta";
import { ProfilerSuggestionsService } from "../../service/profiler/profiler-suggestions.service";
import { Suggestion } from "../../service/profiler/profiler-suggestions";

/**
 * MenuComponent is the top level menu bar that shows
 *  the Texera title and workflow execution button
 *
 * This Component will be the only Component capable of executing
 *  the workflow in the WorkflowEditor Component.
 *
 * Clicking the run button on the top-right hand corner will begin
 *  the execution. During execution, the run button will be replaced
 *  with a pause/resume button to show that graph is under execution.
 *
 * @author Zuozhi Wang
 * @author Henry Chen
 *
 */
@UntilDestroy()
@Component({
  selector: "texera-menu",
  templateUrl: "menu.component.html",
  styleUrls: ["menu.component.scss"],
  imports: [
    NgIf,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzAvatarComponent,
    FormsModule,
    NzWaveDirective,
    NgFor,
    CoeditorUserIconComponent,
    UserIconComponent,
    RouterLink,
    NzUploadComponent,
    NzDropdownDirective,
    NzDropdownMenuComponent,
    NzMenuDirective,
    NzMenuItemComponent,
    NzCheckboxComponent,
    NgTemplateOutlet,
    ComputingUnitSelectionComponent,
    NzPopoverDirective,
    NzSwitchComponent,
    NzBadgeComponent,
    NzTooltipDirective,
    NzSelectComponent,
    NzOptionComponent,
    NzSliderComponent,
    AsyncPipe,
    DatePipe,
    NzSpaceCompactComponent,
  ],
})
export class MenuComponent implements OnInit, OnDestroy {
  public executionState: ExecutionState; // set this to true when the workflow is started
  public ExecutionState = ExecutionState; // make Angular HTML access enum definition
  public ComputingUnitState = ComputingUnitState; // make Angular HTML access enum definition
  public isWorkflowValid: boolean = true; // this will check whether the workflow error or not
  public isWorkflowEmpty: boolean = false;
  public isSaving: boolean = false;
  public isWorkflowModifiable: boolean = false;
  public workflowId?: number;
  public isExportDeactivate: boolean = false;
  public showRegion: boolean = false;
  public showGrid: boolean = false;
  public showNumWorkers: boolean = false;
  public showStatus: boolean = false;
  protected readonly DASHBOARD_USER_WORKFLOW = DASHBOARD_USER_WORKFLOW;

  @Input() public writeAccess: boolean = false;
  @Input() public pid?: number = undefined;
  @Input() public autoSaveState: string = "";
  @Input() public currentWorkflowName: string = ""; // reset workflowName
  @Input() public currentExecutionName: string = ""; // reset executionName
  @Input() public particularVersionDate: string = ""; // placeholder for the metadata information of a particular workflow version
  @ViewChild("workflowNameInput") workflowNameInput: ElementRef<HTMLInputElement> | undefined;

  // variable bound with HTML to decide if the running spinner should show
  public runButtonText = "Run";
  public runIcon = "play-circle";
  public runDisable = false;

  public executionDuration = 0;
  private durationUpdateSubscription: Subscription = new Subscription();

  // flag to display a particular version in the current canvas
  public displayParticularWorkflowVersion: boolean = false;
  public onClickRunHandler: () => void;

  // Computing unit status variables
  private computingUnitStatusSubscription: Subscription = new Subscription();
  public selectedComputingUnit: DashboardWorkflowComputingUnit | null = null;
  public computingUnitStatus: ComputingUnitState = ComputingUnitState.NoComputingUnit;

  @ViewChild(ComputingUnitSelectionComponent) computingUnitSelectionComponent!: ComputingUnitSelectionComponent;

  constructor(
    public executeWorkflowService: ExecuteWorkflowService,
    public workflowActionService: WorkflowActionService,
    public workflowWebsocketService: WorkflowWebsocketService,
    private location: Location,
    public undoRedoService: UndoRedoService,
    public validationWorkflowService: ValidationWorkflowService,
    public workflowPersistService: WorkflowPersistService,
    public workflowVersionService: WorkflowVersionService,
    public userService: UserService,
    private datePipe: DatePipe,
    public workflowResultExportService: WorkflowResultExportService,
    public workflowUtilService: WorkflowUtilService,
    private userProjectService: UserProjectService,
    private notificationService: NotificationService,
    public operatorMenu: OperatorMenuService,
    public coeditorPresenceService: CoeditorPresenceService,
    private modalService: NzModalService,
    private reportGenerationService: ReportGenerationService,
    private panelService: PanelService,
    private computingUnitStatusService: ComputingUnitStatusService,
    protected config: GuiConfigService,
    public profilerService: ProfilerService,
    public profilerSuggestionsService: ProfilerSuggestionsService,
    private profilerHistoryService: ProfilerHistoryService,
    private router: Router
  ) {
    workflowWebsocketService
      .subscribeToEvent("ExecutionDurationUpdateEvent")
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        this.executionDuration = event.duration;
        this.durationUpdateSubscription.unsubscribe();
        if (event.isRunning) {
          this.durationUpdateSubscription = timer(1000, 1000)
            .pipe(untilDestroyed(this))
            .subscribe(() => {
              this.executionDuration += 1000;
            });
        }
      });
    this.executionState = executeWorkflowService.getExecutionState().state;
    // return the run button after the execution is finished, either
    //  when the value is valid or invalid
    const initBehavior = this.getRunButtonBehavior();
    this.runButtonText = initBehavior.text;
    this.runIcon = initBehavior.icon;
    this.runDisable = initBehavior.disable;
    this.onClickRunHandler = initBehavior.onClick;
    this.registerWorkflowModifiableChangedHandler();
    this.registerWorkflowIdUpdateHandler();

    // Subscribe to computing unit
    this.subscribeToComputingUnitSelection();
    this.subscribeToComputingUnitStatus();
  }

  public ngOnInit(): void {
    this.executeWorkflowService
      .getExecutionStateStream()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        this.executionState = event.current.state;
        this.applyRunButtonBehavior(this.getRunButtonBehavior());
      });

    // "Run now" prompt (from profiler-suggestions) routes through the same handler
    // the Run button uses, so execution name / computing-unit / email-notif config
    // is identical. No-op if the Run button is currently disabled.
    this.profilerSuggestionsService
      .getWorkflowRunRequestStream()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        if (!this.runDisable) {
          this.onClickRunHandler();
        }
      });

    // set the map of operatorStatusMap
    this.validationWorkflowService
      .getWorkflowValidationErrorStream()
      .pipe(untilDestroyed(this))
      .subscribe(value => {
        this.isWorkflowEmpty = value.workflowEmpty;
        this.isWorkflowValid = Object.keys(value.errors).length === 0;
        this.applyRunButtonBehavior(this.getRunButtonBehavior());
      });

    // Subscribe to WorkflowResultExportService observable
    this.workflowResultExportService
      .getExportOnAllOperatorsStatusStream()
      .pipe(untilDestroyed(this))
      .subscribe(hasResultToExport => {
        this.isExportDeactivate = !this.config.env.exportExecutionResultEnabled || !hasResultToExport;
      });

    this.registerWorkflowMetadataDisplayRefresh();
    this.handleWorkflowVersionDisplay();
  }

  ngOnDestroy(): void {
    this.workflowResultExportService.resetFlags();
    this.computingUnitStatusSubscription.unsubscribe();
  }

  public profilerPopoverVisible = false;

  public toggleProfilerPopover(): void {
    this.profilerPopoverVisible = !this.profilerPopoverVisible;
  }

  public closeProfilerPopover(): void {
    this.profilerPopoverVisible = false;
  }

  public toggleProfiling(enabled: boolean): void {
    this.profilerService.setEnabled(enabled);
  }

  public setProfilerView(view: ProfilerView): void {
    this.profilerService.setView(view);
  }

  /** Apply a suggestion from the popover list — routes through the service so the
   *  workflow-editor component performs the actual canvas mutation. */
  public applySuggestion(s: Suggestion): void {
    this.profilerSuggestionsService.requestMaterialize(s);
  }

  /** Dismiss a suggestion from the popover list. */
  public dismissSuggestionFromList(s: Suggestion): void {
    this.profilerSuggestionsService.dismiss(s.id);
  }

  /** Short label for one suggestion row — used in the popover list. */
  public suggestionShortLabel(s: Suggestion): string {
    if (s.type === "INSERT_FILTER") {
      return `Insert Filter on edge`;
    }
    return `Bump workers → ${s.proposedWorkers}`;
  }

  public trackSuggestionById(_index: number, s: Suggestion): string {
    return s.id;
  }

  public setProfilerHotThreshold(percentile: number): void {
    this.profilerService.setHotThresholdPercentile(percentile);
  }

  /**
   * Returns true when there's enough data to produce a meaningful profiler report —
   * profiling must be enabled AND at least one operator has stats. The download buttons
   * disable themselves otherwise so users never get an empty file.
   */
  public canDownloadProfilerReport(): boolean {
    const state = this.profilerService.getState();
    return state.enabled && Object.keys(state.scores).length > 0;
  }

  public downloadProfilerReport(format: "markdown" | "json"): void {
    if (!this.canDownloadProfilerReport()) return;

    const state = this.profilerService.getState();
    const graph = this.workflowActionService.getTexeraGraph();
    const generatedAt = new Date();

    const report = buildReport({
      workflowName: this.currentWorkflowName || DEFAULT_WORKFLOW_NAME,
      executionName: this.currentExecutionName?.trim() || undefined,
      generatedAt,
      view: state.view,
      hotThresholdPercentile: state.hotThresholdPercentile,
      scores: state.scores,
      operatorType: id => {
        try {
          return graph.getOperator(id)?.operatorType;
        } catch {
          return undefined;
        }
      },
      displayName: id => {
        try {
          const op = graph.getOperator(id);
          return op?.customDisplayName?.trim() || op?.operatorType || id;
        } catch {
          return id;
        }
      },
      upstreamOps: id => {
        try {
          return graph.getInputLinksByOperatorId(id).map(l => l.source.operatorID);
        } catch {
          return [];
        }
      },
      downstreamOps: id => {
        try {
          return graph.getOutputLinksByOperatorId(id).map(l => l.target.operatorID);
        } catch {
          return [];
        }
      },
    });

    const slug = slugifyForFilename(this.currentWorkflowName || DEFAULT_WORKFLOW_NAME);
    const stamp = formatFilenameTimestamp(generatedAt);
    if (format === "markdown") {
      saveAs(
        new Blob([report.markdown], { type: "text/markdown;charset=utf-8" }),
        `profiler-report-${slug}-${stamp}.md`
      );
    } else {
      saveAs(
        new Blob([JSON.stringify(report.json, null, 2)], { type: "application/json" }),
        `profiler-report-${slug}-${stamp}.json`
      );
    }
  }

  /**
   * Handles the hidden file input's change event for the "Upload baseline"
   * button. Parses the chosen file as a profiler JSON report and, on success,
   * registers it as the comparison baseline on ProfilerService.
   */
  public onBaselineFileSelected(event: Event): void {
    const input = event.target as HTMLInputElement;
    const file = input?.files?.[0];
    // Reset the input value so selecting the same file twice still triggers change.
    if (input) input.value = "";
    if (!file) return;

    const reader = new FileReader();
    reader.onload = () => {
      try {
        const parsed = parseBaselineReport(JSON.parse(reader.result as string));
        if (!parsed) {
          this.notificationService.error(
            "That JSON doesn't look like a profiler report. Use a file downloaded via 'Download report → JSON'."
          );
          return;
        }
        this.profilerService.setBaseline(parsed);
        this.notificationService.success(
          `Baseline loaded: ${parsed.operators.length} operator${parsed.operators.length === 1 ? "" : "s"} from ${parsed.header.workflowName}.`
        );
      } catch {
        this.notificationService.error("Could not parse the selected file as JSON.");
      }
    };
    reader.onerror = () => {
      this.notificationService.error("Failed to read the selected file.");
    };
    reader.readAsText(file);
  }

  public clearProfilerBaseline(): void {
    this.profilerService.clearBaseline();
  }

  // ---------------------------------------------------------------------------
  // P6 — Compare across runs (server-side baseline).
  // The user picks a past execution from a dropdown; we fetch its persisted
  // runtime stats, convert to BaselineReport, and hand to ProfilerService —
  // reusing the exact same delta heatmap + side-panel UI as the upload flow.
  // ---------------------------------------------------------------------------

  /** Past executions of the current workflow. Populated lazily on popover open. */
  public profilerHistoryExecutions: WorkflowExecutionsEntry[] = [];
  public profilerHistoryLoading: boolean = false;
  public profilerHistorySelectedEid: number | null = null;

  /**
   * Fetches the list of completed executions for the current workflow. Idempotent:
   * call from the popover's open-handler so the dropdown is populated when shown.
   */
  public loadProfilerHistoryList(): void {
    if (this.workflowId == null) {
      this.profilerHistoryExecutions = [];
      return;
    }
    this.profilerHistoryLoading = true;
    this.profilerHistoryService
      .listCompletedExecutions(this.workflowId)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: rows => {
          this.profilerHistoryExecutions = rows;
          this.profilerHistoryLoading = false;
        },
        error: () => {
          this.profilerHistoryExecutions = [];
          this.profilerHistoryLoading = false;
        },
      });
  }

  /** Human-readable label for a past execution shown in the dropdown options. */
  public profilerHistoryLabel(entry: WorkflowExecutionsEntry): string {
    const name = entry.name && entry.name.trim().length > 0 ? entry.name : `Execution #${entry.eId}`;
    const when = entry.completionTime
      ? new Date(entry.completionTime).toLocaleString()
      : entry.startingTime
        ? new Date(entry.startingTime).toLocaleString()
        : "";
    return when ? `${name} — ${when}` : name;
  }

  /**
   * Selects a past execution as the comparison baseline. Fetches the persisted
   * stats, converts via the pure helper, and hands the result to ProfilerService.
   */
  public onProfilerHistorySelected(eid: number | null): void {
    this.profilerHistorySelectedEid = eid;
    if (eid == null || this.workflowId == null) return;
    const execution = this.profilerHistoryExecutions.find(e => e.eId === eid);
    if (!execution) {
      this.notificationService.error(`Execution #${eid} is not in the loaded list.`);
      return;
    }
    const workflowName = this.currentWorkflowName ?? `Workflow ${this.workflowId}`;
    this.profilerHistoryService
      .loadBaselineForExecution({ workflowId: this.workflowId, execution, workflowName })
      .pipe(untilDestroyed(this))
      .subscribe(baseline => {
        if (!baseline) {
          this.notificationService.error(
            "No baseline data available for that run (the engine may not have persisted stats)."
          );
          return;
        }
        this.profilerService.setBaseline(baseline);
        this.notificationService.success(
          `Baseline loaded from ${baseline.header.executionName ?? "run"} (${baseline.operators.length} operators).`
        );
      });
  }

  private subscribeToComputingUnitSelection(): void {
    this.computingUnitStatusService
      .getSelectedComputingUnit()
      .pipe(untilDestroyed(this))
      .subscribe(unit => {
        this.selectedComputingUnit = unit;
      });
  }

  /**
   * Subscribe to computing unit status changes from the ComputingUnitStatusService
   */
  private subscribeToComputingUnitStatus(): void {
    // Subscribe to get the computing unit status
    this.computingUnitStatusSubscription.add(
      this.computingUnitStatusService
        .getStatus()
        .pipe(untilDestroyed(this))
        .subscribe(status => {
          this.computingUnitStatus = status;
          this.applyRunButtonBehavior(this.getRunButtonBehavior());
        })
    );
  }

  /**
   * Dynamically adjusts the width of the workflow name input field
   * by creating a hidden span element to measure the text width.
   */
  public adjustWorkflowNameWidth(): void {
    const input = this.workflowNameInput?.nativeElement;
    if (!input) return;

    const tempSpan = document.createElement("span");
    tempSpan.style.visibility = "hidden";
    tempSpan.style.position = "absolute";
    tempSpan.style.whiteSpace = "pre";
    tempSpan.style.font = getComputedStyle(input).font;
    tempSpan.textContent = input.value || input.placeholder;

    document.body.appendChild(tempSpan);
    const width = Math.min(tempSpan.offsetWidth + 20, 800); // +20 for padding
    input.style.width = `${width}px`;
    document.body.removeChild(tempSpan);
  }

  toggleNumWorkers() {
    this.workflowActionService
      .getJointGraphWrapper()
      .mainPaper.el.classList.toggle("hide-worker-count", !this.showNumWorkers);
    this.applyOperatorStatusPosition();
  }

  toggleStatus() {
    this.workflowActionService
      .getJointGraphWrapper()
      .mainPaper.el.classList.toggle("hide-operator-status", !this.showStatus);
    this.applyOperatorStatusPosition();
  }

  private applyOperatorStatusPosition(): void {
    const refY = this.showNumWorkers ? -55 : -35;
    const paperModel = this.workflowActionService.getJointGraphWrapper().mainPaper.model as any;
    paperModel.getElements().forEach((el: any) => {
      el.attr(".operator-status/ref-x", -10);
      el.attr(".operator-status/ref-y", refY);
    });
  }

  public async onClickOpenShareAccess(): Promise<void> {
    const modalRef = this.modalService.create({
      nzContent: ShareAccessComponent,
      nzData: {
        writeAccess: this.writeAccess,
        type: "workflow",
        id: this.workflowId,
        allOwners: await firstValueFrom(this.workflowPersistService.retrieveOwners()),
        inWorkspace: true,
      },
      nzFooter: null,
      nzTitle: "Share this workflow with others",
      nzCentered: true,
      nzWidth: "800px",
    });

    modalRef.afterClose.pipe(untilDestroyed(this)).subscribe(result => {
      if (result?.userRevokedOwnAccess) {
        this.router.navigate([DASHBOARD_USER_WORKFLOW]);
      }
    });
  }

  // apply a behavior to the run button via bound variables
  public applyRunButtonBehavior(behavior: { text: string; icon: string; disable: boolean; onClick: () => void }) {
    this.runButtonText = behavior.text;
    this.runIcon = behavior.icon;
    this.runDisable = behavior.disable;
    this.onClickRunHandler = behavior.onClick;
  }

  public getRunButtonBehavior(): {
    text: string;
    icon: string;
    disable: boolean;
    onClick: () => void;
  } {
    // If workflow is invalid, always disable and show "Invalid Workflow"
    if (!this.isWorkflowValid) {
      return {
        text: "Invalid Workflow",
        icon: "warning",
        disable: true,
        onClick: () => {},
      };
    }

    // If workflow is empty, always disable and show "Empty Workflow"
    if (this.isWorkflowEmpty) {
      return {
        text: "Empty Workflow",
        icon: "info-circle",
        disable: true,
        onClick: () => {},
      };
    }

    // This handles the case where a unit exists but we're not connected to it
    if (this.computingUnitStatus !== ComputingUnitState.NoComputingUnit && !this.workflowWebsocketService.isConnected) {
      return {
        text: "Connecting",
        icon: "loading",
        disable: true,
        onClick: () => {},
      };
    }

    // no computing unit, show "Connect" button
    if (this.computingUnitStatus === ComputingUnitState.NoComputingUnit) {
      return {
        text: "Connect",
        icon: "plus-circle",
        disable: false,
        onClick: () => this.runWorkflow(),
      };
    }

    // Handle execution states when connected to a running computing unit
    switch (this.executionState) {
      case ExecutionState.Uninitialized:
      case ExecutionState.Completed:
      case ExecutionState.Terminated:
      case ExecutionState.Killed:
      case ExecutionState.Failed:
        return {
          text: "Run",
          icon: "play-circle",
          disable: false,
          onClick: () => this.runWorkflow(),
        };
      case ExecutionState.Initializing:
        return {
          text: "Submitting",
          icon: "loading",
          disable: true,
          onClick: () => {},
        };
      case ExecutionState.Running:
        return {
          text: "Pause",
          icon: "loading",
          disable: false,
          onClick: () => this.executeWorkflowService.pauseWorkflow(),
        };
      case ExecutionState.Paused:
        return {
          text: "Resume",
          icon: "pause-circle",
          disable: false,
          onClick: () => this.executeWorkflowService.resumeWorkflow(),
        };
      case ExecutionState.Pausing:
        return {
          text: "Pausing",
          icon: "loading",
          disable: true,
          onClick: () => {},
        };
      case ExecutionState.Resuming:
        return {
          text: "Resuming",
          icon: "loading",
          disable: true,
          onClick: () => {},
        };
      case ExecutionState.Recovering:
        return {
          text: "Recovering",
          icon: "loading",
          disable: true,
          onClick: () => {},
        };
      default:
        return {
          text: "Run",
          icon: "play-circle",
          disable: false,
          onClick: () => this.runWorkflow(),
        };
    }
  }

  public onClickAddCommentBox(): void {
    this.workflowActionService.addCommentBox(this.workflowUtilService.getNewCommentBox());
  }

  public handleKill(): void {
    this.executeWorkflowService.killWorkflow();
  }

  public handleCheckpoint(): void {
    this.executeWorkflowService.takeGlobalCheckpoint();
  }

  public onClickClosePanels(): void {
    this.panelService.closePanels();
  }

  public onClickResetPanels(): void {
    this.panelService.resetPanels();
  }

  /**
   * get the html to export all results.
   */
  public onClickGenerateReport(): void {
    // Get notification and set nzDuration to 0 to prevent it from auto-closing
    this.notificationService.blank("", "The report is being generated...", { nzDuration: 0 });

    const workflowName = this.currentWorkflowName;
    const WorkflowContent: WorkflowContent = this.workflowActionService.getWorkflowContent();

    // Extract operatorIDs from the parsed payload
    const operatorIds = WorkflowContent.operators.map((operator: { operatorID: string }) => operator.operatorID);

    // Invokes the method of the report printing service
    this.reportGenerationService
      .generateWorkflowSnapshot(workflowName)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: (workflowSnapshotURL: string) => {
          this.reportGenerationService
            .getAllOperatorResults(operatorIds)
            .pipe(untilDestroyed(this))
            .subscribe({
              next: (allResults: { operatorId: string; html: string }[]) => {
                const sortedResults = operatorIds.map(
                  id => allResults.find(result => result.operatorId === id)?.html || ""
                );
                // Generate the final report as HTML after all results are retrieved
                this.reportGenerationService.generateReportAsHtml(workflowSnapshotURL, sortedResults, workflowName);

                // Close the notification after the report is generated
                this.notificationService.remove();
                this.notificationService.success("Report successfully generated.");
              },
              error: (error: unknown) => {
                this.notificationService.error("Error in retrieving operator results: " + (error as Error).message);
                // Close the notification on error
                this.notificationService.remove();
              },
            });
        },
        error: (e: unknown) => {
          this.notificationService.error((e as Error).message);
          // Close the notification on error
          this.notificationService.remove();
        },
      });
  }

  public toggleGrid(): void {
    this.workflowActionService.getJointGraphWrapper().mainPaper.setGridSize(this.showGrid ? 2 : 1);
  }

  public toggleRegion(): void {
    this.workflowActionService
      .getJointGraphWrapper()
      .jointGraph.getElements()
      .filter(el => el.get("type") === "region") // small improvement here too
      .forEach(el => el.attr("body/visibility", this.showRegion ? "visible" : "hidden"));
  }

  /**
   * This method will run the autoLayout function
   *
   */
  public onClickAutoLayout(): void {
    if (!this.hasOperators()) {
      return;
    }
    this.workflowActionService.autoLayoutWorkflow();
  }

  /**
   * This is the handler for the execution result export button.
   *
   */
  public onClickExportExecutionResult(): void {
    this.modalService.create({
      nzTitle: "Export All Operators Result",
      nzContent: ResultExportationComponent,
      nzData: {
        workflowName: this.currentWorkflowName,
        sourceTriggered: "menu",
      },
      nzFooter: null,
    });
  }

  /**
   * Restore paper default zoom ratio and paper offset
   */
  public onClickRestoreZoomOffsetDefault(): void {
    this.workflowActionService.getJointGraphWrapper().restoreDefaultZoomAndOffset();
  }

  /**
   * Delete all operators (including hidden ones) on the graph.
   */
  public onClickDeleteAllOperators(): void {
    const allOperatorIDs = this.workflowActionService
      .getTexeraGraph()
      .getAllOperators()
      .map(op => op.operatorID);
    this.workflowActionService.deleteOperatorsAndLinks(allOperatorIDs);
  }

  public onClickImportWorkflow = (file: NzUploadFile): boolean => {
    const reader = new FileReader();
    reader.readAsText(file as any);
    reader.onload = () => {
      try {
        const result = reader.result;
        if (typeof result !== "string") {
          throw new Error("incorrect format: file is not a string");
        }

        const workflowContent = JSON.parse(result) as WorkflowContent;

        // set the workflow name using the file name without the extension
        const fileExtensionIndex = file.name.lastIndexOf(".");
        var workflowName: string;
        if (fileExtensionIndex === -1) {
          workflowName = file.name;
        } else {
          workflowName = file.name.substring(0, fileExtensionIndex);
        }
        if (workflowName.trim() === "") {
          workflowName = DEFAULT_WORKFLOW_NAME;
        }

        const workflow: Workflow = {
          content: workflowContent,
          name: workflowName,
          description: undefined,
          wid: undefined,
          creationTime: undefined,
          lastModifiedTime: undefined,
          readonly: false,
          isPublished: 0,
        };

        this.workflowActionService.enableWorkflowModification();
        // load the fetched workflow
        this.workflowActionService.reloadWorkflow(workflow, true);
        // clear stack
        this.undoRedoService.clearUndoStack();
        this.undoRedoService.clearRedoStack();
      } catch (error) {
        this.notificationService.error(
          "An error occurred when importing the workflow. Please import a workflow json file."
        );
        console.error(error);
      }
    };
    return false;
  };

  public onClickExportWorkflow(): void {
    const workflowContent: WorkflowContent = this.workflowActionService.getWorkflowContent();
    const workflowContentJson = JSON.stringify(workflowContent, null, 2);
    const fileName = this.currentWorkflowName + ".json";
    saveAs(new Blob([workflowContentJson], { type: "text/plain;charset=utf-8" }), fileName);
  }

  /**
   * Calls Markdown Description Component
   */
  public onClickEditDescription(): void {
    const currentWorkflow = this.workflowActionService.getWorkflow();
    const currentDescription = currentWorkflow.description ?? "";

    const modalRef = this.modalService.create<MarkdownDescriptionComponent>({
      nzTitle: "Edit Workflow Description",
      nzContent: MarkdownDescriptionComponent,
      nzData: {
        description: currentDescription,
      },
      nzWidth: "900px",
      nzMaskClosable: true,
      nzKeyboard: true,
      nzClosable: true,
      nzFooter: null,
    });

    const comp: MarkdownDescriptionComponent = modalRef.getContentComponent();

    comp.descriptionChange.pipe(untilDestroyed(this)).subscribe((updatedDescription: string) => {
      const updatedWorkflow: Workflow = {
        ...currentWorkflow,
        description: updatedDescription,
      };

      this.workflowActionService.setWorkflowMetadata(updatedWorkflow);

      if (this.userService.isLogin()) {
        this.persistWorkflow();
      }

      modalRef.close();
    });
  }

  /**
   * Returns true if there's any operator on the graph; false otherwise
   */
  public hasOperators(): boolean {
    return this.workflowActionService.getTexeraGraph().getAllOperators().length > 0;
  }

  public persistWorkflow(): void {
    this.isSaving = true;
    let localPid = this.pid;
    this.workflowPersistService
      .persistWorkflow(this.workflowActionService.getWorkflow())
      .pipe(
        tap((updatedWorkflow: Workflow) => {
          this.workflowActionService.setWorkflowMetadata(updatedWorkflow);
        }),
        filter(workflow => isDefined(localPid) && isDefined(workflow.wid)),
        mergeMap(workflow => this.userProjectService.addWorkflowToProject(localPid!, workflow.wid!)),
        untilDestroyed(this)
      )
      .subscribe({
        error: (e: unknown) => this.notificationService.error((e as Error).message),
      })
      .add(() => (this.isSaving = false));
  }

  /**
   * Handler for changing workflow name input box, updates the cachedWorkflow and persist to database.
   */
  onWorkflowNameChange() {
    this.workflowActionService.setWorkflowName(this.currentWorkflowName);
    if (this.userService.isLogin()) {
      this.persistWorkflow();
    }
  }

  onClickCreateNewWorkflow() {
    this.workflowActionService.resetAsNewWorkflow();
    this.location.go("/");
  }

  registerWorkflowMetadataDisplayRefresh() {
    this.workflowActionService
      .workflowMetaDataChanged()
      .pipe(debounceTime(100))
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.currentWorkflowName = this.workflowActionService.getWorkflowMetadata()?.name;
        // Use timeout to make sure this.adjustWorkflowNameWidth() runs
        // after currentWorkflowName is set. Otherwise, the input width may not match
        // the latest name right after refresh.
        setTimeout(() => this.adjustWorkflowNameWidth(), 0);
        this.autoSaveState =
          this.workflowActionService.getWorkflowMetadata().lastModifiedTime === undefined
            ? ""
            : "Saved at " +
              this.datePipe.transform(
                this.workflowActionService.getWorkflowMetadata().lastModifiedTime,
                "MM/dd/yyyy HH:mm:ss",
                Intl.DateTimeFormat().resolvedOptions().timeZone,
                "en"
              );
      });
  }

  onClickGetAllVersions() {
    this.workflowVersionService.displayWorkflowVersions();
  }

  private handleWorkflowVersionDisplay(): void {
    this.workflowVersionService
      .getDisplayParticularVersionStream()
      .pipe(untilDestroyed(this))
      .subscribe(displayVersionFlag => {
        this.particularVersionDate =
          this.workflowActionService.getWorkflowMetadata().creationTime === undefined
            ? ""
            : "" +
              this.datePipe.transform(
                this.workflowActionService.getWorkflowMetadata().creationTime,
                "MM/dd/yyyy HH:mm:ss",
                Intl.DateTimeFormat().resolvedOptions().timeZone,
                "en"
              );
        this.displayParticularWorkflowVersion = displayVersionFlag;
      });
  }

  closeParticularVersionDisplay() {
    this.workflowVersionService.closeParticularVersionDisplay();
  }

  revertToVersion() {
    this.workflowVersionService.revertToVersion();
    // after swapping the workflows to point to the particular version, persist it in DB
    this.persistWorkflow();
  }

  cloneVersion() {
    this.workflowVersionService
      .cloneWorkflowVersion()
      .pipe(
        catchError(() => {
          this.notificationService.error("Failed to clone workflow. Please try again.");
          return of(null);
        }),
        untilDestroyed(this)
      )
      .subscribe(new_wid => {
        if (new_wid) {
          this.notificationService.success("Workflow cloned successfully! New workflow ID: " + new_wid);
          this.closeParticularVersionDisplay();
        }
      });
  }

  private registerWorkflowModifiableChangedHandler(): void {
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(untilDestroyed(this))
      .subscribe(modifiable => (this.isWorkflowModifiable = modifiable));
  }

  private registerWorkflowIdUpdateHandler(): void {
    this.workflowActionService
      .workflowMetaDataChanged()
      .pipe(untilDestroyed(this))
      .subscribe(metadata => {
        this.workflowId = metadata.wid;
        // consider adding the oprerator reconnect
      });
  }

  /**
   * Attempts to run a workflow based on the current state.
   * If no computing unit is selected but the feature is enabled,
   * it will first create and connect to a new computing unit.
   */
  runWorkflow(): void {
    // Use the existing flags that were already updated via subscriptions
    if (!this.isWorkflowValid || this.isWorkflowEmpty) {
      return;
    }

    // If computing unit manager is enabled and no computing unit is selected
    if (this.computingUnitStatus === ComputingUnitState.NoComputingUnit) {
      // Create a default name based on the workflow name
      const defaultName = this.currentWorkflowName
        ? `${this.currentWorkflowName}'s Computing Unit`
        : "New Computing Unit";

      // Set the default name in the computing unit selection component
      this.computingUnitSelectionComponent.newComputingUnitName = defaultName;

      // Show the existing modal in the ComputingUnitSelectionComponent
      this.computingUnitSelectionComponent.showAddComputeUnitModalVisible();
      return;
    }

    // Regular workflow execution - already connected
    this.executeWorkflowService.executeWorkflowWithEmailNotification(
      this.currentExecutionName || "Untitled Execution",
      this.config.env.workflowEmailNotificationEnabled
    );
  }

  protected readonly Privilege = Privilege;
}
