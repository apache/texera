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

import { DatePipe, Location, NgIf, NgFor, NgTemplateOutlet, AsyncPipe } from "@angular/common";
import { Component, ElementRef, Input, OnDestroy, OnInit, ViewChild, Output, EventEmitter } from "@angular/core";
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
import { HeatmapView } from "../../service/heatmap/heatmap-scoring";
import { WorkflowWebsocketService } from "../../service/workflow-websocket/workflow-websocket.service";
import { WorkflowResultExportService } from "../../service/workflow-result-export/workflow-result-export.service";
import { catchError, debounceTime, filter, mergeMap, switchMap, tap } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowUtilService } from "../../service/workflow-graph/util/workflow-util.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { UserProjectService } from "../../../dashboard/service/user/project/user-project.service";
import { saveAs } from "file-saver";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { OperatorMenuService } from "../../service/operator-menu/operator-menu.service";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
import { EMPTY, firstValueFrom, of, timer, map } from "rxjs";
import { isDefined } from "../../../common/util/predicate";
import { NzModalService } from "ng-zorro-antd/modal";
import { ResultExportationComponent } from "../result-exportation/result-exportation.component";
import { ReportGenerationService } from "../../service/report-generation/report-generation.service";
import { ShareAccessComponent } from "src/app/dashboard/component/user/share-access/share-access.component";
import { PanelService } from "../../service/panel/panel.service";
import { USER_WORKFLOW, USER_WORKSPACE } from "../../../app-routing.constant";
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
import { NzRadioComponent, NzRadioGroupComponent } from "ng-zorro-antd/radio";
import { NzPopoverDirective } from "ng-zorro-antd/popover";
import { NzSwitchComponent } from "ng-zorro-antd/switch";
import { NzBadgeComponent } from "ng-zorro-antd/badge";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { JupyterPanelService } from "../../service/jupyter-panel/jupyter-panel.service";
import { v4 as uuidv4 } from "uuid";
import { Notebook } from "../../service/notebook-migration/migration-llm";
import { NotebookMigrationService } from "../../service/notebook-migration/notebook-migration.service";
import {
  NotebookImportModalComponent,
  NotebookImportModalData,
} from "../notebook-import-modal/notebook-import-modal.component";
import { NzUploadFile } from "ng-zorro-antd/upload";

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
    NzDropdownDirective,
    NzDropdownMenuComponent,
    NzMenuDirective,
    NzMenuItemComponent,
    NzCheckboxComponent,
    NzRadioComponent,
    NzRadioGroupComponent,
    NgTemplateOutlet,
    ComputingUnitSelectionComponent,
    NzPopoverDirective,
    NzSwitchComponent,
    NzBadgeComponent,
    NzTooltipDirective,
    DatePipe,
    NzSpaceCompactComponent,
    AsyncPipe,
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
  public showHeatmap: boolean = false;
  public heatmapView: HeatmapView = HeatmapView.Runtime;
  public HeatmapView = HeatmapView; // make Angular HTML access enum definition
  protected readonly USER_WORKFLOW = USER_WORKFLOW;

  @Input() public writeAccess: boolean = false;
  @Input() public pid?: number = undefined;
  @Input() public autoSaveState: string = "";
  @Input() public currentWorkflowName: string = ""; // reset workflowName
  @Input() public currentExecutionName: string = ""; // reset executionName
  @Input() public particularVersionDate: string = ""; // placeholder for the metadata information of a particular workflow version
  @ViewChild("workflowNameInput") workflowNameInput: ElementRef<HTMLInputElement> | undefined;
  // Emit an event to parent component (workspace) when AI generation starts or stops
  @Output() public setWaitingForLLM = new EventEmitter<boolean>();
  public isWaitingForLLM = false;

  // variable bound with HTML to decide if the running spinner should show
  public runButtonText = "Run";
  public runIcon = "play-circle";
  public runDisable = false;

  public executionDuration = 0;

  // flag to display a particular version in the current canvas
  public displayParticularWorkflowVersion: boolean = false;
  public onClickRunHandler: () => void;

  // Computing unit status variables
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
    private router: Router,
    private jupyterPanelService: JupyterPanelService,
    private notebookMigrationService: NotebookMigrationService
  ) {
    workflowWebsocketService
      .subscribeToEvent("ExecutionDurationUpdateEvent")
      .pipe(
        tap(event => (this.executionDuration = event.duration)),
        // restart the 1s timer on each event, only while running
        switchMap(event => (event.isRunning ? timer(1000, 1000) : EMPTY)),
        untilDestroyed(this)
      )
      .subscribe(() => {
        this.executionDuration += 1000;
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
    this.computingUnitStatusService
      .getStatus()
      .pipe(untilDestroyed(this))
      .subscribe(status => {
        this.computingUnitStatus = status;
        this.applyRunButtonBehavior(this.getRunButtonBehavior());
      });
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
      el.attr(".texera-operator-state/ref-x", -10);
      el.attr(".texera-operator-state/ref-y", refY);
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
        this.router.navigate([USER_WORKFLOW]);
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
    // The editor owns applying this to the shared JointJS model (both canvas and mini-map) and
    // reapplies it whenever regions are recreated during execution (see #5120, #4027).
    this.workflowActionService.getJointGraphWrapper().setRegionsDisplayed(this.showRegion);
  }

  public toggleHeatmap(): void {
    // The editor subscribes to this stream and colors operator fills (canvas + mini-map).
    // A null view turns the overlay off; a view enables it.
    this.workflowActionService.getJointGraphWrapper().setHeatmapView(this.showHeatmap ? this.heatmapView : null);
  }

  public setHeatmapView(view: HeatmapView): void {
    this.heatmapView = view;
    if (this.showHeatmap) {
      this.workflowActionService.getJointGraphWrapper().setHeatmapView(view);
    }
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

  public get pythonNotebookMigrationEnabled(): boolean {
    return this.config.env.pythonNotebookMigrationEnabled;
  }

  // Emits whether the current workflow has an associated Jupyter notebook, used to
  // show the expand button only when there is a notebook to expand.
  public get jupyterNotebookExists$() {
    return this.jupyterPanelService.jupyterNotebookExists$;
  }

  /**
   * Expand and redisplay the Jupyter notebook panel.
   */
  public onClickExpandJupyterNotebookPanel(): void {
    this.jupyterPanelService.openJupyterNotebookPanel();
  }

  public openImportNotebookModal(): void {
    // The modal owns the upload form and the model dropdown. It delegates the decision to
    // proceed back here via requestImport so we keep the overwrite-confirm and the
    // generation pipeline (and the workflow/persist/jupyter state they touch) in the menu.
    this.modalService.create<NotebookImportModalComponent, NotebookImportModalData>({
      nzTitle: "AI Generate Workflow from Python Notebook",
      nzContent: NotebookImportModalComponent,
      nzWidth: 700,
      nzFooter: null,
      // Center in the viewport so the overwrite confirm (also centered) overlays this modal's center.
      nzCentered: true,
      nzData: {
        requestImport: (file, model) => this.confirmAndImport(file, model),
      },
    });
  }

  // Decides whether an import may proceed, then kicks it off. Resolves true when the import
  // has started (the modal should close), false when the user backs out of the overwrite
  // confirmation (the modal should stay open with the selection intact).
  private confirmAndImport(file: NzUploadFile, model: string): Promise<boolean> {
    // Reject a non-notebook file here, before starting anything, so the modal stays open
    // with the selection intact (resolving false) instead of closing on a no-op import.
    const fileExtension = file.name.split(".").pop()?.toLowerCase();
    if (fileExtension !== "ipynb") {
      this.notificationService.error("Please upload a valid Jupyter Notebook (.ipynb) file.");
      return Promise.resolve(false);
    }
    const startImport = () => this.onClickImportNotebook(file, model);
    // Generating overwrites the currently open workflow. Confirm first only when there is
    // actual content to replace; a fresh empty workflow needs no prompt.
    const graph = this.workflowActionService.getTexeraGraph();
    const currentWorkflowHasContent = graph.getAllOperators().length > 0 || graph.getAllCommentBoxes().length > 0;
    if (!currentWorkflowHasContent) {
      startImport();
      return Promise.resolve(true);
    }
    return new Promise<boolean>(resolve => {
      this.modalService.confirm({
        nzTitle: "Overwrite current workflow?",
        nzContent:
          "Generating will replace the contents of the workflow you have open. " +
          "The previous version is kept in this workflow's version history.",
        nzOkText: "Overwrite",
        nzOkDanger: true,
        // Center over the import modal, and leave only Cancel/Overwrite (no X, no click-outside).
        nzCentered: true,
        nzClosable: false,
        nzMaskClosable: false,
        nzOnOk: () => {
          startImport();
          resolve(true);
        },
        nzOnCancel: () => resolve(false),
      });
    });
  }

  public onClickImportNotebook = (file: NzUploadFile, model: string): boolean => {
    const reader = new FileReader();

    // Check if the file is a Jupyter notebook based on its extension
    const fileExtension = file.name.split(".").pop()?.toLowerCase();
    if (fileExtension !== "ipynb") {
      this.notificationService.error("Please upload a valid Jupyter Notebook (.ipynb) file.");
      return false;
    }

    this.emitWaitingForLLM(true); // start loading

    // Read the notebook file as text
    reader.readAsText(file as any);
    reader.onload = async () => {
      try {
        const result = reader.result;
        if (typeof result !== "string") {
          throw new Error("File content is not a valid string.");
        }

        // Parse the content of the .ipynb file (it's in JSON format)
        const notebookContent = JSON.parse(result) as Notebook;

        // Validate the notebook structure
        if (!notebookContent || !Array.isArray(notebookContent.cells)) {
          throw new Error("Invalid notebook structure.");
        }

        // Add UUID's to each cell in the notebook
        for (const cell of notebookContent.cells) {
          if (!cell.metadata) {
            cell.metadata = {};
          }
          cell.metadata.uuid = uuidv4();
        }

        // Get workflow and mapping from LLM
        await this.notebookMigrationService
          .sendToAIGenerateWorkflow(notebookContent, model)
          .then(result => {
            if (result) {
              const { workflowContent, mappingContent } = result;

              const fileExtensionIndex = file.name.lastIndexOf(".");
              let workflowName: string;
              if (fileExtensionIndex === -1) {
                workflowName = file.name;
              } else {
                workflowName = file.name.substring(0, fileExtensionIndex);
              }
              if (workflowName.trim() === "") {
                workflowName = DEFAULT_WORKFLOW_NAME;
              }

              // Always overwrite the current workflow: reuse its wid so persistWorkflow
              // updates that row in place instead of inserting a new one (which would leave
              // a duplicate behind). Read it now, after generation, so a wid assigned by
              // auto-persist during the wait is picked up. If the current workflow was never
              // saved, wid is undefined and a new row is created (there is nothing to overwrite).
              const reuseWid = this.workflowActionService.getWorkflow().wid;

              const workflow: Workflow = {
                content: workflowContent,
                name: `${workflowName}_GENERATED_BY_LLM`,
                isPublished: 0,
                description: undefined,
                wid: reuseWid,
                creationTime: undefined,
                lastModifiedTime: undefined,
                readonly: false,
              };

              this.workflowPersistService
                .persistWorkflow(workflow)
                .pipe(
                  switchMap((updatedWorkflow: Workflow) => {
                    const mappingID = "mapping_wid_" + updatedWorkflow.wid;

                    this.notebookMigrationService.setMapping(mappingID, mappingContent);

                    return this.notebookMigrationService
                      .storeNotebookAndMapping(updatedWorkflow.wid, 1, mappingContent, notebookContent)
                      .pipe(map(() => updatedWorkflow));
                  }),
                  untilDestroyed(this)
                )
                .subscribe({
                  next: updatedWorkflow => {
                    this.notificationService.success("Successfully generated workflow and mapping from notebook.");
                    // Reload the generated workflow onto the current (already live) canvas so it
                    // renders immediately; we never remount the workspace. Render synchronously
                    // (asyncRendering = false) so the operators exist before auto-layout runs.
                    this.workflowActionService.reloadWorkflow(updatedWorkflow, false);
                    // Tidy the LLM-generated layout; the position changes get auto-persisted.
                    this.onClickAutoLayout();
                    if (reuseWid === updatedWorkflow.wid) {
                      // Overwrote the current workflow in place: the wid did not change, so
                      // JupyterPanelService.init() does not react. Send the notebook to Jupyter
                      // and open the panel ourselves. Use openPanel, not openJupyterNotebookPanel:
                      // init()'s wid-change handler is not involved and openPanel opens
                      // unconditionally without the hasMapping gate.
                      // sendNotebookToJupyter never rejects: it resolves 1 on success and 0 on
                      // failure (it toasts the error itself). Open the panel only on success so we
                      // do not float it over a blank iframe, matching the init()-driven path which
                      // opens only when fetchNotebookAndMapping reports the send succeeded.
                      this.notebookMigrationService.sendNotebookToJupyter(notebookContent).then(result => {
                        if (result == 1) {
                          this.jupyterPanelService.openPanel("JupyterNotebookPanel");
                        }
                      });
                    } else {
                      // The current workflow had never been saved, so a new row was created and the
                      // wid changed. reloadWorkflow's synchronous wid change drives init() to fetch
                      // the stored notebook/mapping, send it to Jupyter, and open the panel, so we
                      // do not do that here (doing so would double the "sent to Jupyter" toast).
                      // Point the URL at the generated workflow.
                      this.location.go(`${USER_WORKSPACE}/${updatedWorkflow.wid}`);
                    }
                  },
                  error: (err: unknown) => {
                    this.notificationService.error("Failed to import notebook, check console for detailed error");
                    console.error("Import notebook failed:", err);
                    this.emitWaitingForLLM(false);
                  },
                  complete: () => {
                    this.emitWaitingForLLM(false);
                  },
                });
            } else {
              this.notificationService.error("No workflow was generated from the notebook.");
              console.error("Result is undefined");
              this.emitWaitingForLLM(false);
            }
          })
          .catch(error => {
            this.notificationService.error("Error while communicating with LLM, check console for details");
            console.error("Error while fetching data from LLM: ", error);
            this.emitWaitingForLLM(false);
          });
      } catch (error) {
        this.notificationService.error("Failed to import the notebook.");
        console.error(error);
        this.emitWaitingForLLM(false);
      }
    };

    reader.onerror = () => {
      this.notificationService.error("Failed to read the notebook file.");
      this.emitWaitingForLLM(false);
    };

    return false; // Prevent automatic upload handling
  };

  // Keeps the local waiting flag and the parent-facing output in lockstep so the
  // AI-generate button can be disabled while a conversion is in flight.
  private emitWaitingForLLM(waiting: boolean): void {
    this.isWaitingForLLM = waiting;
    this.setWaitingForLLM.emit(waiting);
  }

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

      // Show the modal in the ComputingUnitSelectionComponent, seeding the name field
      this.computingUnitSelectionComponent.showAddComputeUnitModalVisible(defaultName);
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
