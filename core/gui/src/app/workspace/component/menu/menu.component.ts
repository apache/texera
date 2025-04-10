import { DatePipe, Location } from "@angular/common";
import { Component, ElementRef, Input, OnDestroy, OnInit, ViewChild } from "@angular/core";
import { environment } from "../../../../environments/environment";
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
import { catchError, debounceTime, filter, mergeMap, tap, takeUntil } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowUtilService } from "../../service/workflow-graph/util/workflow-util.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { UserProjectService } from "../../../dashboard/service/user/project/user-project.service";
import { NzUploadFile } from "ng-zorro-antd/upload";
import { saveAs } from "file-saver";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { OperatorMenuService } from "../../service/operator-menu/operator-menu.service";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
import { firstValueFrom, of, Subscription, timer, interval, Subject } from "rxjs";
import { isDefined } from "../../../common/util/predicate";
import { NzModalService } from "ng-zorro-antd/modal";
import { ResultExportationComponent } from "../result-exportation/result-exportation.component";
import { ReportGenerationService } from "../../service/report-generation/report-generation.service";
import { ShareAccessComponent } from "src/app/dashboard/component/user/share-access/share-access.component";
import { PanelService } from "../../service/panel/panel.service";
import { DASHBOARD_USER_WORKFLOW } from "../../../app-routing.constant";
import { WorkflowComputingUnitManagingService } from "../../service/workflow-computing-unit/workflow-computing-unit-managing.service";
import { ComputingUnitStatusService } from "../../service/computing-unit-status/computing-unit-status.service";
import { DashboardWorkflowComputingUnit } from "../../types/workflow-computing-unit";

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
})
export class MenuComponent implements OnInit, OnDestroy {
  public executionState: ExecutionState; // set this to true when the workflow is started
  public ExecutionState = ExecutionState; // make Angular HTML access enum definition
  public emailNotificationEnabled: boolean = environment.workflowEmailNotificationEnabled;
  public isWorkflowValid: boolean = true; // this will check whether the workflow error or not
  public isWorkflowEmpty: boolean = false;
  public isSaving: boolean = false;
  public isWorkflowModifiable: boolean = false;
  public workflowId?: number;
  public isExportDeactivate: boolean = false;
  protected readonly DASHBOARD_USER_WORKFLOW = DASHBOARD_USER_WORKFLOW;

  @Input() public writeAccess: boolean = false;
  @Input() public pid?: number = undefined;
  @Input() public autoSaveState: string = "";
  @Input() public currentWorkflowName: string = ""; // reset workflowName
  @Input() public currentExecutionName: string = ""; // reset executionName
  @Input() public particularVersionDate: string = ""; // placeholder for the metadata information of a particular workflow version
  @ViewChild("nameInput") nameInputBox: ElementRef<HTMLElement> | undefined;

  // variable bound with HTML to decide if the running spinner should show
  public runButtonText = "Run";
  public runIcon = "play-circle";
  public runDisable = false;

  public executionDuration = 0;
  private durationUpdateSubscription: Subscription = new Subscription();

  // whether user dashboard is enabled and accessible from the workspace
  public userSystemEnabled: boolean = environment.userSystemEnabled;
  // flag to display a particular version in the current canvas
  public displayParticularWorkflowVersion: boolean = false;
  public onClickRunHandler: () => void;
  
  // Computing unit status variables
  private computingUnitStatusSubscription: Subscription = new Subscription();
  public computingUnitStatus: string = "";
  private computingUnitConnected: boolean = false;
  
  // Auto-create computing unit variables
  public isCreatingComputingUnit = false;
  public isConnectingToComputingUnit = false;
  private autoRunAfterConnect = false;
  private destroy$ = new Subject<void>();

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
    private computingUnitService: WorkflowComputingUnitManagingService
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
    // this.currentWorkflowName = this.workflowCacheService.getCachedWorkflow();
    this.registerWorkflowModifiableChangedHandler();
    this.registerWorkflowIdUpdateHandler();
    
    // Subscribe to computing unit status changes
    this.subscribeToComputingUnitStatus();
  }

  public ngOnInit(): void {
    this.executeWorkflowService
      .getExecutionStateStream()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        this.executionState = event.current.state;
        
        // Clear the autoRunAfterConnect flag when execution has started or when it completes
        if (event.current.state === ExecutionState.Initializing || 
            event.current.state === ExecutionState.Running) {
          this.autoRunAfterConnect = false;
        }
        
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
        this.isExportDeactivate = !this.workflowResultExportService.exportExecutionResultEnabled || !hasResultToExport;
      });

    this.registerWorkflowMetadataDisplayRefresh();
    this.handleWorkflowVersionDisplay();
  }

  ngOnDestroy(): void {
    this.workflowResultExportService.resetFlags();
    this.computingUnitStatusSubscription.unsubscribe();
    this.destroy$.next();
    this.destroy$.complete();
  }
  
  /**
   * Subscribe to computing unit status changes from the ComputingUnitStatusService
   */
  private subscribeToComputingUnitStatus(): void {
    // First ensure we have the correct connection status
    this.computingUnitConnected = this.workflowWebsocketService.isConnected;
    
    // Default to "Running" if websocket is connected
    if (this.workflowWebsocketService.isConnected) {
      this.computingUnitStatus = "Running";
    } else {
      this.computingUnitStatus = "Disconnected";
    }
    
    // Update the button UI initially
    this.applyRunButtonBehavior(this.getRunButtonBehavior());
    
    // Subscribe to websocket connection changes directly
    this.computingUnitStatusSubscription.add(
      interval(1000)
        .pipe(untilDestroyed(this))
        .subscribe(() => {
          const isConnected = this.workflowWebsocketService.isConnected;
          if (this.computingUnitConnected !== isConnected) {
            this.computingUnitConnected = isConnected;
            this.computingUnitStatus = isConnected ? "Running" : "Disconnected";
            this.applyRunButtonBehavior(this.getRunButtonBehavior());
          }
        })
    );
    
    // Subscribe to computing unit status changes if needed
    if (environment.computingUnitManagerEnabled) {
      // Subscribe to status changes
      this.computingUnitStatusSubscription.add(
        this.computingUnitStatusService
          .getStatus()
          .pipe(untilDestroyed(this))
          .subscribe(status => {
            this.computingUnitStatus = status;
            
            // If status indicates the unit is in a connecting state, update the UI
            if (status === "Pending" || status === "Disconnected") {
              this.isConnectingToComputingUnit = true;
            } else if (status === "Running") {
              // Only clear connecting flag if we're not auto-running
              if (!this.autoRunAfterConnect) {
                this.isConnectingToComputingUnit = false;
              }
            }
            
            // If status indicates no unit or disconnected, update connection status too
            if (status === "No Computing Unit" || status === "Disconnected" || status === "Terminating") {
              this.computingUnitConnected = false;
            }
            
            this.applyRunButtonBehavior(this.getRunButtonBehavior());
          })
      );
      
      // Subscribe to the selected unit for auto-run functionality
      this.computingUnitStatusSubscription.add(
        this.computingUnitStatusService
          .getSelectedComputingUnit()
          .pipe(untilDestroyed(this))
          .subscribe(unit => {
            // If we're not auto-running, update to remove previous auto-run state
            if (!this.autoRunAfterConnect && this.isConnectingToComputingUnit && 
                unit && unit.status === "Running" && this.workflowWebsocketService.isConnected) {
              this.isConnectingToComputingUnit = false;
              this.applyRunButtonBehavior(this.getRunButtonBehavior());
            }
            
            // If we're in the execution state, don't allow status to change
            if (this.executionState === ExecutionState.Initializing || 
                this.executionState === ExecutionState.Running) {
              return;
            }
            
            // If computing unit is not ready, update connection status
            if (!unit || unit.status !== "Running") {
              this.computingUnitConnected = false;
            }
          })
      );
      
      // Subscribe directly to the connection status
      this.computingUnitStatusSubscription.add(
        this.computingUnitStatusService
          .getConnectionStatus()
          .pipe(untilDestroyed(this))
          .subscribe(connected => {
            this.computingUnitConnected = connected;
            if (!connected) {
              this.computingUnitStatus = "Disconnected";
            }
            this.applyRunButtonBehavior(this.getRunButtonBehavior());
          })
      );
    }
  }
  
  /**
   * Create a new computing unit and connect to it
   */
  private createAndConnectComputingUnit(): void {
    if (!environment.computingUnitManagerEnabled || !this.workflowId) {
      return;
    }
    
    this.isCreatingComputingUnit = true;
    this.applyRunButtonBehavior(this.getRunButtonBehavior());
    
    // Get the available configurations
    this.computingUnitService.getComputingUnitLimitOptions()
      .pipe(
        takeUntil(this.destroy$),
        mergeMap(({ cpuLimitOptions, memoryLimitOptions }) => {
          const defaultCpu = cpuLimitOptions[0] || "1";
          const defaultMemory = memoryLimitOptions[0] || "1Gi";
          const unitName = `Workflow ${this.workflowId} Unit`;
          
          // Create the computing unit
          return this.computingUnitService.createComputingUnit(unitName, defaultCpu, defaultMemory);
        })
      )
      .subscribe({
        next: (unit: DashboardWorkflowComputingUnit) => {
          this.isCreatingComputingUnit = false;
          this.isConnectingToComputingUnit = true;
          this.applyRunButtonBehavior(this.getRunButtonBehavior());
          
          // Connect to the unit
          this.connectToComputingUnit(unit);
        },
        error: err => {
          this.isCreatingComputingUnit = false;
          this.isConnectingToComputingUnit = false;
          this.notificationService.error(`Failed to create computing unit: ${err}`);
          this.applyRunButtonBehavior(this.getRunButtonBehavior());
        }
      });
  }
  
  /**
   * Connect to a computing unit
   */
  private connectToComputingUnit(unit: DashboardWorkflowComputingUnit): void {
    if (!this.workflowId) {
      return;
    }
    
    // Set the connecting state
    this.isConnectingToComputingUnit = true;
    this.applyRunButtonBehavior(this.getRunButtonBehavior());
    
    // Select the unit in the service
    this.computingUnitStatusService.selectComputingUnit(unit);
    
    // Connect to the unit's websocket
    this.workflowWebsocketService.closeWebsocket();
    this.workflowWebsocketService.openWebsocket(this.workflowId, undefined, unit.computingUnit.cuid);
    
    // Start polling for connection status
    const connectionCheck = interval(500)
      .pipe(
        takeUntil(this.destroy$),
        filter(() => this.workflowWebsocketService.isConnected && unit.status === "Running")
      )
      .subscribe(() => {
        // Update connection status
        this.computingUnitConnected = true;
        this.computingUnitStatus = "Running";
        
        // Only clear connecting state if we're not auto-running
        if (!this.autoRunAfterConnect) {
          this.isConnectingToComputingUnit = false;
        }
        
        // Immediately update the button to show "Submitting" if we're auto-running
        this.applyRunButtonBehavior(this.getRunButtonBehavior());
        connectionCheck.unsubscribe();
        
        // If we were waiting to run the workflow after connecting, do it now
        if (this.autoRunAfterConnect) {
          // Do not clear the flag yet, it will be cleared in the execution
          // Short delay to ensure everything is ready
          setTimeout(() => {
            // Execute workflow
            this.executeWorkflowService.executeWorkflowWithEmailNotification(
              this.currentExecutionName,
              this.emailNotificationEnabled && environment.userSystemEnabled
            );
            
            // Only clear the flag once execution has started
            this.executeWorkflowService
              .getExecutionStateStream()
              .pipe(
                filter(event => 
                  event.current.state === ExecutionState.Initializing || 
                  event.current.state === ExecutionState.Running
                ),
                takeUntil(this.destroy$)
              )
              .subscribe(() => {
                this.autoRunAfterConnect = false;
                this.isConnectingToComputingUnit = false;
              });
          }, 500);
        }
      });
    
    // Timeout after 15 seconds of waiting
    setTimeout(() => {
      if (this.isConnectingToComputingUnit && !this.autoRunAfterConnect) {
        this.isConnectingToComputingUnit = false;
        this.notificationService.warning("Connection to computing unit is taking longer than expected");
        this.applyRunButtonBehavior(this.getRunButtonBehavior());
      }
    }, 15000);
  }
  
  /**
   * Create a computing unit, connect to it, and run the workflow
   */
  private createConnectAndRun(): void {
    // Set autoRunAfterConnect first, before any async operations
    this.autoRunAfterConnect = true;
    
    // Update button immediately to show connecting state
    this.applyRunButtonBehavior(this.getRunButtonBehavior());
    
    // Then create the computing unit
    this.createAndConnectComputingUnit();
  }

  public async onClickOpenShareAccess(): Promise<void> {
    this.modalService.create({
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
    // Check for empty workflow first
    if (this.isWorkflowEmpty) {
      return {
        text: "Empty",
        icon: "exclamation-circle",
        disable: true,
        onClick: () => {},
      };
    } 
    
    // Then check for validation errors
    if (!this.isWorkflowValid) {
      return {
        text: "Error",
        icon: "exclamation-circle",
        disable: true,
        onClick: () => {},
      };
    }
    
    // Check if we're in the process of creating or connecting to a computing unit
    if (this.isCreatingComputingUnit) {
      return {
        text: "Creating Unit",
        icon: "loading",
        disable: true,
        onClick: () => {},
      };
    }
    
    // If we're connecting but not yet ready to auto-run
    if (this.isConnectingToComputingUnit) {
      return {
        text: "Connecting",
        icon: "loading",
        disable: true,
        onClick: () => {},
      };
    }
    
    // If we're connected and waiting to auto-run, show "Submitting..." instead of "Connecting"
    if (this.autoRunAfterConnect && this.computingUnitConnected && this.computingUnitStatus === "Running") {
      return {
        text: "Submitting",
        icon: "loading",
        disable: true,
        onClick: () => {},
      };
    }
    
    // In cuManager mode, if no computing unit, always allow running which will create one
    if (environment.computingUnitManagerEnabled && 
        (!this.workflowWebsocketService.isConnected || this.computingUnitStatus !== "Running")) {
      return {
        text: "Run",
        icon: "play-circle",
        disable: false,
        onClick: () => this.createConnectAndRun(),
      };
    }
    
    // Handle execution states when connected to a running computing unit
    switch (this.executionState) {
      case ExecutionState.Uninitialized:
      case ExecutionState.Completed:
      case ExecutionState.Killed:
      case ExecutionState.Failed:
        return {
          text: "Run",
          icon: "play-circle",
          disable: false,
          onClick: () =>
            this.executeWorkflowService.executeWorkflowWithEmailNotification(
              this.currentExecutionName,
              this.emailNotificationEnabled && environment.userSystemEnabled
            ),
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
      default:
        return {
          text: "Run",
          icon: "play-circle",
          disable: false,
          onClick: () =>
            this.executeWorkflowService.executeWorkflowWithEmailNotification(
              this.currentExecutionName,
              this.emailNotificationEnabled && environment.userSystemEnabled
            ),
        };
    }
  }

  public onClickAddCommentBox(): void {
    this.workflowActionService.addCommentBox(this.workflowUtilService.getNewCommentBox());
  }

  private async waitForConditions(): Promise<void> {
    const checkConditions = () => {
      return this.workflowWebsocketService.isConnected && !this.displayParticularWorkflowVersion;
    };

    while (!checkConditions()) {
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
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

  /**
   * This method will flip the current status of whether to draw grids in jointPaper.
   * This option is only for the current session and will be cleared on refresh.
   */
  public onClickToggleGrids(): void {
    this.workflowActionService.getJointGraphWrapper().toggleGrids();
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
      .subscribe(metadata => (this.workflowId = metadata.wid));
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

    // If computing unit manager is enabled but no unit is selected/connected
    if (environment.computingUnitManagerEnabled && 
        !this.computingUnitConnected) {
      
      // First create a computing unit
      this.autoRunAfterConnect = true;
      this.currentExecutionName = this.currentExecutionName || "Untitled Execution";
      this.createAndConnectComputingUnit();
      return;
    }
    
    // Regular workflow execution
    this.executeWorkflowService.executeWorkflowWithEmailNotification(
      this.currentExecutionName || "Untitled Execution",
      this.emailNotificationEnabled && environment.userSystemEnabled
    );
  }

  protected readonly environment = environment;
}
