import { DatePipe, Location } from "@angular/common";
import { Component, ElementRef, Input, OnInit, ViewChild } from "@angular/core";
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
import { JointGraphWrapper } from "../../service/workflow-graph/model/joint-graph-wrapper";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { WorkflowWebsocketService } from "../../service/workflow-websocket/workflow-websocket.service";
import { WorkflowResultExportService } from "../../service/workflow-result-export/workflow-result-export.service";
import { debounceTime, filter, mergeMap, tap } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowUtilService } from "../../service/workflow-graph/util/workflow-util.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { UserProjectService } from "../../../dashboard/service/user/project/user-project.service";
import { NzUploadFile } from "ng-zorro-antd/upload";
import { saveAs } from "file-saver";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { OperatorMenuService } from "../../service/operator-menu/operator-menu.service";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
import { firstValueFrom, Subscription, timer } from "rxjs";
import { isDefined } from "../../../common/util/predicate";
import { NzModalService } from "ng-zorro-antd/modal";
import { ResultExportationComponent } from "../result-exportation/result-exportation.component";
import { ReportGenerationService } from "../../service/report-generation/report-generation.service";
import { ShareAccessComponent } from "src/app/dashboard/component/user/share-access/share-access.component";
import { UdfDebugService } from "../../service/operator-debug/udf-debug.service";
import { PanelService } from "../../service/panel/panel.service";

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
export class MenuComponent implements OnInit {
  public executionState: ExecutionState; // set this to true when the workflow is started
  public ExecutionState = ExecutionState; // make Angular HTML access enum definition
  public emailNotificationEnabled: boolean = environment.workflowEmailNotificationEnabled;
  public isWorkflowValid: boolean = true; // this will check whether the workflow error or not
  public isWorkflowEmpty: boolean = false;
  public isSaving: boolean = false;
  public isWorkflowModifiable: boolean = false;
  public workflowId?: number;

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
    private panelService: PanelService
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

    this.registerWorkflowMetadataDisplayRefresh();
    this.handleWorkflowVersionDisplay();
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
    if (this.isWorkflowEmpty) {
      return {
        text: "Empty",
        icon: "exclamation-circle",
        disable: true,
        onClick: () => {},
      };
    } else if (!this.isWorkflowValid) {
      return {
        text: "Error",
        icon: "exclamation-circle",
        disable: true,
        onClick: () => {},
      };
    }
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
      case ExecutionState.Recovering:
        return {
          text: "Recovering",
          icon: "loading",
          disable: true,
          onClick: () => {},
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
  public onClickExportExecutionResult(exportType: string): void {
    this.modalService.create({
      nzTitle: "Export Result and Save to a Dataset",
      nzContent: ResultExportationComponent,
      nzData: {
        exportType: exportType,
        workflowName: this.currentWorkflowName,
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
    this.workflowActionService.deleteOperatorsAndLinks(allOperatorIDs, []);
  }

  public onClickImportNotebook = (file: NzUploadFile): boolean => {
    const reader = new FileReader();

    // Check if the file is a Jupyter notebook based on its extension
    const fileExtension = file.name.split(".").pop()?.toLowerCase();
    if (fileExtension !== "ipynb") {
      this.notificationService.error("Please upload a valid Jupyter Notebook (.ipynb) file.");
      return false;
    }

    // Read the notebook file as text
    reader.readAsText(file as any);
    reader.onload = () => {
      try {
        const result = reader.result;
        if (typeof result !== "string") {
          throw new Error("File content is not a valid string.");
        }

        // Parse the content of the .ipynb file (it's in JSON format)
        const notebookContent = JSON.parse(result);

        // Validate the notebook structure
        if (!notebookContent || !Array.isArray(notebookContent.cells)) {
          throw new Error("Invalid notebook structure.");
        }

        // Print content of notebook (JSON)
        console.log(`Notebook JSON: ${JSON.stringify(notebookContent)}`);

        // Send Notebook JSON to pod to open in jupyterlab
        this.sendNotebookToPod(notebookContent);

        // TODO: Send Notebook JSON to ChatGPT to create Workflow + Mapping : Ensure consistent cell IDs in notebooks
        // this.sendToAIGenerateWorkflow();

        // TODO: Mapping generation needs to be done

        // TODO: Replace below to integrate with a ChatGPT generated Texera Workflow
        // Mock data conversion to a format compatible with Texera workflows
        const workflowContent: WorkflowContent = {
          "settings": {
            "dataTransferBatchSize": 400
          },
          "operators": [
            {
              "operatorID": "CSVFileScan-operator-43e5d7f4-2d3d-498b-a97d-b4d242ee82cf",
              "operatorType": "CSVFileScan",
              "operatorVersion": "1fa249a9d55d4dcad36d93e093c2faed5c4434f0",
              "operatorProperties": {
                "fileEncoding": "UTF_8",
                "customDelimiter": ",",
                "hasHeader": true,
                "fileName": "/ryanyz@uci.edu/diabetes/v1/diabetes.csv"
              },
              "inputPorts": [],
              "outputPorts": [
                {
                  "portID": "output-0",
                  "displayName": "",
                  "allowMultiInputs": false,
                  "isDynamicPort": false
                }
              ],
              "showAdvanced": false,
              "isDisabled": false,
              "customDisplayName": "Read_In_Diabetes_Dataset",
              "dynamicInputPorts": false,
              "dynamicOutputPorts": false
            },
            {
              "operatorID": "Distinct-operator-d6a18641-3c29-4835-b8d2-60351b0b8882",
              "operatorType": "Distinct",
              "operatorVersion": "95280bbcbb5758853cacb3dd29495d0bd5d697b4",
              "operatorProperties": {},
              "inputPorts": [
                {
                  "portID": "input-0",
                  "displayName": "",
                  "allowMultiInputs": false,
                  "isDynamicPort": false,
                  "dependencies": []
                }
              ],
              "outputPorts": [
                {
                  "portID": "output-0",
                  "displayName": "",
                  "allowMultiInputs": false,
                  "isDynamicPort": false
                }
              ],
              "showAdvanced": false,
              "isDisabled": false,
              "customDisplayName": "Remove_Duplicate_Rows",
              "dynamicInputPorts": false,
              "dynamicOutputPorts": false
            },
            {
              "operatorID": "Filter-operator-b46dc58f-8100-478e-80c5-87d93c1f7e04",
              "operatorType": "Filter",
              "operatorVersion": "95280bbcbb5758853cacb3dd29495d0bd5d697b4",
              "operatorProperties": {
                "predicates": [
                  {
                    "attribute": "Pregnancies",
                    "condition": "is not null"
                  },
                  {
                    "attribute": "Glucose",
                    "condition": "is not null"
                  },
                  {
                    "attribute": "BloodPressure",
                    "condition": "is not null"
                  },
                  {
                    "attribute": "SkinThickness",
                    "condition": "is not null"
                  },
                  {
                    "attribute": "Insulin",
                    "condition": "is not null"
                  },
                  {
                    "attribute": "BMI",
                    "condition": "is not null"
                  },
                  {
                    "attribute": "DiabetesPedigreeFunction",
                    "condition": "is not null"
                  },
                  {
                    "attribute": "Age",
                    "condition": "is not null"
                  },
                  {
                    "value": "",
                    "attribute": "Outcome",
                    "condition": "is not null"
                  }
                ]
              },
              "inputPorts": [
                {
                  "portID": "input-0",
                  "displayName": "",
                  "allowMultiInputs": false,
                  "isDynamicPort": false,
                  "dependencies": []
                }
              ],
              "outputPorts": [
                {
                  "portID": "output-0",
                  "displayName": "",
                  "allowMultiInputs": false,
                  "isDynamicPort": false
                }
              ],
              "showAdvanced": false,
              "isDisabled": false,
              "customDisplayName": "Remove_if_any_null",
              "dynamicInputPorts": false,
              "dynamicOutputPorts": false
            },
            {
              "operatorID": "Aggregate-operator-ba7c3a06-09f8-4ad3-977d-69988789e671",
              "operatorType": "Aggregate",
              "operatorVersion": "95280bbcbb5758853cacb3dd29495d0bd5d697b4",
              "operatorProperties": {
                "aggregations": [
                  {
                    "attribute": "Pregnancies",
                    "result attribute": "avg_pregnancies",
                    "aggFunction": "average"
                  },
                  {
                    "result attribute": "avg_glucose",
                    "aggFunction": "average",
                    "attribute": "Glucose"
                  },
                  {
                    "result attribute": "avg_bloodpressure",
                    "aggFunction": "average",
                    "attribute": "BloodPressure"
                  },
                  {
                    "result attribute": "avg_skinthickness",
                    "aggFunction": "average",
                    "attribute": "SkinThickness"
                  },
                  {
                    "result attribute": "avg_insulin",
                    "aggFunction": "average",
                    "attribute": "Insulin"
                  },
                  {
                    "result attribute": "avg_bmi",
                    "aggFunction": "average",
                    "attribute": "BMI"
                  },
                  {
                    "result attribute": "avg_diabetespedigreefunction",
                    "aggFunction": "average",
                    "attribute": "DiabetesPedigreeFunction"
                  },
                  {
                    "result attribute": "avg_age",
                    "aggFunction": "average",
                    "attribute": "Age"
                  },
                  {
                    "result attribute": "avg_outcome",
                    "aggFunction": "average",
                    "attribute": "Outcome"
                  }
                ]
              },
              "inputPorts": [
                {
                  "portID": "input-0",
                  "displayName": "",
                  "allowMultiInputs": false,
                  "isDynamicPort": false,
                  "dependencies": []
                }
              ],
              "outputPorts": [
                {
                  "portID": "output-0",
                  "displayName": "",
                  "allowMultiInputs": false,
                  "isDynamicPort": false
                }
              ],
              "showAdvanced": false,
              "isDisabled": false,
              "customDisplayName": "Average_of_Fields",
              "dynamicInputPorts": false,
              "dynamicOutputPorts": false
            },
            {
              "operatorID": "Aggregate-operator-cd2cbbbf-bad0-46ed-9e13-65ccd1e67c27",
              "operatorType": "Aggregate",
              "operatorVersion": "95280bbcbb5758853cacb3dd29495d0bd5d697b4",
              "operatorProperties": {
                "aggregations": [
                  {
                    "attribute": "Pregnancies",
                    "result attribute": "min_pregnancies",
                    "aggFunction": "min"
                  },
                  {
                    "result attribute": "min_glucose",
                    "aggFunction": "min",
                    "attribute": "Glucose"
                  },
                  {
                    "result attribute": "min_bloodpressure",
                    "aggFunction": "min",
                    "attribute": "BloodPressure"
                  },
                  {
                    "result attribute": "min_skinthickness",
                    "aggFunction": "min",
                    "attribute": "SkinThickness"
                  },
                  {
                    "result attribute": "min_insulin",
                    "aggFunction": "min",
                    "attribute": "Insulin"
                  },
                  {
                    "result attribute": "min_bmi",
                    "aggFunction": "min",
                    "attribute": "BMI"
                  },
                  {
                    "result attribute": "min_diabetespedigreefunction",
                    "aggFunction": "min",
                    "attribute": "DiabetesPedigreeFunction"
                  },
                  {
                    "result attribute": "min_age",
                    "aggFunction": "min",
                    "attribute": "Age"
                  },
                  {
                    "result attribute": "min_outcome",
                    "aggFunction": "min",
                    "attribute": "Outcome"
                  }
                ]
              },
              "inputPorts": [
                {
                  "portID": "input-0",
                  "displayName": "",
                  "allowMultiInputs": false,
                  "isDynamicPort": false,
                  "dependencies": []
                }
              ],
              "outputPorts": [
                {
                  "portID": "output-0",
                  "displayName": "",
                  "allowMultiInputs": false,
                  "isDynamicPort": false
                }
              ],
              "showAdvanced": false,
              "isDisabled": false,
              "customDisplayName": "Min_of_Fields",
              "dynamicInputPorts": false,
              "dynamicOutputPorts": false
            },
            {
              "operatorID": "Aggregate-operator-3abc30e7-9ba3-4a97-ac28-937ca950cccf",
              "operatorType": "Aggregate",
              "operatorVersion": "95280bbcbb5758853cacb3dd29495d0bd5d697b4",
              "operatorProperties": {
                "aggregations": [
                  {
                    "attribute": "Pregnancies",
                    "result attribute": "max_pregnancies",
                    "aggFunction": "max"
                  },
                  {
                    "result attribute": "max_glucose",
                    "aggFunction": "max",
                    "attribute": "Glucose"
                  },
                  {
                    "result attribute": "max_bloodpressure",
                    "aggFunction": "max",
                    "attribute": "BloodPressure"
                  },
                  {
                    "result attribute": "max_skinthickness",
                    "aggFunction": "max",
                    "attribute": "SkinThickness"
                  },
                  {
                    "result attribute": "max_insulin",
                    "aggFunction": "max",
                    "attribute": "Insulin"
                  },
                  {
                    "result attribute": "max_bmi",
                    "aggFunction": "max",
                    "attribute": "BMI"
                  },
                  {
                    "result attribute": "max_diabetespedigreefunction",
                    "aggFunction": "max",
                    "attribute": "DiabetesPedigreeFunction"
                  },
                  {
                    "result attribute": "max_age",
                    "aggFunction": "max",
                    "attribute": "Age"
                  },
                  {
                    "result attribute": "max_outcome",
                    "aggFunction": "max",
                    "attribute": "Outcome"
                  }
                ]
              },
              "inputPorts": [
                {
                  "portID": "input-0",
                  "displayName": "",
                  "allowMultiInputs": false,
                  "isDynamicPort": false,
                  "dependencies": []
                }
              ],
              "outputPorts": [
                {
                  "portID": "output-0",
                  "displayName": "",
                  "allowMultiInputs": false,
                  "isDynamicPort": false
                }
              ],
              "showAdvanced": false,
              "isDisabled": false,
              "customDisplayName": "Max_of_Fields",
              "dynamicInputPorts": false,
              "dynamicOutputPorts": false
            },
            {
              "operatorID": "BoxPlot-operator-9ffd3dce-29b4-4373-ab38-235487393d72",
              "operatorType": "BoxPlot",
              "operatorVersion": "5d03929e14c0871806f53fe06905127405b98a0e",
              "operatorProperties": {
                "orientation": true,
                "Quartile Method": "linear",
                "value": "Pregnancies"
              },
              "inputPorts": [
                {
                  "portID": "input-0",
                  "displayName": "",
                  "allowMultiInputs": false,
                  "isDynamicPort": false,
                  "dependencies": []
                }
              ],
              "outputPorts": [
                {
                  "portID": "output-0",
                  "displayName": "",
                  "allowMultiInputs": false,
                  "isDynamicPort": false
                }
              ],
              "showAdvanced": false,
              "isDisabled": false,
              "customDisplayName": "Pregnancy_BoxPlot",
              "dynamicInputPorts": false,
              "dynamicOutputPorts": false,
              "viewResult": false
            },
            {
              "operatorID": "PythonUDFV2-operator-1d891fad-a245-4e03-ba40-ad9ffbfa2b6b",
              "operatorType": "PythonUDFV2",
              "operatorVersion": "3d69fdcedbb409b47162c4b55406c77e54abe416",
              "operatorProperties": {
                "code": "from pytexera import *\nimport pandas as pd\n\nclass ProcessTableOperator(UDFTableOperator):\n\n    @overrides\n    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:\n        table = pd.DataFrame(table)\n        print(table.describe())\n        yield\n",
                "workers": 1,
                "retainInputColumns": true
              },
              "inputPorts": [
                {
                  "portID": "input-0",
                  "displayName": "",
                  "allowMultiInputs": true,
                  "isDynamicPort": false,
                  "dependencies": []
                }
              ],
              "outputPorts": [
                {
                  "portID": "output-0",
                  "displayName": "",
                  "allowMultiInputs": false,
                  "isDynamicPort": false
                }
              ],
              "showAdvanced": false,
              "isDisabled": false,
              "customDisplayName": "Describe_Data",
              "dynamicInputPorts": true,
              "dynamicOutputPorts": true
            },
            {
              "operatorID": "Split-operator-3e01a035-69bd-487b-bb3c-edf23fb7a64c",
              "operatorType": "Split",
              "operatorVersion": "3ba1dfe9e53f217fbe363af0d2562fcf743a6269",
              "operatorProperties": {
                "training percentage": 80,
                "random seed": 1
              },
              "inputPorts": [
                {
                  "portID": "input-0",
                  "displayName": "",
                  "allowMultiInputs": false,
                  "isDynamicPort": false,
                  "dependencies": []
                }
              ],
              "outputPorts": [
                {
                  "portID": "output-0",
                  "displayName": "training",
                  "allowMultiInputs": false,
                  "isDynamicPort": false
                },
                {
                  "portID": "output-1",
                  "displayName": "testing",
                  "allowMultiInputs": false,
                  "isDynamicPort": false
                }
              ],
              "showAdvanced": false,
              "isDisabled": false,
              "customDisplayName": "Training/Testing Split",
              "dynamicInputPorts": true,
              "dynamicOutputPorts": true
            },
            {
              "operatorID": "SklearnRandomForest-operator-7844ee52-5d28-4421-a537-d267bb52efa8",
              "operatorType": "SklearnRandomForest",
              "operatorVersion": "6dd0927e6ddf688cb924ecd47b03d436dec9881d",
              "operatorProperties": {
                "countVectorizer": false,
                "tfidfTransformer": false,
                "target": "Outcome"
              },
              "inputPorts": [
                {
                  "portID": "input-0",
                  "displayName": "training",
                  "allowMultiInputs": false,
                  "isDynamicPort": false,
                  "dependencies": []
                },
                {
                  "portID": "input-1",
                  "displayName": "testing",
                  "allowMultiInputs": false,
                  "isDynamicPort": false,
                  "dependencies": [
                    {
                      "id": 0,
                      "internal": false
                    }
                  ]
                }
              ],
              "outputPorts": [
                {
                  "portID": "output-0",
                  "displayName": "",
                  "allowMultiInputs": false,
                  "isDynamicPort": false
                }
              ],
              "showAdvanced": false,
              "isDisabled": false,
              "customDisplayName": "Random Forest",
              "dynamicInputPorts": false,
              "dynamicOutputPorts": false
            },
            {
              "operatorID": "SklearnDecisionTree-operator-65ea9d8c-9057-4ebc-8a40-a8c01333f948",
              "operatorType": "SklearnDecisionTree",
              "operatorVersion": "6dd0927e6ddf688cb924ecd47b03d436dec9881d",
              "operatorProperties": {
                "countVectorizer": false,
                "tfidfTransformer": false,
                "target": "Outcome"
              },
              "inputPorts": [
                {
                  "portID": "input-0",
                  "displayName": "training",
                  "allowMultiInputs": false,
                  "isDynamicPort": false,
                  "dependencies": []
                },
                {
                  "portID": "input-1",
                  "displayName": "testing",
                  "allowMultiInputs": false,
                  "isDynamicPort": false,
                  "dependencies": [
                    {
                      "id": 0,
                      "internal": false
                    }
                  ]
                }
              ],
              "outputPorts": [
                {
                  "portID": "output-0",
                  "displayName": "",
                  "allowMultiInputs": false,
                  "isDynamicPort": false
                }
              ],
              "showAdvanced": false,
              "isDisabled": false,
              "customDisplayName": "Decision Tree",
              "dynamicInputPorts": false,
              "dynamicOutputPorts": false
            },
            {
              "operatorID": "SklearnSVM-operator-7adceacc-b3f9-4af6-8319-cd3c3f96d95d",
              "operatorType": "SklearnSVM",
              "operatorVersion": "6dd0927e6ddf688cb924ecd47b03d436dec9881d",
              "operatorProperties": {
                "countVectorizer": false,
                "tfidfTransformer": false,
                "target": "Outcome"
              },
              "inputPorts": [
                {
                  "portID": "input-0",
                  "displayName": "training",
                  "allowMultiInputs": false,
                  "isDynamicPort": false,
                  "dependencies": []
                },
                {
                  "portID": "input-1",
                  "displayName": "testing",
                  "allowMultiInputs": false,
                  "isDynamicPort": false,
                  "dependencies": [
                    {
                      "id": 0,
                      "internal": false
                    }
                  ]
                }
              ],
              "outputPorts": [
                {
                  "portID": "output-0",
                  "displayName": "",
                  "allowMultiInputs": false,
                  "isDynamicPort": false
                }
              ],
              "showAdvanced": false,
              "isDisabled": false,
              "customDisplayName": "Support Vector Machine",
              "dynamicInputPorts": false,
              "dynamicOutputPorts": false
            },
            {
              "operatorID": "SklearnLogisticRegression-operator-eca48ee5-d808-4c5d-bb3c-6614d0bfb548",
              "operatorType": "SklearnLogisticRegression",
              "operatorVersion": "6dd0927e6ddf688cb924ecd47b03d436dec9881d",
              "operatorProperties": {
                "countVectorizer": false,
                "tfidfTransformer": false,
                "target": "Outcome"
              },
              "inputPorts": [
                {
                  "portID": "input-0",
                  "displayName": "training",
                  "allowMultiInputs": false,
                  "isDynamicPort": false,
                  "dependencies": []
                },
                {
                  "portID": "input-1",
                  "displayName": "testing",
                  "allowMultiInputs": false,
                  "isDynamicPort": false,
                  "dependencies": [
                    {
                      "id": 0,
                      "internal": false
                    }
                  ]
                }
              ],
              "outputPorts": [
                {
                  "portID": "output-0",
                  "displayName": "",
                  "allowMultiInputs": false,
                  "isDynamicPort": false
                }
              ],
              "showAdvanced": false,
              "isDisabled": false,
              "customDisplayName": "Logistic Regression",
              "dynamicInputPorts": false,
              "dynamicOutputPorts": false
            }
          ],
          "operatorPositions": {
            "CSVFileScan-operator-43e5d7f4-2d3d-498b-a97d-b4d242ee82cf": {
              "x": -92,
              "y": 222
            },
            "Distinct-operator-d6a18641-3c29-4835-b8d2-60351b0b8882": {
              "x": 61,
              "y": 290
            },
            "Filter-operator-b46dc58f-8100-478e-80c5-87d93c1f7e04": {
              "x": 206,
              "y": 351
            },
            "Aggregate-operator-ba7c3a06-09f8-4ad3-977d-69988789e671": {
              "x": 357,
              "y": 25
            },
            "Aggregate-operator-cd2cbbbf-bad0-46ed-9e13-65ccd1e67c27": {
              "x": 356,
              "y": 158
            },
            "Aggregate-operator-3abc30e7-9ba3-4a97-ac28-937ca950cccf": {
              "x": 468,
              "y": 23
            },
            "BoxPlot-operator-9ffd3dce-29b4-4373-ab38-235487393d72": {
              "x": 468,
              "y": 157
            },
            "PythonUDFV2-operator-1d891fad-a245-4e03-ba40-ad9ffbfa2b6b": {
              "x": 600,
              "y": 156
            },
            "Split-operator-3e01a035-69bd-487b-bb3c-edf23fb7a64c": {
              "x": 304,
              "y": 599
            },
            "SklearnRandomForest-operator-7844ee52-5d28-4421-a537-d267bb52efa8": {
              "x": 487.433349609375,
              "y": 442.433349609375
            },
            "SklearnDecisionTree-operator-65ea9d8c-9057-4ebc-8a40-a8c01333f948": {
              "x": 656,
              "y": 444
            },
            "SklearnSVM-operator-7adceacc-b3f9-4af6-8319-cd3c3f96d95d": {
              "x": 487,
              "y": 802
            },
            "SklearnLogisticRegression-operator-eca48ee5-d808-4c5d-bb3c-6614d0bfb548": {
              "x": 657,
              "y": 805
            }
          },
          "links": [
            {
              "linkID": "link-80bfba92-fd14-4f4a-aaff-c2676c27c43f",
              "source": {
                "operatorID": "CSVFileScan-operator-43e5d7f4-2d3d-498b-a97d-b4d242ee82cf",
                "portID": "output-0"
              },
              "target": {
                "operatorID": "Distinct-operator-d6a18641-3c29-4835-b8d2-60351b0b8882",
                "portID": "input-0"
              }
            },
            {
              "linkID": "link-a69abc52-a583-48bf-a463-059060172d3e",
              "source": {
                "operatorID": "Distinct-operator-d6a18641-3c29-4835-b8d2-60351b0b8882",
                "portID": "output-0"
              },
              "target": {
                "operatorID": "Filter-operator-b46dc58f-8100-478e-80c5-87d93c1f7e04",
                "portID": "input-0"
              }
            },
            {
              "linkID": "link-c53cc370-3628-45f2-bcd9-574e629e0c87",
              "source": {
                "operatorID": "Filter-operator-b46dc58f-8100-478e-80c5-87d93c1f7e04",
                "portID": "output-0"
              },
              "target": {
                "operatorID": "Aggregate-operator-ba7c3a06-09f8-4ad3-977d-69988789e671",
                "portID": "input-0"
              }
            },
            {
              "linkID": "6b3accb0-8c51-4099-8cde-1ea61c551e22",
              "source": {
                "operatorID": "Filter-operator-b46dc58f-8100-478e-80c5-87d93c1f7e04",
                "portID": "output-0"
              },
              "target": {
                "operatorID": "Aggregate-operator-cd2cbbbf-bad0-46ed-9e13-65ccd1e67c27",
                "portID": "input-0"
              }
            },
            {
              "linkID": "69e9e375-b48a-4771-8282-068646f69cd4",
              "source": {
                "operatorID": "Filter-operator-b46dc58f-8100-478e-80c5-87d93c1f7e04",
                "portID": "output-0"
              },
              "target": {
                "operatorID": "Aggregate-operator-3abc30e7-9ba3-4a97-ac28-937ca950cccf",
                "portID": "input-0"
              }
            },
            {
              "linkID": "7eaa7d96-627b-436c-b361-7d84f1ead82f",
              "source": {
                "operatorID": "Filter-operator-b46dc58f-8100-478e-80c5-87d93c1f7e04",
                "portID": "output-0"
              },
              "target": {
                "operatorID": "BoxPlot-operator-9ffd3dce-29b4-4373-ab38-235487393d72",
                "portID": "input-0"
              }
            },
            {
              "linkID": "43a36387-7d39-4c41-8e92-a858c7a79ebe",
              "source": {
                "operatorID": "Filter-operator-b46dc58f-8100-478e-80c5-87d93c1f7e04",
                "portID": "output-0"
              },
              "target": {
                "operatorID": "PythonUDFV2-operator-1d891fad-a245-4e03-ba40-ad9ffbfa2b6b",
                "portID": "input-0"
              }
            },
            {
              "linkID": "ac044fd7-063a-4b5a-96a5-0269924334e4",
              "source": {
                "operatorID": "Filter-operator-b46dc58f-8100-478e-80c5-87d93c1f7e04",
                "portID": "output-0"
              },
              "target": {
                "operatorID": "Split-operator-3e01a035-69bd-487b-bb3c-edf23fb7a64c",
                "portID": "input-0"
              }
            },
            {
              "linkID": "1e344ef6-e00b-47ad-b593-2698ee8af686",
              "source": {
                "operatorID": "Split-operator-3e01a035-69bd-487b-bb3c-edf23fb7a64c",
                "portID": "output-0"
              },
              "target": {
                "operatorID": "SklearnRandomForest-operator-7844ee52-5d28-4421-a537-d267bb52efa8",
                "portID": "input-0"
              }
            },
            {
              "linkID": "f4c74729-e854-4a85-a0c2-3273434045d9",
              "source": {
                "operatorID": "Split-operator-3e01a035-69bd-487b-bb3c-edf23fb7a64c",
                "portID": "output-1"
              },
              "target": {
                "operatorID": "SklearnRandomForest-operator-7844ee52-5d28-4421-a537-d267bb52efa8",
                "portID": "input-1"
              }
            },
            {
              "linkID": "78e7a89f-11f9-4a19-a8ac-0ab9e6bca48d",
              "source": {
                "operatorID": "Split-operator-3e01a035-69bd-487b-bb3c-edf23fb7a64c",
                "portID": "output-0"
              },
              "target": {
                "operatorID": "SklearnDecisionTree-operator-65ea9d8c-9057-4ebc-8a40-a8c01333f948",
                "portID": "input-0"
              }
            },
            {
              "linkID": "ca5c8d48-b027-43aa-83b9-f3c7df9bcb98",
              "source": {
                "operatorID": "Split-operator-3e01a035-69bd-487b-bb3c-edf23fb7a64c",
                "portID": "output-1"
              },
              "target": {
                "operatorID": "SklearnDecisionTree-operator-65ea9d8c-9057-4ebc-8a40-a8c01333f948",
                "portID": "input-1"
              }
            },
            {
              "linkID": "38a4986b-ea49-47e6-b737-9ccffadc305d",
              "source": {
                "operatorID": "Split-operator-3e01a035-69bd-487b-bb3c-edf23fb7a64c",
                "portID": "output-0"
              },
              "target": {
                "operatorID": "SklearnSVM-operator-7adceacc-b3f9-4af6-8319-cd3c3f96d95d",
                "portID": "input-0"
              }
            },
            {
              "linkID": "101d386b-fdc9-4e26-8599-925aa3866970",
              "source": {
                "operatorID": "Split-operator-3e01a035-69bd-487b-bb3c-edf23fb7a64c",
                "portID": "output-1"
              },
              "target": {
                "operatorID": "SklearnSVM-operator-7adceacc-b3f9-4af6-8319-cd3c3f96d95d",
                "portID": "input-1"
              }
            },
            {
              "linkID": "f2a48196-4400-4cb6-b3e0-a29b1181b58b",
              "source": {
                "operatorID": "Split-operator-3e01a035-69bd-487b-bb3c-edf23fb7a64c",
                "portID": "output-0"
              },
              "target": {
                "operatorID": "SklearnLogisticRegression-operator-eca48ee5-d808-4c5d-bb3c-6614d0bfb548",
                "portID": "input-0"
              }
            },
            {
              "linkID": "e97eb3b1-2801-4983-9d94-8762d6d00ccc",
              "source": {
                "operatorID": "Split-operator-3e01a035-69bd-487b-bb3c-edf23fb7a64c",
                "portID": "output-1"
              },
              "target": {
                "operatorID": "SklearnLogisticRegression-operator-eca48ee5-d808-4c5d-bb3c-6614d0bfb548",
                "portID": "input-1"
              }
            }
          ],
          "groups": [],
          "commentBoxes": []
        }

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

        // Create a valid Workflow object with required fields
        const workflow: Workflow = {
          content: workflowContent,
          name: workflowName,
          isPublished: 0,
          description: undefined,
          wid: undefined,
          creationTime: undefined,
          lastModifiedTime: undefined,
          readonly: false,
        };

        // Open the notebook in the Jupyter notebook panel by reloading the workflow
        this.workflowActionService.reloadWorkflow(workflow, true);
        this.openJupyterNotebookPanel();
      } catch (error) {
        this.notificationService.error("Failed to import the notebook.");
        console.error(error);
      }
    };

    return false; // Prevent automatic upload handling
  };

  private openJupyterNotebookPanel(): void {
    // Assuming you have a service that handles the state of various panels
    this.panelService.openPanel("JupyterNotebookPanel");
  }

  private async sendNotebookToPod(notebookContent: JSON) {
    const apiUrl = "http://localhost:5000/set_notebook"; // Flask API URL

    const requestBody = {
      notebookName: "example.ipynb", // TODO: make this into the link used by jupyter-notebook-panel.component.html: e.g) "http://localhost:8888/notebooks/work/example.ipynb?token=mytoken"
      notebookData: notebookContent,
    };

    try {
      const response = await fetch(apiUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(requestBody),
      });

      // Ensure the response status is 200 (OK) before proceeding
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ error: "Unknown error occurred" }));
        throw new Error(`Server Error ${response.status}: ${errorData.error || "Failed to execute notebook"}`);
      }

      const data = await response.json(); // {message: 'Notebook saved successfully', notebookPath: '/home/jovyan/work/example.ipynb'}
      console.log("Notebook successfully sent to pod:", data);
      this.notificationService.success("Notebook opened successfully in JupyterLab.");
      this.openJupyterNotebookPanel(); // Open panel after successful upload
    } catch (error) {
      console.error("Error sending notebook to pod:", error);
      // @ts-ignore
      this.notificationService.error("Error sending notebook to JupyterLab: " + error.message);
    }
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

  protected readonly environment = environment;
}
