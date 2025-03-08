import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Component, inject, Input, OnInit } from "@angular/core";
import { WorkflowResultExportService } from "../../service/workflow-result-export/workflow-result-export.service";
import { DashboardDataset } from "../../../dashboard/type/dashboard-dataset.interface";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";

@UntilDestroy()
@Component({
  selector: "texera-result-exportation-modal",
  templateUrl: "./result-exportation.component.html",
  styleUrls: ["./result-exportation.component.scss"],
})
export class ResultExportationComponent implements OnInit {
  /* Two sources can trigger this dialog, one from context-menu
   which only export highlighted operators
   and second is menu which wants to export all operators
   */
  sourceTriggered: string = inject(NZ_MODAL_DATA).sourceTriggered;
  workflowName: string = inject(NZ_MODAL_DATA).workflowName;
  inputFileName: string = inject(NZ_MODAL_DATA).defaultFileName ?? "default_filename";
  rowIndex: number = inject(NZ_MODAL_DATA).rowIndex ?? -1;
  columnIndex: number = inject(NZ_MODAL_DATA).columnIndex ?? -1;
  destination: string = "";
  exportType: string = inject(NZ_MODAL_DATA).exportType ?? "";
  isTableOutput: boolean = false;
  isVisualizationOutput: boolean = false;
  containsBinaryData: boolean = false;
  inputDatasetName = "";

  userAccessibleDatasets: DashboardDataset[] = [];
  filteredUserAccessibleDatasets: DashboardDataset[] = [];

  constructor(
    public workflowResultExportService: WorkflowResultExportService,
    private modalRef: NzModalRef,
    private datasetService: DatasetService,
    private workflowActionService: WorkflowActionService,
    private workflowResultService: WorkflowResultService
  ) {}

  ngOnInit(): void {
    this.datasetService
      .retrieveAccessibleDatasets()
      .pipe(untilDestroyed(this))
      .subscribe(datasets => {
        this.userAccessibleDatasets = datasets.filter(dataset => dataset.accessPrivilege === "WRITE");
        this.filteredUserAccessibleDatasets = [...this.userAccessibleDatasets];
      });
    this.updateOutputType();
  }

  updateOutputType(): void {
    // Determine if the caller of this component is menu or context menu
    // if its menu then we need to export all operators else we need to export only highlighted operators
    // TODO: currently, user need to set `view result` to true in order to export result but
    //  we should allow user to export result without setting `view result` to true
    let operatorIds: readonly string[];
    if (this.sourceTriggered === "menu") {
      operatorIds = this.workflowActionService
        .getTexeraGraph()
        .getAllOperators()
        .map(op => op.operatorID);
    } else {
      operatorIds = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    }

    if (operatorIds.length === 0) {
      // No operators highlighted
      this.isTableOutput = false;
      this.isVisualizationOutput = false;
      this.containsBinaryData = false;
      return;
    }

    // Assume they're all table or visualization
    // until we find an operator that isn't
    let allTable = true;
    let allVisualization = true;
    let anyBinaryData = false;

    for (const operatorId of operatorIds) {
      const outputTypes = this.workflowResultService.determineOutputTypes(operatorId);
      if (!outputTypes.hasAnyResult) {
        continue;
      }
      if (!outputTypes.isTableOutput) {
        allTable = false;
      }
      if (!outputTypes.isVisualizationOutput) {
        allVisualization = false;
      }
      if (outputTypes.containsBinaryData) {
        anyBinaryData = true;
      }
    }

    this.isTableOutput = allTable;
    this.isVisualizationOutput = allVisualization;
    this.containsBinaryData = anyBinaryData;
  }

  onUserInputDatasetName(event: Event): void {
    const value = this.inputDatasetName;

    if (value) {
      this.filteredUserAccessibleDatasets = this.userAccessibleDatasets.filter(
        dataset => dataset.dataset.did && dataset.dataset.name.toLowerCase().includes(value)
      );
    }
  }

  onClickExportResult(destination: "dataset" | "local", dataset: DashboardDataset = {} as DashboardDataset) {
    const datasetIds =
      destination === "dataset" ? [dataset.dataset.did].filter((id): id is number => id !== undefined) : [];
    this.workflowResultExportService.exportWorkflowExecutionResult(
      this.exportType,
      this.workflowName,
      datasetIds,
      this.rowIndex,
      this.columnIndex,
      this.inputFileName,
      this.sourceTriggered === "menu",
      destination
    );
    this.modalRef.close();
  }
}
