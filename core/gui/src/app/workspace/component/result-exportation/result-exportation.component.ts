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
      .subscribe(response => {
        const datasets = response.datasets;
        this.userAccessibleDatasets = datasets.filter(dataset => dataset.accessPrivilege === "WRITE");
        this.filteredUserAccessibleDatasets = [...this.userAccessibleDatasets];
      });
    this.updateOutputType();
  }

  updateOutputType(): void {
    const highlightedOperatorIds = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    if (highlightedOperatorIds.length === 1) {
      const operatorId = highlightedOperatorIds[0];
      const outputTypes = this.workflowResultService.determineOutputTypes(operatorId);
      this.isTableOutput = outputTypes.isTableOutput;
      this.isVisualizationOutput = outputTypes.isVisualizationOutput;
      this.containsBinaryData = outputTypes.containsBinaryData;
    } else {
      // TODO: handle multiple operators
      this.isTableOutput = false;
      this.isVisualizationOutput = false;
      this.containsBinaryData = false;
    }
  }

  onUserInputDatasetName(event: Event): void {
    const value = this.inputDatasetName;

    if (value) {
      this.filteredUserAccessibleDatasets = this.userAccessibleDatasets.filter(
        dataset => dataset.dataset.did && dataset.dataset.name.toLowerCase().includes(value)
      );
    }
  }

  onClickSaveResultFileToDatasets(dataset: DashboardDataset) {
    if (dataset.dataset.did) {
      this.workflowResultExportService.exportWorkflowExecutionResult(
        this.exportType,
        this.workflowName,
        [dataset.dataset.did],
        this.rowIndex,
        this.columnIndex,
        this.inputFileName,
        this.sourceTriggered === "menu"
      );
      this.modalRef.close();
    }
  }

  onClickExportAllResult() {
    this.workflowResultExportService.exportOperatorsResultToLocal(this.sourceTriggered === "menu");
    this.modalRef.close();
  }
}
