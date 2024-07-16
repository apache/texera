import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Component, inject, Input, OnInit } from "@angular/core";
import { environment } from "../../../../environments/environment";
import { WorkflowResultExportService } from "../../service/workflow-result-export/workflow-result-export.service";
import { DashboardDataset } from "../../../dashboard/type/dashboard-dataset.interface";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";

@UntilDestroy()
@Component({
  selector: "texera-result-exportation-modal",
  templateUrl: "./result-exportation.component.html",
  styleUrls: ["./result-exportation.component.scss"],
})
export class ResultExportationComponent implements OnInit {
  exportType: string = inject(NZ_MODAL_DATA).exportType;

  workflowName: string = inject(NZ_MODAL_DATA).workflowName;

  inputDatasetName = "";

  userAccessibleDatasets: DashboardDataset[] = [];
  filteredUserAccessibleDatasets: DashboardDataset[] = [];

  constructor(
    private modalRef: NzModalRef,
    private workflowResultExportService: WorkflowResultExportService,
    private datasetService: DatasetService
  ) {}

  ngOnInit(): void {
    this.datasetService
      .retrieveAccessibleDatasets()
      .pipe(untilDestroyed(this))
      .subscribe(datasets => {
        this.userAccessibleDatasets = datasets.filter(dataset => dataset.accessPrivilege === "WRITE");
        this.filteredUserAccessibleDatasets = [...this.userAccessibleDatasets];
      });
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
      this.workflowResultExportService.exportWorkflowExecutionResult(this.exportType, this.workflowName, [
        dataset.dataset.did,
      ]);
      this.modalRef.close();
    }
  }
}
