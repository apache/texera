import { Component } from "@angular/core";
import { FieldType, FieldTypeConfig } from "@ngx-formly/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { NzModalService } from "ng-zorro-antd/modal";
import { FileSelectionComponent } from "../file-selection/file-selection.component";
import { environment } from "../../../../environments/environment";
import { DatasetFileNode, getFullPathFromDatasetFileNode } from "../../../common/type/datasetVersionFileTree";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";

@UntilDestroy()
@Component({
  selector: "texera-input-autocomplete-template",
  templateUrl: "./input-autocomplete.component.html",
  styleUrls: ["input-autocomplete.component.scss"],
})
export class InputAutoCompleteComponent extends FieldType<FieldTypeConfig> {
  constructor(
    private modalService: NzModalService,
    public workflowActionService: WorkflowActionService,
    public datasetService: DatasetService
  ) {
    super();
  }

  onClickOpenFileSelectionModal(): void {
    this.datasetService
      .retrieveAccessibleDatasets()
      .pipe(untilDestroyed(this))
      .subscribe(response => {
        const datasets = response.datasets;
        const modal = this.modalService.create({
          nzTitle: "Please select one file from datasets",
          nzContent: FileSelectionComponent,
          nzFooter: null,
          nzData: {
            datasets: datasets,
          },
        });
        // Handle the selection from the modal
        modal.afterClose.pipe(untilDestroyed(this)).subscribe(fileNode => {
          const node: DatasetFileNode = fileNode as DatasetFileNode;
          this.formControl.setValue(getFullPathFromDatasetFileNode(node));
        });
      });
  }

  get isFileSelectionEnabled(): boolean {
    return environment.userSystemEnabled;
  }
}
