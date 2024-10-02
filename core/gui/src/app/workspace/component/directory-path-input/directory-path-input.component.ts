import { Component } from "@angular/core";
import { FieldType, FieldTypeConfig } from "@ngx-formly/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { NzModalService } from "ng-zorro-antd/modal";
import { DirectorySelectionComponent } from "../directory-selection/directory-selection.component";
import { environment } from "../../../../environments/environment";
import { DatasetFileNode, getFullPathFromDatasetFileNode } from "../../../common/type/datasetVersionFileTree";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";

@UntilDestroy()
@Component({
  selector: "texera-directory-path-input",
  templateUrl: "./directory-path-input.component.html",
  styleUrls: ["./directory-path-input.component.scss"],
})
export class DirectoryPathInputComponent extends FieldType<FieldTypeConfig> {
  constructor(
    private modalService: NzModalService,
    public workflowActionService: WorkflowActionService,
    public datasetService: DatasetService
  ) {
    super();
  }

  onClickOpenDirectorySelectionModal(): void {
    this.datasetService
      .retrieveAccessibleDatasets()
      .pipe(untilDestroyed(this))
      .subscribe(response => {
        const datasets = response.datasets;
        const modal = this.modalService.create({
          nzTitle: "Please select one directory from datasets",
          nzContent: DirectorySelectionComponent,
          nzFooter: null,
          nzData: {
            datasets: datasets,
            selectedDirectoryPath: this.formControl.getRawValue(),
          },
        });
        // Handle the selection from the modal
        modal.afterClose.pipe(untilDestroyed(this)).subscribe(directopryPath => {
          this.formControl.setValue(directopryPath);
        });
      });
  }

  get isDirectorySelectionEnabled(): boolean {
    return environment.userSystemEnabled;
  }

  get selectedDirectoryPath(): string | null {
    return this.formControl.value;
  }
}
