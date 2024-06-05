import { Component } from "@angular/core";
import { FieldType, FieldTypeConfig } from "@ngx-formly/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { debounceTime } from "rxjs/operators";
import { map } from "rxjs";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { NzModalService } from "ng-zorro-antd/modal";
import { FileSelectionComponent } from "../file-selection/file-selection.component";
import { environment } from "../../../../environments/environment";
import {DatasetService} from "../../../dashboard/user/service/user-dataset/dataset.service";

@UntilDestroy()
@Component({
  selector: "texera-input-autocomplete-template",
  templateUrl: "./input-autocomplete.component.html",
  styleUrls: ["input-autocomplete.component.scss"],
})
export class InputAutoCompleteComponent extends FieldType<FieldTypeConfig> {
  // the autocomplete selection list
  public suggestions: string[] = [];

  constructor(
    private modalService: NzModalService,
    public workflowActionService: WorkflowActionService,
    public workflowPersistService: WorkflowPersistService,
    public datasetService: DatasetService
  ) {
    super();
  }

  onClickOpenFileSelectionModal(): void {

    this.datasetService
      .retrieveAccessibleDatasets()
      .pipe(untilDestroyed(this))
      .subscribe(datasets => {
        const modal = this.modalService.create({
          nzTitle: "Please select one file from datasets",
          nzContent: FileSelectionComponent,
          nzFooter: null,
          nzData: {
            datasets: datasets,
          },
        });
        // Handle the selection from the modal
        modal.afterClose.pipe(untilDestroyed(this)).subscribe(result => {
          if (result) {
            this.formControl.setValue(result); // Assuming 'result' is the selected value
          }
        });
      })
  }

  get isFileSelectionEnabled(): boolean {
    return environment.userSystemEnabled;
  }
}
