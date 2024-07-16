import { Component } from "@angular/core";
import { FieldType, FieldTypeConfig } from "@ngx-formly/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { debounceTime } from "rxjs/operators";
import { map } from "rxjs";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { EnvironmentService } from "../../../dashboard/service/user/environment/environment.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { NzModalService } from "ng-zorro-antd/modal";
import { FileSelectionComponent } from "../file-selection/file-selection.component";
import { environment } from "../../../../environments/environment";

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
    public environmentService: EnvironmentService,
    public workflowActionService: WorkflowActionService,
    public workflowPersistService: WorkflowPersistService
  ) {
    super();
  }

  onClickOpenFileSelectionModal(): void {
    const wid = this.workflowActionService.getWorkflowMetadata()?.wid;
    if (wid) {
      this.workflowPersistService
        .retrieveWorkflowEnvironment(wid)
        .pipe(untilDestroyed(this))
        .subscribe(env => {
          // then we fetch the file list inorder to do the autocomplete, perform auto-complete based on the current input
          const eid = env.eid;
          if (eid) {
            this.environmentService
              .getDatasetsFileNodeList(eid)
              .pipe(untilDestroyed(this))
              .subscribe(fileNodes => {
                const modal = this.modalService.create({
                  nzTitle: "Please select one file from datasets",
                  nzContent: FileSelectionComponent,
                  nzFooter: null,
                  nzData: {
                    fileTreeNodes: fileNodes,
                  },
                });
                // Handle the selection from the modal
                modal.afterClose.pipe(untilDestroyed(this)).subscribe(result => {
                  if (result) {
                    this.formControl.setValue(result); // Assuming 'result' is the selected value
                  }
                });
              });
          }
        });
    }
  }

  get isFileSelectionEnabled(): boolean {
    return environment.userSystemEnabled;
  }
}
