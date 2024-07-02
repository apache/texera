import { Component } from "@angular/core";
import { FieldType, FieldTypeConfig } from "@ngx-formly/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { NzModalService } from "ng-zorro-antd/modal";
import { FileSelectionComponent } from "../file-selection/file-selection.component";
import { environment } from "../../../../environments/environment";
import { DatasetService } from "../../../dashboard/user/service/user-dataset/dataset.service";
import {DatasetVersionFileTreeNode, getFullPathFromFileTreeNode} from "../../../common/type/datasetVersionFileTree";

@UntilDestroy()
@Component({
  selector: "texera-input-autocomplete-template",
  templateUrl: "./input-autocomplete.component.html",
  styleUrls: ["input-autocomplete.component.scss"],
})
export class InputAutoCompleteComponent extends FieldType<FieldTypeConfig> {
  // the autocomplete selection list
  public displayFilePath: string = "";

  constructor(
    private modalService: NzModalService,
    public workflowActionService: WorkflowActionService,
    public datasetService: DatasetService
  ) {
    super();
    // const rawPath = this.formControl.getRawValue();
    // this.displayFilePath = rawPath as string
  }

  onClickOpenFileSelectionModal(): void {
    this.datasetService
      .retrieveAccessibleDatasets(true)
      .pipe(untilDestroyed(this))
      .subscribe(datasets => {
        let datasetRootFileNodes: DatasetVersionFileTreeNode[] = [];
        datasets.forEach(dataset => datasetRootFileNodes.push(dataset.datasetRootFileNode));
        const modal = this.modalService.create({
          nzTitle: "Please select one file from datasets",
          nzContent: FileSelectionComponent,
          nzFooter: null,
          nzData: {
            datasetRootFileNodes: datasetRootFileNodes,
          },
        });
        // Handle the selection from the modal
        modal.afterClose.pipe(untilDestroyed(this)).subscribe(fileNode => {
          const node: DatasetVersionFileTreeNode = fileNode as DatasetVersionFileTreeNode;
          // embed the IDs into the file path for the operator to capture the information
          this.formControl.setValue(getFullPathFromFileTreeNode(node, true));
          // when display the file path to the users, don't show the embedded IDs
          this.displayFilePath = getFullPathFromFileTreeNode(node, false)
        });
      });
  }

  get isFileSelectionEnabled(): boolean {
    return environment.userSystemEnabled;
  }
}
