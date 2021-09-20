import { Component, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowVersionEntry } from "../../../../dashboard/type/workflow-version-entry";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowVersionService } from "../../../service/workflow-version/workflow-version.service";
import { NzTableQueryParams } from "ng-zorro-antd/table";

@UntilDestroy()
@Component({
  selector: "texera-formly-form-frame",
  templateUrl: "./versions-display.component.html",
  styleUrls: ["./versions-display.component.scss"],
})
export class VersionsListDisplayComponent implements OnInit {

  public versionsList: WorkflowVersionEntry[] | undefined;

  public versionTableHeaders: string[] = ['Version#', 'Timestamp'];
  currentPageIndex: number = 1;

  constructor(
    private workflowPersistService: WorkflowPersistService,
    private workflowActionService: WorkflowActionService,
    private workflowVersionService: WorkflowVersionService
  ) {}


  ngOnInit(): void {
    // listens to the workflow versions event, and updates the workflow versions table displayed on the form
    this.registerWorkflowVersionsDisplayHandler();

  }

  getVersion(vid: number) {
    this.workflowPersistService.retrieveWorkflowByVersion(<number>this.workflowActionService.
    getWorkflowMetadata()?.wid, vid).subscribe(workflow => {
      this.workflowActionService.reloadWorkflow(workflow);
    });
  }

  registerWorkflowVersionsDisplayHandler(): void {
    this.workflowVersionService.workflowVersionsChosen()
      .pipe(untilDestroyed(this))
      .subscribe(versionsList => {
        this.versionsList = versionsList;
      });
  }

  onTableQueryParamsChange(params: NzTableQueryParams) {
    this.currentPageIndex = params.pageIndex;
  }
}
