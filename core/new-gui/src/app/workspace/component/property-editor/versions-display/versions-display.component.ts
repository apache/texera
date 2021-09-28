import { Component, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowVersionEntry } from "../../../../dashboard/type/workflow-version-entry";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import {
  VERSIONS_BASE_URL,
  WorkflowVersionService,
} from "../../../../dashboard/service/workflow-version/workflow-version.service";
import { Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { HttpClient } from "@angular/common/http";

@UntilDestroy()
@Component({
  selector: "texera-formly-form-frame",
  templateUrl: "./versions-display.component.html",
  styleUrls: ["./versions-display.component.scss"],
})
export class VersionsListDisplayComponent implements OnInit {
  public versionsList: WorkflowVersionEntry[] | undefined;

  public versionTableHeaders: string[] = ["Version#", "Timestamp"];

  constructor(
    private http: HttpClient,
    private workflowActionService: WorkflowActionService,
    private workflowVersionService: WorkflowVersionService
  ) {}

  ngOnInit(): void {
    // gets the versions result and updates the workflow versions table displayed on the form
    this.displayWorkflowVersions();
  }

  getVersion(vid: number) {
    this.workflowVersionService
      .retrieveWorkflowByVersion(<number>this.workflowActionService.getWorkflowMetadata()?.wid, vid)
      .pipe(untilDestroyed(this))
      .subscribe(workflow => {
        this.workflowActionService.reloadWorkflow(workflow);
      });
  }

  /**
   * retrieves a list of versions for a particular workflow from backend database
   */
  public retrieveVersionsOfWorkflow(wid: number): Observable<WorkflowVersionEntry[]> {
    return this.http.get<WorkflowVersionEntry[]>(`${AppSettings.getApiEndpoint()}/${VERSIONS_BASE_URL}/${wid}`);
  }

  /**
   * calls the http get request service to display the versions result in the table
   */
  displayWorkflowVersions(): void {
    const wid = this.workflowActionService.getWorkflowMetadata()?.wid;
    if (wid === undefined) {
      return;
    }
    this.workflowVersionService
      .retrieveVersionsOfWorkflow(wid)
      .pipe(untilDestroyed(this))
      .subscribe(versionsList => {
        this.versionsList = versionsList;
      });
  }
}
