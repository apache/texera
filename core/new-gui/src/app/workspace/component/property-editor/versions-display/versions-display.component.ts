import { Component, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import {
  WorkflowVersionCollapsableEntry,
  WorkflowVersionEntry,
} from "../../../../dashboard/type/workflow-version-entry";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowVersionService } from "../../../../dashboard/service/workflow-version/workflow-version.service";
import { Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { HttpClient } from "@angular/common/http";
import { Workflow } from "src/app/common/type/workflow";
import { filter, map } from "rxjs/operators";
import { WorkflowUtilService } from "src/app/workspace/service/workflow-graph/util/workflow-util.service";

export const WORKFLOW_VERSIONS_API_BASE_URL = "version";

@UntilDestroy()
@Component({
  selector: "texera-formly-form-frame",
  templateUrl: "./versions-display.component.html",
  styleUrls: ["./versions-display.component.scss"],
})
export class VersionsListDisplayComponent implements OnInit {
  public versionsList: WorkflowVersionCollapsableEntry[] | undefined;

  public versionTableHeaders: string[] = ["Version#", "Timestamp"];

  constructor(
    private http: HttpClient,
    private workflowActionService: WorkflowActionService,
    private workflowVersionService: WorkflowVersionService
  ) {}

  collapse(index: number, $event: boolean): void {
    if (this.versionsList == undefined) {
      return;
    }
    if (!$event) {
      while (++index < this.versionsList.length && !this.versionsList[index].importance) {
        this.versionsList[index].expand = false;
      }
    } else {
      while (++index < this.versionsList.length && !this.versionsList[index].importance) {
        this.versionsList[index].expand = true;
      }
    }
  }
  ngOnInit(): void {
    // gets the versions result and updates the workflow versions table displayed on the form
    this.displayWorkflowVersions();
  }

  getVersion(vid: number) {
    this.retrieveWorkflowByVersion(<number>this.workflowActionService.getWorkflowMetadata()?.wid, vid)
      .pipe(untilDestroyed(this))
      .subscribe(workflow => {
        this.workflowVersionService.displayParticularVersion(workflow);
      });
  }

  /**
   * calls the http get request service to display the versions result in the table
   */
  displayWorkflowVersions(): void {
    const wid = this.workflowActionService.getWorkflowMetadata()?.wid;
    if (wid === undefined) {
      return;
    }
    this.retrieveVersionsOfWorkflow(wid)
      .pipe(untilDestroyed(this))
      .subscribe(versionsList => {
        this.versionsList = versionsList.map(version => ({
          vId: version.vId,
          creationTime: version.creationTime,
          content: version.content,
          importance: version.importance,
          expand: false,
        }));
      });
  }

  /**
   * retrieves a list of versions for a particular workflow from backend database
   */
  retrieveVersionsOfWorkflow(wid: number): Observable<WorkflowVersionEntry[]> {
    return this.http.get<WorkflowVersionEntry[]>(
      `${AppSettings.getApiEndpoint()}/${WORKFLOW_VERSIONS_API_BASE_URL}/${wid}`
    );
  }

  /**
   * retrieves a version of the workflow from backend database
   */
  retrieveWorkflowByVersion(wid: number, vid: number): Observable<Workflow> {
    return this.http
      .get<Workflow>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_VERSIONS_API_BASE_URL}/${wid}/${vid}`)
      .pipe(
        filter((updatedWorkflow: Workflow) => updatedWorkflow != null),
        map(WorkflowUtilService.parseWorkflowInfo)
      );
  }
}
