import { Component, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowVersionCollapsableEntry } from "../../../../dashboard/user/type/workflow-version-entry";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowVersionService } from "../../../../dashboard/user/service/workflow-version/workflow-version.service";

@UntilDestroy()
@Component({
  selector: "texera-versions-display-frame",
  templateUrl: "./versions-display-frame.component.html",
  styleUrls: ["./versions-display-frame.component.scss"],
})
export class VersionsDisplayFrameComponent implements OnInit {
  public versionsList: WorkflowVersionCollapsableEntry[] | undefined;

  public versionTableHeaders: string[] = ["", "Version#", "Timestamp"];

  constructor(
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
    this.workflowVersionService
      .retrieveWorkflowByVersion(<number>this.workflowActionService.getWorkflowMetadata()?.wid, vid)
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
    this.workflowVersionService
      .retrieveVersionsOfWorkflow(wid)
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
}
