import { Injectable } from "@angular/core";
import { WorkflowVersionEntry } from "../../type/workflow-version-entry";
import { Observable, Subject } from "rxjs";
import { WorkflowActionService } from "../../../workspace/service/workflow-graph/model/workflow-action.service";
import { AppSettings } from "../../../common/app-setting";
import { Workflow } from "../../../common/type/workflow";
import { filter, map } from "rxjs/operators";
import { HttpClient } from "@angular/common/http";
import { WorkflowUtilService } from "../../../workspace/service/workflow-graph/util/workflow-util.service";
export const VERSIONS_BASE_URL = "version";

@Injectable({
  providedIn: "root",
})
export class WorkflowVersionService {
  private workflowVersionsObservable = new Subject<boolean>();
  private versionDisplayClicked: boolean = false;
  constructor(private http: HttpClient, private workflowActionService: WorkflowActionService) {}

  public clickedVersionsDisplayButton(): void {
    this.prepareWorkflowVersions();
    this.versionDisplayClicked = true;
    this.workflowVersionsObservable.next(true);
  }

  public resetVersionsDisplayButton(): void {
    this.versionDisplayClicked = false;
  }

  /**
   * retrieves a list of versions for a particular workflow from backend database
   */
  public retrieveVersionsOfWorkflow(wid: number): Observable<WorkflowVersionEntry[]> {
    return this.http.get<WorkflowVersionEntry[]>(`${AppSettings.getApiEndpoint()}/${VERSIONS_BASE_URL}/${wid}`);
  }

  /**
   * retrieves a version of the workflow from backend database
   */
  public retrieveWorkflowByVersion(wid: number, vid: number): Observable<Workflow> {
    return this.http.get<Workflow>(`${AppSettings.getApiEndpoint()}/${VERSIONS_BASE_URL}/${wid}/${vid}`).pipe(
      filter((updatedWorkflow: Workflow) => updatedWorkflow != null),
      map(WorkflowUtilService.parseWorkflowInfo)
    );
  }

  public prepareWorkflowVersions(): void {
    const elements = this.workflowActionService.getJointGraphWrapper().getCurrentHighlights();
    this.workflowActionService.getJointGraphWrapper().unhighlightElements(elements);
  }

  public workflowVersionsDisplayObservable(): Observable<boolean> {
    return this.workflowVersionsObservable.asObservable();
  }

  public isVersionDisplayClicked(): boolean {
    return this.versionDisplayClicked;
  }
}
