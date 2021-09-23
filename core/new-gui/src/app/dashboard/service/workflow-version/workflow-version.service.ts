import { Injectable } from "@angular/core";
import { WorkflowVersionEntry } from "../../type/workflow-version-entry";
import { Observable, Subject } from "rxjs";
import { WorkflowActionService } from "../../../workspace/service/workflow-graph/model/workflow-action.service";
import { AppSettings } from "../../../common/app-setting";
import { Workflow } from "../../../common/type/workflow";
import { filter, map } from "rxjs/operators";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { HttpClient } from "@angular/common/http";
export const VERSIONS_BASE_URL = "version";

@Injectable({
  providedIn: "root",
})
export class WorkflowVersionService {

  private workflowVersionsObservable = new Subject<boolean>();
  private versionDisplayHighlighted: boolean = false;
  constructor (private http: HttpClient, private workflowActionService: WorkflowActionService) {
  }

  public highlightVersionsDisplay(): void {
    this.prepareWorkflowVersions();
    this.versionDisplayHighlighted = true;
    this.workflowVersionsObservable.next(true);
  }

  public unhighlightVersionsDisplay(): void {
    this.versionDisplayHighlighted = false;
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
    return this.http.get<Workflow>(`${AppSettings.getApiEndpoint()}/${VERSIONS_BASE_URL}/${wid}/${vid}`)
      .pipe(filter((updatedWorkflow: Workflow) => updatedWorkflow != null), map(WorkflowPersistService.parseWorkflowInfo));
  }

  public prepareWorkflowVersions(): void {
    const elements = this.workflowActionService.getJointGraphWrapper().getCurrentHighlights();
    this.workflowActionService.getJointGraphWrapper().unhighlightElements(elements);
  }

  public isWorkflowVersionsChosen(): Observable<boolean> {
    return this.workflowVersionsObservable.asObservable();
  }

  public getVersionDisplayHighlighted(): boolean {
    return this.versionDisplayHighlighted;
  }
}
