import { Injectable } from "@angular/core";
import { WorkflowVersionEntry } from "../../type/workflow-version-entry";
import { Observable, Subject } from "rxjs";
import { WorkflowActionService } from "../../../workspace/service/workflow-graph/model/workflow-action.service";
import { AppSettings } from "../../../common/app-setting";
import { Workflow } from "../../../common/type/workflow";
import { filter, map } from "rxjs/operators";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { HttpClient } from "@angular/common/http";
export const VERSIONS_BASE_URL = "version"
export const VERSIONS_URL = VERSIONS_BASE_URL + "/versions";
export const WORKFLOW_VERSION_URL = VERSIONS_BASE_URL + "/version";

@Injectable({
  providedIn: "root",
})
export class WorkflowVersionService {

  private workflowVersions: WorkflowVersionEntry[] = [];
  private workflowVersionsObservable = new Subject<WorkflowVersionEntry[]> ();
  constructor (private http: HttpClient, private workflowActionService: WorkflowActionService) {
  }

  /**
   * retrieves a list of versions for a particular workflow from backend database
   */
  public retrieveVersionsOfWorkflow(wid: number): Observable<WorkflowVersionEntry[]> {
    return this.http.get<WorkflowVersionEntry[]>(`${AppSettings.getApiEndpoint()}/${VERSIONS_URL}/${wid}`);
  }

  /**
   * retrieves a version of the workflow from backend database
   */
  public retrieveWorkflowByVersion(wid: number, vid: number): Observable<Workflow> {
    const formData: FormData = new FormData();
    formData.append("wid", wid.toString());
    formData.append("vid", vid.toString());
    return this.http.post<Workflow>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_VERSION_URL}`,
      formData)
      .pipe(filter((updatedWorkflow: Workflow) => updatedWorkflow != null), map(WorkflowPersistService.parseWorkflowInfo));
  }

  public prepareWorkflowVersions(versionsList: WorkflowVersionEntry[]): void {
    const elements = this.workflowActionService.getJointGraphWrapper().getCurrentHighlights();
    this.workflowActionService.getJointGraphWrapper().unhighlightElements(elements);
    this.workflowVersions = versionsList;
    this.workflowVersionsObservable.next(versionsList);
  }

  public getWorkflowVersions(): WorkflowVersionEntry[] {
    return this.workflowVersions;
  }

  public resetResults(): void {
    this.workflowVersions = [];
  }

  public workflowVersionsChosen() : Observable<WorkflowVersionEntry[]> {
    return this.workflowVersionsObservable.asObservable();
  }
}
