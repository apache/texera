import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { filter, map } from "rxjs/operators";
import { AppSettings } from "../../app-setting";
import { Workflow, WorkflowContent } from "../../type/workflow";
import { DashboardWorkflowEntry } from "../../../dashboard/type/dashboard-workflow-entry";
import { WorkflowUtilService } from "../../../workspace/service/workflow-graph/util/workflow-util.service";

export const WORKFLOW_BASE_URL = "workflow";
export const WORKFLOW_PERSIST_URL = WORKFLOW_BASE_URL + "/persist";
export const WORKFLOW_LIST_URL = WORKFLOW_BASE_URL + "/list";
export const WORKFLOW_CREATE_URL = WORKFLOW_BASE_URL + "/create";
export const WORKFLOW_DUPLICATE_URL = WORKFLOW_BASE_URL + "/duplicate";

@Injectable({
  providedIn: "root",
})
export class WorkflowPersistService {
  constructor(private http: HttpClient) {}

  /**
   * persists a workflow to backend database and returns its updated information (e.g., new wid)
   * @param workflow
   */
  public persistWorkflow(workflow: Workflow): Observable<Workflow> {
    return this.http
      .post<Workflow>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_PERSIST_URL}`, {
        wid: workflow.wid,
        name: workflow.name,
        content: JSON.stringify(workflow.content),
      })
      .pipe(
        filter((updatedWorkflow: Workflow) => updatedWorkflow != null),
        map(WorkflowUtilService.parseWorkflowInfo)
      );
  }

  /**
   * creates a workflow and insert it to backend database and return its information
   * @param newWorkflowName
   * @param newWorkflowContent
   */
  public createWorkflow(
    newWorkflowContent: WorkflowContent,
    newWorkflowName: string = "Untitled workflow"
  ): Observable<DashboardWorkflowEntry> {
    return this.http
      .post<DashboardWorkflowEntry>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_CREATE_URL}`, {
        name: newWorkflowName,
        content: JSON.stringify(newWorkflowContent),
      })
      .pipe(filter((createdWorkflow: DashboardWorkflowEntry) => createdWorkflow != null));
  }

  /**
   * creates a workflow and insert it to backend database and return its information
   * @param targetWid
   */
  public duplicateWorkflow(targetWid: number): Observable<DashboardWorkflowEntry> {
    return this.http
      .post<DashboardWorkflowEntry>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_DUPLICATE_URL}`, { wid: targetWid })
      .pipe(filter((createdWorkflow: DashboardWorkflowEntry) => createdWorkflow != null));
  }

  /**
   * retrieves a workflow from backend database given its id. The user in the session must have access to the workflow.
   * @param wid, the workflow id.
   */
  public retrieveWorkflow(wid: number): Observable<Workflow> {
    return this.http.get<Workflow>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_BASE_URL}/${wid}`).pipe(
      filter((workflow: Workflow) => workflow != null),
      map(WorkflowUtilService.parseWorkflowInfo)
    );
  }

  /**
   * retrieves a list of workflows from backend database that belongs to the user in the session.
   */
  public retrieveWorkflowsBySessionUser(): Observable<DashboardWorkflowEntry[]> {
    return this.http.get<DashboardWorkflowEntry[]>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_LIST_URL}`).pipe(
      map((dashboardWorkflowEntries: DashboardWorkflowEntry[]) =>
        dashboardWorkflowEntries.map((workflowEntry: DashboardWorkflowEntry) => {
          return {
            ...workflowEntry,
            dashboardWorkflowEntry: WorkflowUtilService.parseWorkflowInfo(workflowEntry.workflow),
          };
        })
      )
    );
  }

  /**
   * deletes the given workflow, the user in the session must own the workflow.
   */
  public deleteWorkflow(wid: number): Observable<Response> {
    return this.http.delete<Response>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_BASE_URL}/${wid}`);
  }
}
