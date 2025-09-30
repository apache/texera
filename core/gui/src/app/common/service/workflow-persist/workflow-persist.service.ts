/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { HttpClient, HttpParams } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable, throwError } from "rxjs";
import { filter, map, catchError } from "rxjs/operators";
import { AppSettings } from "../../app-setting";
import { Workflow, WorkflowContent } from "../../type/workflow";
import { DashboardWorkflow } from "../../../dashboard/type/dashboard-workflow.interface";
import { WorkflowUtilService } from "../../../workspace/service/workflow-graph/util/workflow-util.service";
import { NotificationService } from "../notification/notification.service";
import { SearchFilterParameters, toQueryStrings } from "../../../dashboard/type/search-filter-parameters";
import { User } from "../../type/user";
import { checkIfWorkflowBroken } from "../../util/workflow-check";

export const WORKFLOW_BASE_URL = "workflow";
export const WORKFLOW_PERSIST_URL = WORKFLOW_BASE_URL + "/persist";
export const WORKFLOW_LIST_URL = WORKFLOW_BASE_URL + "/list";
export const WORKFLOW_SEARCH_URL = WORKFLOW_BASE_URL + "/search";
export const WORKFLOW_CREATE_URL = WORKFLOW_BASE_URL + "/create";
export const WORKFLOW_DUPLICATE_URL = WORKFLOW_BASE_URL + "/duplicate";
export const WORKFLOW_DELETE_URL = WORKFLOW_BASE_URL + "/delete";
export const WORKFLOW_UPDATENAME_URL = WORKFLOW_BASE_URL + "/update/name";
export const WORKFLOW_UPDATEDESCRIPTION_URL = WORKFLOW_BASE_URL + "/update/description";
export const WORKFLOW_OWNER_URL = WORKFLOW_BASE_URL + "/user-workflow-owners";
export const WORKFLOW_ID_URL = WORKFLOW_BASE_URL + "/user-workflow-ids";
export const WORKFLOW_OWNER_USER = WORKFLOW_BASE_URL + "/owner_user";
export const WORKFLOW_NAME = WORKFLOW_BASE_URL + "/workflow_name";
export const WORKFLOW_PUBLIC_WORKFLOW = WORKFLOW_BASE_URL + "/publicised";
export const WORKFLOW_DESCRIPTION = WORKFLOW_BASE_URL + "/workflow_description";
export const WORKFLOW_USER_ACCESS = WORKFLOW_BASE_URL + "/workflow_user_access";
export const WORKFLOW_SIZE = WORKFLOW_BASE_URL + "/size";

export const DEFAULT_WORKFLOW_NAME = "Untitled workflow";

@Injectable({
  providedIn: "root",
})
export class WorkflowPersistService {
  // flag to disable workflow persist when displaying the read only particular version
  private workflowPersistFlag = true;

  constructor(
    private http: HttpClient,
    private notificationService: NotificationService
  ) {}

  /**
   * persists a workflow to backend database and returns its updated information (e.g., new wid)
   * @param workflow
   */
  public persistWorkflow(workflow: Workflow): Observable<Workflow> {
    if (checkIfWorkflowBroken(workflow)) {
      this.notificationService.error(
        "Sorry! The workflow is broken and cannot be persisted. Please contact the system admin."
      );
    }

    return this.http
      .post<Workflow>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_PERSIST_URL}`, {
        wid: workflow.wid,
        name: workflow.name,
        description: workflow.description,
        content: JSON.stringify(workflow.content),
        isPublic: workflow.isPublished,
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
    newWorkflowName: string = DEFAULT_WORKFLOW_NAME
  ): Observable<DashboardWorkflow> {
    return this.http
      .post<DashboardWorkflow>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_CREATE_URL}`, {
        name: newWorkflowName,
        content: JSON.stringify(newWorkflowContent),
      })
      .pipe(filter((createdWorkflow: DashboardWorkflow) => createdWorkflow != null));
  }

  /**
   * creates a workflow and insert it to backend database and return its information
   * @param targetWids
   * @param pid
   */
  public duplicateWorkflow(targetWids: number[], pid?: number): Observable<DashboardWorkflow[]> {
    return this.http
      .post<DashboardWorkflow[]>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_DUPLICATE_URL}`, {
        wids: targetWids,
        ...(pid !== undefined && { pid }),
      })
      .pipe(filter((createdWorkflows: DashboardWorkflow[]) => createdWorkflows != null && createdWorkflows.length > 0));
  }

  /**
   * retrieves a workflow from backend database given its id. The user in the session must have access to the workflow.
   * @param wid
   */
  public retrieveWorkflow(wid: number): Observable<Workflow> {
    return this.http.get<Workflow>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_BASE_URL}/${wid}`).pipe(
      filter((workflow: Workflow) => workflow != null),
      map(WorkflowUtilService.parseWorkflowInfo)
    );
  }

  private makeRequestAndFormatWorkflowResponse(url: string): Observable<DashboardWorkflow[]> {
    return this.http.get<DashboardWorkflow[]>(url).pipe(
      map((dashboardWorkflowEntries: DashboardWorkflow[]) =>
        dashboardWorkflowEntries.map((workflowEntry: DashboardWorkflow) => {
          return {
            ...workflowEntry,
            dashboardWorkflowEntry: WorkflowUtilService.parseWorkflowInfo(workflowEntry.workflow),
          };
        })
      )
    );
  }

  /**
   * retrieves a list of workflows from backend database that belongs to the user in the session.
   */
  public retrieveWorkflowsBySessionUser(): Observable<DashboardWorkflow[]> {
    return this.makeRequestAndFormatWorkflowResponse(`${AppSettings.getApiEndpoint()}/${WORKFLOW_LIST_URL}`);
  }

  /**
   * Search workflows by a text query from backend database that belongs to the user in the session.
   */
  public searchWorkflows(keywords: string[], params: SearchFilterParameters): Observable<DashboardWorkflow[]> {
    return this.makeRequestAndFormatWorkflowResponse(
      `${AppSettings.getApiEndpoint()}/${WORKFLOW_SEARCH_URL}?${toQueryStrings(keywords, params)}`
    );
  }

  /**
   * deletes the given workflow, the user in the session must own the workflow.
   */
  public deleteWorkflow(wids: number[]): Observable<Response> {
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_DELETE_URL}`, {
      wids: wids,
    });
  }

  /**
   * updates the name of a given workflow, the user in the session must own the workflow.
   */
  public updateWorkflowName(wid: number, name: string): Observable<Response> {
    return this.http
      .post<Response>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_UPDATENAME_URL}`, {
        wid: wid,
        name: name,
      })
      .pipe(
        catchError((error: unknown) => {
          // @ts-ignore
          this.notificationService.error(error.error.message);
          return throwError(error);
        })
      );
  }

  /**
   * updates the description of a given workflow
   */
  public updateWorkflowDescription(wid: number, description: string): Observable<Response> {
    return this.http
      .post<Response>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_UPDATEDESCRIPTION_URL}`, {
        wid: wid,
        description: description,
      })
      .pipe(
        catchError((error: unknown) => {
          // @ts-ignore
          this.notificationService.error(error.error.message);
          return throwError(error);
        })
      );
  }

  public getWorkflowIsPublished(wid: number): Observable<string> {
    return this.http.get(`${AppSettings.getApiEndpoint()}/${WORKFLOW_BASE_URL}/type/${wid}`, { responseType: "text" });
  }

  public updateWorkflowIsPublished(wid: number, isPublished: boolean): Observable<void> {
    if (isPublished) {
      return this.http.put<void>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_BASE_URL}/public/${wid}`, null);
    } else {
      return this.http.put<void>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_BASE_URL}/private/${wid}`, null);
    }
  }

  public setWorkflowPersistFlag(flag: boolean): void {
    this.workflowPersistFlag = flag;
  }

  public isWorkflowPersistEnabled(): boolean {
    return this.workflowPersistFlag;
  }

  /**
   * retrieves all workflow owners
   */
  public retrieveOwners(): Observable<string[]> {
    return this.http.get<string[]>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_OWNER_URL}`);
  }

  /**
   * retrieves all workflow IDs
   */
  public retrieveWorkflowIDs(): Observable<number[]> {
    return this.http.get<number[]>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_ID_URL}`);
  }

  /**
   * retrieve the complete information of the owner corresponding to the wid
   * can be used without logging in
   * @param wid
   */
  public getOwnerUser(wid: number): Observable<User> {
    const params = new HttpParams().set("wid", wid);
    return this.http.get<User>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_OWNER_USER}`, { params });
  }

  /**
   * retrieve the name of the workflow corresponding to the wid
   * can be used without logging in
   * @param wid
   */
  public getWorkflowName(wid: number): Observable<string> {
    const params = new HttpParams().set("wid", wid);
    return this.http.get(`${AppSettings.getApiEndpoint()}/${WORKFLOW_NAME}`, { params, responseType: "text" });
  }

  /**
   * retrieve the complete information of the workflow corresponding to the wid
   * can be used without logging in
   * @param wid
   */
  public retrievePublicWorkflow(wid: number): Observable<Workflow> {
    return this.http.get<Workflow>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_PUBLIC_WORKFLOW}/${wid}`).pipe(
      filter((workflow: Workflow) => workflow != null),
      map(WorkflowUtilService.parseWorkflowInfo)
    );
  }

  /**
   * retrieve the description of the workflow corresponding to the wid
   * can be used without logging in
   * @param wid
   */
  public getWorkflowDescription(wid: number): Observable<string> {
    const params = new HttpParams().set("wid", wid);
    return this.http.get(`${AppSettings.getApiEndpoint()}/${WORKFLOW_DESCRIPTION}`, { params, responseType: "text" });
  }

  /**
   * Batch-fetch the JSON sizes of workflows by their IDs.
   * Can be used without logging in
   *
   * @param wids Array of workflow IDs to query.
   * @returns An object mapping each workflow ID to its JSON size.
   */
  public getSizes(wids: number[]): Observable<Record<number, number>> {
    let params = new HttpParams();
    wids.forEach(wid => {
      params = params.append("wid", wid.toString());
    });
    return this.http.get<Record<number, number>>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_SIZE}`, { params });
  }
}
