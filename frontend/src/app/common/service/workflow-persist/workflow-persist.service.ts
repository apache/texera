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
import { Injectable, Injector } from "@angular/core";
import { EMPTY, Observable, of, ReplaySubject, Subject, throwError } from "rxjs";
import { catchError, concatMap, filter, finalize, map, take, tap } from "rxjs/operators";
import { AppSettings } from "../../app-setting";
import { Workflow, WorkflowContent } from "../../type/workflow";
import { DashboardWorkflow } from "../../../dashboard/type/dashboard-workflow.interface";
import { DefaultView } from "../../../dashboard/type/workflow-metadata.interface";
import { WorkflowUtilService } from "../../../workspace/service/workflow-graph/util/workflow-util.service";
import {
  DEFAULT_WORKFLOW,
  WorkflowActionService,
} from "../../../workspace/service/workflow-graph/model/workflow-action.service";
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
export const WORKFLOW_OWNER_NAME = WORKFLOW_BASE_URL + "/owner_name";
export const WORKFLOW_NAME = WORKFLOW_BASE_URL + "/workflow_name";
export const WORKFLOW_PUBLIC_WORKFLOW = WORKFLOW_BASE_URL + "/publicised";
export const WORKFLOW_DESCRIPTION = WORKFLOW_BASE_URL + "/workflow_description";
export const WORKFLOW_USER_ACCESS = WORKFLOW_BASE_URL + "/workflow_user_access";
export const WORKFLOW_SIZE = WORKFLOW_BASE_URL + "/size";
export const WORKFLOW_SET_DEFAULT_VIEW_URL = WORKFLOW_BASE_URL + "/set-default-view";

export const DEFAULT_WORKFLOW_NAME = "Untitled workflow";

@Injectable({
  providedIn: "root",
})
export class WorkflowPersistService {
  // flag to disable workflow persist when displaying the read only particular version
  private workflowPersistFlag = true;

  /**
   * Saves, one at a time and in call order. Two saves in flight at once can reach the backend out
   * of order, and then the older content wins: the canvas's autosave (debounced) and a Save or a
   * view switch are independent requests, and the Form View's own queue only orders that page's
   * saves. Ordering them here, at the one place every save goes through, covers all of them and
   * lets a caller that hands over on completion (the view switches) know that everything asked for
   * before it has landed too. Each request snapshots its payload when asked for; it is sent when its
   * turn comes, and its outcome is relayed to that caller alone. A failed save fails its own caller
   * and does not hold up the next.
   *
   * A response is relayed with the page's current name and description in place of its own (see
   * withLocalEdits): those are the two fields a user edits, and a response answers the save it was
   * sent for, which may be older than an edit made since. Callers feed the response back as the
   * workflow's metadata; without this, a rename made while a save was out came back undone.
   */
  private readonly persistQueue = new Subject<{ send: Observable<Workflow>; result: Subject<Workflow> }>();

  /** Saves asked for and not yet answered (or failed); see whenSavesDrained. */
  private pendingSaves = 0;
  private readonly savesDrained = new Subject<void>();

  constructor(
    private http: HttpClient,
    private notificationService: NotificationService,
    // Looked up lazily, at response time: the persist service is also used by the dashboard, where
    // no workflow is open and constructing the (graph-owning) action service would be a side effect.
    private injector: Injector
  ) {
    this.persistQueue
      .pipe(
        concatMap(({ send, result }) =>
          send.pipe(
            map(updated => this.withLocalEdits(updated)),
            tap({
              next: updated => result.next(updated),
              error: (err: unknown) => result.error(err),
              complete: () => result.complete(),
            }),
            catchError(() => EMPTY),
            finalize(() => this.saveDone())
          )
        )
      )
      .subscribe();
  }

  /**
   * Emits once every save asked for so far has been answered or has failed; at once when none is
   * pending. For a caller that leaves its view on completion (the Form View switch): its own save
   * completing is not enough. A save queued behind it (a rename's, a description's) is still sent
   * -- this service outlives the view -- but it answers to the component that asked for it, and a
   * component the route has destroyed shows no error and feeds back no response.
   */
  public whenSavesDrained(): Observable<void> {
    return this.pendingSaves === 0 ? of(undefined) : this.savesDrained.pipe(take(1));
  }

  private saveDone(): void {
    this.pendingSaves -= 1;
    if (this.pendingSaves === 0) {
      this.savesDrained.next();
    }
  }

  /**
   * The response with the page's current name and description: a response carries the values the
   * save was sent with, and an edit made since would be undone by feeding them back. Left alone when
   * another workflow is open by now (nothing local belongs to this response); a workflow just created
   * still carries the default id locally, and its rename made meanwhile is kept too.
   */
  private withLocalEdits(response: Workflow): Workflow {
    const current = this.injector.get(WorkflowActionService).getWorkflowMetadata();
    if (current.wid !== response.wid && current.wid !== DEFAULT_WORKFLOW.wid) {
      return response;
    }
    return { ...response, name: current.name, description: current.description };
  }

  /**
   * persists a workflow to backend database and returns its updated information (e.g., new wid).
   * The request is queued behind any save still in flight (see persistQueue); the returned
   * observable completes once this save has come back.
   * @param workflow
   */
  public persistWorkflow(workflow: Workflow): Observable<Workflow> {
    if (checkIfWorkflowBroken(workflow)) {
      this.notificationService.error(
        "Sorry! The workflow is broken and cannot be persisted. Please contact the system admin."
      );
    }

    // A save carries name, description and content only. The publish flag is not sent: the
    // backend does not read it on this endpoint (publishing goes through /public and /private),
    // and it is not reliably known here anyway, since the metadata fed back after a save names
    // it differently (see WorkflowUtilService.parseWorkflowInfo).
    const send = this.http
      .post<Workflow>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_PERSIST_URL}`, {
        wid: workflow.wid,
        name: workflow.name,
        description: workflow.description,
        content: JSON.stringify(workflow.content),
      })
      .pipe(
        filter((updatedWorkflow: Workflow) => updatedWorkflow != null),
        map(WorkflowUtilService.parseWorkflowInfo)
      );
    // Replayed, so a caller that subscribes after the queue has already relayed the outcome (a
    // save that was quick, or a synchronous test double) still receives it.
    const result = new ReplaySubject<Workflow>(1);
    this.pendingSaves += 1;
    this.persistQueue.next({ send, result });
    return result.asObservable();
  }

  /**
   * creates a workflow and insert it to backend database and return its information
   * @param newWorkflowName
   * @param newWorkflowContent
   */
  public createWorkflow(
    newWorkflowContent: WorkflowContent,
    newWorkflowName: string = DEFAULT_WORKFLOW_NAME,
    defaultView?: DefaultView
  ): Observable<DashboardWorkflow> {
    return this.http
      .post<DashboardWorkflow>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_CREATE_URL}`, {
        name: newWorkflowName,
        content: JSON.stringify(newWorkflowContent),
        // Bound onto the workflow row on the server, so an uploaded form-default workflow
        // still opens as a form. Omitted (server default CANVAS) when the file carries none.
        ...(defaultView === undefined ? {} : { defaultView }),
      })
      .pipe(filter((createdWorkflow: DashboardWorkflow) => createdWorkflow != null));
  }

  /**
   * creates a workflow and insert it to backend database and return its information
   * @param targetWids
   */
  public duplicateWorkflow(targetWids: number[]): Observable<DashboardWorkflow[]> {
    return this.http
      .post<DashboardWorkflow[]>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_DUPLICATE_URL}`, {
        wids: targetWids,
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
   * Retrieve workflow owner name (no login required).
   * @param wid workflow id
   */
  public getOwnerName(wid: number): Observable<string> {
    const params = new HttpParams().set("wid", wid);
    return this.http.get(`${AppSettings.getApiEndpoint()}/${WORKFLOW_OWNER_NAME}`, { params, responseType: "text" });
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

  /** Set which view the workflow opens in by default. Only this preference moves; the Form
   *  View definition lives in the workflow content, so switching the default keeps the setup. */
  public setDefaultView(wid: number, view: DefaultView): Observable<void> {
    return this.http.put<void>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_SET_DEFAULT_VIEW_URL}/${wid}`, { view });
  }
}
