import { Injectable } from "@angular/core";

import { Observable } from "rxjs";
import { Workflow } from "../../type/workflow";
import { DashboardWorkflowEntry } from "../../../dashboard/type/dashboard-workflow-entry";

export const WORKFLOW_BASE_URL = "workflow";
export const WORKFLOW_SEARCH_URL = WORKFLOW_BASE_URL + "/search";

@Injectable()
export class StubWorkflowPersistService {
  constructor(private testWorkflows: DashboardWorkflowEntry[]) {}

  public retrieveWorkflowByOperator(operator: string): Observable<string[]> {
    return new Observable(observer => observer.next([]));
  }

  public retrieveWorkflow(wid: number): Observable<Workflow> {
    return new Observable(observer => observer.next(this.testWorkflows.find(e => e.workflow.wid == wid)?.workflow));
  }

  public searchWorkflowsBySessionUser(keywords: string[]): Observable<DashboardWorkflowEntry[]> {
    return new Observable(observer => {
      if (keywords.length == 0) {
        return observer.next(this.testWorkflows);
      }
      return observer.next(this.testWorkflows.filter(e => keywords.some(k => k == e.workflow.name)));
    });
  }
}
