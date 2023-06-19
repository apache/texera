import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { DashboardWorkflow } from "../../../dashboard/user/type/dashboard-workflow.interface";
import { Workflow } from "../../type/workflow";
import { SearchFilterParameters, searchTestEntries } from "src/app/dashboard/user/type/search-filter-parameters";
import { DashboardEntry } from "src/app/dashboard/user/type/dashboard-entry";

export const WORKFLOW_BASE_URL = "workflow";
export const WORKFLOW_SEARCH_URL = WORKFLOW_BASE_URL + "/search";

@Injectable()
export class StubWorkflowPersistService {
  constructor(private testWorkflows: DashboardEntry[]) {}

  public retrieveWorkflow(wid: number): Observable<Workflow> {
    return new Observable(observer =>
      observer.next(this.testWorkflows.find(w => w.workflow.workflow.wid == wid)?.workflow.workflow)
    );
  }

  public searchWorkflows(keywords: string[], params: SearchFilterParameters): Observable<DashboardWorkflow[]> {
    return new Observable(observer => {
      return observer.next(searchTestEntries(keywords, params, this.testWorkflows, "workflow").map(i => i.workflow));
    });
  }
  /**
   * retrieves all workflow owners
   */
  public retrieveOwners(): Observable<string[]> {
    const names = this.testWorkflows.filter(i => i).map(i => i.workflow.ownerName) as string[];
    return new Observable(observer => {
      observer.next([...new Set(names)]);
    });
  }

  /**
   * retrieves all workflow IDs
   */
  public retrieveWorkflowIDs(): Observable<number[]> {
    return new Observable(observer => {
      observer.next(this.testWorkflows.map(i => i.workflow.workflow.wid as number).filter(i => i));
    });
  }
}
