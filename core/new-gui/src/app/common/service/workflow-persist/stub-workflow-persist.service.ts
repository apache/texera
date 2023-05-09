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
    if (operator === "NlpSentiment,SimpleSink" || operator === "Aggregation") {
      return new Observable(observer => observer.next(["1", "2", "3"]));
    }
    if (operator === "NlpSentiment") {
      return new Observable(observer => observer.next(["3"]));
    }
    return new Observable(observer => observer.next([]));
  }

  public retrieveWorkflow(wid: number): Observable<Workflow> {
    return new Observable(observer => observer.next(this.testWorkflows.find(e => e.workflow.wid == wid)?.workflow));
  }

  public searchWorkflows(
    keywords: string[],
    createDateStart: Date | null,
    createDateEnd: Date | null,
    modifiedDateStart: Date | null,
    modifiedDateEnd: Date | null,
    owners: string[],
    ids: string[],
    operators: string[],
    projectIds: number[]
  ): Observable<DashboardWorkflowEntry[]> {
    // A simple mock search implementation that only work for the test dataset.
    return new Observable(observer => {
      var results = this.testWorkflows;
      if (keywords.length > 0) {
        results = results.filter(e => keywords.some(k => e.workflow.name.indexOf(k) !== -1))
      }
      if (createDateStart) {
        results = results.filter(e => e.workflow.creationTime && e.workflow.creationTime > createDateStart.getTime());
      }
      if (createDateEnd) {
        results = results.filter(e => e.workflow.creationTime && e.workflow.creationTime < createDateEnd.getTime());
      }
      if (modifiedDateStart) {
        results = results.filter(e => e.workflow.lastModifiedTime && e.workflow.lastModifiedTime > modifiedDateStart.getTime());
      }
      if (modifiedDateEnd) {
        results = results.filter(e => e.workflow.lastModifiedTime && e.workflow.lastModifiedTime < modifiedDateEnd.getTime());
      }
      if (owners.length > 0) {
        results = results.filter(e => owners.some(o => e.ownerName === o));
      }
      if (ids.length > 0) {
        results = results.filter(e => ids.some(i => e.workflow.wid && e.workflow.wid.toString() === i));
      }
      if (operators.length > 0) {
        results = results.filter(e => e.workflow.content.operators.some(operator => operators.some(operatorTypeFilterBy => operatorTypeFilterBy === operator.operatorType)));
      }
      if (projectIds.length > 0) {
        results = results.filter(e => e.projectIDs.some(id => projectIds.some(projectIdToFilterBy => projectIdToFilterBy == id)));
      }
      return observer.next(results);
    });
  }
}
