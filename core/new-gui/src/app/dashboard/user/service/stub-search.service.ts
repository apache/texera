import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { SearchResult } from "../type/search-result";
import { SearchFilterParameters, searchTestEntries } from "../type/search-filter-parameters";
import { DashboardEntry } from "../type/dashboard-entry";

@Injectable({
  providedIn: "root",
})
export class StubSearchService {
  constructor(private testEntries: DashboardEntry[]) {}

  /**
   * retrieves a workflow from backend database given its id. The user in the session must have access to the workflow.
   * @param wid, the workflow id.
   */
  public search(keywords: string[], params: SearchFilterParameters): Observable<SearchResult[]> {
    return new Observable(observer => {
      observer.next(
        searchTestEntries(keywords, params, this.testEntries).map(i => ({
          resourceType: i.type,
          workflow: i.type === "workflow" ? i.workflow : undefined,
          project: i.type === "project" ? i.project : undefined,
        }))
      );
    });
  }
}
