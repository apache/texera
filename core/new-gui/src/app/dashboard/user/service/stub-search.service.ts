import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { SearchResult } from "../type/search-result";
import { SearchFilterParameters, searchTestEntries } from "../type/search-filter-parameters";
import { DashboardEntry } from "../type/dashboard-entry";
import { SortMethod } from "../type/sort-method";

@Injectable({
  providedIn: "root",
})
export class StubSearchService {
  constructor(private testEntries: DashboardEntry[]) {}

  /**
   * retrieves a workflow from backend database given its id. The user in the session must have access to the workflow.
   * @param wid, the workflow id.
   */
  public search(
    keywords: string[],
    params: SearchFilterParameters,
    start: number,
    count: number,
    type: "workflow" | "project" | "file" | null,
    orderBy: SortMethod
  ): Observable<SearchResult> {
    // Igoring start count and orderBy as they are not tested in the unit tests.
    return new Observable(observer => {
      observer.next({
        results: searchTestEntries(keywords, params, this.testEntries, type).map(i => ({
          resourceType: i.type,
          workflow: i.type === "workflow" ? i.workflow : undefined,
          project: i.type === "project" ? i.project : undefined,
        })),
        more: false,
      });
    });
  }
}
