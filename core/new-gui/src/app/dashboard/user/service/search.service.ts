import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { SearchResult } from "../type/search-result";
import { AppSettings } from "src/app/common/app-setting";
import { SearchFilterParameters, toQueryStrings } from "../type/search-filter-parameters";
import { SortMethod } from "../type/sort-method";

const DASHBOARD_SEARCH_URL = "dashboard/search";

@Injectable({
  providedIn: "root",
})
export class SearchService {
  constructor(private http: HttpClient) {}

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
    return this.http.get<SearchResult>(
      `${AppSettings.getApiEndpoint()}/${DASHBOARD_SEARCH_URL}?${toQueryStrings(
        keywords,
        params,
        start,
        count,
        type,
        orderBy
      )}`
    );
  }
}
