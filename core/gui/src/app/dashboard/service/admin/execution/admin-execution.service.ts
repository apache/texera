import { HttpClient, HttpParams } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { Execution } from "../../../../common/type/execution";

export const WORKFLOW_BASE_URL = `${AppSettings.getApiEndpoint()}/admin/execution`;

@Injectable({
  providedIn: "root",
})
export class AdminExecutionService {
  constructor(private http: HttpClient) {}

  public getExecutionList(
    pageSize: number,
    pageIndex: number,
    sortField: string,
    sortDirection: string,
    filter: string[]
  ): Observable<ReadonlyArray<Execution>> {
    const params = new HttpParams().set("filter", filter.join(","));
    return this.http.get<ReadonlyArray<Execution>>(
      `${WORKFLOW_BASE_URL}/executionList/${pageSize}/${pageIndex}/${sortField}/${sortDirection}`,
      { params }
    );
  }

  public getTotalWorkflows(): Observable<number> {
    return this.http.get<number>(`${WORKFLOW_BASE_URL}/totalWorkflow`);
  }
}
