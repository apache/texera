import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { Workflow, ExecutionQuota } from "../../../../common/type/user";
import { DatasetQuota } from "src/app/dashboard/type/quota-statistic.interface";

export const USER_BASE_URL = `${AppSettings.getApiEndpoint()}/quota`;
export const USER_CREATED_DATASETS = `${USER_BASE_URL}/created_datasets`;
export const USER_CREATED_WORKFLOWS = `${USER_BASE_URL}/created_workflows`;
export const USER_ACCESS_WORKFLOWS = `${USER_BASE_URL}/access_workflows`;
export const USER_QUOTA_SIZE = `${USER_BASE_URL}/user_quota_size`;
export const USER_DELETE_EXECUTION_COLLECTION = `${USER_BASE_URL}/deleteCollection`;

@Injectable({
  providedIn: "root",
})
export class UserQuotaService {
  constructor(private http: HttpClient) {}

  public getCreatedDatasets(uid: number): Observable<ReadonlyArray<DatasetQuota>> {
    return this.http.get<ReadonlyArray<DatasetQuota>>(`${USER_CREATED_DATASETS}`);
  }

  public getCreatedWorkflows(uid: number): Observable<ReadonlyArray<Workflow>> {
    return this.http.get<ReadonlyArray<Workflow>>(`${USER_CREATED_WORKFLOWS}`);
  }

  public getAccessWorkflows(uid: number): Observable<ReadonlyArray<number>> {
    return this.http.get<ReadonlyArray<number>>(`${USER_ACCESS_WORKFLOWS}`);
  }

  public getExecutionQuota(uid: number): Observable<ReadonlyArray<ExecutionQuota>> {
    return this.http.get<ReadonlyArray<ExecutionQuota>>(`${USER_QUOTA_SIZE}`);
  }

  public deleteExecutionCollection(eid: number): Observable<void> {
    return this.http.delete<void>(`${USER_DELETE_EXECUTION_COLLECTION}/${eid.toString()}`);
  }
}
