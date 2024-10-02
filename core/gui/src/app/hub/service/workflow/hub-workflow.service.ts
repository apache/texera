import { HttpClient, HttpParams } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { HubWorkflow } from "../../component/type/hub-workflow.interface";
import { User } from "src/app/common/type/user";
import { Workflow } from "../../../common/type/workflow";
import { filter, map } from "rxjs/operators";
import { WorkflowUtilService } from "../../../workspace/service/workflow-graph/util/workflow-util.service";

@Injectable({
  providedIn: "root",
})
export class HubWorkflowService {
  readonly BASE_URL: string = `${AppSettings.getApiEndpoint()}/hub/workflow`;

  constructor(private http: HttpClient) {}

  public getWorkflowCount(): Observable<number> {
    return this.http.get<number>(`${this.BASE_URL}/count`);
  }

  public getWorkflowList(): Observable<HubWorkflow[]> {
    return this.http.get<HubWorkflow[]>(`${this.BASE_URL}/list`);
  }

  public getOwnerUser(wid: number): Observable<User> {
    const params = new HttpParams().set("wid", wid);
    return this.http.get<User>(`${this.BASE_URL}/owner_user/`, { params });
  }

  public getWorkflowName(wid: number): Observable<string> {
    const params = new HttpParams().set("wid", wid);
    return this.http.get(`${this.BASE_URL}/workflow_name/`, { params, responseType: "text" });
  }

  public retrievePublicWorkflow(wid: number): Observable<Workflow> {
    return this.http.get<Workflow>(`${this.BASE_URL}/public/${wid}`).pipe(
      filter((workflow: Workflow) => workflow != null),
      map(WorkflowUtilService.parseWorkflowInfo)
    );
  }

  public getWorkflowDescription(wid: number): Observable<string> {
    const params = new HttpParams().set("wid", wid);
    return this.http.get(`${this.BASE_URL}/workflow_description/`, { params, responseType: "text" });
  }
}
