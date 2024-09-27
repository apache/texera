import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { UserService } from "../../../common/service/user/user.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { PowerState } from "../../component/power-button/power-button.component";
import { AppSettings } from "src/app/common/app-setting";
import { lastValueFrom } from "rxjs";

@Injectable({
  providedIn: "root",
})
export class WorkflowPodBrainService {
  private static readonly TEXERA_CREATE_POD_ENDPOINT = "create";
  private static readonly TEXERA_DELETE_POD_ENDPOINT = "terminate";

  private readonly uid: number | undefined;
  private readonly wid: number | undefined;

  constructor(
    private http: HttpClient,
    private userService: UserService,
    private workflowActionService: WorkflowActionService
  ) {
    this.uid = this.userService.getCurrentUser()?.uid ?? 1;
    this.wid = this.workflowActionService.getWorkflowMetadata()?.wid;
  }

  public async sendRequest(requestType: string): Promise<Response | undefined> {
    try {
      const body = {
        wid: this.wid,
        uid: this.uid,
      };

      return await lastValueFrom(
        this.http.post<Response>(
          `${AppSettings.getWorkflowPodEndpoint()}/${this.getRequestTypePath(requestType)}`,
          body,
          {
            responseType: "text" as "json",
          }
        )
      );
    } catch (error) {
      console.error("Error sending request:", error);
      throw error;
    }
  }

  private getRequestTypePath(requestType: string): string {
    if (requestType === PowerState.Initializing) {
      return WorkflowPodBrainService.TEXERA_CREATE_POD_ENDPOINT;
    } else {
      return WorkflowPodBrainService.TEXERA_DELETE_POD_ENDPOINT;
    }
  }
}
