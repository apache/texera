import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { UserService } from "../../../common/service/user/user.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { PowerState } from "../../component/power-button/power-button.component";
import { lastValueFrom } from "rxjs";

@Injectable({
  providedIn: "root",
})
export class WorkflowPodBrainService {
  private static readonly TEXERA_CREATE_POD_ENDPOINT = "create";
  private static readonly TEXERA_DELETE_POD_ENDPOINT = "terminate";
  private static readonly TEXERA_WORKFLOW_POD_ENDPOINT = "workflowpod";

  constructor(
    private http: HttpClient,
    private userService: UserService,
    private workflowActionService: WorkflowActionService
  ) {}

  public async sendRequest(requestType: string): Promise<Response | undefined> {
    try {
      const body = {
        wid: this.workflowActionService.getWorkflowMetadata()?.wid,
        uid: this.userService.getCurrentUser()?.uid ?? 1,
      };

      return await lastValueFrom(
        this.http.post<Response>(
          `${WorkflowPodBrainService.TEXERA_WORKFLOW_POD_ENDPOINT}/${this.getRequestTypePath(requestType)}`,
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
    }
    return WorkflowPodBrainService.TEXERA_DELETE_POD_ENDPOINT;
  }
}
