import { environment } from "../../environments/environment";

export class AppSettings {
  public static getApiEndpoint(): string {
    return environment.apiUrl;
  }

  public static getWorkflowPodEndpoint(): string {
    return environment.workflowPodUrl;
  }
}
