import { Injectable } from "@angular/core";
import { firstValueFrom } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { HttpClient } from "@angular/common/http";

export const AI_ASSISTANT_API_BASE_URL = `${AppSettings.getApiEndpoint()}/aiassistant`;

@Injectable({
  providedIn: "root",
})
export class AiAssistantService {
  constructor(private http: HttpClient) {}

  public checkAiAssistantEnabled(): Promise<boolean> {
    const apiUrl = `${AI_ASSISTANT_API_BASE_URL}/isenabled`;
    return firstValueFrom(this.http.get<boolean>(apiUrl))
      .then(response => {
        const isEnabled = response !== undefined ? response : false;
        console.log(
          isEnabled ? "AI Assistant successfully started" : "No AI Assistant or OpenAI authentication key error"
        );
        return isEnabled;
      })
      .catch(() => {
        console.log("No AI Assistant or OpenAI authentication key error");
        return false;
      });
  }
}
