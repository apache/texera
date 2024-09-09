import { Injectable } from "@angular/core";
import { firstValueFrom } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { HttpClient, HttpHeaders } from "@angular/common/http";

export const AI_ASSISTANT_API_BASE_URL = `${AppSettings.getApiEndpoint()}/aiassistant`;

@Injectable({
  providedIn: "root",
})
export class AIAssistantService {
  constructor(private http: HttpClient) {}

  public checkAIAssistantEnabled(): Promise<string> {
    const apiUrl = `${AI_ASSISTANT_API_BASE_URL}/isenabled`;
    return firstValueFrom(this.http.get(apiUrl, { responseType: "text" }))
      .then(response => {
        const isEnabled = response !== undefined ? response : "NoAIAssistant";
        console.log(
          isEnabled === "OpenAI"
            ? "AI Assistant successfully started"
            : "No AI Assistant or OpenAI authentication key error"
        );
        return isEnabled;
      })
      .catch(() => {
        return "NoAIAssistant";
      });
  }

  public getTypeAnnotations(code: string, lineNumber: number, allcode: string): Promise<string> {
    const headers = new HttpHeaders({ "Content-Type": "application/json" });
    const requestBody = { code, lineNumber, allcode };
    return firstValueFrom(
      this.http.post<any>(`${AI_ASSISTANT_API_BASE_URL}/annotationresult`, requestBody, { headers })
    )
      .then(response => {
        if (response.choices && response.choices.length > 0) {
          return response.choices[0].message.content.trim();
        } else {
          console.error("Error from backend:", response.body);
          return "";
        }
      })
      .catch(error => {
        console.error("Request to backend failed:", error);
        return "";
      });
  }
}
