import { Injectable } from "@angular/core";
import { firstValueFrom } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { HttpClient, HttpHeaders } from "@angular/common/http";
import { Observable } from "rxjs";

// The type annotation return from the LLM
export type TypeAnnotationResponse = {
  choices: {
    message: {
      content: string;
    };
  }[];
};

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
        const isEnabled = response !== undefined ? response : "NoAiAssistant";
        console.log(
          isEnabled === "OpenAI"
            ? "AI Assistant successfully started"
            : "No AI Assistant or OpenAI authentication key error"
        );
        return isEnabled;
      })
      .catch(() => {
        return "NoAiAssistant";
      });
  }

  public getTypeAnnotations(code: string, lineNumber: number, allcode: string): Observable<TypeAnnotationResponse> {
    const headers = new HttpHeaders({ "Content-Type": "application/json" });
    const requestBody = { code, lineNumber, allcode };
    return this.http.post<TypeAnnotationResponse>(`${AI_ASSISTANT_API_BASE_URL}/annotationresult`, requestBody, {
      headers,
    });
  }
}
