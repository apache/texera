import { Injectable } from "@angular/core";
import { firstValueFrom } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { HttpClient, HttpHeaders } from "@angular/common/http";
import { Observable, of } from "rxjs";
import { map, catchError } from "rxjs/operators";

// The type annotation return from the LLM
export type TypeAnnotationResponse = {
  choices: ReadonlyArray<{
    message: {
      content: string;
    };
  }>;
};

export const AI_ASSISTANT_API_BASE_URL = `${AppSettings.getApiEndpoint()}/aiassistant`;

@Injectable({
  providedIn: "root",
})
export class AIAssistantService {
  constructor(private http: HttpClient) {}

  public checkAIAssistantEnabled(): Observable<string> {
    const apiUrl = `${AI_ASSISTANT_API_BASE_URL}/isenabled`;
    return this.http.get(apiUrl, { responseType: "text" }).pipe(
      map(response => {
        const isEnabled = response !== undefined ? response : "NoAiAssistant";
        console.log(
          isEnabled === "OpenAI"
            ? "AI Assistant successfully started"
            : "No AI Assistant or OpenAI authentication key error"
        );
        return isEnabled;
      }),
      catchError(() => {
        return of("NoAiAssistant");
      })
    );
  }

  public getTypeAnnotations(code: string, lineNumber: number, allcode: string): Observable<TypeAnnotationResponse> {
    const headers = new HttpHeaders({});
    const requestBody = { code, lineNumber, allcode };
    return this.http.post<TypeAnnotationResponse>(`${AI_ASSISTANT_API_BASE_URL}/annotationresult`, requestBody, {
      headers,
    });
  }
}
