import { Injectable } from "@angular/core";
import { AppSettings } from "../../../common/app-setting";
import { HttpClient, HttpHeaders } from "@angular/common/http";
import { Observable, of } from "rxjs";
import { map, catchError } from "rxjs/operators";

// The type annotation return from the LLM
export type TypeAnnotationResponse = {
  choices: ReadonlyArray<{
    message: Readonly<{
      content: string;
    }>;
  }>;
};

// Define AI model type
export const AI_ASSISTANT_API_BASE_URL = `${AppSettings.getApiEndpoint()}/aiassistant`;
export const AI_MODEL = {
  OpenAI: "OpenAI",
  NoAiAssistant: "NoAiAssistant",
} as const;
export type AI_MODEL = (typeof AI_MODEL)[keyof typeof AI_MODEL];

@Injectable({
  providedIn: "root",
})
export class AIAssistantService {
  constructor(private http: HttpClient) {}

  /**
   * Checks if AI Assistant is enabled and returns the AI model in use.
   *
   * @returns {Observable<AI_MODEL>} - An Observable that emits the type of AI model in use ("OpenAI" or "NoAiAssistant").
   */
  // To get the backend AI flag to check if the user want to use the AI feature
  // valid returns: ["OpenAI", "NoAiAssistant"]
  public checkAIAssistantEnabled(): Observable<AI_MODEL> {
    const apiUrl = `${AI_ASSISTANT_API_BASE_URL}/isenabled`;
    return this.http.get(apiUrl, { responseType: "text" }).pipe(
      map(response => {
        const isEnabled: AI_MODEL = response === "OpenAI" ? "OpenAI" : "NoAiAssistant";
        console.log(
          isEnabled === "OpenAI"
            ? "AI Assistant successfully started"
            : "No AI Assistant or OpenAI authentication key error"
        );
        return isEnabled;
      }),
      catchError(() => {
        return of("NoAiAssistant" as AI_MODEL);
      })
    );
  }

  /**
   * Sends a request to the backend to get type annotation suggestions from LLM for the provided code.
   *
   * @param {string} code - The selected code for which the user wants type annotation suggestions.
   * @param {number} lineNumber - The line number where the selected code locates.
   * @param {string} allcode - The entire code of the UDF (User Defined Function) to provide context for the AI assistant.
   *
   * @returns {Observable<TypeAnnotationResponse>} - An Observable that emits the type annotation suggestions
   * returned by the LLM.
   */
  public getTypeAnnotations(code: string, lineNumber: number, allcode: string): Observable<TypeAnnotationResponse> {
    const requestBody = { code, lineNumber, allcode };
    return this.http.post<TypeAnnotationResponse>(`${AI_ASSISTANT_API_BASE_URL}/annotationresult`, requestBody, {});
  }
}
