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

export interface UnannotatedArgument {
  name: string;
  startLine: number;
  startColumn: number;
  endLine: number;
  endColumn: number;
}

interface UnannotatedArgumentItem {
  underlying: {
    name: { value: string };
    startLine: { value: number };
    startColumn: { value: number };
    endLine: { value: number };
    endColumn: { value: number };
  };
}

interface UnannotatedArgumentResponse {
  underlying: {
    result: {
      value: UnannotatedArgumentItem[];
    };
  };
}

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

  public locateUnannotated(selectedCode: string, startLine: number): Observable<UnannotatedArgument[]> {
    const requestBody = { selectedCode, startLine };

    return this.http.post<UnannotatedArgumentResponse>(`${AI_ASSISTANT_API_BASE_URL}/getArgument`, requestBody).pipe(
      map(response => {
        if (response) {
          return response.underlying.result.value.map(
            (item: UnannotatedArgumentItem): UnannotatedArgument => ({
              name: item.underlying.name.value,
              startLine: item.underlying.startLine.value,
              startColumn: item.underlying.startColumn.value,
              endLine: item.underlying.endLine.value,
              endColumn: item.underlying.endColumn.value,
            })
          );
        } else {
          console.error("Unexpected response format:", response);
          return [];
        }
      }),
      catchError((error: unknown) => {
        console.error("Request to backend failed:", error);
        return of([]);
      })
    );
  }
}
