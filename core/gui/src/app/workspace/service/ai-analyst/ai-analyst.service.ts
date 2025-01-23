// Define a response type for OpenAI API
interface OpenAIResponse {
  choices: {
    message: {
      content: string;
    };
  }[];
}

import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { firstValueFrom, of, catchError, Observable } from "rxjs";
import { map } from "rxjs/operators";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { AppSettings } from "../../../common/app-setting";

const AI_ASSISTANT_API_BASE_URL = `${AppSettings.getApiEndpoint()}`;
const api_Url_Is_Enabled = `${AI_ASSISTANT_API_BASE_URL}/aiassistant/isenabled`;
const api_Url_Openai = `${AI_ASSISTANT_API_BASE_URL}/aiassistant/openai`;

@Injectable({
  providedIn: "root",
})
/**
 * This class `AiAnalystService` is responsible for integrating with the AI Assistant feature to generate insightful comments
 * based on the provided prompts. It is mainly used for generating automated feedback or explanations for workflow components
 */
export class AiAnalystService {
  private isAIAssistantEnabled: boolean | null = null;
  constructor(
    private http: HttpClient,
    public workflowActionService: WorkflowActionService
  ) {}
  /**
   * Checks if the AI Assistant feature is enabled by sending a request to the API.
   *
   * @returns {Promise<boolean>} A promise that resolves to a boolean indicating whether the AI Assistant is enabled.
   *                             Returns `false` if the request fails or the response is undefined.
   */
  public isOpenAIEnabled(): Observable<boolean> {
    if (this.isAIAssistantEnabled !== null) {
      return of(this.isAIAssistantEnabled);
    }

    return this.http.get(api_Url_Is_Enabled, { responseType: "text" }).pipe(
      map(response => {
        const isEnabled = response === "OpenAI";
        return isEnabled;
      }),
      catchError(() => of(false))
    );
  }

  /**
   * Generates an insightful feedback for the given input prompt by utilizing the AI Assistant service.
   *
   * @param {string} inputPrompt - The operator information in JSON format, which will be used to generate the comment.
   * @returns {Promise<string>} A promise that resolves to a string containing the generated comment or an error message
   *                            if the generation fails or the AI Assistant is not enabled.
   */
  public sendPromptToOpenAI(inputPrompt: string): Observable<string> {
    const prompt = inputPrompt;

    // Create an observable to handle the single request
    return new Observable<string>(observer => {
      this.isOpenAIEnabled().subscribe(
        (AIEnabled: boolean) => {
          if (!AIEnabled) {
            observer.next(""); // If AI Assistant is not enabled, return an empty string
            observer.complete();
          } else {
            // Perform the HTTP request without retries
            this.http
              .post<OpenAIResponse>(api_Url_Openai, { prompt })
              .pipe(
                map(response => {
                  const content = response.choices[0]?.message?.content.trim() || "";
                  return content;
                })
              )
              .subscribe({
                next: content => {
                  observer.next(content); // Return the response content if successful
                  observer.complete();
                },
                error: () => {
                  observer.next(""); // If there's an error, return an empty string
                  observer.complete();
                },
              });
          }
        },
        () => {
          observer.next(""); // If AI Assistant status check fails, return an empty string
          observer.complete();
        }
      );
    });
  }
}
