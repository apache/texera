import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { firstValueFrom, of, catchError, Observable } from "rxjs";
import { map } from "rxjs/operators";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { AppSettings } from "../../../common/app-setting";

const AI_ASSISTANT_API_BASE_URL = `${AppSettings.getApiEndpoint()}`;
const apiUrlIsEnabled = `${AI_ASSISTANT_API_BASE_URL}/aiassistant/isenabled`;
const apiUrlOpenai = `${AI_ASSISTANT_API_BASE_URL}/aiassistant/openai`;

@Injectable({
  providedIn: "root",
})
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
  public checkAIAssistantEnabled(): Observable<boolean> {
    if (this.isAIAssistantEnabled !== null) {
      return of(this.isAIAssistantEnabled);
    }

    return this.http.get(apiUrlIsEnabled, { responseType: "text" }).pipe(
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
  public openai(inputPrompt: string): Observable<string> {
    const prompt = inputPrompt;

    const maxRetries = 2; // Maximum number of retries
    let attempts = 0;

    // Create an observable to handle retries
    return new Observable<string>(observer => {
      // Check if AI Assistant is enabled
      this.checkAIAssistantEnabled().subscribe(
        (AIEnabled: boolean) => {
          if (!AIEnabled) {
            observer.next(""); // If AI Assistant is not enabled, return an empty string
            observer.complete();
          } else {
            // Retry logic for up to maxRetries attempts
            const tryRequest = () => {
              this.http
                .post<any>(apiUrlOpenai, { prompt })
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
                  error: (error: unknown) => {
                    attempts++;
                    if (attempts > maxRetries) {
                      observer.error(`Failed after ${maxRetries + 1} attempts: ${error || "Unknown error"}`);
                    } else {
                      console.error(`Attempt ${attempts} failed:`, error);
                      tryRequest(); // Retry if attempts are not exhausted
                    }
                  },
                });
            };
            tryRequest(); // Start the first attempt
          }
        },
        (error: unknown) => {
          observer.error(`Error checking AI Assistant status: ${error || "Unknown error"}`);
        }
      );
    });
  }
}
