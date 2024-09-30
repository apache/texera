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
export class AiAssistantService {
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
   * Generates an insightful comment for the given operator information by utilizing the AI Assistant service.
   * The comment is tailored for an educated audience without a deep understanding of statistics.
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

  /**
   * Generates an insightful comment for the given operator information by utilizing the AI Assistant service.
   * The comment is tailored for an educated audience without a deep understanding of statistics.
   *
   * @param {any} operatorInfo - The operator information in JSON format, which will be used to generate the comment.
   * @returns {Promise<string>} A promise that resolves to a string containing the generated comment or an error message
   *                            if the generation fails or the AI Assistant is not enabled.
   */
  public generateComment(operatorInfo: any): Observable<string> {
    const prompt = `
      You are a statistical analysis expert.
      You will be provided with operator information in JSON format and an HTML result.
      Your task is to analyze the data and provide a detailed, which means at least 80 words, insightful comment tailored for an audience that is highly educated but does not understand statistics.
      Operator Info: ${JSON.stringify(operatorInfo, null, 2)}
      The output cannot be in markdown format, and must be plain text.

      Follow these steps to generate your response:

      Parse the provided JSON data under “Operator Info.”
      Use the appropriate template based on the operator type to create a comment:
      For general operators, use: “This operator processes data to achieve specific goals.”
      For “CSVFileScan” type operators, use: “Briefly introduce the data composition, such as included data types.”
      For “PythonUDFV2” type operators, use: “Refer to the ‘code’ section in the operator detail to understand its purpose. Ensure correct HTML rendering of results.”
      For “Visualization” type operators, use: “This type of operator is usually associated with a chart or plot. You need to remind them that the graph is interactive and to care about the size and variation of the data.”

      Again, the output comment should follow the format specified above and should be insightful for non-experts.`;

    // Call the openai function and pass the generated prompt
    return this.openai(prompt);
  }

  /**
   * Generates a concise and insightful summary comment for the given operator information by utilizing the AI Assistant service.
   * The summary is tailored for an educated audience without a deep understanding of statistics, focusing on the key findings,
   * notable patterns, and potential areas of improvement related to the workflow and its components, particularly UDFs.
   *
   * @param {any} operatorInfo - The operator information in JSON format, which will be used to generate the summary comment.
   * @returns {Promise<string>} A promise that resolves to a string containing the generated summary comment or an error message
   *                            if the generation fails or the AI Assistant is not enabled.
   */
  public generateSummaryComment(operatorInfo: any): Observable<string> {
    const prompt = `
      You are a statistical analysis expert.
      You will be provided with operator information in JSON format and an HTML result.
      You should provide a concise (which means at least 150 words) and insightful summary comment for an audience who is highly educated but does not understand statistics (non-experts).
      Operator Info: ${JSON.stringify(operatorInfo, null, 2)}
      The output cannot be in markdown format, and must be plain text.

      Follow these steps to generate your response:
      The summary should:
      1. Highlight the key findings and overall performance of the workflow, with particular attention to the UDFs as they are often the most critical components.
      2. Mention any notable patterns, trends, or anomalies observed in the operator results, especially those related to UDFs.
      3. Suggest potential areas of improvement or further investigation, particularly regarding the efficiency and accuracy of the UDFs.
      4. Ensure the summary helps the reader gain a comprehensive understanding of the workflow and its global implications.

      Again, the output comment should follow the format specified above and should be insightful for non-experts.`;

    // Call the openai function and pass the generated prompt
    return this.openai(prompt);
  }
}
