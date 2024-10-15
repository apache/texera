import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import html2canvas from "html2canvas";
import { forkJoin, Observable, Observer } from "rxjs";
import { map } from "rxjs/operators";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { AiAnalystService } from "../ai-analyst/ai-analyst.service";
import { AppSettings } from "src/app/common/app-setting";

const AI_ASSISTANT_API_BASE_URL = `${AppSettings.getApiEndpoint()}`;

@Injectable({
  providedIn: "root",
})
export class ReportGenerationService {
  private isAIAssistantEnabled: boolean | null = null;
  constructor(
    private http: HttpClient,
    public workflowActionService: WorkflowActionService,
    private workflowResultService: WorkflowResultService,
    private notificationService: NotificationService,
    private aiAnalystService: AiAnalystService
  ) {}

  /**
   * Captures a snapshot of the workflow editor and returns it as a base64-encoded PNG image URL.
   * @param {string} workflowName - The name of the workflow.
   * @returns {Observable<string>} An observable that emits the base64-encoded PNG image URL of the workflow snapshot.
   */
  public generateWorkflowSnapshot(workflowName: string): Observable<string> {
    return new Observable((observer: Observer<string>) => {
      const element = document.querySelector("#workflow-editor") as HTMLElement;

      if (!element) {
        observer.error("Workflow editor element not found");
        return;
      }

      // Query all the images (from SVG or other tags)
      const images = element.querySelectorAll("image");

      // Create promises to load and convert images to Base64
      const promises: Promise<void>[] = Array.from(images).map(img => {
        const imgSrc = img.getAttribute("xlink:href") || img.getAttribute("href");

        if (imgSrc) {
          return this.fetchImageAsBase64(imgSrc)
            .then(base64 => {
              // Set the Base64 image as the source of the SVG or img element
              img.setAttribute("href", base64);
            })
            .catch(error => {
              console.error(`Failed to load image: ${imgSrc}`, error);
            });
        }

        return Promise.resolve(); // If there's no src, resolve immediately
      });

      // Wait for all images to be converted to Base64
      Promise.all(promises)
        .then(() => {
          // Render the element after all images are ready
          return html2canvas(element, {
            logging: true,
            useCORS: true,
            allowTaint: true,
            foreignObjectRendering: true,
          });
        })
        .then((canvas: HTMLCanvasElement) => {
          const dataUrl: string = canvas.toDataURL("image/png");
          observer.next(dataUrl);
          observer.complete();
        })
        .catch(error => {
          observer.error(error);
        });
    });
  }

  private fetchImageAsBase64(imageUrl: string): Promise<string> {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.onload = function () {
        const reader = new FileReader();
        reader.onloadend = function () {
          resolve(reader.result as string); // Base64 string result
        };
        reader.onerror = function () {
          reject("Failed to convert image to Base64");
        };
        reader.readAsDataURL(xhr.response); // Convert to Base64
      };
      xhr.onerror = function () {
        reject(`Failed to load image from ${imageUrl}`);
      };
      xhr.open("GET", imageUrl);
      xhr.responseType = "blob"; // Get the image as binary data
      xhr.send();
    });
  }

  /**
   * Retrieves and processes results for all specified operators within the workflow.
   * * This function iterates over each operator ID, fetches the corresponding result details via `retrieveOperatorInfoReport`,
   * and collects these results into an array. The function returns an observable that emits the processed results,
   * which can be used to generate a comprehensive HTML report or for further processing.
   *
   * @param {string[]} operatorIds - An array of operator IDs representing each operator in the workflow.
   * @returns {Observable<{operatorId: string, html: string}[]>} - An observable that emits an array of objects,
   * each containing an `operatorId` and its corresponding HTML representation of the result.
   * This result array can be used to generate an HTML report or for other purposes.
   */
  public getAllOperatorResults(operatorIds: string[]): Observable<{ operatorId: string; html: string }[]> {
    const observables = operatorIds.map(operatorId => {
      const allResults: { operatorId: string; html: string }[] = [];
      return this.retrieveOperatorInfoReport(operatorId, allResults).pipe(map(() => allResults[0]));
    });

    return forkJoin(observables);
  }

  /**
   * Retrieves and processes results for all specified operators within the workflow.
   * This function iterates over each operator ID, fetches the corresponding result details via `retrieveOperatorInfoReport`,
   * and collects these results into an array. The function returns an observable that emits the processed results,
   * which can be used to generate a comprehensive HTML report or for further processing.
   *
   * @param operatorId
   * @param allResults
   * @returns {Promise<void>}
   */
  public retrieveOperatorInfoReport(
    operatorId: string,
    allResults: { operatorId: string; html: string }[]
  ): Observable<void> {
    return new Observable(observer => {
      this.aiAnalystService.isOpenAIEnabled().subscribe(AIEnabled => {
        try {
          // Retrieve the result service and paginated result service for the operator
          const resultService = this.workflowResultService.getResultService(operatorId);
          const paginatedResultService = this.workflowResultService.getPaginatedResultService(operatorId);

          const workflowContent = this.workflowActionService.getWorkflowContent();
          const operatorDetails = workflowContent.operators.find(op => op.operatorID === operatorId);

          const operatorDetailsHtml = `
              <div style="text-align: center;">
                  <h4>Operator Details</h4>
                  <div id="json-editor-${operatorId}" style="height: 400px;"></div>
                  <script>
                      document.addEventListener('DOMContentLoaded', function() {
                          const container = document.querySelector("#json-editor-${operatorId}");
                          const options = { mode: 'view', language: 'en' };
                          const editor = new JSONEditor(container, options);
                          editor.set(${JSON.stringify(operatorDetails)});
                      });
                  </script>
              </div>`;

          this.generateComment(operatorDetails).subscribe(comment => {
            if (paginatedResultService) {
              paginatedResultService.selectPage(1, 10).subscribe({
                next: (pageData: any) => {
                  const table: any[] = pageData.table;
                  if (!table.length) {
                    allResults.push({
                      operatorId,
                      html: `
                                      <h3>Operator ID: ${operatorId}</h3>
                                      <p>No results found for operator</p>
                                      <button onclick="toggleDetails('details-${operatorId}')">Toggle Details</button>
                                      <div id="details-${operatorId}" style="display: none;">${operatorDetailsHtml}</div>
                                      <div contenteditable="true" id="comment-${operatorId}" style="width: 100%; margin-top: 10px; border: 1px solid black; padding: 10px;">${comment}</div>`,
                    });
                    observer.next();
                    observer.complete();
                    return;
                  }

                  const columns: string[] = Object.keys(table[0]);
                  const rows: any[][] = table.map(row => columns.map(col => row[col]));

                  const htmlContent: string = `
                              <div style="width: 50%; margin: 0 auto; text-align: center;">
                                  <h3>Operator ID: ${operatorId}</h3>
                                  <table style="width: 100%; border-collapse: collapse; margin: 0 auto;">
                                      <thead>
                                          <tr>${columns
                                            .map(
                                              col =>
                                                `<th style="border: 1px solid black; padding: 8px; text-align: center;">${col}</th>`
                                            )
                                            .join("")}</tr>
                                      </thead>
                                      <tbody>
                                          ${rows
                                            .map(
                                              row =>
                                                `<tr>${row
                                                  .map(
                                                    cell =>
                                                      `<td style="border: 1px solid black; padding: 8px; text-align: center;">${String(
                                                        cell
                                                      )}</td>`
                                                  )
                                                  .join("")}</tr>`
                                            )
                                            .join("")}
                                      </tbody>
                                  </table>
                                  <button onclick="toggleDetails('details-${operatorId}')">Toggle Details</button>
                                  <div id="details-${operatorId}" style="display: none;">${operatorDetailsHtml}</div>
                                  <div contenteditable="true" id="comment-${operatorId}" style="width: 100%; margin-top: 10px; border: 1px solid black; padding: 10px;">${comment}</div>
                              </div>`;

                  allResults.push({ operatorId, html: htmlContent });
                  observer.next();
                  observer.complete();
                },
                error: (error: unknown) => {
                  const errorMessage = (error as Error).message || "Unknown error";
                  this.notificationService.error(
                    `Error processing results for operator ${operatorId}: ${errorMessage}`
                  );
                  observer.error(error);
                },
              });
            } else if (resultService) {
              const data = resultService.getCurrentResultSnapshot();
              if (data) {
                const parser = new DOMParser();
                const lastData = data[data.length - 1];
                const doc = parser.parseFromString(Object(lastData)["html-content"], "text/html");

                doc.documentElement.style.height = "50%";
                doc.body.style.height = "50%";

                const firstDiv = doc.body.querySelector("div");
                if (firstDiv) firstDiv.style.height = "100%";

                const serializer = new XMLSerializer();
                const newHtmlString = serializer.serializeToString(doc);

                const visualizationHtml = `
                          <h3 style="text-align: center;">Operator ID: ${operatorId}</h3>
                          ${newHtmlString}
                          <button onclick="toggleDetails('details-${operatorId}')">Toggle Details</button>
                          <div id="details-${operatorId}" style="display: none;">${operatorDetailsHtml}</div>
                          <div contenteditable="true" id="comment-${operatorId}" style="width: 100%; margin-top: 10px; border: 1px solid black; padding: 10px;">${comment}</div>`;

                allResults.push({ operatorId, html: visualizationHtml });
                observer.next();
                observer.complete();
              } else {
                allResults.push({
                  operatorId,
                  html: `
                          <h3>Operator ID: ${operatorId}</h3>
                          <p>No data found for operator</p>
                          <button onclick="toggleDetails('details-${operatorId}')">Toggle Details</button>
                          <div id="details-${operatorId}" style="display: none;">${operatorDetailsHtml}</div>
                          <div contenteditable="true" id="comment-${operatorId}" style="width: 100%; margin-top: 10px; border: 1px solid black; padding: 10px;">${comment}</div>`,
                });
                observer.next();
                observer.complete();
              }
            } else {
              allResults.push({
                operatorId,
                html: `
                        <h3>Operator ID: ${operatorId}</h3>
                        <p>No results found for operator</p>
                        <button onclick="toggleDetails('details-${operatorId}')">Toggle Details</button>
                        <div id="details-${operatorId}" style="display: none;">${operatorDetailsHtml}</div>
                        <div contenteditable="true" id="comment-${operatorId}" style="width: 100%; margin-top: 10px; border: 1px solid black; padding: 10px;">${comment}</div>`,
              });
              observer.next();
              observer.complete();
            }
          });
        } catch (error: unknown) {
          const errorMessage = (error as Error).message || "Unknown error";
          this.notificationService.error(
            `Unexpected error in retrieveOperatorInfoReport for operator ${operatorId}: ${errorMessage}`
          );
          observer.error(error);
        }
      });
    });
  }

  /**
   * Generates an HTML report containing the workflow snapshot and all operator results, and triggers a download of the report.
   *
   * @param {string} workflowSnapshot - The base64-encoded PNG image URL of the workflow snapshot.
   * @param {string[]} allResults - An array of HTML strings representing the results of each operator.
   * @param {string} workflowName - The name of the workflow, used for naming the final report.
   * @returns {void}
   */
  public generateReportAsHtml(workflowSnapshot: string, allResults: string[], workflowName: string): void {
    const workflowContent = this.workflowActionService.getWorkflowContent();

    // Call generateSummaryComment and subscribe to its result
    this.generateSummaryComment(workflowContent).subscribe(comment => {
      const finalComment = comment; // Get the generated comment

      const htmlContent = `
    <html>
      <head>
        <title>Operator Results</title>
        <!-- Link to JSONEditor CSS file -->
        <link href="https://cdn.jsdelivr.net/npm/jsoneditor@10.1.0/dist/jsoneditor.min.css" rel="stylesheet" type="text/css" />
        <script src="https://cdn.jsdelivr.net/npm/jsoneditor@10.1.0/dist/jsoneditor.min.js"></script>
        <style>
          .button {
            margin-top: 20px;
            padding: 10px 20px;
            border: 1px solid #ccc;
            background-color: #f8f8f8;
            color: #333;
            border-radius: 5px;
            cursor: pointer;
            font-size: 14px;
          }
          .button:hover {
            background-color: #e8e8e8;
          }
          .json-editor-container {
            height: 400px;
          }
          .comment-box {
            margin-top: 20px;
            padding: 10px;
            border: 1px solid #ccc;
            border-radius: 5px;
            width: 80%;
            margin: 0 auto;
          }
          .editable-comment-box {
            width: 100%;
            margin-top: 10px;
            border: 1px solid black;
            padding: 10px;
            text-align: left;
          }
          .operator-result {
            margin: 20px auto;
            width: 80%;
            text-align: left;
          }
          table {
            width: 100%;
            border-collapse: collapse;
            margin: 0 auto;
          }
          th, td {
            border: none;
            padding: 8px;
            text-align: left;
          }
        </style>
        <script>
          function toggleDetails(id) {
            const detailsElement = document.getElementById(id);
            if (detailsElement.style.display === "none") {
              detailsElement.style.display = "block";
            } else {
              detailsElement.style.display = "none";
            }
          }

          function downloadJson() {
            const workflowContent = ${JSON.stringify(this.workflowActionService.getWorkflowContent())};
            const jsonBlob = new Blob([JSON.stringify(workflowContent, null, 2)], {type: "application/json"});
            const url = URL.createObjectURL(jsonBlob);
            const a = document.createElement("a");
            a.href = url;
            a.download = "${workflowName}-workflow.json";
            a.click();
            URL.revokeObjectURL(url);
          }
        </script>
      </head>
      <body>
        <div style="text-align: center;">
          <h2>${workflowName} Static State</h2>
          <img src="${workflowSnapshot}" alt="Workflow Snapshot" style="display: block; margin: 0 auto; width: 80%;">
        </div>
        ${allResults.map(result => `<div class="operator-result">${result}</div>`).join("")}
        <div style="text-align: center; margin-top: 20px;">
          <div class="comment-box">
            <h3>Summary</h3>
            <div contenteditable="true" id="comment-summary" class="editable-comment-box">${finalComment}</div>
          </div>
          <button class="button" onclick="downloadJson()">Download Workflow JSON</button>
        </div>
      </body>
    </html>
    `;

      const blob = new Blob([htmlContent], { type: "text/html" });
      const url = URL.createObjectURL(blob);
      const fileName = `${workflowName}-report.html`;
      const a = document.createElement("a");
      a.href = url;
      a.download = fileName;
      a.click();
      URL.revokeObjectURL(url);
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
    return this.aiAnalystService.sendPromptToOpenAI(prompt);
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
    return this.aiAnalystService.sendPromptToOpenAI(prompt);
  }
}
