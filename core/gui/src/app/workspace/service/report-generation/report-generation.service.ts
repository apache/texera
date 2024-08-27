import { Injectable } from "@angular/core";
import html2canvas from "html2canvas";
import { Observable, Observer } from "rxjs";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";
import { NotificationService } from "src/app/common/service/notification/notification.service";

@Injectable({
  providedIn: "root",
})
export class ReportGenerationService {
  constructor(
    public workflowActionService: WorkflowActionService,
    private workflowResultService: WorkflowResultService,
    private notificationService: NotificationService
  ) {}

  /**
   * Captures a snapshot of the workflow editor and returns it as a base64-encoded PNG image URL.
   * @param {string} workflowName - The name of the workflow.
   * @returns {Observable<string>} An observable that emits the base64-encoded PNG image URL of the workflow snapshot.
   */
  public generateWorkflowSnapshot(workflowName: string): Observable<string> {
    return new Observable((observer: Observer<string>) => {
      const element = document.querySelector("#workflow-editor") as HTMLElement;
      if (element) {
        // Ensure all resources are loaded
        const promises: Promise<void>[] = [];
        const images = element.querySelectorAll("image");

        // Create promises for each image to ensure they are loaded
        images.forEach(img => {
          const imgSrc = img.getAttribute("xlink:href");
          if (imgSrc) {
            promises.push(
              new Promise((resolve, reject) => {
                const imgElement = new Image();
                imgElement.src = imgSrc;
                imgElement.onload = () => resolve();
                imgElement.onerror = () => reject();
              })
            );
          }
        });

        // Wait for all images to load
        Promise.all(promises)
          .then(() => {
            // Capture the snapshot using html2canvas
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
          .catch((error: any) => {
            observer.error(error);
          });
      } else {
        observer.error("Workflow editor element not found");
      }
    });
  }

  /**
   * Retrieves and processes results for all specified operators within the workflow.
   * This function iterates over each operator ID, fetches the corresponding result details via `retrieveOperatorInfoReport`,
   * and collects these results into an array. The function returns an observable that emits the processed results,
   * which can be used to generate a comprehensive HTML report or for further processing.
   *
   * @param {string[]} operatorIds - An array of operator IDs representing each operator in the workflow.
   *
   * @returns {Observable<{operatorId: string, html: string}[]>} - An observable that emits an array of objects,
   * each containing an `operatorId` and its corresponding HTML representation of the result.
   * This result array can be used to generate an HTML report or for other purposes.
   */
  public getAllOperatorResults(operatorIds: string[]): Observable<{ operatorId: string; html: string }[]> {
    return new Observable(observer => {
      const allResults: { operatorId: string; html: string }[] = [];
      const promises = operatorIds.map(operatorId => {
        return this.retrieveOperatorInfoReport(operatorId, allResults);
      });

      Promise.all(promises)
        .then(() => {
          observer.next(allResults);
          observer.complete();
        })
        .catch(error => {
          observer.error("Error in retrieving operator results: " + error.message);
        });
    });
  }

  /**
   * Retrieves and processes detailed results for a specified operator, generating a structured HTML representation.
   * This function covers multiple cases, including handling paginated results, snapshot data, and scenarios where
   * no results are found for the operator. The resulting HTML content is stored in the `allResults` array.
   *
   * @param {string} operatorId - The unique identifier of the operator for which results are being processed.
   * @param {any} operatorInfo - Metadata and configuration details of the operator, utilized to render a JSON editor within the HTML.
   * @param {Array<{operatorId: string, html: string}>} allResults - An array used to collect HTML content for each operator's processed result.
   *
   * @returns {Promise<void>} - A promise that resolves once the operator's results have been processed and the HTML has been appended to `allResults`.
   */
  public retrieveOperatorInfoReport(
    operatorId: string,
    allResults: { operatorId: string; html: string }[]
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        // Retrieve the result service and paginated result service for the operator
        const resultService = this.workflowResultService.getResultService(operatorId);
        const paginatedResultService = this.workflowResultService.getPaginatedResultService(operatorId);

        // Generate the HTML content for operator details, which will be included in the report
        const operatorDetailsHtml = `<div style="text-align: center;">
        <h4>Operator Details</h4>
        <div id="json-editor-${operatorId}" style="height: 400px;"></div>
        <script>
          document.addEventListener('DOMContentLoaded', function() {
            const container = document.querySelector("#json-editor-${operatorId}");
            const options = { mode: 'view', language: 'en' };
            const editor = new JSONEditor(container, options);
          });
        </script>
     </div>`;

        // Check if the paginated result service is available
        if (paginatedResultService) {
          paginatedResultService.selectPage(1, 10).subscribe({
            next: pageData => {
              try {
                // Handle the paginated results
                const table = pageData.table;
                let htmlContent = `<h3>Operator ID: ${operatorId}</h3>`;

                if (!table.length) {
                  // If no results are found, display a message
                  htmlContent += "<p>No results found for operator</p>";
                } else {
                  // Generate an HTML table to display the results
                  const columns: string[] = Object.keys(table[0]);
                  const rows: any[][] = table.map(row => columns.map(col => row[col]));

                  htmlContent += `<div style="width: 50%; margin: 0 auto; text-align: center;">
                   <table style="width: 100%; border-collapse: collapse; margin: 0 auto;">
                     <thead>
                       <tr>${columns.map(col => `<th style="border: 1px solid black; padding: 8px; text-align: center;">${col}</th>`).join("")}</tr>
                     </thead>
                     <tbody>
                       ${rows.map(row => `<tr>${row.map(cell => `<td style="border: 1px solid black; padding: 8px; text-align: center;">${String(cell)}</td>`).join("")}</tr>`).join("")}
                     </tbody>
                   </table>
                 </div>`;
                }

                // Add the generated HTML content to the allResults array
                allResults.push({ operatorId, html: htmlContent });
                resolve();
              } catch (error: unknown) {
                // Handle any errors during the result processing
                const errorMessage = (error as Error).message || "Unknown error";
                this.notificationService.error(`Error processing results for operator ${operatorId}: ${errorMessage}`);
                reject(error);
              }
            },
            error: (error: unknown) => {
              // Handle errors that occur during the retrieval of paginated results
              const errorMessage = (error as Error).message || "Unknown error";
              this.notificationService.error(
                `Error retrieving paginated results for operator ${operatorId}: ${errorMessage}`
              );
              reject(error);
            },
          });
        } else if (resultService) {
          try {
            // Retrieve the current snapshot of results
            const data = resultService.getCurrentResultSnapshot();
            let htmlContent = `<h3>Operator ID: ${operatorId}</h3>`;

            if (data) {
              // Parse the HTML content from the snapshot data
              const parser = new DOMParser();
              const lastData = data[data.length - 1];
              const doc = parser.parseFromString(Object(lastData)["html-content"], "text/html");

              // Ensure the document's height is set correctly
              doc.documentElement.style.height = "50%";
              doc.body.style.height = "50%";

              const firstDiv = doc.body.querySelector("div");
              if (firstDiv) firstDiv.style.height = "100%";

              const serializer = new XMLSerializer();
              const newHtmlString = serializer.serializeToString(doc);

              htmlContent += newHtmlString;
            } else {
              // If no data is found, display a message
              htmlContent += "<p>No data found for operator</p>";
            }

            // Add the generated HTML content to the allResults array
            allResults.push({ operatorId, html: htmlContent });
            resolve();
          } catch (error: unknown) {
            // Handle any errors during snapshot result processing
            const errorMessage = (error as Error).message || "Unknown error";
            this.notificationService.error(
              `Error processing snapshot results for operator ${operatorId}: ${errorMessage}`
            );
            reject(error);
          }
        } else {
          try {
            // If no result services are available, provide a default message
            allResults.push({
              operatorId,
              html: `<h3>Operator ID: ${operatorId}</h3>
             <p>No results found for operator</p>`,
            });
            resolve();
          } catch (error: unknown) {
            // Handle any errors when pushing the default result
            const errorMessage = (error as Error).message || "Unknown error";
            this.notificationService.error(`Error pushing default result for operator ${operatorId}: ${errorMessage}`);
            reject(error);
          }
        }
      } catch (error: unknown) {
        // Handle any unexpected errors that occur in the main logic
        const errorMessage = (error as Error).message || "Unknown error";
        this.notificationService.error(
          `Unexpected error in retrieveOperatorInfoReport for operator ${operatorId}: ${errorMessage}`
        );
        reject(error);
      }
    });
  }

  /**
   * Generates an HTML report containing the workflow snapshot and all operator results, and triggers a download of the report.
   *
   * @param {string} workflowSnapshot - The base64-encoded PNG image URL of the workflow snapshot.
   * @param {string[]} allResults - An array of HTML strings representing the results of each operator.
   * @param {string} workflowName - The name of the workflow, used for naming the final report.
   *
   * @returns {void}
   */
  public generateReportAsHtml(workflowSnapshot: string, allResults: string[], workflowName: string): void {
    const htmlContent = `
  <html>
    <head>
      <title>Operator Results</title>
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
        .hidden-input {
          display: none;
        }
        .json-editor-container {
          height: 400px;
        }
        .comment-box {
          margin-top: 20px;
          padding: 10px;
          border: 1px solid #ccc;
          border-radius: 5px;
        }
      </style>
      <script>
        // JavaScript functions for comment handling and JSON downloading...
      </script>
    </head>
    <body>
      <div style="text-align: center;">
        <h2>${workflowName} Static State</h2>
        <img src="${workflowSnapshot}" alt="Workflow Snapshot" style="width: 100%; max-width: 800px;">
      </div>
      ${allResults.join("")}
    </body>
  </html>
  `;

    const blob = new Blob([htmlContent], { type: "text/html" });
    const url = URL.createObjectURL(blob);
    const fileName = `${workflowName}-report.html`; // Use workflowName to generate the file name
    const a = document.createElement("a");
    a.href = url;
    a.download = fileName;
    a.click();
    URL.revokeObjectURL(url);
  }
}
