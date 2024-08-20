import { Injectable } from "@angular/core";
import html2canvas from "html2canvas";
import { Observable, Observer } from "rxjs";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";

@Injectable({
  providedIn: "root",
})
export class ReportGenerationService {
  constructor(
    public workflowActionService: WorkflowActionService,
    private workflowResultService: WorkflowResultService
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
   * Collects all operator results from the workflow and generates a comprehensive HTML report.
   * @param {Object} payload - The payload containing operator IDs, operator details, workflow snapshot, and workflow name.
   * @param {string[]} payload.operatorIds - Array of operator IDs present in the workflow.
   * @param {any[]} payload.operators - Array of operator details, including configurations and metadata.
   * @param {string} payload.workflowSnapshot - Base64-encoded PNG image URL of the workflow snapshot.
   * @param {string} payload.workflowName - The name of the workflow, used for naming the final report.
   * @returns {void}
   */

  public getAllOperatorResults(payload: {
    operatorIds: string[];
    operators: any[];
    workflowSnapshot: string;
    workflowName: string;
  }): void {
    console.log("Starting getAllOperatorResults with payload: ", payload);
    console.log("Type of payload.operatorIds: ", typeof payload.operatorIds);
    console.log("Is Array: ", Array.isArray(payload.operatorIds));

    const allResults: { operatorId: string; html: string }[] = [];

    const promises = payload.operatorIds.map(operatorId => {
      const operatorInfo = payload.operators.find((op: any) => op.operatorID === operatorId);
      return this.displayResult(operatorId, operatorInfo, allResults);
    });

    Promise.all(promises)
      .then(() => {
        console.log("All results before sorting: ", allResults);
        console.log("Payload: ", payload);

        const sortedResults = payload.operatorIds.map(
          id => allResults.find(result => result.operatorId === id)?.html || ""
        );

        console.log("All results after sorting: ", sortedResults);

        // Pass the workflowName to generateReportAsHtml
        this.generateReportAsHtml(payload.workflowSnapshot, sortedResults, payload.workflowName);
        console.log("Completed getAllOperatorResults");
      })
      .catch(error => {
        console.error("Error in getAllOperatorResults: ", error);
      });
  }

  /**
   * Retrieves and processes the result for a given operator, generating an HTML representation.
   * This function handles different scenarios including paginated results, snapshot results,
   * and cases where no results are available. The generated HTML is pushed to the `allResults` array.
   *
   * @param {string} operatorId - The unique identifier of the operator whose results are to be displayed.
   * @param {any} operatorInfo - Detailed information of the operator, used to create a JSON view in the HTML.
   * @param {Array<{operatorId: string, html: string}>} allResults - An array that will store the HTML content for each operator's result.
   *
   * @returns {Promise<void>} - A promise that resolves once the operator's result has been successfully processed and added to `allResults`.
   */
  public displayResult(
    operatorId: string,
    operatorInfo: any,
    allResults: { operatorId: string; html: string }[]
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        const resultService = this.workflowResultService.getResultService(operatorId);
        const paginatedResultService = this.workflowResultService.getPaginatedResultService(operatorId);

        const operatorDetailsHtml = `<div style="text-align: center;">
        <h4>Operator Details</h4>
        <div id="json-editor-${operatorId}" style="height: 400px;"></div>
        <script>
          document.addEventListener('DOMContentLoaded', function() {
            const container = document.querySelector("#json-editor-${operatorId}");
            const options = { mode: 'view', language: 'en' };
            const editor = new JSONEditor(container, options);
            editor.set(${JSON.stringify(operatorInfo)});
          });
        </script>
     </div>`;

        if (paginatedResultService) {
          paginatedResultService.selectPage(1, 10).subscribe({
            next: pageData => {
              try {
                const table = pageData.table;
                let htmlContent = `<h3>Operator ID: ${operatorId}</h3>`;

                if (!table.length) {
                  htmlContent += "<p>No results found for operator</p>";
                } else {
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

                allResults.push({ operatorId, html: htmlContent });
                resolve();
              } catch (error) {
                console.error(`Error processing results for operator ${operatorId}:`, error);
                reject(error);
              }
            },
            error: (error: unknown) => {
              console.error(`Error retrieving paginated results for operator ${operatorId}:`, error);
              reject(error);
            },
          });
        } else if (resultService) {
          try {
            const data = resultService.getCurrentResultSnapshot();
            let htmlContent = `<h3>Operator ID: ${operatorId}</h3>`;

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

              htmlContent += newHtmlString;
            } else {
              htmlContent += "<p>No data found for operator</p>";
            }

            allResults.push({ operatorId, html: htmlContent });
            resolve();
          } catch (error) {
            console.error(`Error processing snapshot results for operator ${operatorId}:`, error);
            reject(error);
          }
        } else {
          try {
            allResults.push({
              operatorId,
              html: `<h3>Operator ID: ${operatorId}</h3>
             <p>No results found for operator</p>`,
            });
            resolve();
          } catch (error) {
            console.error(`Error pushing default result for operator ${operatorId}:`, error);
            reject(error);
          }
        }
      } catch (error) {
        console.error(`Unexpected error in displayResult for operator ${operatorId}:`, error);
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
