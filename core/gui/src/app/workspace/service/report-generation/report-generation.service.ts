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
   * Generates an HTML file containing the workflow snapshot and triggers a download of the file.
   * @param {string} workflowSnapshotURL - The base64-encoded PNG image URL of the workflow snapshot.
   * @param {string} workflowName - The name of the workflow.
   */
  public generateReportAsHtml(workflowSnapshotURL: string, workflowName: string): void {
    const htmlContent = `
  <html>
    <head>
      <title>${workflowName} Snapshot</title>
    </head>
    <body>
      <div style="text-align: center;">
        <h2>${workflowName} Static State</h2>
        <img src="${workflowSnapshotURL}" alt="Workflow Snapshot" style="width: 100%; max-width: 800px;">
      </div>
    </body>
  </html>
  `;
    const blob = new Blob([htmlContent], { type: "text/html" });
    const url = URL.createObjectURL(blob);
    const fileName = workflowName + "-snapshot.html";
    const a = document.createElement("a");
    a.href = url;
    a.download = fileName;
    a.click();
    URL.revokeObjectURL(url);
  }
}
