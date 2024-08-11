import { Injectable } from "@angular/core";
import html2canvas from "html2canvas";
import * as joint from "jointjs";
import { Observable, Observer } from "rxjs";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";

@Injectable({
  providedIn: "root",
})
export class ReportPrintService {
  constructor(
    public workflowActionService: WorkflowActionService,
    private workflowResultService: WorkflowResultService
  ) {}

  /**
   * Captures the current workflow snapshot and returns it as a base64-encoded PNG image URL.
   * @returns {Observable<string>} An Observable that emits the base64-encoded PNG image URL of the workflow snapshot.
   */
  public getWorkflowSnapshot(): Observable<string> {
    return new Observable((observer: Observer<string>) => {
      const element = document.querySelector("#workflow-editor") as HTMLElement;
      if (element) {
        const promises: Promise<void>[] = [];
        const images = element.querySelectorAll("image");

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

        Promise.all(promises)
          .then(() => {
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
   * @param {string} workflowSnapshot - The base64-encoded PNG image URL of the workflow snapshot.
   */
  public downloadResultsAsHtml(workflowSnapshot: string): void {
    const htmlContent = `
    <html>
      <head>
        <title>Workflow Snapshot</title>
      </head>
      <body>
        <div style="text-align: center;">
          <h2>Workflow Static State</h2>
          <img src="${workflowSnapshot}" alt="Workflow Snapshot" style="width: 100%; max-width: 1000px;">
        </div>
      </body>
    </html>
    `;
    const blob = new Blob([htmlContent], { type: "text/html" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "workflow-snapshot.html";
    a.click();
    URL.revokeObjectURL(url);
  }
}
