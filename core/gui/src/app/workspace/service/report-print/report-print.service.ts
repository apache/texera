import { Injectable } from "@angular/core";
import html2canvas from "html2canvas";
import * as joint from "jointjs";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";

@Injectable({
  providedIn: "root",
})
export class ReportPrintService {
  private paper!: joint.dia.Paper;
  constructor(
    public workflowActionService: WorkflowActionService,
    private workflowResultService: WorkflowResultService,
  ) {}

  /**
   * Captures the current workflow snapshot and returns it as a base64-encoded PNG image URL.
   * @returns {Promise<string>} A promise that resolves with the base64-encoded PNG image URL of the workflow snapshot.
   */
  public async getWorkflowSnapshot(): Promise<string> {
    return new Promise((resolve, reject) => {
      const element = document.querySelector("#workflow-editor") as HTMLElement;
      if (element) {
        const snapshotContainer = document.createElement("div");
        snapshotContainer.style.width = "75%";
        snapshotContainer.style.height = "75%";
        snapshotContainer.style.position = "fixed";
        snapshotContainer.style.top = "0";
        snapshotContainer.style.left = "0";
        snapshotContainer.style.zIndex = "-1";
        snapshotContainer.style.background = "#FFFFFF";
        document.body.appendChild(snapshotContainer);

        const jointGraph = this.workflowActionService.getJointGraphWrapper().jointGraph;
        this.paper = new joint.dia.Paper({
          el: snapshotContainer,
          model: jointGraph,
          width: snapshotContainer.clientWidth,
          height: snapshotContainer.clientHeight,
          interactive: false,
          background: { color: "#FFFFFF" },
        });

        this.paper.scale(0.75, 0.75);

        setTimeout(() => {
          html2canvas(snapshotContainer, {
            logging: true,
            useCORS: true,
            allowTaint: true,
            foreignObjectRendering: true,
          })
            .then((canvas: HTMLCanvasElement) => {
              const dataUrl: string = canvas.toDataURL("image/png");
              resolve(dataUrl);
              document.body.removeChild(snapshotContainer);
            })
            .catch((error) => {
              reject(error);
              document.body.removeChild(snapshotContainer);
            });
        }, 1000);
      } else {
        reject("Workflow canvas element not found");
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
          <img src="${workflowSnapshot}" alt="Workflow Snapshot" style="width: 100%; max-width: 800px;">
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
