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
  private paper!: joint.dia.Paper;

  // Define constants for styles
  private readonly SNAPSHOT_CONTAINER_WIDTH = "75%";
  private readonly SNAPSHOT_CONTAINER_HEIGHT = "75%";
  private readonly SNAPSHOT_CONTAINER_POSITION = "fixed";
  private readonly SNAPSHOT_CONTAINER_TOP = "0";
  private readonly SNAPSHOT_CONTAINER_LEFT = "0";
  private readonly SNAPSHOT_CONTAINER_ZINDEX = "-1";
  private readonly SNAPSHOT_CONTAINER_BACKGROUND = "#FFFFFF";



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
        const snapshotContainer = document.createElement("div");
        snapshotContainer.style.width = this.SNAPSHOT_CONTAINER_WIDTH;
        snapshotContainer.style.height = this.SNAPSHOT_CONTAINER_HEIGHT;
        snapshotContainer.style.position = this.SNAPSHOT_CONTAINER_POSITION;
        snapshotContainer.style.top = this.SNAPSHOT_CONTAINER_TOP;
        snapshotContainer.style.left = this.SNAPSHOT_CONTAINER_LEFT;
        snapshotContainer.style.zIndex = this.SNAPSHOT_CONTAINER_ZINDEX;
        snapshotContainer.style.background = this.SNAPSHOT_CONTAINER_BACKGROUND;
        document.body.appendChild(snapshotContainer);

        const jointGraph = this.workflowActionService.getJointGraphWrapper().jointGraph;
        const paper = new joint.dia.Paper({
          el: snapshotContainer,
          model: jointGraph,
          width: snapshotContainer.clientWidth,
          height: snapshotContainer.clientHeight,
          interactive: false,
          background: { color: "#FFFFFF" },
        });

        paper.scale(0.75, 0.75);

        setTimeout(() => {
          html2canvas(snapshotContainer, {
            logging: true,
            useCORS: true,
            allowTaint: true,
            foreignObjectRendering: true,
          })
            .then((canvas) => {
              const dataUrl = canvas.toDataURL("image/png");
              observer.next(dataUrl);
              observer.complete();
              document.body.removeChild(snapshotContainer);
            })
            .catch((error) => {
              observer.error(error);
              document.body.removeChild(snapshotContainer);
            });
        }, 1000);
      } else {
        observer.error("Workflow canvas element not found");
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
