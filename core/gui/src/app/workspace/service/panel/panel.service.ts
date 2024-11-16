import { Injectable } from "@angular/core";
import { BehaviorSubject } from "rxjs";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service"; // Import WorkflowActionService
import mapping from "../../../../assets/migration_tool/mapping";
import { OperatorLink } from "../../types/workflow-common.interface";

@Injectable({
  providedIn: "root",
})
export class PanelService {
  private jupyterNotebookPanelVisible = new BehaviorSubject<boolean>(false);
  public jupyterNotebookPanelVisible$ = this.jupyterNotebookPanelVisible.asObservable();

  private iframeRef: HTMLIFrameElement | null = null; // Store reference to iframe element
  private cellContent: string[] = []; // Store the content of the cells
  private highlightedCell: number | null = null; // Track the highlighted cell

  // Precomputed dictionary for cell to highlight mapping
  private cellToHighlightMapping: Record<string, { components: string[]; edges: string[] }> = {};

  constructor(private workflowActionService: WorkflowActionService) {
    this.workflowActionService.workflowChanged().subscribe(() => {
      console.log("Workflow graph updated, recomputing highlight mapping...");
      this.precomputeHighlightMapping();
    });
    window.addEventListener("message", this.handleNotebookMessage);
  }

  // Precompute the dictionary for O(1) highlighting
  private precomputeHighlightMapping(): void {
    const cellToOperator = mapping.cell_to_operator;
    const allLinks: OperatorLink[] = this.workflowActionService.getTexeraGraph().getAllLinks();

    if (allLinks.length === 0) {
      console.warn("No links found in the graph during precompute.");
      return;
    }

    for (const cellUUID in cellToOperator) {
      // @ts-ignore
      const components = cellToOperator[cellUUID] || [];
      const componentSet = new Set(components);
      const edges: string[] = [];

      allLinks.forEach(link => {
        const sourceOperatorID = link.source.operatorID;
        const targetOperatorID = link.target.operatorID;

        if (
          componentSet.has(sourceOperatorID) &&
          componentSet.has(targetOperatorID) &&
          sourceOperatorID !== targetOperatorID
        ) {
          edges.push(link.linkID);
        }
      });

      this.cellToHighlightMapping[cellUUID] = { components, edges };
    }

    console.log("Precomputed highlight mapping:", this.cellToHighlightMapping);
  }

  // Set the iframe reference (from the component's ViewChild)
  setIframeRef(iframe: HTMLIFrameElement) {
    this.iframeRef = iframe;
    this.iframeRef.onload = () => console.log("Iframe loaded successfully.");
  }

  // Trigger a cell click inside the iframe
  triggerCellClickInsideIframe(cellUUID: string) {
    if (this.iframeRef && this.iframeRef.contentWindow) {
      this.iframeRef.contentWindow.postMessage(
        { action: "triggerCellClick", cellUUID },
        "http://localhost:8888"
      );
    } else {
      console.error("Iframe reference is null or not loaded.");
    }
  }

  // Open the Jupyter Notebook panel
  openPanel(panelName: string): void {
    if (panelName === "JupyterNotebookPanel") {
      this.jupyterNotebookPanelVisible.next(true);
    }
  }

  // Close the Jupyter Notebook panel
  closeJupyterNotebookPanel(): void {
    this.jupyterNotebookPanelVisible.next(false);
  }

  // Handle messages from the Jupyter notebook iframe
  private handleNotebookMessage = (event: MessageEvent) => {
    const allowedOrigins = ["http://localhost:4200", "http://localhost:8888"];
    if (!allowedOrigins.includes(event.origin)) {
      console.log("Invalid origin:", event.origin);
      return;
    }

    const { action, cellIndex, cellContent, cellUUID } = event.data;

    if (action === "cellClicked") {
      this.highlightedCell = cellIndex;
      this.cellContent[cellIndex] = cellContent || `Cell ${cellIndex + 1}`;
      this.highlightFromCell(cellUUID);
    }
  };

  // Highlight operators and edges based on the clicked cell
  private highlightFromCell(cellUUID: string): void {
    const highlightData = this.cellToHighlightMapping[cellUUID] || { components: [], edges: [] };

    // Unhighlight all operators and links
    this.workflowActionService.unhighlightOperators(
      ...this.workflowActionService.getTexeraGraph().getAllOperators().map(op => op.operatorID)
    );
    this.workflowActionService.unhighlightLinks(
      ...this.workflowActionService.getTexeraGraph().getAllLinks().map(link => link.linkID)
    );

    // Highlight components and edges
    if (highlightData.components.length > 0) {
      this.workflowActionService.highlightOperators(true, ...highlightData.components);
    }
    if (highlightData.edges.length > 0) {
      this.workflowActionService.highlightLinks(true, ...highlightData.edges);
    }

    console.log(`Highlighted components: ${highlightData.components}, edges: ${highlightData.edges}`);
  }

  // Handle when a Texera component is clicked to trigger the corresponding notebook cell
  onWorkflowComponentClick(cellUUID: string): void {
    if (this.iframeRef && this.iframeRef.contentWindow) {
      // @ts-ignore
      const operatorArray = mapping["operator_to_cell"][cellUUID];
      if (operatorArray) {
        this.iframeRef.contentWindow.postMessage(
          { action: "triggerCellClick", operators: operatorArray },
          "http://localhost:8888"
        );
      } else {
        console.error(`No operators found for cellUUID: ${cellUUID}`);
      }
    } else {
      console.error("Iframe reference is null.");
    }
  }
}
