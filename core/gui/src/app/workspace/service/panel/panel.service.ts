import { Injectable } from "@angular/core";
import { BehaviorSubject } from "rxjs";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service"; // Import WorkflowActionService
import mapping from "../../../../assets/migration_tool/mapping";
import { LogicalPort, OperatorLink } from "../../types/workflow-common.interface";

@Injectable({
  providedIn: "root",
})
export class PanelService {
  // Subject to track the visibility of the Jupyter Notebook panel
  private jupyterNotebookPanelVisible = new BehaviorSubject<boolean>(false);
  public jupyterNotebookPanelVisible$ = this.jupyterNotebookPanelVisible.asObservable();

  private iframeRef: HTMLIFrameElement | null = null; // Store reference to iframe element
  private cellContent: string[] = []; // Store the content of the cells
  private highlightedCell: number | null = null; // Track the highlighted cell

  constructor(private workflowActionService: WorkflowActionService) {
    // Listen for messages from the Jupyter notebook iframe
    window.addEventListener("message", this.handleNotebookMessage);
  }

  // Set the iframe reference (from the component's ViewChild)
  setIframeRef(iframe: HTMLIFrameElement) {
    this.iframeRef = iframe;

    // Ensure iframe is loaded before using it
    this.iframeRef.onload = () => {
      console.log("Iframe loaded successfully.");
    };
  }

  triggerCellClickInsideIframe(cellUUID: string) {
    if (this.iframeRef && this.iframeRef.contentWindow) {
      console.log(`Highlight Jupyter cell UUID: ${cellUUID}`);
      this.iframeRef.contentWindow.postMessage(
        { action: "triggerCellClick", cellUUID },
        "http://localhost:8888"
      );
    } else {
      console.error("iframeRef or contentWindow is null. Iframe may not have loaded yet.");
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
      console.log(`Highlighted cell: ${this.highlightedCell}, Content: ${cellContent}`);

      // Find corresponding Texera components using mapping
      // @ts-ignore
      const components = mapping.cell_to_operator[cellUUID] || []; // list of component uuid

      // highlight edges, use DFS algo
      const allLinks: OperatorLink[] = this.workflowActionService.getTexeraGraph().getAllLinks();
      // only highlight edges that lead from one operator to another operator from components
      const linksToBeHighlighted: string[] = allLinks
        .filter(link => {
          // Check if the source and target components are in the list of components
          const sourceComponentUUID = link.source.operatorID;
          const targetComponentUUID = link.target.operatorID;

          // Check if the link goes from one component in the list to another
          return components.includes(sourceComponentUUID) && components.includes(targetComponentUUID);
        })
        .map(link => link.linkID);

      // Unhighlight all current operators
      this.workflowActionService.unhighlightOperators(
        ...this.workflowActionService.getTexeraGraph().getAllOperators().map(op => op.operatorID)
      );
      this.workflowActionService.unhighlightLinks(...this.workflowActionService.getTexeraGraph().getAllLinks().map(op => op.linkID));

      // Highlight corresponding components
      if (components.length > 0) {
        this.workflowActionService.highlightOperators(true, ...components);
        this.workflowActionService.highlightLinks(true, ...linksToBeHighlighted);
      }
    }
  };

  // Handle when a Texera component is clicked to trigger the corresponding notebook cell
  onWorkflowComponentClick(cellUUID: string): void {
    console.log(`Sending message to notebook: triggerCellClick, UUID: ${cellUUID}`);

    // Check if the iframe reference and contentWindow are available
    if (this.iframeRef && this.iframeRef.contentWindow) {
      // Check if the operator mapping exists for the given cellUUID
      // @ts-ignore
      const operatorArray = mapping["operator_to_cell"][cellUUID];

      if (operatorArray) {
        // Send the message with the correct operators array
        this.iframeRef.contentWindow.postMessage(
          { action: "triggerCellClick", operators: operatorArray },
          "http://localhost:8888"
        );
        console.log(`Message sent for cellUUID: ${cellUUID} with operators: ${operatorArray}`);
      } else {
        console.error(`No operators found for cellUUID: ${cellUUID}`);
      }
    } else {
      console.error("iframeRef or contentWindow is null.");
    }
  }


}
