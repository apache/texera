import { Injectable, NgZone } from "@angular/core";
import { BehaviorSubject, Observable } from "rxjs";
import { Workflow } from "../../../common/type/workflow";
import { WorkflowSuggestion } from "../../types/workflow-suggestion.interface";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { NzMessageService } from "ng-zorro-antd/message";
import { cloneDeep } from "lodash";
import { OperatorPredicate } from "../../types/workflow-common.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowSuggestionService } from "../workflow-suggestion/workflow-suggestion.service"; // For managing subscriptions in the service if needed

// @UntilDestroy() // Add if service has long-lived subscriptions to self-manage
@Injectable({
  providedIn: "root",
})
export class SuggestionActionService {
  private activePreviewIdSubject = new BehaviorSubject<string | null>(null);
  public readonly activePreviewId$: Observable<string | null> = this.activePreviewIdSubject.asObservable();

  private isInPreviewModeSubject = new BehaviorSubject<boolean>(false);
  public readonly isInPreviewMode$: Observable<boolean> = this.isInPreviewModeSubject.asObservable();

  private workflowBeforePreview: Workflow | null = null;
  private viewStateBeforeRestore: { zoom: number; tx: number; ty: number } | null = null;

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowUtilService: WorkflowUtilService,
    private workflowPersistService: WorkflowPersistService,
    private messageService: NzMessageService,
    private workflowSuggestionService: WorkflowSuggestionService,
    private ngZone: NgZone
  ) {}

  public getActivePreviewId(): string | null {
    return this.activePreviewIdSubject.getValue();
  }

  public isInPreviewMode(): boolean {
    return this.isInPreviewModeSubject.getValue();
  }

  private saveWorkflowState(): void {
    this.workflowBeforePreview = cloneDeep(this.workflowActionService.getWorkflow());
    const wrapper = this.workflowActionService.getJointGraphWrapper();
    try {
      const paper = wrapper.getMainJointPaper();
      const translate = paper.translate();
      this.viewStateBeforeRestore = { zoom: wrapper.getZoomRatio(), tx: translate.tx, ty: translate.ty };
    } catch {
      this.viewStateBeforeRestore = null;
    }
  }

  private restoreWorkflowState(): void {
    const snapshot = this.workflowBeforePreview;
    this.workflowBeforePreview = null;

    if (snapshot) {
      this.workflowActionService.reloadWorkflow(cloneDeep(snapshot)); // Use a deep copy to prevent modifications
    }

    if (this.viewStateBeforeRestore) {
      // It's better to run this after the graph has been reloaded and rendered.
      // Using a timeout ensures that the DOM and JointJS have settled.
      setTimeout(() => {
        this.ngZone.run(() => {
          // Ensure running within Angular's zone if it involves UI updates triggered by JointJS
          const wrapper = this.workflowActionService.getJointGraphWrapper();
          const paper = wrapper.getMainJointPaper(); // Get a fresh reference
          if (this.viewStateBeforeRestore) {
            // Check again as it might be cleared by another async operation
            wrapper.setZoomProperty(this.viewStateBeforeRestore.zoom);
            paper.translate(this.viewStateBeforeRestore.tx, this.viewStateBeforeRestore.ty);
            this.viewStateBeforeRestore = null;
          }
        });
      }, 0);
    }
  }

  private applyWorkflowSuggestionChanges(
    suggestion: WorkflowSuggestion,
    options: { preview: boolean }
  ): Map<string, string> {
    const texeraGraph = this.workflowActionService.getTexeraGraph();
    const jointGraph = this.workflowActionService.getJointGraph();
    const jointGraphWrapper = this.workflowActionService.getJointGraphWrapper();
    const operatorIDMap = new Map<string, string>(); // Maps original suggested ID to newly created ID
    const operatorsAndPositions: { op: OperatorPredicate; pos: { x: number; y: number } }[] = [];

    // Handle operators to add or modify properties
    suggestion.changes.operatorsToAdd.forEach((opDetails, index) => {
      const isExisting = texeraGraph.hasOperator(opDetails.operatorID);

      if (isExisting) {
        // Operator exists, so we're modifying its properties
        this.workflowActionService.setOperatorProperty(opDetails.operatorID, { ...opDetails.operatorProperties });
        if (options.preview) {
          const cell = jointGraph.getCell(opDetails.operatorID);
          if (cell) {
            cell.attr({ rect: { fill: "rgba(255, 255, 204, 0.6)", stroke: "#1890ff", "stroke-width": 2 } });
          }
        }
        operatorIDMap.set(opDetails.operatorID, opDetails.operatorID); // Map to itself as it exists
        return;
      }

      // Operator does not exist, create and add it
      const newOp = this.workflowUtilService.getNewOperatorPredicate(opDetails.operatorType);
      Object.assign(newOp.operatorProperties, opDetails.operatorProperties);
      // Important: Use the newOp.operatorID for mapping and further operations
      operatorIDMap.set(opDetails.operatorID, newOp.operatorID);

      let pos = { x: 100, y: 100 + index * 100 }; // Default position
      const anchorLink = suggestion.changes.linksToAdd.find(l => l.target.operatorID === opDetails.operatorID);
      if (anchorLink) {
        const sourceOpInGraphID = texeraGraph.hasOperator(anchorLink.source.operatorID)
          ? anchorLink.source.operatorID
          : operatorIDMap.get(anchorLink.source.operatorID);

        if (sourceOpInGraphID && texeraGraph.hasOperator(sourceOpInGraphID)) {
          const anchorPos = jointGraphWrapper.getElementPosition(sourceOpInGraphID);
          pos = { x: anchorPos.x + 200, y: anchorPos.y };
        }
      }
      operatorsAndPositions.push({ op: newOp, pos }); // Use newOp here
    });

    // Apply additions and deletions
    if (!options.preview) {
      // For permanent application, delete operators first
      if (suggestion.changes.operatorsToDelete.length > 0) {
        this.workflowActionService.deleteOperatorsAndLinks(suggestion.changes.operatorsToDelete); // Assuming linksToDelete is empty or handled separately
      }
      // Then add new/modified ones
      this.workflowActionService.addOperatorsAndLinks(operatorsAndPositions, []); // Assuming linksToAdd handled next
    } else {
      // For preview, just add with visual styling
      operatorsAndPositions.forEach(({ op, pos }) => {
        this.workflowActionService.addOperator(op, pos);
        const cell = jointGraph.getCell(op.operatorID); // Use the actual ID of the added operator
        if (cell) {
          cell.attr({ ".": { opacity: 0.6 }, rect: { stroke: "#1890ff", "stroke-width": 2 } });
        }
      });
    }

    // Handle links to add
    suggestion.changes.linksToAdd.forEach(linkDetails => {
      // IMPORTANT: Use the operatorIDMap to get the *actual* IDs in the graph
      const sourceIDInGraph = operatorIDMap.get(linkDetails.source.operatorID) ?? linkDetails.source.operatorID;
      const targetIDInGraph = operatorIDMap.get(linkDetails.target.operatorID) ?? linkDetails.target.operatorID;

      if (texeraGraph.hasOperator(sourceIDInGraph) && texeraGraph.hasOperator(targetIDInGraph)) {
        const linkObject = {
          linkID: options.preview
            ? `link-preview-${Date.now()}-${Math.random().toString(36).substring(2, 7)}`
            : `link-${Date.now()}-${Math.random().toString(36).substring(2, 7)}`, // Ensure unique ID
          source: { operatorID: sourceIDInGraph, portID: linkDetails.source.portID },
          target: { operatorID: targetIDInGraph, portID: linkDetails.target.portID },
        };
        this.workflowActionService.addLink(linkObject);

        if (options.preview) {
          const cell = jointGraph.getCell(linkObject.linkID);
          if (cell) {
            cell.attr({
              ".connection": { opacity: 0.6, stroke: "#1890ff" },
              ".marker-target": { opacity: 0.6, fill: "#1890ff" },
            });
          }
        }
      } else {
        console.warn(
          "Could not add link, source or target operator not found in graph or ID map:",
          linkDetails,
          "Mapped IDs:",
          sourceIDInGraph,
          targetIDInGraph
        );
      }
    });
    return operatorIDMap;
  }

  public togglePreview(suggestion: WorkflowSuggestion): void {
    const currentPreviewId = this.activePreviewIdSubject.getValue();

    if (currentPreviewId === suggestion.suggestionID) {
      this.clearPreviewAndRestoreWorkflow();
    } else {
      if (currentPreviewId) {
        this.clearPreviewAndRestoreWorkflow(); // Clear existing preview first
      }
      this.saveWorkflowState();
      this.activePreviewIdSubject.next(suggestion.suggestionID);
      this.isInPreviewModeSubject.next(true);
      this.workflowSuggestionService.setPreviewActive(true); // Notify other services

      this.ngZone.runOutsideAngular(() => {
        setTimeout(() => {
          // Defer to allow UI updates
          this.ngZone.run(() => {
            // Ensure graph operations run in Angular zone if they trigger changes
            this.applyWorkflowSuggestionChanges(suggestion, { preview: true });
            this.messageService.info("Suggestion preview active. Some functionalities might be limited.");
          });
        });
      });
    }
  }

  public clearPreviewAndRestoreWorkflow(): void {
    if (!this.isInPreviewModeSubject.getValue()) return;

    this.restoreWorkflowState(); // Restore graph first
    this.activePreviewIdSubject.next(null);
    this.isInPreviewModeSubject.next(false);
    this.workflowSuggestionService.setPreviewActive(false); // Notify other services
    this.messageService.info("Suggestion preview cancelled.");
  }

  public applySuggestion(suggestion: WorkflowSuggestion): void {
    if (this.isInPreviewModeSubject.getValue()) {
      this.clearPreviewAndRestoreWorkflow(); // Clear preview before applying
    }
    try {
      this.applyWorkflowSuggestionChanges(suggestion, { preview: false });
      const workflow = this.workflowActionService.getWorkflow();
      // No need for untilDestroyed(this) if this service is root-provided and lives for app duration
      this.workflowPersistService.persistWorkflow(workflow).subscribe(() => {
        this.messageService.success("Suggestion applied and workflow saved!");
        this.workflowSuggestionService.removeSuggestionById(suggestion.suggestionID);
      });
    } catch (error) {
      console.error("Error applying suggestion:", error);
      this.messageService.error("Failed to apply the suggestion.");
    }
  }
}
