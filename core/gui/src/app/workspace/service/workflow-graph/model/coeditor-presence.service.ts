import { Injectable } from "@angular/core";
import { WorkflowGraph } from "./workflow-graph";
import * as joint from "jointjs";
import { JointGraphWrapper } from "./joint-graph-wrapper";
import { Coeditor, CoeditorState } from "../../../../common/type/user";
import { WorkflowActionService } from "./workflow-action.service";
import { JointUIService } from "../../joint-ui/joint-ui.service";
import { Observable, Subject } from "rxjs";
import { isEqual } from "lodash";

/**
 * "Coeditor" means "collaboratively editing user".
 * CoeditorPresenceService handles user-presence updates from other editors in the same shared-editing room
 * and shows them on the UI. It also keeps some states in itself for some necessary UI update information,
 * like which co-editors are currently highlighting a particular operator.
 */

@Injectable({
  providedIn: "root",
})
export class CoeditorPresenceService {
  private readonly coeditorOpenedCodeEditorSubject = new Subject<{ operatorId: string }>();
  private readonly coeditorClosedCodeEditorSubject = new Subject<{ operatorId: string }>();
  public shadowingModeEnabled = false;
  public shadowingCoeditor?: Coeditor;
  public coeditors: Coeditor[] = [];
  private jointGraph: joint.dia.Graph;
  private texeraGraph: WorkflowGraph;
  private jointGraphWrapper: JointGraphWrapper;
  private coeditorCurrentlyEditing = new Map<string, string | undefined>();
  private coeditorOperatorHighlights = new Map<string, readonly string[]>();
  private coeditorOperatorPropertyChanged = new Map<string, string | undefined>();
  private coeditorEditingCode = new Map<string, boolean>();
  private coeditorStates = new Map<string, CoeditorState>();
  private currentlyEditingTimers = new Map<string, NodeJS.Timer>();

  constructor(private workflowActionService: WorkflowActionService) {
    this.texeraGraph = workflowActionService.getTexeraGraph() as WorkflowGraph;
    this.jointGraph = workflowActionService.getJointGraph();
    this.jointGraphWrapper = workflowActionService.getJointGraphWrapper();
    this.observeUserState();
    this.texeraGraph.newYDocLoadedSubject.subscribe(_ => {
      this.observeUserState();
    });
  }

  /**
   * Start shawoding an co-editor.
   * @param coeditor
   */
  public shadowCoeditor(coeditor: Coeditor): void {
    this.shadowingModeEnabled = true;
    this.shadowingCoeditor = coeditor;
    if (coeditor.clientId) {
      const currentlyEditing = this.coeditorCurrentlyEditing.get(coeditor.clientId);
      if (currentlyEditing) {
        this.workflowActionService.highlightOperators(false, currentlyEditing);
        const currentlyEditingCode = this.coeditorEditingCode.get(coeditor.clientId);
        if (currentlyEditingCode) this.coeditorOpenedCodeEditorSubject.next({ operatorId: currentlyEditing });
      }
    }
  }

  /**
   * End shadowing.
   */
  public stopShadowing() {
    this.shadowingModeEnabled = false;
  }

  public getCoeditorOpenedCodeEditorSubject(): Observable<{ operatorId: string }> {
    return this.coeditorOpenedCodeEditorSubject.asObservable();
  }

  public getCoeditorClosedCodeEditorSubject(): Observable<{ operatorId: string }> {
    return this.coeditorClosedCodeEditorSubject.asObservable();
  }

  /**
   * Listens to changes of co-editors' presence infos and lets <code>{@link CoeditorPresenceService}</code> handle them.
   */
  private observeUserState(): void {
    // destroy previous user states if any
    for (const coeditor of this.coeditors) {
      if (coeditor.clientId) this.removeCoeditor(coeditor.clientId);
    }

    // first time logic
    const currentStates = this.getCoeditorStatesArray().filter(
      userState => userState.user && userState.user.clientId && userState.user.clientId !== this.getLocalClientId()
    );
    for (const state of currentStates) {
      this.addCoeditor(state);
    }

    this.texeraGraph.sharedModel.awareness.on(
      "change",
      (change: { added: number[]; updated: number[]; removed: number[] }) => {
        for (const clientId of change.added) {
          const coeditorState = this.getCoeditorStatesMap().get(clientId);
          if (coeditorState && coeditorState.user.clientId !== this.getLocalClientId()) this.addCoeditor(coeditorState);
        }

        for (const clientId of change.removed) {
          if (!this.getCoeditorStatesMap().has(clientId)) this.removeCoeditor(clientId.toString());
        }

        for (const clientId of change.updated) {
          const coeditorState = this.getCoeditorStatesMap().get(clientId);
          if (coeditorState && clientId.toString() !== this.getLocalClientId()) {
            if (!this.hasCoeditor(clientId.toString())) {
              this.addCoeditor(coeditorState);
            } else {
              this.updateCoeditorState(clientId.toString(), coeditorState);
            }
          }
        }
      }
    );
  }

  /**
   * Returns whether this co-editor is already recorded here.
   * @param clientId
   */
  private hasCoeditor(clientId?: string): boolean {
    return this.coeditors.find(v => v.clientId === clientId) !== undefined;
  }

  /**
   * Adds a new co-editor and initialize UI-updates for this editor.
   * @param coeditorState
   */
  private addCoeditor(coeditorState: CoeditorState): void {
    const coeditor = coeditorState.user;
    if (!this.hasCoeditor(coeditor.clientId) && coeditor.clientId) {
      this.coeditors.push(coeditor);
      this.coeditorStates.set(coeditor.clientId, coeditorState);
      this.updateCoeditorState(coeditor.clientId, coeditorState);
    }
  }

  /**
   * Removes a co-editor and clean up states recorded in this service.
   * @param clientId
   */
  private removeCoeditor(clientId: string): void {
    for (let i = 0; i < this.coeditors.length; i++) {
      const coeditor = this.coeditors[i];
      if (coeditor.clientId === clientId) {
        this.updateCoeditorState(clientId, {
          user: coeditor,
          userCursor: { x: 0, y: 0 },
          currentlyEditing: undefined,
          isActive: false,
          highlighted: undefined,
          changed: undefined,
        });
        this.coeditors.splice(i);
      }
    }
    this.coeditorStates.delete(clientId);
    if (this.shadowingModeEnabled && this.shadowingCoeditor?.clientId === clientId) {
      this.stopShadowing();
    }
  }

  /**
   * Given a new <code>{@link CoeditorState}</code> with specified clientId, this method updates this co-editor's
   * presence information and corresponding UIs. This is an incremental update, i.e., it will first check existing
   * states and only update what is new. The update is handled separately in each sub-method called.
   * @param clientId
   * @param coeditorState
   */
  private updateCoeditorState(clientId: string, coeditorState: CoeditorState): void {
    this.updateCoeditorCursor(coeditorState);
    this.updateCoeditorHighlightedOperators(clientId, coeditorState);
    this.updateCoeditorCurrentlyEditing(clientId, coeditorState);
    this.updateCoeditorChangedProperty(clientId, coeditorState);
    this.updateCoeditorOpenAndCloseCode(clientId, coeditorState);
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //                         Below are methods to update different co-editor states.                                  //
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  private updateCoeditorCursor(coeditorState: CoeditorState) {
    // Update pointers
    const existingPointer: joint.dia.Cell | undefined = this.jointGraph.getCell(
      JointUIService.getJointUserPointerName(coeditorState.user)
    );
    const userColor = coeditorState.user.color;
    if (existingPointer) {
      if (coeditorState.isActive) {
        if (coeditorState.userCursor !== existingPointer.position()) {
          existingPointer.remove();
          if (userColor) {
            const newPoint = JointUIService.getJointUserPointerCell(
              coeditorState.user,
              coeditorState.userCursor,
              userColor
            );
            this.jointGraph.addCell(newPoint);
            this.jointGraphWrapper.getMainJointPaper().findViewByModel(newPoint.id).setInteractivity(false);
          }
        }
      } else existingPointer.remove();
    } else {
      if (coeditorState.isActive && userColor) {
        // create new user point (directly updating the point would cause unknown errors)
        const newPoint = JointUIService.getJointUserPointerCell(
          coeditorState.user,
          coeditorState.userCursor,
          userColor
        );
        this.jointGraph.addCell(newPoint);
        this.jointGraphWrapper.getMainJointPaper().findViewByModel(newPoint.id).setInteractivity(false);
      }
    }
  }

  private updateCoeditorHighlightedOperators(clientId: string, coeditorState: CoeditorState) {
    // Update operator highlights
    const previousHighlighted = this.coeditorOperatorHighlights.get(clientId);
    const currentHighlighted = coeditorState.highlighted;
    if (!isEqual(previousHighlighted, currentHighlighted)) {
      if (previousHighlighted) {
        for (const operatorId of previousHighlighted) {
          if (!currentHighlighted || !currentHighlighted.includes(operatorId)) {
            this.jointGraphWrapper.deleteCoeditorOperatorHighlight(coeditorState.user, operatorId);
          }
        }
      }

      if (currentHighlighted) {
        for (const operatorId of currentHighlighted) {
          if (!previousHighlighted || !previousHighlighted.includes(operatorId)) {
            this.jointGraphWrapper.addCoeditorOperatorHighlight(coeditorState.user, operatorId);
          }
        }
        this.coeditorOperatorHighlights.set(clientId, currentHighlighted);
      } else {
        this.coeditorOperatorHighlights.delete(clientId);
      }
    }
  }

  private updateCoeditorCurrentlyEditing(clientId: string, coeditorState: CoeditorState) {
    // Update currently editing status
    const previousEditing = this.coeditorCurrentlyEditing.get(clientId);
    const previousIntervalId = this.currentlyEditingTimers.get(clientId);
    const currentEditing = coeditorState.currentlyEditing;
    if (previousEditing !== currentEditing) {
      if (
        previousEditing &&
        previousIntervalId &&
        this.workflowActionService.getTexeraGraph().hasOperator(previousEditing)
      ) {
        this.jointGraphWrapper.removeCurrentEditing(coeditorState.user, previousEditing, previousIntervalId);
        this.coeditorCurrentlyEditing.delete(clientId);
        this.currentlyEditingTimers.delete(clientId);
        if (this.shadowingModeEnabled && this.shadowingCoeditor?.clientId === coeditorState.user.clientId) {
          this.workflowActionService.unhighlightOperators(previousEditing);
        }
      }
      if (currentEditing && this.workflowActionService.getTexeraGraph().hasOperator(currentEditing)) {
        const intervalId = this.jointGraphWrapper.setCurrentEditing(coeditorState.user, currentEditing);
        this.coeditorCurrentlyEditing.set(clientId, currentEditing);
        this.currentlyEditingTimers.set(clientId, intervalId);
        if (this.shadowingModeEnabled && this.shadowingCoeditor?.clientId === coeditorState.user.clientId) {
          this.workflowActionService.highlightOperators(false, currentEditing);
        }
      }
    }
  }

  private updateCoeditorChangedProperty(clientId: string, coeditorState: CoeditorState) {
    // Update property changed status
    const previousChanged = this.coeditorOperatorPropertyChanged.get(clientId);
    const currentChanged = coeditorState.changed;
    if (previousChanged !== currentChanged) {
      if (currentChanged) {
        this.coeditorOperatorPropertyChanged.set(clientId, currentChanged);
        // Set for 3 seconds
        this.jointGraphWrapper.setPropertyChanged(coeditorState.user, currentChanged);
        setTimeout(() => {
          this.coeditorOperatorPropertyChanged.delete(clientId);
          this.jointGraphWrapper.removePropertyChanged(coeditorState.user, currentChanged);
        }, 2000);
      }
    }
  }

  private updateCoeditorOpenAndCloseCode(clientId: string, coeditorState: CoeditorState) {
    const previousEditingCode = this.coeditorEditingCode.get(clientId);
    const currentEditingCode = coeditorState.editingCode;
    if (previousEditingCode !== currentEditingCode) {
      if (currentEditingCode) {
        this.coeditorEditingCode.set(clientId, currentEditingCode);
        if (
          this.shadowingModeEnabled &&
          this.shadowingCoeditor?.clientId === clientId.toString() &&
          coeditorState.currentlyEditing
        ) {
          this.coeditorOpenedCodeEditorSubject.next({ operatorId: coeditorState.currentlyEditing });
        }
      } else {
        if (previousEditingCode) {
          this.coeditorEditingCode.delete(clientId);
          if (
            this.shadowingModeEnabled &&
            this.shadowingCoeditor?.clientId === clientId.toString() &&
            coeditorState.currentlyEditing
          ) {
            this.coeditorClosedCodeEditorSubject.next({ operatorId: coeditorState.currentlyEditing });
          }
        }
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //                                       Below are internal utility methods                                         //
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  private getCoeditorStatesArray(): CoeditorState[] {
    return Array.from(this.texeraGraph.sharedModel.awareness.getStates().values() as IterableIterator<CoeditorState>);
  }

  private getCoeditorStatesMap(): Map<number, CoeditorState> {
    return this.texeraGraph.sharedModel.awareness.getStates() as Map<number, CoeditorState>;
  }

  private getLocalClientId() {
    return this.texeraGraph.sharedModel.clientId;
  }
}
