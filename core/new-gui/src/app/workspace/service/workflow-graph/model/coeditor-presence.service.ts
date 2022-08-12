import { Injectable } from "@angular/core";
import {WorkflowGraph} from "./workflow-graph";
import * as joint from "jointjs";
import {JointGraphWrapper} from "./joint-graph-wrapper";
import {User, UserState} from "../../../../common/type/user";
import {WorkflowActionService} from "./workflow-action.service";
import {JointUIService} from "../../joint-ui/joint-ui.service";

function isEqual(array1: any[] | undefined, array2: any[] | undefined): boolean {
  if (!array1 && !array2) return true;
  else {
    if (array1 && array2) {
      return array1.length === array2.length && array1.every((value, index) => value === array2[index]);
    } else return false;
  }
}

@Injectable({
  providedIn: "root"
})
export class CoeditorPresenceService {
  private jointGraph: joint.dia.Graph;
  private texeraGraph: WorkflowGraph;
  private jointGraphWrapper: JointGraphWrapper;
  private coeditorCurrentlyEditing = new Map<string, string | undefined>();
  private coeditorOperatorHighlights = new Map<string, string[]>();
  private coeditorOperatorPropertyChanged = new Map<string, string | undefined>();
  private coeditorStates = new Map<string, UserState>();
  public shadowingModeEnabled = false;
  public shadowingCoeditor?: User;
  public coeditors: User[] = [];

  constructor(
    private workflowActionService: WorkflowActionService,
    private jointUIService: JointUIService
  ) {
    this.texeraGraph = workflowActionService.texeraGraph;
    this.jointGraph = workflowActionService.jointGraph;
    this.jointGraphWrapper = workflowActionService.jointGraphWrapper;
  }

  public hasCoeditor(clientId?: string) {
    return this.coeditors.find(v=>v.clientId === clientId);
  }

  public addCoeditor(coeditorState: UserState) {
    const coeditor = coeditorState.user;
    if (!this.hasCoeditor(coeditor.clientId) && coeditor.clientId) {
      this.coeditors.push(coeditor);
      this.coeditorStates.set(coeditor.clientId, coeditorState);
      this.updateCoeditorState(coeditor.clientId, coeditorState);
    }
  }

  public removeCoeditor(clientId: string) {
    for (let i = 0; i < this.coeditors.length; i++) {
      const coeditor = this.coeditors[i];
      if (coeditor.clientId === clientId) {
        this.updateCoeditorState(clientId, {
          user: coeditor,
          userCursor: {x: 0, y: 0},
          currentlyEditing: undefined,
          isActive: false,
          highlighted: undefined,
          changed: undefined
        });
        this.coeditors.splice(i);
      }
    }
    this.coeditorStates.delete(clientId);
  }

  public updateCoeditorState(clientId: string, coeditorState: UserState) {
    // Update pointers
    const existingPointer: joint.dia.Cell | undefined = this.jointGraph.getCell(JointUIService.getJointUserPointerName(coeditorState.user));
    const userColor = coeditorState.user.color;
    if (existingPointer) {
      if (coeditorState.isActive) {
        if (coeditorState.userCursor !== existingPointer.position()) {
          existingPointer.remove();
          if (userColor) {
            const newPoint = JointUIService.getJointUserPointerCell(coeditorState.user, coeditorState.userCursor, userColor);
            this.jointGraph.addCell(newPoint);
          }
        }
      } else
        existingPointer.remove();
    } else {
      if (coeditorState.isActive && userColor) {
        // create new user point (directly updating the point would cause unknown errors)
        const newPoint = JointUIService.getJointUserPointerCell(coeditorState.user, coeditorState.userCursor, userColor);
        this.jointGraph.addCell(newPoint);
      }
    }

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

    // Update currently editing status
    const previousEditing = this.coeditorCurrentlyEditing.get(clientId);
    const currentEditing = coeditorState.currentlyEditing;
    if (previousEditing !== currentEditing) {
      if (previousEditing) {
        this.jointGraphWrapper.removeCurrentEditing(coeditorState.user, previousEditing);
        this.coeditorCurrentlyEditing.delete(clientId);
        if (this.shadowingModeEnabled && this.shadowingCoeditor?.clientId === coeditorState.user.clientId) {
          this.workflowActionService.unhighlightOperators(previousEditing);
        }
      }
      if (currentEditing) {
        this.jointGraphWrapper.setCurrentEditing(coeditorState.user, currentEditing);
        this.coeditorCurrentlyEditing.set(clientId, currentEditing);
        if (this.shadowingModeEnabled && this.shadowingCoeditor?.clientId === coeditorState.user.clientId) {
          this.workflowActionService.highlightOperators(false, currentEditing);
        }
      }
    }

    // Update property changed status
    const previousChanged = this.coeditorOperatorPropertyChanged.get(clientId);
    const currentChanged = coeditorState.changed;
    if (previousChanged !== currentChanged) {
      if (currentChanged) {
        this.coeditorOperatorPropertyChanged.set(clientId, currentChanged);
        // Set for 3 seconds
        this.jointGraphWrapper.setPropertyChanged(coeditorState.user, currentChanged);
        setTimeout(()=>{
          this.coeditorOperatorPropertyChanged.delete(clientId);
          this.jointGraphWrapper.removePropertyChanged(coeditorState.user, currentChanged);
        }, 2000);
      }
    }
  }

  shadowCoeditor(coeditor: User) {
    this.shadowingModeEnabled = true;
    this.shadowingCoeditor = coeditor;
    if (coeditor.clientId) {
      const currentlyEditing = this.coeditorCurrentlyEditing.get(coeditor.clientId);
      if (currentlyEditing)
        this.workflowActionService.highlightOperators(false, currentlyEditing);
    }
  }

  stopShadowing() {
    this.shadowingModeEnabled = false;
  }
}
