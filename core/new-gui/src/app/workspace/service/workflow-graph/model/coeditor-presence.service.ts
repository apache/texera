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
  private coeditorJointStates = new Map<string, User>();
  private coeditorOperatorHighlights = new Map<string, string[]>();
  private coeditorStates = new Map<string, UserState>();
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
        this.coeditors.splice(i);
      }
    }
    this.coeditorStates.delete(clientId);
  }

  public updateCoeditorState(clientId: string, coeditorState: UserState) {
    // Update pointers
    const existingPointer: joint.dia.Cell | undefined = this.jointGraph.getCell(JointUIService.getJointUserPointerName(clientId));
    const userColor = coeditorState.user.color;
    if (existingPointer) {
      if (coeditorState.isActive) {
        if (coeditorState.userCursor !== existingPointer.position()) {
          existingPointer.remove();
          if (userColor) {
            const newPoint = JointUIService.getJointUserPointerCell(clientId, coeditorState.userCursor, userColor);
            this.jointGraph.addCell(newPoint);
          }
        }
      } else
        existingPointer.remove();
    } else {
      if (coeditorState.isActive && userColor) {
        // create new user point (directly updating the point would cause unknown errors)
        const newPoint = JointUIService.getJointUserPointerCell(clientId, coeditorState.userCursor, userColor);
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
  }
}
