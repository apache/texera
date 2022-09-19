import {WorkflowGraph, WorkflowGraphReadonly} from "./workflow-graph";
import {JointGraphWrapper} from "./joint-graph-wrapper";
import * as Y from "yjs";
import {
  Breakpoint,
  CommentBox,
  Comment,
  OperatorLink,
  OperatorPredicate,
  Point
} from "../../../types/workflow-common.interface";
import {Injectable} from "@angular/core";
import {JointUIService} from "../../joint-ui/joint-ui.service";
import {WorkflowActionService} from "./workflow-action.service";
import * as joint from "jointjs";
import {environment} from "../../../../../environments/environment";
import {User, UserState} from "../../../../common/type/user";
import _remove from "lodash/remove";
import {YMapEvent} from "yjs";
import {CoeditorPresenceService} from "./coeditor-presence.service";
import {YType} from "../../../types/shared-editing.interface";

@Injectable({
  providedIn: "root"
})
export class SyncJointModelService {
  private texeraGraph: WorkflowGraphReadonly;
  private jointGraph: joint.dia.Graph;
  private jointGraphWrapper: JointGraphWrapper;

  constructor(
    private workflowActionService: WorkflowActionService,
    private jointUIService: JointUIService,
    private coeditorPresenceService: CoeditorPresenceService
  ) {
    this.texeraGraph = workflowActionService.getTexeraGraph();
    this.jointGraph = workflowActionService.getJointGraph();
    this.jointGraphWrapper = workflowActionService.getJointGraphWrapper();
    this.texeraGraph.newYDocLoadedSubject.subscribe( _ => {
        this.handleOperatorAddAndDelete();
        this.handleLinkAddAndDelete();
        this.handleElementPositionChange();
        this.handleCommentBoxAddAndDelete();
        this.handleBreakpointAddAndDelete();
        this.handleOperatorDeep();
        this.handleCommentBoxDeep();
        this.handleJointElementDrag();
        this.observeUserState();
      }
    );
  }


  /**
   * Reflects add and delete operator changes from TexeraGraph onto JointGraph.
   * @private
   */
  private handleOperatorAddAndDelete(): void {
    // A new key in the map means a new operator
    this.texeraGraph.sharedModel.operatorIDMap.observe((event: Y.YMapEvent<YType<OperatorPredicate>>) => {
      const jointElementsToAdd: joint.dia.Element[] = [];
      const newOpIDs: string[] = [];
      event.changes.keys.forEach((change, key) => {
        if (change.action === "add") {
          const newOperator = this.texeraGraph.sharedModel.operatorIDMap.get(key) as YType<OperatorPredicate>;
          // Also find its position
          if (this.texeraGraph.sharedModel.elementPositionMap?.has(key)) {
            const newPos = this.texeraGraph.sharedModel.elementPositionMap?.get(key) as Point;
            // Add the operator into joint graph
            const jointOperator = this.jointUIService.getJointOperatorElement(newOperator.toJSON(), newPos);
            jointElementsToAdd.push(jointOperator);
            newOpIDs.push(key);
          } else {
            throw new Error(`operator with key ${key} does not exist in position map`);
          }
        }
        if (change.action === "delete") {
          // Disables JointGraph -> TexeraGraph sync temporarily
          this.texeraGraph.setSyncTexeraGraph(false);
          this.jointGraph.getCell(key).remove();
          // Emit the event streams here, after joint graph is synced.
          this.texeraGraph.setSyncTexeraGraph(true);
          this.texeraGraph.operatorDeleteSubject.next({deletedOperatorID:key});
        }
      });

      if (environment.asyncRenderingEnabled) {
        // Group add
        this.jointGraphWrapper.jointGraphContext.withContext({ async: true }, () => {
          this.jointGraph.addCells(jointElementsToAdd);
        });
      } else {
        // Add one by one
        for (let i = 0; i < jointElementsToAdd.length; i++) {
          this.jointGraph.addCell(jointElementsToAdd[i]);
        }
      }

      // Emit the event streams here, after joint graph is synced and before highlighting.
      for (let i = 0; i < newOpIDs.length; i++) {
        const newOpID = newOpIDs[i];
        const newOperator = this.texeraGraph.sharedModel.operatorIDMap.get(newOpID) as YType<OperatorPredicate>;
        this.texeraGraph.operatorAddSubject.next(newOperator.toJSON());
      }

      if (event.transaction.local) {
        // Only highlight when this is added by current user.
        this.jointGraphWrapper.highlightOperators(...newOpIDs);
      }
      this.texeraGraph.setSyncTexeraGraph(true);
    });
  }

  /**
   * Syncs link add and delete.
   * @private
   */
  private handleLinkAddAndDelete(): void {
    this.texeraGraph.sharedModel.operatorLinkMap.observe((event: Y.YMapEvent<OperatorLink>) => {
        const jointElementsToAdd: joint.dia.Link[] = [];
        const linksToAdd: OperatorLink[] = [];
        const keysToDelete: string[] = [];
        const linksToDelete: OperatorLink[] = [];
        event.changes.keys.forEach((change, key)=>{
          if (change.action === "add") {
            const newLink = this.texeraGraph.sharedModel.operatorLinkMap.get(key) as OperatorLink;
            const jointLinkCell = JointUIService.getJointLinkCell(newLink);
            jointElementsToAdd.push(jointLinkCell);
            linksToAdd.push(newLink);
          }
          if (change.action === "delete") {
            keysToDelete.push(key);
            linksToDelete.push(change.oldValue);
          }
        });

        // Disables JointGraph -> TexeraGraph sync temporarily
        this.texeraGraph.setSyncTexeraGraph(false);
        for (let i = 0; i < keysToDelete.length; i++) {
          if (this.jointGraph.getCell(keysToDelete[i]))
            this.jointGraph.getCell(keysToDelete[i]).remove();
        }
        if (environment.asyncRenderingEnabled) {
          this.jointGraphWrapper.jointGraphContext.withContext({async: true}, () => {
            this.jointGraph.addCells(jointElementsToAdd.filter(x => x !== undefined));
          });
        } else {
          for (let i = 0; i < jointElementsToAdd.length; i++) {
            this.jointGraph.addCell(jointElementsToAdd[i]);
          }
        }
        this.texeraGraph.setSyncTexeraGraph(true);

        // Emit event streams
        for (let i = 0; i < linksToAdd.length; i++) {
          const link = linksToAdd[i];
          this.texeraGraph.linkAddSubject.next(link);
        }

        for (let i = 0; i < linksToDelete.length; i++) {
        const link = linksToDelete[i];
        this.texeraGraph.linkDeleteSubject.next({deletedLink:link});
      }

    });
  }

  private handleElementPositionChange(): void {
    this.texeraGraph.sharedModel.elementPositionMap?.observe((event: Y.YMapEvent<Point>) => {
      event.changes.keys.forEach((change, key)=> {
        if (change.action === "update") {
          this.texeraGraph.setSyncTexeraGraph(false);
          const newPosition = this.texeraGraph.sharedModel.elementPositionMap?.get(key);
          if (newPosition) {
            this.jointGraphWrapper.setListenPositionChange(false);
            this.jointGraphWrapper.setAbsolutePosition(key, newPosition.x, newPosition.y);
            this.jointGraphWrapper.setListenPositionChange(true);
          }
          this.texeraGraph.setSyncTexeraGraph(true);
        }
      });
    });
  }

  private handleCommentBoxAddAndDelete(): void {
    this.texeraGraph.sharedModel.commentBoxMap.observe((event: Y.YMapEvent<YType<CommentBox>>) => {
      event.changes.keys.forEach((change, key) => {
        if (change.action === "add") {
          const commentBox = this.texeraGraph.sharedModel.commentBoxMap.get(key) as YType<CommentBox>;
          const commentElement = this.jointUIService.getCommentElement(commentBox.toJSON());
          this.jointGraph.addCell(commentElement);
          this.texeraGraph.commentBoxAddSubject.next(commentBox.toJSON());
        }
        if (change.action === "delete") {
          this.jointGraph.getCell(key).remove();
        }
      });
    });
  }

  private handleBreakpointAddAndDelete(): void {
    this.texeraGraph.sharedModel.linkBreakpointMap.observe((event: Y.YMapEvent<Breakpoint>) => {
      event.changes.keys.forEach((change, key) => {
        const oldBreakpoint = change.oldValue as Breakpoint | undefined;
        if (change.action === "add") {
          this.jointGraphWrapper.showLinkBreakpoint(key);
          this.texeraGraph.breakpointChangeStream.next({ oldBreakpoint, linkID: key });
        }
        if (change.action === "delete") {
          if (this.texeraGraph.sharedModel.operatorLinkMap.has(key)) {
            this.jointGraphWrapper.hideLinkBreakpoint(key);
            this.texeraGraph.breakpointChangeStream.next({ oldBreakpoint, linkID: key });
          }
        }
      });
    });
  }

  private handleOperatorDeep(): void {
    this.texeraGraph.sharedModel.operatorIDMap.observeDeep((events: Y.YEvent<Y.Map<any>>[]) => {
      events.forEach(event => {
        if (event.target !== this.texeraGraph.sharedModel.operatorIDMap) {
          const operatorID = event.path[0] as string;
          if (event.path[event.path.length-1] === "customDisplayName") {
            const newName = this.texeraGraph.sharedModel.operatorIDMap.get(operatorID)?.get("customDisplayName") as Y.Text;
            this.texeraGraph.operatorDisplayNameChangedSubject.next({operatorID: operatorID, newDisplayName: newName.toJSON()});
          } else if (event.path.includes("operatorProperties")) {
            const operator = this.texeraGraph.getOperator(operatorID);
            this.texeraGraph.operatorPropertyChangeSubject.next({operator: operator});
          } else if (event.path.length === 1) {
            for (const entry of event.changes.keys.entries()) {
              const contentKey = entry[0];
              const contentValue = entry[1];
              if (contentKey === "operatorProperties") {
                const oldProperty = contentValue?.oldValue as Readonly<{ [key: string]: any }>;
                const operator = this.texeraGraph.getOperator(operatorID);
                this.texeraGraph.operatorPropertyChangeSubject.next({operator: operator});
              } else if (contentKey === "isCached") {
                const newCachedStatus = this.texeraGraph.sharedModel.operatorIDMap.get(operatorID)?.get("isCached") as boolean;
                if (newCachedStatus) {
                  this.texeraGraph.cachedOperatorChangedSubject.next({
                    newCached: [operatorID],
                    newUnCached: [],
                  });
                } else {
                  this.texeraGraph.cachedOperatorChangedSubject.next({
                    newCached: [],
                    newUnCached: [operatorID],
                  });
                }
              } else if (contentKey === "isDisabled") {
                const newDisabledStatus = this.texeraGraph.sharedModel.operatorIDMap.get(operatorID)?.get("isDisabled") as boolean;
                if (newDisabledStatus) {
                  this.texeraGraph.disabledOperatorChangedSubject.next({
                    newDisabled: [operatorID],
                    newEnabled: [],
                  });
                } else {
                  this.texeraGraph.disabledOperatorChangedSubject.next({
                    newDisabled: [],
                    newEnabled: [operatorID],
                  });
                }
              }
            }
          }
        }
      });
    });
  }

  private handleCommentBoxDeep(): void {
    this.texeraGraph.sharedModel.commentBoxMap.observeDeep((events: Y.YEvent<any>[]) => {
      events.forEach(event => {
        if (event.target !== this.texeraGraph.sharedModel.commentBoxMap) {
          const commentBox: CommentBox = this.texeraGraph.getCommentBox(event.path[0] as string);
          if (event.path.length === 2 && event.path[event.path.length-1] === "comments") {
            const addedComments = Array.from(event.changes.added.values());
            const deletedComments = Array.from(event.changes.deleted.values());
            if (addedComments.length == deletedComments.length) {
              this.texeraGraph.commentBoxEditCommentSubject.next({commentBox: commentBox});
            } else {
              if (addedComments.length > 0) {
                const newComment = addedComments[0].content.getContent()[0];
                this.texeraGraph.commentBoxAddCommentSubject.next({ addedComment: newComment, commentBox: commentBox });
              }
              if (deletedComments.length > 0) {
                this.texeraGraph.commentBoxDeleteCommentSubject.next({commentBox: commentBox});
              }
            }
          }
        }
      });
    });
  }

  private handleJointElementDrag(): void {
    this.jointGraphWrapper.getElementPositionChangeEvent().subscribe(element => {
      if (this.texeraGraph.getSyncTexeraGraph() && this.texeraGraph.sharedModel.elementPositionMap.get(element.elementID) as Point != element.newPosition) {
        this.texeraGraph.sharedModel.elementPositionMap?.set(element.elementID, element.newPosition);
        if (element.elementID.includes("commentBox")) {
          this.texeraGraph.sharedModel.commentBoxMap.get(element.elementID)?.set("commentBoxPosition", element.newPosition);
        }

      }
    });
  }

  /**
   * Handles changes of other users' cursors.
   */
  private observeUserState(): void {
    // first time logic
    const currentStates = Array.from(this.texeraGraph.sharedModel.awareness.getStates().values() as IterableIterator<UserState>)
      .filter((userState) => userState.user && userState.user.clientId && userState.user.clientId !== this.texeraGraph.sharedModel.clientId);
    for (const state of currentStates) {
      this.coeditorPresenceService.addCoeditor(state);
    }

    this.texeraGraph.sharedModel.awareness.on("change", (change: { added: number[], updated: number[], removed: number[] }) => {
      const currentStates = Array.from(this.texeraGraph.sharedModel.awareness.getStates().values() as IterableIterator<UserState>)
        .filter((userState) => userState.user.clientId && userState.user.clientId !== this.texeraGraph.sharedModel.clientId);
      for (const clientId of change.added) {
        const coeditorState = this.texeraGraph.sharedModel.awareness.getStates().get(clientId) as UserState;
        if (coeditorState.user.clientId !== this.texeraGraph.sharedModel.clientId)
          this.coeditorPresenceService.addCoeditor(coeditorState);
      }

      for (const clientId of change.removed) {
        if (!this.texeraGraph.sharedModel.awareness.getStates().has(clientId))
          this.coeditorPresenceService.removeCoeditor(clientId.toString());
      }

      for (const clientId of change.updated) {
        const coeditorState = this.texeraGraph.sharedModel.awareness.getStates().get(clientId) as UserState;
        if (clientId.toString() !== this.texeraGraph.sharedModel.clientId) {
          if (!this.coeditorPresenceService.hasCoeditor(clientId.toString())) {
            this.coeditorPresenceService.addCoeditor(coeditorState);
          } else {
            this.coeditorPresenceService.updateCoeditorState(clientId.toString(), coeditorState);
          }
        }
      }
    });
  }
}
