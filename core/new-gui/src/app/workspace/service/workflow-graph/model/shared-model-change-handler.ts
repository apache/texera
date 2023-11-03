import { WorkflowGraph } from "./workflow-graph";
import { JointGraphWrapper } from "./joint-graph-wrapper";
import * as Y from "yjs";
import {
  Breakpoint,
  CommentBox,
  OperatorLink,
  OperatorPredicate,
  Point,
  PortDescription,
} from "../../../types/workflow-common.interface";
import { JointUIService } from "../../joint-ui/joint-ui.service";
import * as joint from "jointjs";
import { environment } from "../../../../../environments/environment";
import { YType } from "../../../types/shared-editing.interface";
import { isDefined } from "../../../../common/util/predicate";

/**
 * SyncJointModelService listens to changes to the TexeraGraph (SharedModel) and updates Joint graph correspondingly,
 * regardless of whether the changes are local or from other co-editors.
 *
 * In all the handlers, this service will also trigger the necessary subjects, which were written in
 * <code>{@link WorkflowGraph}</code> in previous implementations and now all migrated here.
 *
 */

export class SharedModelChangeHandler {
  constructor(
    private texeraGraph: WorkflowGraph,
    private jointGraph: joint.dia.Graph,
    private jointGraphWrapper: JointGraphWrapper,
    private jointUIService: JointUIService
  ) {
    this.handleOperatorAddAndDelete();
    this.handleLinkAddAndDelete();
    this.handleElementPositionChange();
    this.handleCommentBoxAddAndDelete();
    this.handleBreakpointAddAndDelete();
    this.handleOperatorDeep();
    this.handleCommentBoxDeep();
    this.texeraGraph.newYDocLoadedSubject.subscribe(_ => {
      this.handleOperatorAddAndDelete();
      this.handleLinkAddAndDelete();
      this.handleElementPositionChange();
      this.handleCommentBoxAddAndDelete();
      this.handleBreakpointAddAndDelete();
      this.handleOperatorDeep();
      this.handleCommentBoxDeep();
    });
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
          // Unhighlight every operator and link to prevent sync error.
          if (event.transaction.local) {
            this.jointGraphWrapper.unhighlightElements({
              operators: this.jointGraphWrapper.getCurrentHighlightedOperatorIDs(),
              links: this.jointGraphWrapper.getCurrentHighlightedLinkIDs(),
              groups: [],
              commentBoxes: [],
              ports: this.jointGraphWrapper.getCurrentHighlightedPortIDs(),
            });
          }
          this.jointGraph.getCell(key).remove();
          // Emit the event streams here, after joint graph is synced.
          this.texeraGraph.setSyncTexeraGraph(true);
          this.texeraGraph.operatorDeleteSubject.next({ deletedOperatorID: key });
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

      if (event.transaction.local && !this.jointGraphWrapper.getReloadingWorkflow()) {
        // Only highlight when this is added by current user.
        this.jointGraphWrapper.setMultiSelectMode(newOpIDs.length > 1);
        this.jointGraphWrapper.highlightOperators(...newOpIDs);
      }
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
      event.changes.keys.forEach((change, key) => {
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
        if (this.texeraGraph.getSyncJointGraph() && this.jointGraph.getCell(keysToDelete[i]))
          this.jointGraph.getCell(keysToDelete[i]).remove();
      }
      if (environment.asyncRenderingEnabled) {
        this.jointGraphWrapper.jointGraphContext.withContext({ async: true }, () => {
          this.jointGraph.addCells(jointElementsToAdd.filter(x => x !== undefined));
        });
      } else {
        for (let i = 0; i < jointElementsToAdd.length; i++) {
          this.jointGraph.addCell(jointElementsToAdd[i]);
        }
      }
      this.texeraGraph.setSyncTexeraGraph(true);

      // Emit event streams and highlight
      for (let i = 0; i < linksToAdd.length; i++) {
        const link = linksToAdd[i];
        this.texeraGraph.linkAddSubject.next(link);
      }

      for (let i = 0; i < linksToDelete.length; i++) {
        const link = linksToDelete[i];
        this.texeraGraph.linkDeleteSubject.next({ deletedLink: link });
      }

      // Uncomment this if you also want link to be highlighted when adding but this conflicts with test cases.
      // if (event.transaction.local) {
      //   this.jointGraphWrapper.setMultiSelectMode(this.jointGraphWrapper.getCurrentHighlightedOperatorIDs().length + linksToAdd.length > 1);
      //   // Only highlight when this is added by current user.
      //   this.jointGraphWrapper.highlightLinks(...linksToAdd.map(link => link.linkID));
      // }
    });
  }

  /**
   * Syncs element positions. Will temporarily block local updates.
   * @private
   */
  private handleElementPositionChange(): void {
    this.texeraGraph.sharedModel.elementPositionMap?.observe((event: Y.YMapEvent<Point>) => {
      event.changes.keys.forEach((change, key) => {
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

  /**
   * Syncs the addition and deletion of comment boxes.
   * @private
   */
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

  /**
   * Syncs the addition and deletion of breakpoints.
   * @private
   */
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

  /**
   * Syncs changes that are on nested-structures of operators, including changes on:
   *  - <code>customDisplayName</code>
   *  - <code>operatorProperties</code>
   *  - <code>operatorPorts</code>
   *  - <code>viewResult</code>
   *  - <code>isDisabled</code>
   * @private
   */
  private handleOperatorDeep(): void {
    this.texeraGraph.sharedModel.operatorIDMap.observeDeep((events: Y.YEvent<Y.Map<any>>[]) => {
      events.forEach(event => {
        if (event.target !== this.texeraGraph.sharedModel.operatorIDMap) {
          const operatorID = event.path[0] as string;
          if (event.path.length === 1) {
            // Changes one level below the operatorPredicate type
            for (const entry of event.changes.keys.entries()) {
              const contentKey = entry[0];
              if (contentKey === "viewResult") {
                const newViewOpResultStatus = this.texeraGraph.sharedModel.operatorIDMap
                  .get(operatorID)
                  ?.get("viewResult") as boolean;
                if (newViewOpResultStatus) {
                  this.texeraGraph.viewResultOperatorChangedSubject.next({
                    newViewResultOps: [operatorID],
                    newUnviewResultOps: [],
                  });
                } else {
                  this.texeraGraph.viewResultOperatorChangedSubject.next({
                    newViewResultOps: [],
                    newUnviewResultOps: [operatorID],
                  });
                }
              } else if (contentKey === "markedForReuse") {
                const newReuseCacheOps = this.texeraGraph.sharedModel.operatorIDMap
                  .get(operatorID)
                  ?.get("markedForReuse") as boolean;
                if (newReuseCacheOps) {
                  this.texeraGraph.reuseOperatorChangedSubject.next({
                    newReuseCacheOps: [operatorID],
                    newUnreuseCacheOps: [],
                  });
                } else {
                  this.texeraGraph.reuseOperatorChangedSubject.next({
                    newReuseCacheOps: [],
                    newUnreuseCacheOps: [operatorID],
                  });
                }
              } else if (contentKey === "isDisabled") {
                const newDisabledStatus = this.texeraGraph.sharedModel.operatorIDMap
                  .get(operatorID)
                  ?.get("isDisabled") as boolean;
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
              } else if (contentKey === "operatorProperties") {
                this.onOperatorPropertyChanged(operatorID, event.transaction.local);
              }
            }
          } else if (event.path[event.path.length - 1] === "customDisplayName") {
            const newName = this.texeraGraph.sharedModel.operatorIDMap
              .get(operatorID)
              ?.get("customDisplayName") as Y.Text;
            this.texeraGraph.operatorDisplayNameChangedSubject.next({
              operatorID: operatorID,
              newDisplayName: newName.toJSON(),
            });
          } else if (event.path.includes("operatorProperties")) {
            this.onOperatorPropertyChanged(operatorID, event.transaction.local);
          } else if (event.path.includes("inputPorts")) {
            this.handlePortEvent(event, operatorID, true);
          } else if (event.path.includes("outputPorts")) {
            this.handlePortEvent(event, operatorID, false);
          } else {
            throw new Error(`undefined operation on shared type: .${event}`);
          }
        }
      });
    });
  }

  /**
   * Handles the additon, deletion and deeper changes to operator ports.
   * @param event
   * @param operatorID
   * @param isInput Since input and output ports are separate properties, need to access them differently.
   * @private
   */
  private handlePortEvent(event: Y.YEvent<Y.Map<any>>, operatorID: string, isInput: boolean) {
    if (event.path.length === 2) {
      // Port added or deleted inferred by event.delta
      const addedPort = event.delta[1]?.insert;
      if (isDefined(addedPort)) {
        this.onPortAdded(operatorID, isInput, (addedPort as Y.Map<any>[])[0].toJSON() as PortDescription);
      } else if (isDefined(event.delta[0]?.delete) || isDefined(event.delta[1]?.delete)) {
        this.onPortRemoved(operatorID, isInput);
      }
    } else {
      const changedOperator = this.texeraGraph.getOperator(operatorID);
      if (event.path.includes("displayName")) {
        // Display name changed (via shared text editor)
        const changedPort = isInput
          ? changedOperator.inputPorts[event.path[2] as number]
          : changedOperator.outputPorts[event.path[2] as number];
        this.texeraGraph.portDisplayNameChangedSubject.next({
          operatorID: operatorID,
          portID: changedPort.portID,
          newDisplayName: event.target.toJSON() as unknown as string,
        });
      } else if (event.path.length >= 3) {
        // Port property changed
        const newPortDescription = isInput
          ? changedOperator.inputPorts[event.path[2] as number]
          : changedOperator.outputPorts[event.path[2] as number];
        if (isDefined(newPortDescription.partitionRequirement) && isDefined(newPortDescription.dependencies))
          this.texeraGraph.portPropertyChangedSubject.next({
            operatorPortID: {
              operatorID: operatorID,
              portID: newPortDescription.portID,
            },
            newProperty: {
              partitionInfo: newPortDescription.partitionRequirement,
              dependencies: newPortDescription.dependencies,
            },
          });
      } else {
        throw new Error(`undefined port operation on shared type: .${event}`);
      }
    }
  }

  /**
   * Also update awareness info here to accommodate different paths of updates.
   * @param operatorID
   * @param isLocal
   * @private
   */
  private onOperatorPropertyChanged(operatorID: string, isLocal: boolean) {
    const operator = this.texeraGraph.getOperator(operatorID);
    this.texeraGraph.operatorPropertyChangeSubject.next({ operator: operator });
    if (isLocal) {
      // emit operator property changed here
      const localState = this.texeraGraph.sharedModel.awareness.getLocalState();
      if (localState && localState["currentlyEditing"] === operatorID) {
        this.texeraGraph.updateSharedModelAwareness("changed", operatorID);
        this.texeraGraph.updateSharedModelAwareness("changed", undefined);
      }
    }
  }

  private onPortAdded(operatorID: string, isInput: boolean, port: PortDescription) {
    const operatorJointElement = <joint.dia.Element>this.jointGraph.getCell(operatorID);
    const portGroup = isInput ? "in" : "out";
    operatorJointElement.addPort({
      group: portGroup,
      id: port.portID,
      attrs: {
        ".port-label": {
          text: port.displayName ?? "",
        },
      },
    });

    const operator = this.texeraGraph.getOperator(operatorID);
    this.texeraGraph.portAddedOrDeletedSubject.next({ newOperator: operator });
  }

  private onPortRemoved(operatorID: string, isInput: boolean) {
    const operatorJointElement = <joint.dia.Element>this.jointGraph.getCell(operatorID);
    const portGroup = isInput ? "in" : "out";
    let lastPort;
    for (let p of operatorJointElement.getPorts()) {
      if (p.group === portGroup) {
        lastPort = p;
      }
    }
    if (lastPort) {
      operatorJointElement.removePort(lastPort);
    }

    const operator = this.texeraGraph.getOperator(operatorID);
    this.texeraGraph.portAddedOrDeletedSubject.next({ newOperator: operator });
  }

  /**
   * Syncs changes that are on nested-structures of comment boxes, including changes of:
   *  - adding comments
   *  - deleting comments
   *  - editing comments (processed as deleting and then adding in-place)
   * @private
   */
  private handleCommentBoxDeep(): void {
    this.texeraGraph.sharedModel.commentBoxMap.observeDeep((events: Y.YEvent<any>[]) => {
      events.forEach(event => {
        if (event.target !== this.texeraGraph.sharedModel.commentBoxMap) {
          const commentBox: CommentBox = this.texeraGraph.getCommentBox(event.path[0] as string);
          if (event.path.length === 2 && event.path[event.path.length - 1] === "comments") {
            const addedComments = Array.from(event.changes.added.values());
            const deletedComments = Array.from(event.changes.deleted.values());
            if (addedComments.length == deletedComments.length) {
              this.texeraGraph.commentBoxEditCommentSubject.next({ commentBox: commentBox });
            } else {
              if (addedComments.length > 0) {
                const newComment = addedComments[0].content.getContent()[0];
                this.texeraGraph.commentBoxAddCommentSubject.next({ addedComment: newComment, commentBox: commentBox });
              }
              if (deletedComments.length > 0) {
                this.texeraGraph.commentBoxDeleteCommentSubject.next({ commentBox: commentBox });
              }
            }
          }
        }
      });
    });
  }
}
