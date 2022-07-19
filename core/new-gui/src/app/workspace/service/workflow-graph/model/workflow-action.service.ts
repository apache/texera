import { Injectable } from "@angular/core";

import * as joint from "jointjs";
import { cloneDeep } from "lodash-es";
import { BehaviorSubject, merge, Observable, Subject } from "rxjs";
import { Workflow, WorkflowContent } from "../../../../common/type/workflow";
import { mapToRecord, recordToMap } from "../../../../common/util/map";
import { WorkflowMetadata } from "../../../../dashboard/type/workflow-metadata.interface";
import {
  Breakpoint,
  OperatorLink,
  OperatorPort,
  OperatorPredicate,
  Point,
  CommentBox,
  Comment,
} from "../../../types/workflow-common.interface";
import { JointUIService } from "../../joint-ui/joint-ui.service";
import { OperatorMetadataService } from "../../operator-metadata/operator-metadata.service";
import { UndoRedoService } from "../../undo-redo/undo-redo.service";
import { WorkflowUtilService } from "../util/workflow-util.service";
import { JointGraphWrapper } from "./joint-graph-wrapper";
import { Group, OperatorGroup, OperatorGroupReadonly } from "./operator-group";
import { SyncOperatorGroup } from "./sync-operator-group";
import { SyncTexeraModel } from "./sync-texera-model";
import { WorkflowGraph, WorkflowGraphReadonly } from "./workflow-graph";
import { auditTime, debounceTime, filter } from "rxjs/operators";
import { WorkflowCollabService } from "../../workflow-collab/workflow-collab.service";
import { Command, commandFuncs, CommandMessage } from "src/app/workspace/types/command.interface";
import { isDefined } from "../../../../common/util/predicate";
import { environment } from "../../../../../environments/environment";
import * as Y from "yjs";

type OperatorPosition = {
  position: Point;
  layer: number;
};

/**
 *
 * WorkflowActionService exposes functions (actions) to modify the workflow graph model of both JointJS and Texera,
 *  such as addOperator, deleteOperator, addLink, deleteLink, etc.
 * WorkflowActionService performs checks the validity of these actions,
 *  for example, throws an error if deleting an nonsexist operator
 *
 * All changes(actions) to the workflow graph should be called through WorkflowActionService,
 *  then WorkflowActionService will propagate these actions to JointModel and Texera Model automatically.
 *
 * For an overview of the services in WorkflowGraphModule, see workflow-graph-design.md
 *
 */


@Injectable({
  providedIn: "root",
})
export class WorkflowActionService {
  private static readonly DEFAULT_WORKFLOW_NAME = "Untitled Workflow";
  private static readonly DEFAULT_WORKFLOW = {
    name: WorkflowActionService.DEFAULT_WORKFLOW_NAME,
    wid: undefined,
    creationTime: undefined,
    lastModifiedTime: undefined,
  };

  public readonly texeraGraph: WorkflowGraph;
  public readonly jointGraph: joint.dia.Graph;
  public readonly jointGraphWrapper: JointGraphWrapper;
  private readonly operatorGroup: OperatorGroup;
  public readonly syncTexeraModel: SyncTexeraModel;
  public readonly syncOperatorGroup: SyncOperatorGroup;
  // variable to temporarily hold the current workflow to switch view to a particular version
  private tempWorkflow?: Workflow;
  private workflowModificationEnabled = true;
  private enableModificationStream = new BehaviorSubject<boolean>(true);
  private lockListenEnabled = true;

  private workflowMetadata: WorkflowMetadata;
  private workflowMetadataChangeSubject: Subject<void> = new Subject<void>();

  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private jointUIService: JointUIService,
    private undoRedoService: UndoRedoService,
    private workflowUtilService: WorkflowUtilService,
    private workflowCollabService: WorkflowCollabService
  ) {
    this.texeraGraph = new WorkflowGraph();
    this.jointGraph = new joint.dia.Graph();
    this.jointGraphWrapper = new JointGraphWrapper(this.jointGraph);
    this.operatorGroup = new OperatorGroup(
      this.texeraGraph,
      this.jointGraph,
      this.jointGraphWrapper,
      this.workflowUtilService,
      this.jointUIService
    );
    this.syncTexeraModel = new SyncTexeraModel(this.texeraGraph, this.jointGraphWrapper, this.operatorGroup);
    this.syncOperatorGroup = new SyncOperatorGroup(this.texeraGraph, this.jointGraphWrapper, this.operatorGroup);
    this.workflowMetadata = WorkflowActionService.DEFAULT_WORKFLOW;

    this.handleJointLinkAdd();
    this.handleJointElementDrag();
    this.handleHighlightedElementPositionChange();
    this.listenToRemoteChange();
    this.listenToLockChange();
  }

  public observeFromTexeraGraph(): void {
      this.handleJointElementDrag();
  }

  public setNewYModel(workflowId: number) {
    this.texeraGraph.loadNewYModel(workflowId);
    this.undoRedoService.setUndoManager(this.texeraGraph.undoManager);
    this.observeFromTexeraGraph();
  }

  public destroyYModel(): void {
    this.texeraGraph.destroyYModel();
  }

  /**
   * Dummy method used to send a CommandMessage for undo or redo.
   */
  public undoredo(): void {}

  /**
   * Used for temporarily enabling or disabling propagation of changes so that reload won't affect other clients.
   */
  public toggleSendData(toggle: boolean): void {
    this.workflowCollabService.setPropagationEnabled(toggle);
  }

  /**
   * Used for temporarily blocking any changes to the lock so that reload can be successfully executed.
   */
  public toggleLockListen(toggle: boolean): void {
    this.lockListenEnabled = toggle;
  }

  // workflow modification lock interface (allows or prevents commands that would modify the workflow graph)
  public enableWorkflowModification() {
    if (this.workflowModificationEnabled) {
      return;
    }
    this.workflowModificationEnabled = true;
    this.enableModificationStream.next(true);
    this.undoRedoService.enableWorkFlowModification();
  }

  public disableWorkflowModification() {
    if (!this.workflowModificationEnabled) {
      return;
    }
    this.workflowModificationEnabled = false;
    this.enableModificationStream.next(false);
    this.undoRedoService.disableWorkFlowModification();
  }

  public checkWorkflowModificationEnabled(): boolean {
    return this.workflowModificationEnabled;
  }

  public getWorkflowModificationEnabledStream(): Observable<boolean> {
    return this.enableModificationStream.asObservable();
  }

  // TODO: process separately
  public handleJointLinkAdd(): void {
  }

  public handleJointElementDrag(): void {
    this.getJointGraphWrapper().getElementPositionChangeEvent().subscribe(element => {
      if (this.texeraGraph.getSyncTexeraGraph() && this.texeraGraph.elementPositionMap.get(element.elementID) as Point != element.newPosition) {
        this.texeraGraph.elementPositionMap?.set(element.elementID, element.newPosition);
        if (element.elementID.includes("commentBox")) {
          this.texeraGraph.commentBoxMap.get(element.elementID)?.set("commentBoxPosition", element.newPosition);
        };
      }
    });
  }

  /**
   * Subscribes to element position change event stream,
   *  checks if the element (operator or group) is moved by user and
   *  if the moved element is currently highlighted,
   *  if it is, moves other highlighted elements (operators and groups) along with it,
   *    links will automatically move with operators.
   *
   * If a group is highlighted, we consider the whole group as highlighted, including all the
   *  operators embedded in the group and regardless of whether or not they're actually highlighted.
   *  Thus, when a highlighted group moves, all its embedded operators move along with it.
   */
  public handleHighlightedElementPositionChange(): void {
    this.jointGraphWrapper
      .getElementPositionChangeEvent()
      .pipe(
        filter(() => this.jointGraphWrapper.getListenPositionChange()),
        filter(() => this.undoRedoService.listenJointCommand),
        filter(
          movedElement =>
            this.jointGraphWrapper.getCurrentHighlightedOperatorIDs().includes(movedElement.elementID) ||
            this.jointGraphWrapper.getCurrentHighlightedGroupIDs().includes(movedElement.elementID)
        )
      )
      .subscribe(movedElement => {
        const selectedElements = this.jointGraphWrapper.getCurrentHighlightedGroupIDs().slice(); // operators added to this list later
        const movedGroup = this.operatorGroup.getGroupByOperator(movedElement.elementID);

        if (movedGroup && selectedElements.includes(movedGroup.groupID)) {
          movedGroup.operators.forEach((operatorInfo, operatorID) => selectedElements.push(operatorID));
          selectedElements.splice(selectedElements.indexOf(movedGroup.groupID), 1);
        }
        this.jointGraphWrapper.getCurrentHighlightedOperatorIDs().forEach(operatorID => {
          const group = this.operatorGroup.getGroupByOperator(operatorID);
          // operators move with their groups,
          // do not add elements that are in a group that will also be moved
          if (!group || !this.jointGraphWrapper.getCurrentHighlightedGroupIDs().includes(group.groupID)) {
            selectedElements.push(operatorID);
          }
        });
        const offsetX = movedElement.newPosition.x - movedElement.oldPosition.x;
        const offsetY = movedElement.newPosition.y - movedElement.oldPosition.y;
        this.jointGraphWrapper.setListenPositionChange(false);
        this.undoRedoService.setListenJointCommand(false);
        selectedElements
          .filter(elementID => elementID !== movedElement.elementID)
          .forEach(elementID => this.jointGraphWrapper.setElementPosition(elementID, offsetX, offsetY));
        this.jointGraphWrapper.setListenPositionChange(true);
        this.undoRedoService.setListenJointCommand(true);
      });
  }

  /**
   * Gets the read-only version of the TexeraGraph
   *  to access the properties and event streams.
   *
   * Texera Graph contains information about the logical workflow plan of Texera,
   *  such as the types and properties of the operators.
   */
  public getTexeraGraph(): WorkflowGraphReadonly {
    return this.texeraGraph;
  }

  /**
   * Gets the JointGraph Wrapper, which contains
   *  getter for properties and event streams as RxJS Observables.
   *
   * JointJS Graph contains information about the UI,
   *  such as the position of operator elements, and the event of user dragging a cell around.
   */
  public getJointGraphWrapper(): JointGraphWrapper {
    return this.jointGraphWrapper;
  }

  /**
   * Gets the read-only version of the OperatorGroup
   *  which provides access to properties, event streams,
   *  and some helper functions.
   */
  public getOperatorGroup(): OperatorGroupReadonly {
    return this.operatorGroup;
  }



  // Below are actions

  /**
   * Adds an operator to the workflow graph at a point.
   * Throws an Error if the operator ID already existed in the Workflow Graph.
   *
   * @param operator
   * @param point
   */
  public addOperator(operator: OperatorPredicate, point: Point): void {
    // turn off multiselect since there's only one operator added
    this.jointGraphWrapper.setMultiSelectMode(false);
    // add operator
    this.addOperatorsInternal([{ operator, point }]);
    // highlight the newly added operator
    // TODO: highlight
  }

  /**
   * Deletes an operator from the workflow graph
   * Throws an Error if the operator ID doesn't exist in the Workflow Graph.
   * @param operatorID
   */
  public deleteOperator(operatorID: string): void {
    this.texeraGraph.yDoc.transact(()=> {
      const linksToDelete = new Map<OperatorLink, number>();
      this.getTexeraGraph()
        .getAllLinks()
        .filter(link => link.source.operatorID === operatorID || link.target.operatorID === operatorID)
        .forEach(link => linksToDelete.set(link, this.getOperatorGroup().getLinkLayerByGroup(link.linkID)));
      linksToDelete.forEach((linkLayer, link) => this.deleteLinkWithIDInternal(link.linkID));
      this.deleteOperatorInternal(operatorID);
    });
  }

  public addCommentBox(commentBox: CommentBox): void {
    const currentHighlights = this.jointGraphWrapper.getCurrentHighlights();
    this.jointGraphWrapper.unhighlightElements(currentHighlights);
    this.jointGraphWrapper.setMultiSelectMode(false);
    this.addCommentBoxInternal(commentBox);
  }

  /**
   * Adds given operators and links to the workflow graph.
   * @param operatorsAndPositions
   * @param links
   * @param groups
   * @param breakpoints
   * @param commentBoxes
   */
  public addOperatorsAndLinks(
    operatorsAndPositions: readonly { op: OperatorPredicate; pos: Point }[],
    links?: readonly OperatorLink[],
    groups?: readonly Group[],
    breakpoints?: ReadonlyMap<string, Breakpoint>,
    commentBoxes?: ReadonlyArray<CommentBox>
  ): void {
    // remember currently highlighted operators and groups
    const currentHighlights = this.jointGraphWrapper.getCurrentHighlights();
    // unhighlight previous highlights
    this.jointGraphWrapper.unhighlightElements(currentHighlights);
    this.jointGraphWrapper.setMultiSelectMode(operatorsAndPositions.length > 1);
    this.texeraGraph.yDoc.transact(() => {
      this.addOperatorsInternal(operatorsAndPositions.map(o => ({ operator: o.op, point: o.pos })));
      if (links) {
        this.addLinksInternal(links);
        if (breakpoints !== undefined) {
          breakpoints.forEach((breakpoint, linkID) => this.setLinkBreakpointInternal(linkID, breakpoint));
        }
      }
      if (isDefined(commentBoxes)) {
        commentBoxes.forEach(commentBox => this.addCommentBox(commentBox));
      }
    });
  }

  public deleteCommentBox(commentBoxID: string): void {
    this.deleteCommentBoxInternal(commentBoxID);
  }

  /**
   * Deletes given operators and links from the workflow graph.
   * @param operatorIDs
   * @param linkIDs
   */
  public deleteOperatorsAndLinks(
    operatorIDs: readonly string[],
    linkIDs: readonly string[],
    groupIDs?: readonly string[]
  ): void {
    // combines operators in selected groups and operators explicitly
    const operatorIDsCopy = Array.from(
      new Set(
        operatorIDs.concat(
          (groupIDs ?? []).flatMap(groupID =>
            Array.from(this.operatorGroup.getGroup(groupID).operators.values()).map(
              operatorInfo => operatorInfo.operator.operatorID
            )
          )
        )
      )
    );

    // save links to be deleted, including links explicitly deleted and implicitly deleted with their operators
    const linksToDelete = new Map<OperatorLink, number>();

    this.texeraGraph.yDoc.transact(()=> {
      // delete links required by this command
      linkIDs
        .map(linkID => this.getTexeraGraph().getLinkWithID(linkID))
        .forEach(link => linksToDelete.set(link, this.getOperatorGroup().getLinkLayerByGroup(link.linkID)));
      // delete links related to the deleted operator
      this.getTexeraGraph()
        .getAllLinks()
        .filter(
          link => operatorIDsCopy.includes(link.source.operatorID) || operatorIDsCopy.includes(link.target.operatorID)
        )
        .forEach(link => linksToDelete.set(link, this.getOperatorGroup().getLinkLayerByGroup(link.linkID)));
      linksToDelete.forEach((layer, link) => this.deleteLinkWithIDInternal(link.linkID));
      operatorIDsCopy.forEach(operatorID => {
        this.deleteOperatorInternal(operatorID);
      });
    });
  }

  /**
   * Handles the auto layout function
   *
   * @param Workflow
   */
  // Originally: drag Operator
  public autoLayoutWorkflow(): void {
    this.jointGraphWrapper.autoLayoutJoint();
  }

  /**
   * Adds a link to the workflow graph
   * Throws an Error if the link ID or the link with same source and target already exists.
   * @param link
   */
  public addLink(link: OperatorLink): void {
    this.addLinksInternal([link]);
  }

  /**
   * Deletes a link with the linkID from the workflow graph
   * Throws an Error if the linkID doesn't exist in the workflow graph.
   * @param linkID
   */
  public deleteLinkWithID(linkID: string): void {
    this.deleteLinkWithIDInternal(linkID);
  }

  public deleteLink(source: OperatorPort, target: OperatorPort): void {
    const link = this.getTexeraGraph().getLink(source, target);
    this.deleteLinkWithID(link.linkID);
  }


  public setOperatorProperty(operatorID: string, newProperty: object): void {
    this.setOperatorPropertyInternal(operatorID, newProperty);
    // TODO
    // unhighlight everything but the operator being modified
    const currentHighlightedOperators = <string[]>this.jointGraphWrapper.getCurrentHighlightedOperatorIDs().slice();
    if (!currentHighlightedOperators.includes(operatorID)) {
      this.jointGraphWrapper.setMultiSelectMode(false);
      this.jointGraphWrapper.highlightOperators(operatorID);
    }
  }

  /**
   * set a given link's breakpoint properties to specific values
   */
  public setLinkBreakpoint(linkID: string, newBreakpoint: Breakpoint | undefined): void {
    this.setLinkBreakpointInternal(linkID, newBreakpoint);
  }

  /**
   * Set the link's breakpoint property to empty to remove the breakpoint
   *
   * @param linkID
   */
  public removeLinkBreakpoint(linkID: string): void {
    this.setLinkBreakpoint(linkID, undefined);
  }

  /**
   * Reload the given workflow, update workflowMetadata and workflowContent.
   */
  public reloadWorkflow(workflow: Workflow | undefined, asyncRendering = environment.asyncRenderingEnabled): void {
    this.jointGraphWrapper.jointGraphContext.withContext({ async: asyncRendering }, () => {
      this.setWorkflowMetadata(workflow);
      // remove the existing operators on the paper currently

      this.deleteOperatorsAndLinks(
        this.getTexeraGraph()
          .getAllOperators()
          .map(op => op.operatorID),
        []
      );

      this.getTexeraGraph()
        .getAllCommentBoxes()
        .forEach(commentBox => this.deleteCommentBox(commentBox.commentBoxID));

      if (workflow === undefined) {
        return;
      }

      const workflowContent: WorkflowContent = workflow.content;

      const operatorsAndPositions: { op: OperatorPredicate; pos: Point }[] = [];
      workflowContent.operators.forEach(op => {
        const opPosition = workflowContent.operatorPositions[op.operatorID];
        if (!opPosition) {
          throw new Error(`position error: ${op.operatorID}`);
        }
        operatorsAndPositions.push({ op: op, pos: opPosition });
      });

      const links: OperatorLink[] = workflowContent.links;

      const groups: readonly Group[] = workflowContent.groups.map(group => {
        return {
          groupID: group.groupID,
          operators: recordToMap(group.operators),
          links: recordToMap(group.links),
          inLinks: group.inLinks,
          outLinks: group.outLinks,
          collapsed: group.collapsed,
        };
      });

      const breakpoints = new Map(Object.entries(workflowContent.breakpoints));

      const commentBoxes = workflowContent.commentBoxes;

      this.addOperatorsAndLinks(operatorsAndPositions, links, groups, breakpoints, commentBoxes);

      // operators shouldn't be highlighted during page reload
      const jointGraphWrapper = this.getJointGraphWrapper();
      jointGraphWrapper.unhighlightOperators(...jointGraphWrapper.getCurrentHighlightedOperatorIDs());
      // restore the view point
      this.getJointGraphWrapper().restoreDefaultZoomAndOffset();
    });
    this.toggleSendData(true);
  }

  public workflowChanged(): Observable<unknown> {
    return merge(
      this.getTexeraGraph().getOperatorAddStream(),
      this.getTexeraGraph().getOperatorDeleteStream(),
      this.getTexeraGraph().getLinkAddStream(),
      this.getTexeraGraph().getLinkDeleteStream(),
      this.getOperatorGroup().getGroupAddStream(),
      this.getOperatorGroup().getGroupDeleteStream(),
      this.getOperatorGroup().getGroupCollapseStream(),
      this.getOperatorGroup().getGroupExpandStream(),
      this.getTexeraGraph().getOperatorPropertyChangeStream(),
      this.getTexeraGraph().getBreakpointChangeStream(),
      this.getJointGraphWrapper().getElementPositionChangeEvent(),
      this.getTexeraGraph().getDisabledOperatorsChangedStream(),
      this.getTexeraGraph().getCommentBoxAddStream(),
      this.getTexeraGraph().getCommentBoxDeleteStream(),
      this.getTexeraGraph().getCommentBoxAddCommentStream(),
      this.getTexeraGraph().getCommentBoxDeleteCommentStream(),
      this.getTexeraGraph().getCommentBoxEditCommentStream(),
      this.getTexeraGraph().getCachedOperatorsChangedStream(),
      this.getTexeraGraph().getOperatorDisplayNameChangedStream()
    );
  }

  public workflowMetaDataChanged(): Observable<void> {
    return this.workflowMetadataChangeSubject.asObservable();
  }

  public setWorkflowMetadata(workflowMetaData: WorkflowMetadata | undefined): void {
    if (this.workflowMetadata === workflowMetaData) {
      return;
    }

    this.workflowMetadata = workflowMetaData === undefined ? WorkflowActionService.DEFAULT_WORKFLOW : workflowMetaData;
    this.workflowMetadataChangeSubject.next();
  }

  public getWorkflowMetadata(): WorkflowMetadata {
    return this.workflowMetadata;
  }

  public getWorkflowContent(): WorkflowContent {
    // collect workflow content
    const texeraGraph = this.getTexeraGraph();
    const operators = texeraGraph.getAllOperators();
    const links = texeraGraph.getAllLinks();
    const operatorPositions: { [key: string]: Point } = {};
    const commentBoxes = texeraGraph.getAllCommentBoxes();

    const groups = this.getOperatorGroup()
      .getAllGroups()
      .map(group => {
        return {
          groupID: group.groupID,
          operators: mapToRecord(group.operators),
          links: mapToRecord(group.links),
          inLinks: group.inLinks,
          outLinks: group.outLinks,
          collapsed: group.collapsed,
        };
      });
    const breakpointsMap = texeraGraph.getAllLinkBreakpoints();
    const breakpoints: Record<string, Breakpoint> = {};
    breakpointsMap.forEach((value, key) => (breakpoints[key] = value));
    texeraGraph
      .getAllOperators()
      .forEach(
        op => (operatorPositions[op.operatorID] = this.texeraGraph.elementPositionMap?.get(op.operatorID) as Point)
      );
    commentBoxes.forEach(
      commentBox =>
        (commentBox.commentBoxPosition = this.texeraGraph.elementPositionMap?.get(commentBox.commentBoxID) as Point)
    );
    const workflowContent: WorkflowContent = {
      operators,
      operatorPositions,
      links,
      groups,
      breakpoints,
      commentBoxes,
    };
    return workflowContent;
  }

  public getWorkflow(): Workflow {
    return {
      ...this.workflowMetadata,
      ...{ content: this.getWorkflowContent() },
    };
  }

  public addComment(comment: Comment, commentBoxID: string): void {
    this.texeraGraph.addCommentToCommentBox(comment, commentBoxID);
  }

  public deleteComment(creatorID: number, creationTime: string, commentBoxID: string): void {
    this.texeraGraph.deleteCommentFromCommentBox(creatorID, creationTime, commentBoxID);
  }

  public editComment(creatorID: number, creationTime: string, commentBoxID: string, newContent: string): void {
    this.texeraGraph.editCommentInCommentBox(creatorID, creationTime, commentBoxID, newContent);
  }

  public setTempWorkflow(workflow: Workflow): void {
    this.tempWorkflow = workflow;
  }

  public resetTempWorkflow(): void {
    this.tempWorkflow = undefined;
  }

  public getTempWorkflow(): Workflow | undefined {
    return this.tempWorkflow;
  }

  public setOperatorCustomName(operatorId: string, newDisplayName: string, userFriendlyName: string): void {
    this.getTexeraGraph().changeOperatorDisplayName(operatorId, newDisplayName);
  }

  // TODO
  public setWorkflowName(name: string): void {
    this.workflowMetadata.name = name.trim().length > 0 ? name : WorkflowActionService.DEFAULT_WORKFLOW_NAME;
    this.workflowMetadataChangeSubject.next();
  }

  public resetAsNewWorkflow() {
    this.destroyYModel();
    this.reloadWorkflow(undefined);
  }

  public highlightOperators(multiSelect: boolean, ...ops: string[]): void {
    this.getJointGraphWrapper().setMultiSelectMode(multiSelect);
    this.getJointGraphWrapper().highlightOperators(...ops);
  }

  public unhighlightOperators(...ops: string[]): void {
    this.getJointGraphWrapper().unhighlightOperators(...ops);
  }

  public highlightLinks(multiSelect: boolean, ...links: string[]): void {
    this.getJointGraphWrapper().setMultiSelectMode(multiSelect);
    this.getJointGraphWrapper().highlightLinks(...links);
  }

  public unhighlightLinks(...links: string[]): void {
    this.getJointGraphWrapper().unhighlightLinks(...links);
  }

  public disableOperators(ops: readonly string[]): void {
    this.texeraGraph.yDoc.transact(()=> {
      ops.forEach(op => {
        this.getTexeraGraph().disableOperator(op);
      });
    });
  }

  public enableOperators(ops: readonly string[]): void {
    this.texeraGraph.yDoc.transact(()=> {
      ops.forEach(op => {
        this.getTexeraGraph().enableOperator(op);
      });
    });
  }

  public cacheOperators(ops: readonly string[]): void {
    this.texeraGraph.yDoc.transact(()=> {
      ops.forEach(op => {
        this.getTexeraGraph().cacheOperator(op);
      });
    });
  }

  public unCacheOperators(ops: readonly string[]): void {
    this.texeraGraph.yDoc.transact(()=> {
      ops.forEach(op => {
        this.getTexeraGraph().unCacheOperator(op);
      });
    });
  }

  // Internal methods

  private addCommentBoxInternal(commentBox: CommentBox): void {
    this.texeraGraph.addCommentBox(commentBox);
  }

  private addOperatorsInternal(operatorsAndPositions: readonly { operator: OperatorPredicate; point: Point }[]): void {
    // TODO: (Maybe?) wrap this callback inside TexeraGraph as an API.
    this.texeraGraph.yDoc?.transact(()=> {
      for (let i = 0; i < operatorsAndPositions.length; i++) {
        let operator = operatorsAndPositions[i].operator;
        // check that the operator doesn't exist
        this.texeraGraph.assertOperatorNotExists(operator.operatorID);
        // check that the operator type exists
        if (!this.operatorMetadataService.operatorTypeExists(operator.operatorType)) {
          throw new Error(`operator type ${operator.operatorType} is invalid`);
        }
        // add operator to texera graph
        this.texeraGraph.elementPositionMap?.set(operator.operatorID, operatorsAndPositions[i].point);
        this.texeraGraph.addOperator(operator);
      }
    });
  }

  private deleteOperatorInternal(operatorID: string): void {
    this.texeraGraph.assertOperatorExists(operatorID);
    this.texeraGraph.deleteOperator(operatorID);
  }

  private addLinksInternal(links: readonly OperatorLink[]): void {
    this.texeraGraph.yDoc?.transact(()=> {
      for (let i = 0; i < links.length; i++) {
        let link = links[i];
        this.texeraGraph.assertLinkNotExists(link);
        this.texeraGraph.assertLinkIsValid(link);
        this.texeraGraph.addLink(link);
      }
    });
  }

  private deleteLinkWithIDInternal(linkID: string): void {
    this.texeraGraph.assertLinkWithIDExists(linkID);
    this.texeraGraph.deleteLinkWithID(linkID);
  }

  // use this to modify properties
  private setOperatorPropertyInternal(operatorID: string, newProperty: object) {
    this.texeraGraph.setOperatorProperty(operatorID, newProperty);
  }

  private deleteCommentBoxInternal(commentBoxID: string): void {
    this.texeraGraph.assertCommentBoxExists(commentBoxID);
    this.texeraGraph.deleteCommentBox(commentBoxID);
  }

  private setLinkBreakpointInternal(linkID: string, newBreakpoint: Breakpoint | undefined): void {
    this.texeraGraph.setLinkBreakpoint(linkID, newBreakpoint);
  }


  // TODO: delete old imp

  private listenToRemoteChange(): void {
    this.workflowCollabService.getChangeStream().subscribe(message => {
      if (message.type === "execute") {
        this.workflowCollabService.handleRemoteChange(() => {
          const func: commandFuncs = message.action;
          const previousModificationEnabledStatus = this.workflowModificationEnabled;
          this.enableWorkflowModification();
          (this[func] as any).apply(this, message.parameters);
          if (!previousModificationEnabledStatus) this.disableWorkflowModification();
        });
      }
    });
  }

  /**
   * Handles lock status change.
   */
  private listenToLockChange(): void {
    this.workflowCollabService.getLockStatusStream().subscribe(isLockGranted => {
      if (this.lockListenEnabled) {
        if (isLockGranted) this.enableWorkflowModification();
        else this.disableWorkflowModification();
      }
    });
  }

  /**
   * Used after temporarily blocking lock changes.
   */
  public syncLock(): void {
    if (this.lockListenEnabled) {
      if (this.workflowCollabService.isLockGranted()) {
        this.enableWorkflowModification();
      } else {
        this.disableWorkflowModification();
      }
    }
  }
}
