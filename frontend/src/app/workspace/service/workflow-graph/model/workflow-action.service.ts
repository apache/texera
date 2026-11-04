/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Injectable } from "@angular/core";

import * as joint from "jointjs";
import * as Y from "yjs";
import { isEqual } from "lodash-es";
import { BehaviorSubject, merge, Observable, Subject } from "rxjs";
import {
  ExecutionMode,
  getDefaultFormBinding,
  FormBindingConfig,
  Workflow,
  WorkflowContent,
  WorkflowSettings,
} from "../../../../common/type/workflow";
import { WorkflowMetadata } from "../../../../dashboard/type/workflow-metadata.interface";
import {
  Comment,
  CommentBox,
  LogicalPort,
  OperatorLink,
  OperatorPredicate,
  Point,
  PortDescription,
} from "../../../types/workflow-common.interface";
import { JointUIService } from "../../joint-ui/joint-ui.service";
import { OperatorMetadataService } from "../../operator-metadata/operator-metadata.service";
import { UndoRedoService } from "../../undo-redo/undo-redo.service";
import { WorkflowUtilService } from "../util/workflow-util.service";
import { JointGraphWrapper } from "./joint-graph-wrapper";
import { SyncTexeraModel } from "./sync-texera-model";
import { WorkflowGraph, WorkflowGraphReadonly } from "./workflow-graph";
import { filter } from "rxjs/operators";
import { isDefined } from "../../../../common/util/predicate";
import { User } from "../../../../common/type/user";
import { SharedModelChangeHandler } from "./shared-model-change-handler";
import { ContentMetaKey, ContentMetaValue } from "./shared-model";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { WorkflowColor } from "../../../types/workflow-color";

/** A content-meta seed waiting to land (see seedContentMeta): its identity, and how to stop
 *  listening for the sync it waits on once it is superseded or cancelled. */
interface ContentMetaSeed {
  detach?: () => void;
}

export const DEFAULT_WORKFLOW_NAME = "Untitled Workflow";
export const DEFAULT_WORKFLOW = {
  name: DEFAULT_WORKFLOW_NAME,
  description: undefined,
  wid: 0,
  creationTime: undefined,
  lastModifiedTime: undefined,
  isPublished: 0,
  readonly: false,
};

/**
 *
 * WorkflowActionService exposes functions (actions) to modify the workflow graph model of Texera,
 *  such as addOperator, deleteOperator, addLink, deleteLink, etc.
 *
 * WorkflowActionService bundles a series of steps into atomic actions, like adding an operator and its outgoing link.
 *  It also checks the validity of these actions, for example, throws an error if deleting a nonsexist operator.
 *
 * All changes(actions) to the workflow graph should be called through WorkflowActionService,
 *
 * With the introduction of shared editing using yjs, WorkflowActionService will only make changes to its internal
 *  <code>{@link WorkflowGraph}</code>, and <code>{@link SharedModelChangeHandler}</code> will listen to changes to the
 *  WorkflowGraph to update JointGraph.
 *
 * For an overview of the services and updates with shared editing in WorkflowGraphModule, see workflow-graph-design.md.
 *
 */

@Injectable({
  providedIn: "root",
})
export class WorkflowActionService {
  private readonly texeraGraph: WorkflowGraph;
  private readonly jointGraph: joint.dia.Graph;
  private readonly jointGraphWrapper: JointGraphWrapper;
  private readonly syncTexeraModel: SyncTexeraModel;
  private readonly sharedModelChangeHandler: SharedModelChangeHandler;
  // variable to temporarily hold the current workflow to switch view to a particular version
  private tempWorkflow?: Workflow;
  private workflowModificationEnabled = true;
  private enableModificationStream = new BehaviorSubject<boolean>(true);
  private highlightingEnabled = false;
  private centerPoint: Point = { x: 0, y: 0 };

  private workflowMetadata: WorkflowMetadata;
  private workflowMetadataChangeSubject: Subject<WorkflowMetadata> = new Subject<WorkflowMetadata>();
  private resultPanelOpenSubject = new Subject<boolean>();
  public readonly resultPanelOpen$: Observable<boolean> = this.resultPanelOpenSubject.asObservable();

  private workflowResetSubject = new Subject<void>();

  // The Form View definition. Presentation, not structure, but it still lives in the shared
  // doc (shared-model contentMetaMap, key "formBinding") so a co-editor sees it live and no
  // collaborator's whole-content autosave overwrites it with a stale copy. workflowSettings
  // is kept there too (key "settings") for the same reason.
  private formBindingChangeSubject = new Subject<FormBindingConfig>();
  public readonly formBindingChanged$: Observable<FormBindingConfig> = this.formBindingChangeSubject.asObservable();
  // A settings change, this client's or a co-editor's, for the settings panel to refresh from. Not
  // part of workflowChanged, unlike the form definition (see observeContentMeta).
  private workflowSettingsChangeSubject = new Subject<WorkflowSettings>();
  public readonly workflowSettingsChanged$: Observable<WorkflowSettings> =
    this.workflowSettingsChangeSubject.asObservable();
  // The shared content-map observer, tracked so re-attaching on a new doc detaches the old one.
  private contentMetaObserver?: (event: Y.YMapEvent<ContentMetaValue | null>) => void;
  private observedContentMetaMap?: Y.Map<ContentMetaValue | null>;
  // Per document, all reset when a new one is loaded (see observeContentMeta): database copies,
  // and edits, waiting for the document's first sync (see seedContentMeta), read from meanwhile; the seed
  // each key is waiting on, which a later reload replaces and a local edit of the key cancels;
  // the keys a seed is writing right now, which is not an edit and so is not announced (an edit
  // held for the sync is announced when it is made, see writeContentMeta); the keys
  // whose entry came from the room, which the first seed of the key yields to; and the keys seeded
  // on this document already, whose next seed is a reload that replaces the value.
  private pendingContentMeta = new Map<ContentMetaKey, { value: ContentMetaValue; edit: boolean }>();
  private contentMetaSeeds = new Map<ContentMetaKey, ContentMetaSeed>();
  private seedingContentMetaKeys = new Set<ContentMetaKey>();
  private roomOwnedContentMeta = new Set<ContentMetaKey>();
  private seededContentMetaKeys = new Set<ContentMetaKey>();

  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private jointUIService: JointUIService,
    private undoRedoService: UndoRedoService,
    private workflowUtilService: WorkflowUtilService,
    private config: GuiConfigService
  ) {
    this.texeraGraph = new WorkflowGraph();
    this.jointGraph = new joint.dia.Graph();
    this.jointGraphWrapper = new JointGraphWrapper(this.jointGraph);

    this.syncTexeraModel = new SyncTexeraModel(this.texeraGraph, this.jointGraphWrapper);
    this.sharedModelChangeHandler = new SharedModelChangeHandler(
      this.texeraGraph,
      this.jointGraph,
      this.jointGraphWrapper,
      this.jointUIService
    );
    this.sharedModelChangeHandler.setConfigService(this.config);
    this.workflowMetadata = DEFAULT_WORKFLOW;
    this.undoRedoService.setUndoManager(this.texeraGraph.sharedModel.undoManager);

    // Watch the shared content map, re-attaching whenever the shared model is recreated
    // (opening another workflow), the same way SharedModelChangeHandler re-attaches its
    // graph observers. A formBinding change from a local edit or a co-editor is republished
    // on formBindingChanged$ so the Form View re-renders and the existing autosave picks it
    // up. The reload seed is skipped -- like the graph seed -- so opening a workflow is not
    // announced as an edit and does not save on every open.
    this.observeContentMeta();
    this.texeraGraph.newYDocLoadedSubject.subscribe(() => this.observeContentMeta());

    this.handleJointElementDrag();
  }

  private observeContentMeta(): void {
    // Detach the observer from the previous shared model before attaching to the new one, so
    // re-attaching on each opened workflow does not stack listeners on the same map.
    this.observedContentMetaMap?.unobserve(this.contentMetaObserver!);
    // A new document starts with no seed state: nothing in it came from the previous room, no key
    // has been seeded on it yet, and whatever copy the previously open workflow left waiting is
    // not this one's -- dropped here, so it cannot leak into the next save of a workflow opened
    // (or a blank one started) on this document.
    for (const key of [...this.contentMetaSeeds.keys()]) {
      this.cancelContentMetaSeed(key); // drops the copy it holds, and stops its listening
    }
    this.roomOwnedContentMeta.clear();
    this.seededContentMetaKeys.clear();
    const contentMetaMap = this.texeraGraph.sharedModel.contentMetaMap;
    this.contentMetaObserver = event => {
      if (!event.transaction.local) {
        // An entry that arrived from the room belongs to the room, a co-editor's value or its
        // cleared mark alike: the seed a workflow is opened with yields to it (see seedContentMeta).
        // The first sync delivers the room's state as a remote transaction, which is how an
        // opening client learns what the room already holds.
        for (const key of event.changes.keys.keys()) {
          this.roomOwnedContentMeta.add(key as ContentMetaKey);
        }
      }
      if (this.jointGraphWrapper.getReloadingWorkflow()) {
        return;
      }
      if (event.changes.keys.has("formBinding") && this.announces("formBinding")) {
        this.formBindingChangeSubject.next(this.getFormBinding());
      }
      // Settings are announced the same way, so a co-editor's change reaches the settings panel,
      // which refreshes from this stream, instead of sitting stale until an unrelated edit. Not
      // through workflowChanged, though: the panel persists a settings change itself, and the
      // autosave behind workflowChanged would save it a second time, cutting a second version.
      if (event.changes.keys.has("settings") && this.announces("settings")) {
        this.workflowSettingsChangeSubject.next(this.getWorkflowSettings());
      }
    };
    contentMetaMap.observe(this.contentMetaObserver);
    this.observedContentMetaMap = contentMetaMap;
  }

  /** Whether a change to a key in the shared map is announced: not a seed's own write, which is an
   *  open and not an edit; and not while an edit held for the first sync owns the key -- that edit
   *  was announced when it was made, reads answer it, and its landing is a seed's write. */
  private announces(key: ContentMetaKey): boolean {
    return !this.seedingContentMetaKeys.has(key) && !this.pendingContentMeta.get(key)?.edit;
  }

  private getDefaultSettings(): WorkflowSettings {
    return {
      dataTransferBatchSize: this.config.env.defaultDataTransferBatchSize,
      executionMode: this.config.env.defaultExecutionMode,
    };
  }

  /**
   * Workflow modification lock interface (allows or prevents commands that would modify the workflow graph).
   */
  public enableWorkflowModification() {
    if (!this.workflowMetadata.readonly && !this.workflowModificationEnabled) {
      this.workflowModificationEnabled = true;
      this.enableModificationStream.next(true);
      this.undoRedoService.enableWorkFlowModification();
    }
  }

  public disableWorkflowModification() {
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

  /**
   * Gets joint paper, mainly used for co-editor presence.
   */
  public getJointGraph(): joint.dia.Graph {
    return this.jointGraph;
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

  public getCenterPoint(): Point {
    return this.centerPoint;
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //                                      Below are all the actions available.                                        //
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    // check that the operator doesn't exist
    this.texeraGraph.assertOperatorNotExists(operator.operatorID);
    // check that the operator type exists
    if (!this.operatorMetadataService.operatorTypeExists(operator.operatorType)) {
      throw new Error(`operator type ${operator.operatorType} is invalid`);
    }

    this.texeraGraph.bundleActions(() => {
      // add operator to texera graph
      this.texeraGraph.addOperator(operator);
      this.texeraGraph.sharedModel.elementPositionMap?.set(operator.operatorID, point);
    });
  }

  /**
   * Deletes an operator from the workflow graph, also deleting associated links.
   * Throws an Error if the operator ID doesn't exist in the Workflow Graph.
   * @param operatorID
   */
  public deleteOperator(operatorID: string): void {
    this.unhighlightOperators(operatorID);
    this.texeraGraph.bundleActions(() => {
      this.getTexeraGraph()
        .getAllLinks()
        .filter(link => link.source.operatorID === operatorID || link.target.operatorID === operatorID)
        .forEach(link => this.deleteLinkWithID(link.linkID));
      this.texeraGraph.assertOperatorExists(operatorID);
      this.texeraGraph.deleteOperator(operatorID);
      if (this.texeraGraph.sharedModel.elementPositionMap.has(operatorID))
        this.texeraGraph.sharedModel.elementPositionMap.delete(operatorID);
    });
  }

  public addPort(operatorID: string, isInput: boolean, disallowMultiInputs?: boolean): void {
    const operator = this.texeraGraph.getOperator(operatorID);
    // TODO: use uniform serde to calculate the portID
    const prefix = isInput ? "input-" : "output-";
    let suffix = isInput ? operator.inputPorts.length : operator.outputPorts.length;
    let portID = prefix + suffix;
    // make sure portID has no conflict
    while (operator.inputPorts.find(p => p.portID === portID) !== undefined) {
      suffix += 1;
      portID = prefix + suffix;
    }

    const port: PortDescription = {
      portID,
      displayName: "",
      disallowMultiInputs,
      isDynamicPort: true,
      dependencies: [],
    };

    if (!operator.dynamicInputPorts && isInput) {
      throw new Error(`operator ${operatorID} does not have dynamic input ports`);
    }
    if (!operator.dynamicOutputPorts && !isInput) {
      throw new Error(`operator ${operatorID} does not have dynamic output ports`);
    }
    if (!isInput && disallowMultiInputs !== undefined) {
      throw new Error("error: disallowMultiInputs property of an output port should not be specified");
    }

    this.texeraGraph.bundleActions(() => {
      // add port to the operator
      this.texeraGraph.assertOperatorExists(operatorID);
      this.texeraGraph.addPort(operatorID, port, isInput);
    });
  }

  public removePort(operatorID: string, isInput: boolean): void {
    this.texeraGraph.bundleActions(() => {
      this.texeraGraph.assertOperatorExists(operatorID);
      this.texeraGraph.removePort(operatorID, isInput);
    });
  }

  /**
   * Unhighlight currently selected elements and adds a comment box.
   * @param commentBox
   */
  public addCommentBox(commentBox: CommentBox): void {
    const currentHighlights = this.jointGraphWrapper.getCurrentHighlights();
    this.jointGraphWrapper.unhighlightElements(currentHighlights);
    this.jointGraphWrapper.setMultiSelectMode(false);
    this.texeraGraph.bundleActions(() => {
      this.texeraGraph.addCommentBox({ ...commentBox, comments: [] });
      for (const comment of commentBox.comments) {
        this.addComment(comment, commentBox.commentBoxID);
      }
    });
  }

  /**
   * Adds given operators and links to the workflow graph.
   * @param operatorsAndPositions
   * @param links
   * @param commentBoxes
   */
  public addOperatorsAndLinks(
    operatorsAndPositions: readonly { op: OperatorPredicate; pos: Point }[],
    links?: readonly OperatorLink[],
    commentBoxes?: ReadonlyArray<CommentBox>
  ): void {
    // remember currently highlighted operators and groups
    const currentHighlights = this.jointGraphWrapper.getCurrentHighlights();
    // unhighlight previous highlights
    this.jointGraphWrapper.unhighlightElements(currentHighlights);
    this.jointGraphWrapper.setMultiSelectMode(operatorsAndPositions.length > 1);
    this.texeraGraph.bundleActions(() => {
      for (const operatorsAndPosition of operatorsAndPositions) {
        this.addOperator(operatorsAndPosition.op, operatorsAndPosition.pos);
      }
      if (links) {
        for (let i = 0; i < links.length; i++) {
          this.addLink(links[i]);
        }
      }
      if (isDefined(commentBoxes)) {
        commentBoxes.forEach(commentBox => this.addCommentBox(commentBox));
      }
    });
  }

  /**
   * Deletes a comment box.
   * @param commentBoxID
   */
  public deleteCommentBox(commentBoxID: string): void {
    this.texeraGraph.assertCommentBoxExists(commentBoxID);
    this.texeraGraph.deleteCommentBox(commentBoxID);
  }

  public setCommentBoxColor(commentBoxID: string, color: WorkflowColor | undefined): void {
    if (!this.checkWorkflowModificationEnabled() || this.workflowMetadata.readonly) {
      return;
    }
    this.texeraGraph.setCommentBoxColor(commentBoxID, color);
  }

  /**
   * Deletes given operators and links from the workflow graph.
   * @param operatorIDs
   */
  public deleteOperatorsAndLinks(operatorIDs: readonly string[]): void {
    const operatorIDsCopy = Array.from(new Set(operatorIDs));
    this.texeraGraph.bundleActions(() => {
      // delete links related to the deleted operator
      this.getTexeraGraph()
        .getAllLinks()
        .filter(
          link => operatorIDsCopy.includes(link.source.operatorID) || operatorIDsCopy.includes(link.target.operatorID)
        )
        .forEach(link => this.deleteLinkWithID(link.linkID));
      operatorIDsCopy.forEach(operatorID => {
        this.deleteOperator(operatorID);
      });
    });
  }

  /**
   * Handles the auto layout function
   *
   */
  // Originally: drag Operator
  public autoLayoutWorkflow(): void {
    // This also changes element positions, but we handle this separately.
    this.texeraGraph.bundleActions(() => {
      this.undoRedoService.setListenJointCommand(false);
      this.jointGraphWrapper.autoLayoutJoint();
      for (const operator of this.texeraGraph.getAllOperators()) {
        const operatorID = operator.operatorID;
        const newPosition = this.jointGraphWrapper.getElementPosition(operatorID);
        if (this.texeraGraph.sharedModel.elementPositionMap.get(operatorID) !== newPosition) {
          this.texeraGraph.sharedModel.elementPositionMap.set(operatorID, newPosition);
        }
      }
      for (const commentBox of this.texeraGraph.getAllCommentBoxes()) {
        const commentBoxID = commentBox.commentBoxID;
        const newPosition = this.jointGraphWrapper.getElementPosition(commentBoxID);
        if (this.texeraGraph.sharedModel.elementPositionMap.get(commentBoxID) !== newPosition) {
          this.texeraGraph.sharedModel.elementPositionMap.set(commentBoxID, newPosition);
        }
      }
      this.undoRedoService.setListenJointCommand(true);
    });
  }

  /**
   * Calculating the top-left (minimum x and y) position of all operators
   */
  public calculateTopLeftOperatorPosition(): void {
    this.texeraGraph.bundleActions(() => {
      this.undoRedoService.setListenJointCommand(false);
      const allOperators = this.getTexeraGraph().getAllOperators();
      if (allOperators.length === 0) return;

      let minX = Infinity;
      let minY = Infinity;

      for (const operator of allOperators) {
        const operatorID = operator.operatorID;
        const position = this.jointGraphWrapper.getElementPosition(operatorID);

        if (position.x < minX) {
          minX = position.x;
        }
        if (position.y < minY) {
          minY = position.y;
        }
      }

      this.centerPoint = { x: minX, y: minY };

      this.undoRedoService.setListenJointCommand(true);
    });
  }

  /**
   * Adds a link to the workflow graph
   * Throws an Error if the link ID or the link with same source and target already exists.
   * @param link
   */
  public addLink(link: OperatorLink): void {
    this.texeraGraph.assertLinkNotExists(link);
    this.texeraGraph.assertLinkIsValid(link);
    this.texeraGraph.addLink(link);
  }

  /**
   * Deletes a link with the linkID from the workflow graph
   * Throws an Error if the linkID doesn't exist in the workflow graph.
   * @param linkID
   */
  public deleteLinkWithID(linkID: string): void {
    this.texeraGraph.assertLinkWithIDExists(linkID);
    this.unhighlightLinks(linkID);
    this.texeraGraph.deleteLinkWithID(linkID);
  }

  /**
   * Deletes a link based on the source and target port.
   * @param source
   * @param target
   */
  public deleteLink(source: LogicalPort, target: LogicalPort): void {
    const link = this.getTexeraGraph().getLink(source, target);
    this.deleteLinkWithID(link.linkID);
  }

  /**
   * Replaces the property object with a new one. This is a coarse-grained method for shared-editing.
   * @param operatorID
   * @param newProperty
   */
  public setOperatorProperty(operatorID: string, newProperty: object): void {
    this.texeraGraph.bundleActions(() => {
      this.texeraGraph.setOperatorProperty(operatorID, newProperty);
    });
  }

  public setPortProperty(operatorPortID: LogicalPort, newProperty: object) {
    this.texeraGraph.bundleActions(() => {
      this.texeraGraph.setPortProperty(operatorPortID, newProperty);
    });
  }

  public addComment(comment: Comment, commentBoxID: string): void {
    this.texeraGraph.bundleActions(() => {
      this.texeraGraph.addCommentToCommentBox(comment, commentBoxID);
    });
  }

  public deleteComment(creatorID: number, creationTime: string, commentBoxID: string): void {
    this.texeraGraph.bundleActions(() => {
      this.texeraGraph.deleteCommentFromCommentBox(creatorID, creationTime, commentBoxID);
    });
  }

  public editComment(creatorID: number, creationTime: string, commentBoxID: string, newContent: string): void {
    this.texeraGraph.bundleActions(() => {
      this.texeraGraph.editCommentInCommentBox(creatorID, creationTime, commentBoxID, newContent);
    });
  }

  public highlightOperators(multiSelect: boolean, ...ops: string[]): void {
    this.getJointGraphWrapper().setMultiSelectMode(multiSelect);
    this.getJointGraphWrapper().highlightOperators(...ops);
    this.getTexeraGraph().updateSharedModelAwareness(
      "highlighted",
      this.jointGraphWrapper.getCurrentHighlightedOperatorIDs()
    );
  }

  public unhighlightOperators(...ops: string[]): void {
    this.getJointGraphWrapper().unhighlightOperators(...ops);
    this.getTexeraGraph().updateSharedModelAwareness(
      "highlighted",
      this.jointGraphWrapper.getCurrentHighlightedOperatorIDs()
    );
  }

  public highlightLinks(multiSelect: boolean, ...links: string[]): void {
    this.getJointGraphWrapper().setMultiSelectMode(multiSelect);
    this.getJointGraphWrapper().highlightLinks(...links);
  }

  public unhighlightLinks(...links: string[]): void {
    this.getJointGraphWrapper().unhighlightLinks(...links);
  }

  public highlightCommentBoxes(multiSelect: boolean, ...commentBoxIDs: string[]): void {
    this.getJointGraphWrapper().setMultiSelectMode(multiSelect);
    this.getJointGraphWrapper().highlightCommentBoxes(...commentBoxIDs);
  }

  public highlightElements(multiSelect: boolean, ...elementIDs: string[]): void {
    this.getJointGraphWrapper().setMultiSelectMode(multiSelect);
    this.highlightOperators(multiSelect, ...elementIDs.filter(id => this.texeraGraph.hasOperator(id)));
    this.highlightLinks(multiSelect, ...elementIDs.filter(id => this.texeraGraph.hasLinkWithID(id)));
    this.highlightCommentBoxes(multiSelect, ...elementIDs.filter(id => this.texeraGraph.hasCommentBox(id)));
  }

  public highlightPorts(multiSelect: boolean, ...ports: LogicalPort[]): void {
    this.getJointGraphWrapper().setMultiSelectMode(multiSelect);
    this.getJointGraphWrapper().highlightPorts(...ports);
  }

  public unhighlightPorts(...ports: LogicalPort[]): void {
    this.getJointGraphWrapper().unhighlightPorts(...ports);
  }

  public disableOperators(ops: readonly string[]): void {
    this.texeraGraph.bundleActions(() => {
      ops.forEach(op => {
        this.getTexeraGraph().disableOperator(op);
      });
    });
  }

  public enableOperators(ops: readonly string[]): void {
    this.texeraGraph.bundleActions(() => {
      ops.forEach(op => {
        this.getTexeraGraph().enableOperator(op);
      });
    });
  }

  public markReuseResults(ops: readonly string[]): void {
    this.texeraGraph.bundleActions(() => {
      ops.forEach(op => {
        this.getTexeraGraph().markReuseResult(op);
      });
    });
  }

  public removeMarkReuseResults(ops: readonly string[]): void {
    this.texeraGraph.bundleActions(() => {
      ops.forEach(op => {
        this.getTexeraGraph().removeMarkReuseResult(op);
      });
    });
  }

  public setViewOperatorResults(ops: readonly string[]): void {
    this.texeraGraph.bundleActions(() => {
      ops.forEach(op => {
        this.getTexeraGraph().setViewOperatorResult(op);
      });
    });
  }

  public unsetViewOperatorResults(ops: readonly string[]): void {
    this.texeraGraph.bundleActions(() => {
      ops.forEach(op => {
        this.getTexeraGraph().unsetViewOperatorResult(op);
      });
    });
  }

  public setOperatorVersion(operatorId: string, newVersion: string): void {
    this.getTexeraGraph().changeOperatorVersion(operatorId, newVersion);
  }

  public openResultPanel(): void {
    this.resultPanelOpenSubject.next(true);
  }

  public closeResultPanel(): void {
    this.resultPanelOpenSubject.next(false);
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //                             Below are workflow-level and metadata-related methods.                               //
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Refreshes the internal shared model and joins a new shared-editing room.
   *
   * This method also updates the undo manager.
   * @param workflowId optional, but needed if you want to join shared editing.
   * @param user optional, but needed if you want to have user presence.
   */
  public setNewSharedModel(workflowId?: number, user?: User) {
    this.texeraGraph.loadNewYModel(workflowId, user, this.config.env.productionSharedEditingServer);
    this.undoRedoService.setUndoManager(this.texeraGraph.sharedModel.undoManager);
  }

  /**
   * Destroys shared-editing related structures and quits the shared editing session.
   */
  public destroySharedModel(): void {
    this.texeraGraph.destroyYModel();
  }

  /**
   * Whether this page already holds `workflowId` open: the shared document is in that workflow's
   * co-editing room, so the graph, the undo history and the room membership are the live ones.
   *
   * The operator canvas and the Form View are two views of one open workflow and hand the session
   * over between them rather than each building its own. The arriving view asks this before
   * loading: seeding a second document for the same workflow would leave the room and rejoin it,
   * which is what used to leave a ghost of yourself in the co-editor list.
   *
   * A workflow that was open and has since been left does not answer true here, and the reason is
   * worth stating because it is not local: `destroyYModel` destroys the document but keeps the
   * object, `wid` and all. What clears it is `clearWorkflow` going on to `reloadWorkflow(undefined)`,
   * which seeds a fresh model with no `wid`. Were that to stop happening, a canvas re-entered from
   * the dashboard would attach to a destroyed document instead of loading. Pinned by a test.
   */
  public hasWorkflowOpen(workflowId: number | undefined): boolean {
    return !!workflowId && this.texeraGraph.sharedModel.wid === workflowId;
  }

  /**
   * The workflow whose co-editing room the shared document is in, or undefined when it is in none:
   * a brand-new canvas, or a workflow created in this session, whose first autosave gave the
   * metadata an id while the document stayed in the private room it was seeded with.
   *
   * This, and not the metadata's id, is what both views key the hand-over on. The departing view
   * asks whether it is leaving this workflow; the arriving view asks whether this workflow is
   * already open. Keyed on different ids, the two answered differently for a workflow created in
   * this session -- the canvas kept the session, the Form View declined it and reloaded -- so both
   * ask about the room, and such a workflow is simply rebuilt on its first switch, as it is today.
   */
  public getOpenWorkflowId(): number | undefined {
    return this.texeraGraph.sharedModel.wid || undefined;
  }

  /**
   * Announce the metadata already in hand, unchanged, for a view that arrived on a workflow that
   * was already open.
   *
   * `workflowMetaDataChanged()` is a plain Subject, so it carries no current value: a subscriber
   * that arrives after the metadata was set hears nothing until the next change. Everything a
   * view puts on screen about the workflow -- its name and id in the menu, the computing unit it
   * last ran on, whether this user may write to it -- is learnt only from that stream, and a view
   * handed an open workflow never sets the metadata, because it is already right. Without this
   * they would each sit at their initial value until the next edit happened to save.
   *
   * `setWorkflowMetadata` cannot do the job: it returns early for the value it already holds.
   */
  public republishWorkflowMetadata(): void {
    this.workflowMetadataChangeSubject.next(this.workflowMetadata);
  }

  /**
   * Reload the given workflow, update workflowMetadata and workflowContent.
   * This method is based on the assumption that this is on a new SharedModel.
   *
   * <b>Warning: this resets the workflow but not the SharedModel, so make sure to quit the shared-editing session
   * (<code>{@link destroySharedModel}</code>) before using this method.</b>
   */
  public reloadWorkflow(
    workflow: Readonly<Workflow> | undefined,
    asyncRendering = this.config.env.asyncRenderingEnabled,
    restoreViewport = true
  ): void {
    this.jointGraphWrapper.setReloadingWorkflow(true);
    this.jointGraphWrapper.jointGraphContext.withContext({ async: asyncRendering }, () => {
      this.setWorkflowMetadata(workflow);
      // remove the existing operators on the paper currently

      this.deleteOperatorsAndLinks(
        this.getTexeraGraph()
          .getAllOperators()
          .map(op => op.operatorID)
      );

      this.getTexeraGraph()
        .getAllCommentBoxes()
        .forEach(commentBox => this.deleteCommentBox(commentBox.commentBoxID));

      this.jointGraphWrapper.jointGraph.clear();

      if (workflow === undefined) {
        // A blank workflow starts on a fresh document, which carries no settings and no Form View
        // definition; a copy the previously open workflow left waiting goes with the old document
        // (see observeContentMeta), so neither can leak into this workflow's next save.
        this.setNewSharedModel();
        return;
      }

      const workflowContent: WorkflowContent = workflow.content;
      this.hydrateSettings(workflowContent.settings);
      this.hydrateFormBinding(workflowContent.formBinding);

      let operatorsAndPositions: { op: OperatorPredicate; pos: Point }[] = [];
      workflowContent.operators.forEach(op => {
        const opPosition = workflowContent.operatorPositions[op.operatorID];
        if (!opPosition) {
          throw new Error(`position error: ${op.operatorID}`);
        }
        operatorsAndPositions.push({ op: op, pos: opPosition });
      });

      const links: OperatorLink[] = workflowContent.links;

      const commentBoxes = workflowContent.commentBoxes;

      operatorsAndPositions = this.updateOperatorVersions(operatorsAndPositions);

      this.addOperatorsAndLinks(operatorsAndPositions, links, commentBoxes);

      // restore the view point
      if (restoreViewport) {
        this.getJointGraphWrapper().restoreDefaultZoomAndOffset();
      }
    });
    this.jointGraphWrapper.setReloadingWorkflow(false);

    // After reloading a workflow, need to clear undo/redo stacks because some of the actions involved in reloading
    // may remain in the undo manager.

    this.undoRedoService.clearUndoStack();
    this.undoRedoService.clearRedoStack();
  }

  public workflowChanged(): Observable<unknown> {
    return merge(
      this.getTexeraGraph().getOperatorAddStream(),
      this.getTexeraGraph().getOperatorDeleteStream(),
      this.getTexeraGraph().getLinkAddStream(),
      this.getTexeraGraph().getLinkDeleteStream(),
      this.getTexeraGraph().getPortAddedOrDeletedStream(),
      this.getTexeraGraph().getOperatorPropertyChangeStream(),
      this.getJointGraphWrapper().getElementPositionChangeEvent(),
      this.getTexeraGraph().getDisabledOperatorsChangedStream(),
      this.getTexeraGraph().getCommentBoxAddStream(),
      this.getTexeraGraph().getCommentBoxDeleteStream(),
      this.getTexeraGraph().getCommentBoxAddCommentStream(),
      this.getTexeraGraph().getCommentBoxDeleteCommentStream(),
      this.getTexeraGraph().getCommentBoxEditCommentStream(),
      this.getTexeraGraph().getCommentBoxColorChangedStream(),
      this.getTexeraGraph().getViewResultOperatorsChangedStream(),
      this.getTexeraGraph().getReuseCacheOperatorsChangedStream(),
      this.getTexeraGraph().getOperatorDisplayNameChangedStream(),
      this.getTexeraGraph().getOperatorVersionChangedStream(),
      this.getTexeraGraph().getPortDisplayNameChangedSubject(),
      this.getTexeraGraph().getPortPropertyChangedStream(),
      this.formBindingChanged$,
      this.workflowResetSubject.asObservable()
    );
  }

  public workflowMetaDataChanged(): Observable<WorkflowMetadata> {
    return this.workflowMetadataChangeSubject.asObservable();
  }

  /**
   * This is not included in shared editing.
   * @param workflowMetaData
   */
  public setWorkflowMetadata(workflowMetaData: WorkflowMetadata | undefined): void {
    if (this.workflowMetadata === workflowMetaData) {
      return;
    }

    const newMetadata = workflowMetaData === undefined ? DEFAULT_WORKFLOW : workflowMetaData;
    this.workflowMetadata = newMetadata;
    this.workflowMetadataChangeSubject.next(newMetadata);
  }

  public setWorkflowSettings(workflowSettings: WorkflowSettings | undefined): void {
    const newSettings = workflowSettings === undefined ? this.getDefaultSettings() : workflowSettings;
    // Skip a redundant write: setting the same value would still cut a Yjs update and observer
    // churn for every collaborator.
    if (isEqual(this.getWorkflowSettings(), newSettings)) {
      return;
    }
    this.writeContentMeta("settings", newSettings, () => this.workflowSettingsChangeSubject.next(newSettings));
  }

  public getWorkflowSettings(): WorkflowSettings {
    return (this.readContentMeta("settings") as WorkflowSettings) ?? this.getDefaultSettings();
  }

  /**
   * Seed workflowSettings into the shared model while opening a workflow (see seedContentMeta).
   * Called under the reloading flag, so the seed is not announced as an edit.
   */
  public hydrateSettings(workflowSettings: WorkflowSettings | undefined): void {
    this.seedContentMeta("settings", workflowSettings);
  }

  /**
   * Load a definition into the shared model while opening a workflow (see seedContentMeta).
   * Called under the reloading flag, so the shared-map observer skips this seed and opening a
   * workflow is not announced as an edit.
   */
  public hydrateFormBinding(formBinding: FormBindingConfig | undefined): void {
    this.seedContentMeta("formBinding", formBinding);
  }

  /**
   * Put the database's copy of a content-meta value into the shared model as a workflow opens,
   * without taking a co-editor's newer one away.
   *
   * The document is created and connected just before this runs, so the provider has usually not
   * synced yet. Writing straight away is a concurrent whole-value write, and Yjs resolves those
   * by client id rather than by recency: the copy the database handed us could win over an edit a
   * co-editor has not saved yet -- the lost update this all exists to prevent. So wait for the
   * first sync, then write only when the key is still absent; a value that came from the room is
   * authoritative, whether it is a co-editor's edit or our own from another tab.
   *
   * Meanwhile the value is not lost: reads fall back to the copy held here, so the page shows it
   * and an autosave that fires before the sync carries it rather than an empty one. The wait is
   * not bounded: a copy published before the sync would be the concurrent write above, on a room
   * that is slow to answer as much as on one that never does. For as long as the room stays out
   * of reach the copy is read from here and every save carries it, and it goes into the document
   * when the sync eventually comes -- or never, which loses nothing, since nobody else was in that
   * document either.
   *
   * A local edit of the key while the seed waits replaces it (see writeContentMeta): the edit is
   * what goes in then, and the copy -- or the clearing, for a workflow opened without a value --
   * must neither write over it nor undo it.
   *
   * The room's precedence is for the first seed of a key on a document, the one a workflow is
   * opened with: it yields to what the room put there by then, a co-editor's value or the mark
   * that the key was cleared (`null`, written by whoever opened or reloaded a workflow that
   * carries none). The mark is what lets a joining client see the clearing at all -- a deletion
   * would leave nothing in the room's state to learn from, and a client whose database copy has
   * not caught up with the clearing would put the value back. Anything else the document holds,
   * a value this client wrote before the reload included, the seed replaces. A later reload into
   * the same document -- a version shown, or returned from; an agent's rewrite -- is the intent,
   * and its seed replaces whatever the document holds, a co-editor's value included; `undefined`
   * then marks the key cleared, so a version that carries no settings or no form definition does
   * not keep the open workflow's.
   *
   * An edit made before the first sync (`edit`) is held the same way: written straight into the
   * document it would be one more concurrent whole-value write, settled by client id, that could
   * lose to a value the room holds or take it over unseen. Held, it is read from and announced at
   * once (see writeContentMeta), and goes in after the sync as a write that follows the room's
   * state and replaces it -- which is what an edit means -- so it yields to nothing.
   */
  private seedContentMeta(key: ContentMetaKey, value: ContentMetaValue | undefined, edit = false): void {
    // This seed replaces any the key was still waiting on (and stops its listening); it has an
    // identity of its own, since a cleared key (undefined) has no value to be told apart by.
    this.cancelContentMetaSeed(key);
    const seed: ContentMetaSeed = {};
    this.contentMetaSeeds.set(key, seed);
    if (value !== undefined) {
      this.pendingContentMeta.set(key, { value, edit });
    }
    const shared = this.texeraGraph.sharedModel;
    // Runs at once or when the sync comes, and then for this seed only: a later seed of the key, a
    // local edit of it and the loading of another document all detach the listening before they
    // would make this seed stale (see cancelContentMetaSeed, observeContentMeta).
    const land = () => {
      this.cancelContentMetaSeed(key);
      const first = !this.seededContentMetaKeys.has(key);
      this.seededContentMetaKeys.add(key);
      // Over the first seed the room's word wins, a value or the cleared mark, whether a
      // co-editor's or this client's from another tab; a later reload, or an edit, replaces it.
      if (first && !edit && this.roomOwnedContentMeta.has(key)) {
        return;
      }
      const stored = value ?? null;
      // Opening a workflow is not an edit, so the seed is not announced (see observeContentMeta).
      this.seedingContentMetaKeys.add(key);
      try {
        if (!isEqual(shared.contentMetaMap.get(key), stored)) {
          shared.contentMetaMap.set(key, stored);
        }
      } finally {
        this.seedingContentMetaKeys.delete(key);
      }
    };
    if (!shared.wsProvider.shouldConnect || shared.wsProvider.synced) {
      land();
      return;
    }
    // Listening is undone by land itself (through cancelContentMetaSeed) and by whatever supersedes
    // or cancels the seed, so a document that never syncs does not collect a listener per reload.
    const onSync = () => land();
    seed.detach = () => shared.wsProvider.off("sync", onSync);
    shared.wsProvider.on("sync", onSync);
  }

  /** Forget the seed a key is waiting on, stop its listening, and drop the copy it holds; the
   *  document's value stands. */
  private cancelContentMetaSeed(key: ContentMetaKey): void {
    this.contentMetaSeeds.get(key)?.detach?.();
    this.contentMetaSeeds.delete(key);
    this.pendingContentMeta.delete(key);
  }

  /** An edit still waiting for the first sync, which is going to replace whatever the document
   *  holds; else the shared document's value for a key -- none, if the document marks it cleared
   *  -- or, for a key the document says nothing about yet, the database copy still waiting to go in. */
  private readContentMeta(key: ContentMetaKey): ContentMetaValue | undefined {
    const pending = this.pendingContentMeta.get(key);
    if (pending?.edit) {
      return pending.value;
    }
    const held = this.texeraGraph.sharedModel.contentMetaMap.get(key);
    if (held !== undefined) {
      return held ?? undefined;
    }
    return pending?.value;
  }

  /**
   * An edit of a content-meta value: into the document, where the map's observer announces it --
   * unless the document has not had its first sync yet, when the edit is held and announced here
   * instead, and goes in after the sync (see seedContentMeta).
   */
  private writeContentMeta(key: ContentMetaKey, value: ContentMetaValue, announce: () => void): void {
    const shared = this.texeraGraph.sharedModel;
    if (shared.wsProvider.shouldConnect && !shared.wsProvider.synced) {
      this.seedContentMeta(key, value, true);
      announce();
      return;
    }
    shared.contentMetaMap.set(key, value);
  }

  /** Whether the workflow has a value for a key: in the document, or still waiting to go in. */
  private hasContentMeta(key: ContentMetaKey): boolean {
    return this.readContentMeta(key) !== undefined;
  }

  /** A form binding worth persisting: an author populated it (fields, a chosen result list -- an
   *  empty one included, it means "none" -- or an instruction), as opposed to the empty default a
   *  plain workflow carries. */
  private isFormBindingNonEmpty(fb: FormBindingConfig): boolean {
    return fb.fields.length > 0 || fb.shownResultIds !== undefined || fb.instruction !== undefined;
  }

  /**
   * Replace the definition as an edit. The shared map's observer republishes it on
   * `formBindingChanged$`, which feeds workflowChanged() and so reaches the existing autosave.
   */
  public setFormBinding(formBinding: FormBindingConfig): void {
    // Skip a redundant write (as setWorkflowSettings does): an unchanged value would still cut a
    // Yjs update and re-fire the observer for every collaborator.
    if (isEqual(this.getFormBinding(), formBinding)) {
      return;
    }
    this.writeContentMeta("formBinding", formBinding, () => this.formBindingChangeSubject.next(formBinding));
  }

  public getFormBinding(): FormBindingConfig {
    return (this.readContentMeta("formBinding") as FormBindingConfig) ?? getDefaultFormBinding();
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
    const settings = this.getWorkflowSettings();
    // Read the binding once so the has-check and the value are the same snapshot.
    const formBinding = this.getFormBinding();

    texeraGraph
      .getAllOperators()
      .forEach(
        op =>
          (operatorPositions[op.operatorID] = this.texeraGraph.sharedModel.elementPositionMap?.get(
            op.operatorID
          ) as Point)
      );
    return {
      operators,
      operatorPositions,
      links,
      commentBoxes,
      settings,
      // Carry formBinding only when the workflow has one (opened with it, or an author
      // populated it since), so a plain workflow's content is unchanged and its save cuts no
      // needless version. hasContentMeta stands in for the old "loaded" flag: hydrate sets the
      // key (or holds the copy until the sync) for a workflow opened with a binding and deletes
      // it for one without. Presence and value come from the same place, so a binding that is
      // present but empty is carried while the seed waits, not dropped.
      ...(this.hasContentMeta("formBinding") || this.isFormBindingNonEmpty(formBinding) ? { formBinding } : {}),
    };
  }

  public getWorkflow(): Workflow {
    return {
      ...this.workflowMetadata,
      ...{ content: this.getWorkflowContent() },
    };
  }

  /**
   * Used for previewing a version. Will clean-up shared editing session before doing so.
   * @param workflow
   */
  public setTempWorkflow(workflow: Workflow): void {
    if (this.texeraGraph.sharedModel.wsProvider.shouldConnect) {
      this.texeraGraph.sharedModel.wsProvider.disconnect();
    }
    this.tempWorkflow = workflow;
  }

  /**
   * Used for ending version preview. Will re-connect to shared editing session after doing so.
   */
  public resetTempWorkflow(): void {
    this.tempWorkflow = undefined;
    this.texeraGraph.sharedModel.wsProvider.connect();
  }

  public getTempWorkflow(): Workflow | undefined {
    return this.tempWorkflow;
  }

  /**
   * This is not included in shared editing.
   * @param name
   */
  public setWorkflowName(name: string): void {
    const newName = name.trim().length > 0 ? name : DEFAULT_WORKFLOW_NAME;
    this.setWorkflowMetadata({ ...this.workflowMetadata, name: newName });
  }

  public setWorkflowDataTransferBatchSize(size: number): void {
    if (size > 0 && size != null) {
      this.setWorkflowSettings({ ...this.getWorkflowSettings(), dataTransferBatchSize: size });
    }
  }

  public updateExecutionMode(mode: ExecutionMode): void {
    this.setWorkflowSettings({ ...this.getWorkflowSettings(), executionMode: mode });
  }

  public clearWorkflow(): void {
    this.destroySharedModel();
    this.setWorkflowMetadata(undefined);
    // The settings go back to the defaults with the fresh document the blank reload creates, like
    // the form definition: written here they would go into the document just destroyed and be
    // announced as an edit.
    this.reloadWorkflow(undefined);
    this.setHighlightingEnabled(false);
  }

  public setWorkflowIsPublished(newPublishState: number): void {
    this.setWorkflowMetadata({ ...this.workflowMetadata, isPublished: newPublishState });
  }

  /**
   * Need to quit shared-editing room at first.
   */
  public resetAsNewWorkflow() {
    this.destroySharedModel();
    this.reloadWorkflow(undefined);
    this.workflowResetSubject.next();
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //                                          Below are private methods.                                              //
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Subscribes to element position changes from joint graph and updates them in TexeraGraph.
   *
   * Also subscribes to element position change event stream,
   *  checks if the element (operator) is moved by user and
   *  if the moved element is currently highlighted,
   *  if it is, moves other highlighted elements (operators) along with it,
   *    links will automatically move with operators.
   *
   *  The subscriptions need and only need to be initiated once,
   *    unlike observers in <code>{@link SharedModelChangeHandler}</code>.
   * @private
   */
  private handleJointElementDrag(): void {
    this.jointGraphWrapper
      .getElementPositionChangeEvent()
      .pipe(
        filter(() => this.jointGraphWrapper.getListenPositionChange()),
        filter(() => this.undoRedoService.listenJointCommand),
        filter(() => this.texeraGraph.getSyncTexeraGraph()),
        filter(movedElement =>
          this.jointGraphWrapper
            .getCurrentHighlightedOperatorIDs()
            .concat(this.jointGraphWrapper.getCurrentHighlightedCommentBoxIDs())
            .includes(movedElement.elementID)
        )
      )
      .subscribe(movedElement => {
        this.texeraGraph.bundleActions(() => {
          if (
            this.texeraGraph.sharedModel.elementPositionMap.get(movedElement.elementID) !== movedElement.newPosition
          ) {
            // For syncing ops/comment boxes in shared editing
            this.texeraGraph.sharedModel.elementPositionMap.set(movedElement.elementID, movedElement.newPosition);
            // For moving all highlighted operators
            const selectedElements = this.jointGraphWrapper
              .getCurrentHighlightedOperatorIDs()
              .concat(this.jointGraphWrapper.getCurrentHighlightedCommentBoxIDs());
            const offsetX = movedElement.newPosition.x - movedElement.oldPosition.x;
            const offsetY = movedElement.newPosition.y - movedElement.oldPosition.y;
            this.jointGraphWrapper.setListenPositionChange(false);
            this.undoRedoService.setListenJointCommand(false);
            // Persistence and shared-editing syncing for comment boxes have different interfaces.
            // Setting positions inside commentBoxes here only for persistence.
            // Syncing uses elementPositionMap.
            selectedElements
              .filter(elementID => elementID.includes("commentBox"))
              .forEach(elementID => {
                this.texeraGraph.sharedModel.commentBoxMap
                  .get(elementID)
                  ?.set("commentBoxPosition", this.jointGraphWrapper.getElementPosition(elementID));
              });
            // Move other highlighted operators.
            selectedElements
              .filter(elementID => elementID !== movedElement.elementID)
              .forEach(elementID => {
                this.jointGraphWrapper.setElementPosition(elementID, offsetX, offsetY);
                this.texeraGraph.sharedModel.elementPositionMap.set(
                  elementID,
                  this.jointGraphWrapper.getElementPosition(elementID)
                );
              });
            this.jointGraphWrapper.setListenPositionChange(true);
            this.undoRedoService.setListenJointCommand(true);
          }
        });
      });
  }

  private updateOperatorVersions(operatorsAndPositions: { op: OperatorPredicate; pos: Point }[]) {
    const updatedOperators: { op: OperatorPredicate; pos: Point }[] = [];
    for (const operatorsAndPosition of operatorsAndPositions) {
      updatedOperators.push({
        op: this.workflowUtilService.updateOperatorVersion(operatorsAndPosition.op),
        pos: operatorsAndPosition.pos,
      });
    }
    return updatedOperators;
  }

  public setHighlightingEnabled(enabled: boolean): void {
    this.highlightingEnabled = enabled;
  }

  public getHighlightingEnabled() {
    return this.highlightingEnabled;
  }
}
