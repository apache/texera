import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';
import { debounceTime } from 'rxjs/operators';
import { Point } from '../../../types/workflow-common.interface';
import { UndoRedoService } from './../../undo-redo/undo-redo.service';

type operatorIDsType = { operatorIDs: string[] };
type linkIDType = { linkID: string };


type JointModelEventInfo = {
  add: boolean,
  merge: boolean,
  remove: boolean,
  changes: {
    added: joint.dia.Cell[],
    merged: joint.dia.Cell[],
    removed: joint.dia.Cell[]
  }
};

// argument type of callback event on a JointJS Model,
// which is a 3-element tuple:
// 1. the JointJS model (Cell) of the event
// 2 and 3. additional information of the event
type JointModelEvent = [
  joint.dia.Cell,
  { graph: joint.dia.Graph, models: joint.dia.Cell[] },
  JointModelEventInfo
];

type JointLinkChangeEvent = [
  joint.dia.Link,
  { x: number, y: number },
  { ui: boolean, updateConnectionOnly: boolean }
];

type JointPositionChangeEvent = [
  joint.dia.Element,
  { x: number, y: number }
];

type PositionInfo = {
  currPos: Point,
  lastPos: Point | undefined
};

/**
 * JointGraphWrapper wraps jointGraph to provide:
 *  - getters of the properties (to hide the methods that could alther the jointGraph directly)
 *  - event streams of JointGraph in RxJS Observables (instead of the callback functions to fit our use of RxJS)
 *
 * JointJS Graph only contains information related the UI, such as:
 *  - position of operator elements
 *  - events of a cell (operator or link) being dragging around
 *  - events of adding/deleting a link on the UI,
 *      this doesn't necessarily corresponds to adding/deleting a link logically on the graph
 *      because the link might not connect to a target operator while user is dragging the link
 *
 * If an external module needs to access more properties of JointJS graph,
 *  or to make changes **irrelevant** to the graph data structure, but related direcly to the UI,
 *  (such as changing the color of an operator), more methods can be added in this class.
 *
 * For an overview of the services in WorkflowGraphModule, see workflow-graph-design.md
 */
export class JointGraphWrapper {

  // zoom diff represents the ratio that is zoom in/out everytime, for clicking +/- buttons or using mousewheel
  public static readonly ZOOM_CLICK_DIFF: number = 0.05;
  public static readonly ZOOM_MOUSEWHEEL_DIFF: number = 0.01;
  public static readonly INIT_ZOOM_VALUE: number = 1;
  public static readonly INIT_PAN_OFFSET: Point = {x: 0, y: 0};

  public static readonly ZOOM_MINIMUM: number = 0.70;
  public static readonly ZOOM_MAXIMUM: number = 1.30;

  private operatorPositions: Map<string, PositionInfo> = new Map<string, PositionInfo>();
  private listenPositionChange: boolean = true;

  // flag that indicates whether multiselect mode is on
  private multiSelect: boolean = false;
  // the current highlighted operators' ID
  private currentHighlightedOperators: string[] = [];
  // event stream of highlighting an operator
  private jointCellHighlightStream = new Subject<operatorIDsType>();
  // event stream of un-highlighting an operator
  private jointCellUnhighlightStream = new Subject<operatorIDsType>();
  // event stream of zooming the jointJs paper
  private workflowEditorZoomSubject: Subject<number> = new Subject<number>();
  // event stream of restoring zoom / offset default of the jointJS paper
  private restorePaperOffsetSubject: Subject<Point> = new Subject<Point>();
  // event stream of panning to make mini-map and main workflow paper compatible in offset
  private panPaperOffsetSubject: Subject<Point> = new Subject<Point>();
  // event stream of highlighing a link
  private jointLinkHighlightStream = new Subject<linkIDType>();
  // event stream of unhighlighing a link
  private jointLinkUnhighlightStream = new Subject<linkIDType>();
  // event stream of showing the breakpoint button of a link
  private jointLinkBreakpointShowStream = new Subject<linkIDType>();
  // event stream of hiding the breakpoint button of a link
  private jointLinkBreakpointHideStream = new Subject<linkIDType>();
  // the currently highlighted links' ids
  private currentHighlightedLinks: string[] = [];
  // the linkIDs of those links with a breakpoint
  private linksWithBreakpoints: string[] = [];

  // current zoom ratio
  private zoomRatio: number = JointGraphWrapper.INIT_ZOOM_VALUE;
  // panOffset, a point of panning offset alongside x and y axis
  private panOffset: Point = JointGraphWrapper.INIT_PAN_OFFSET;

  /**
   * This will capture all events in JointJS
   *  involving the 'add' operation
   */
  private jointCellAddStream = Observable
    .fromEvent<JointModelEvent>(this.jointGraph, 'add')
    .map(value => value[0]);

  /**
   * This will capture all events in JointJS
   *  involving the 'change position' operation
   */
  private jointCellDragStream = Observable
    .fromEvent<JointModelEvent>(this.jointGraph, 'change:position')
    .map(value => value[0]);

  /**
   * This will capture all events in JointJS
   *  involving the 'remove' operation
   */
  private jointCellDeleteStream = Observable
    .fromEvent<JointModelEvent>(this.jointGraph, 'remove')
    .map(value => value[0]);


  constructor(private jointGraph: joint.dia.Graph, private undoRedoService: UndoRedoService) {
    // handle if the current highlighted operator is deleted, it should be unhighlighted
    this.handleOperatorDeleteUnhighlight();
    this.jointCellAddStream.filter(cell => cell.isElement()).subscribe(element => {
      const initPosition = {currPos: (element as joint.dia.Element).position(), lastPos: undefined};
      this.operatorPositions.set(element.id.toString(), initPosition);
    });

    // handle if the current highlighted operator's position is changed,
    // other highlighted operators should move with it.
    this.handleHighlightedOperatorPositionChange();
    // handle if a link is deleted, it should be unhighlighted
    this.handleLinkDeleteUnhighlight();
  }


  /**
   * This method is used to toggle the multiselect mode.
   * @param multiSelect
   */
  public setMultiSelectMode(multiSelect: boolean): void {
    this.multiSelect = multiSelect;
  }

  /**
   * This method is used to get the current status of the multiselect mode.
   */
  public getMultiSelectMode(): boolean {
    return this.multiSelect;
  }

  /**
   * Gets the operator ID of the current highlighted operators.
   * Returns an empty list if there is no highlighted operator.
   *
   * The returned array is not the original one so that other
   * services/components can't modify it directly.
   */
  public getCurrentHighlightedOperatorIDs(): string[] {
    return Object.assign([], this.currentHighlightedOperators);
  }

  /**
   * Returns an Observable stream capturing the operator position change event in JointJS graph.
   *
   * - operatorID: the moved operator's ID
   * - oldPosition: the operator's position before moving
   * - newPosition: where the operator is moved to
   */
  public getOperatorPositionChangeEvent(): Observable<{ operatorID: string, oldPosition: Point, newPosition: Point }> {
    return Observable
      .fromEvent<JointPositionChangeEvent>(this.jointGraph, 'change:position').map(e => {
        const operatorID = e[0].id.toString();
        const oldPosition = this.operatorPositions.get(operatorID);
        const newPosition = {x: e[1].x, y: e[1].y};
        if (!oldPosition) {
          throw new Error(`internal error: cannot find operator position for ${operatorID}`);
        }
        if (!oldPosition.lastPos || oldPosition.currPos.x !== newPosition.x || oldPosition.currPos.y !== newPosition.y) {
          oldPosition.lastPos = oldPosition.currPos;
        }
        this.operatorPositions.set(operatorID, {currPos: newPosition, lastPos: oldPosition.lastPos});
        return {
          operatorID: operatorID,
          oldPosition: oldPosition.lastPos,
          newPosition: newPosition
        };
      });
  }

  /**
   * Highlights the operator with given operatorID.
   * Emits an event to the operator highlight stream.
   * @param operatorID
   */
  public highlightOperator(operatorID: string): void {
    // if there are highlighed links, unhighlight them
    if (this.currentHighlightedLinks.length > 0) {
      const highlightedLinks = Object.assign([], this.currentHighlightedLinks);
      highlightedLinks.forEach(highlightedLink => this.unhighlightLink(highlightedLink));
    }

    const highlightedOperatorIDs: string[] = [];
    this.highlightOperatorInternal(operatorID, highlightedOperatorIDs);
    if (highlightedOperatorIDs.length > 0) {
      this.jointCellHighlightStream.next({ operatorIDs: highlightedOperatorIDs });
    }
  }

  /**
   * Highlights operators in the given list.
   *
   * Emits an event to the operator highlight stream with a list of operatorIDs
   * that are highlighted.
   *
   * @param operatorIDs
   */
  public highlightOperators(operatorIDs: string[]): void {

    // if there are highlighed links, unhighlight them
    if (this.currentHighlightedLinks.length > 0) {
      const highlightedLinks = Object.assign([], this.currentHighlightedLinks);
      highlightedLinks.forEach(highlightedLink => this.unhighlightLink(highlightedLink));
    }
    // then
    const highlightedOperatorIDs: string[] = [];
    operatorIDs.forEach(operatorID => this.highlightOperatorInternal(operatorID, highlightedOperatorIDs));
    if (highlightedOperatorIDs.length > 0) {
      this.jointCellHighlightStream.next({ operatorIDs: highlightedOperatorIDs });
    }
  }

  /**
   * Unhighlights the given highlighted operator.
   * Emits an event to the operator unhighlight stream.
   * @param operatorID
   */
  public unhighlightOperator(operatorID: string): void {
    const unhighlightedOperatorIDs: string[] = [];
    this.unhighlightOperatorInternal(operatorID, unhighlightedOperatorIDs);
    if (unhighlightedOperatorIDs.length > 0) {
      this.jointCellUnhighlightStream.next({ operatorIDs: unhighlightedOperatorIDs });
    }
  }

  /**
   * Unhighlights operators in the given list.
   *
   * Emits an event to the operator unhighlight stream with a list of operatorIDs
   * that are unhighlighted.
   *
   * @param operatorIDs
   */
  public unhighlightOperators(operatorIDs: string[]): void {
    const unhighlightedOperatorIDs: string[] = [];
    operatorIDs.forEach(operatorID => this.unhighlightOperatorInternal(operatorID, unhighlightedOperatorIDs));
    if (unhighlightedOperatorIDs.length > 0) {
      this.jointCellUnhighlightStream.next({ operatorIDs: unhighlightedOperatorIDs });
    }
  }

  /**
   * Gets the event stream of an operator being highlighted.
   */
  public getJointCellHighlightStream(): Observable<operatorIDsType> {
    return this.jointCellHighlightStream.asObservable();
  }

  /**
   * Gets the event stream of an operator being unhighlighted.
   * The operator could be unhighlighted because it's deleted.
   */
  public getJointCellUnhighlightStream(): Observable<operatorIDsType> {
    return this.jointCellUnhighlightStream.asObservable();
  }

  /**
   * get the ids of all the links that are currently highlighted
   */
  public getCurrentHighlightedLinkIDs(): string[] {
    return Object.assign([], this.currentHighlightedLinks);
  }

  /**
   * get the ids of all the links that have a breakpoint
   */
  public getLinkIDsWithBreakpoint(): string[] {
    return Object.assign([], this.linksWithBreakpoints);
  }

  /**
   * get the event stream of a link being highlighted.
   */
  public getLinkHighlightStream(): Observable<linkIDType> {
    return this.jointLinkHighlightStream.asObservable();
  }

  /**
   * get the event stream of a link being unhighlighted.
   */
  public getLinkUnhighlightStream(): Observable<linkIDType> {
    return this.jointLinkUnhighlightStream.asObservable();
  }

  /**
   * get the event stream of showing the breakpoint button of a link
   */
  public getLinkBreakpointShowStream(): Observable<linkIDType> {
    return this.jointLinkBreakpointShowStream.asObservable();
  }

  /**
   * get the event stream of hiding the breakpoint button of a link
   */
  public getLinkBreakpointHideStream(): Observable<linkIDType> {
    return this.jointLinkBreakpointHideStream.asObservable();
  }

  /**
   * Gets the event stream of an operator being dragged.
   */
  public getJointOperatorCellDragStream(): Observable<joint.dia.Element> {
    const jointOperatorDragStream = this.jointCellDragStream
      .filter(cell => cell.isElement())
      .map(cell => <joint.dia.Element>cell);
    return jointOperatorDragStream;
  }

  /**
   * Returns an Observable stream capturing the operator cell delete event in JointJS graph.
   */
  public getJointOperatorCellDeleteStream(): Observable<joint.dia.Element> {
    const jointOperatorDeleteStream = this.jointCellDeleteStream
      .filter(cell => cell.isElement())
      .map(cell => <joint.dia.Element>cell);
    return jointOperatorDeleteStream;
  }

  /**
   * Returns an Observable stream capturing the link cell add event in JointJS graph.
   *
   * Notice that a link added to JointJS graph doesn't mean it will be added to Texera Workflow Graph as well
   *  because the link might not be valid (not connected to a target operator and port yet).
   * This event only represents that a link cell is visually added to the UI.
   *
   */
  public getJointLinkCellAddStream(): Observable<joint.dia.Link> {
    const jointLinkAddStream = this.jointCellAddStream
      .filter(cell => cell.isLink())
      .map(cell => <joint.dia.Link>cell);

    return jointLinkAddStream;
  }


  /**
   * Returns an Observable stream capturing the link cell delete event in JointJS graph.
   *
   * Notice that a link deleted from JointJS graph doesn't mean the same event happens for Texera Workflow Graph
   *  because the link might not be valid and doesn't exist logically in the Workflow Graph.
   * This event only represents that a link cell visually disappears from the UI.
   *
   */
  public getJointLinkCellDeleteStream(): Observable<joint.dia.Link> {
    const jointLinkDeleteStream = this.jointCellDeleteStream
      .filter(cell => cell.isLink())
      .map(cell => <joint.dia.Link>cell);

    return jointLinkDeleteStream;
  }

  public getPanPaperOffsetStream(): Observable<Point> {
    return this.panPaperOffsetSubject.asObservable();
  }

  /**
   * This method will update the panning offset so that dropping
   *  a new operator will appear at the correct location on the UI.
   *
   * @param panOffset new offset from panning
   */
  public setPanningOffset(panOffset: Point): void {
    this.panOffset = panOffset;
    this.panPaperOffsetSubject.next(panOffset);
  }

  /**
   * This method will update the zoom ratio, which will be used
   *  in calculating the position of the operator dropped on the UI.
   *
   * @param ratio new ratio from zooming
   */
  public setZoomProperty(ratio: number): void {
      this.zoomRatio = ratio;
      this.workflowEditorZoomSubject.next(this.zoomRatio);
  }

  /**
   * Check if the zoom ratio reaches the minimum.
   */
  public isZoomRatioMin(): boolean {
    return this.zoomRatio <= JointGraphWrapper.ZOOM_MINIMUM;
  }

  /**
   * Check if the zoom ratio reaches the maximum.
   */
  public isZoomRatioMax(): boolean {
    return this.zoomRatio >= JointGraphWrapper.ZOOM_MAXIMUM;
  }

  /**
   * Returns an observable stream containing the new zoom ratio
   *  for the jointJS paper.
   */
  public getWorkflowEditorZoomStream(): Observable<number> {
    return this.workflowEditorZoomSubject.asObservable();
  }

  /**
   * This method will fetch current pan offset of the paper.
   */
  public getPanningOffset(): Point {
    return this.panOffset;
  }

  /**
   * This method will fetch current zoom ratio of the paper.
   */
  public getZoomRatio(): number {
    return this.zoomRatio;
  }

  /**
   * This method will restore the default zoom ratio and offset for
   *  the jointjs paper by sending an event to restorePaperSubject.
   */
  public restoreDefaultZoomAndOffset(): void {
    this.setZoomProperty(JointGraphWrapper.INIT_ZOOM_VALUE);
    this.panOffset = JointGraphWrapper.INIT_PAN_OFFSET;
    this.restorePaperOffsetSubject.next(this.panOffset);
  }

  /**
   * Returns an Observable stream capturing the event of restoring
   *  default offset
   */
  public getRestorePaperOffsetStream(): Observable<Point> {
    return this.restorePaperOffsetSubject.asObservable();
  }

  /**
   * Returns an Observable stream capturing the link cell delete event in JointJS graph.
   *
   * Notice that the link change event will be triggered whenever the link's source or target is changed:
   *  - one end of the link is attached to a port
   *  - one end of the link is detached to a port and become a point (coordinate) in the paper
   *  - one end of the link is moved from one point to another point in the paper
   */
  public getJointLinkCellChangeStream(): Observable<joint.dia.Link> {
    const jointLinkChangeStream = Observable
      .fromEvent<JointLinkChangeEvent>(this.jointGraph, 'change:source change:target')
      .map(value => value[0]);

    return jointLinkChangeStream;
  }

  /**
   * This method will get the operator position on the JointJS paper.
   */
  public getOperatorPosition(operatorID: string): Point {
    const cell: joint.dia.Cell | undefined = this.jointGraph.getCell(operatorID);
    if (! cell) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    if (! cell.isElement()) {
      throw new Error(`${operatorID} is not an operator`);
    }
    const element = <joint.dia.Element> cell;
    const position = element.position();
    return { x: position.x, y: position.y };
  }

  /**
   * This method repositions the operator according to given offsets.
   */
  public setOperatorPosition(operatorID: string, offsetX: number, offsetY: number): void {
    const cell: joint.dia.Cell | undefined = this.jointGraph.getCell(operatorID);
    if (! cell) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    if (! cell.isElement()) {
      throw new Error(`${operatorID} is not an operator`);
    }
    const element = <joint.dia.Element> cell;
    element.translate(offsetX, offsetY);
  }

  /**
   * Highlights the link with given linkID.
   * Emits an event to the link highlight stream.
   * If the target link is already highlighted, the action will be ignored.
   * At current design, there can only be one link highlighted at a time,
   *  no mutiselect mode for links.
   * Before a link is highlighted, all the currently highlighted operators will
   *  be unhighlighted.
   *
   * @param linkID
   */
  public highlightLink(linkID: string): void {
    if (!this.jointGraph.getCell(linkID)) {
      throw new Error(`link with ID ${linkID} doesn't exist`);
    }
    if (this.currentHighlightedLinks.includes(linkID)) {
      return;
    }
    // only allow one link highlighted at a time
    if (this.currentHighlightedLinks.length > 0) {
      const highlightedLinks = Object.assign([], this.currentHighlightedLinks);
      highlightedLinks.forEach(highlightedLink => this.unhighlightLink(highlightedLink));
    }
    this.getCurrentHighlightedOperatorIDs()
        .forEach(operatorID => this.unhighlightOperator(operatorID));
    this.currentHighlightedLinks.push(linkID);
    this.jointLinkHighlightStream.next({ linkID });
  }

  /**
   * Unhighlights the given highlighted link.
   * Emits an event to the link unhighlight stream.
   * @param unhighlightedLinkID
   */
  public unhighlightLink(linkID: string): void {
    if (!this.currentHighlightedLinks.includes(linkID)) {
      return;
    }
    const unhighlightedLinkIndex = this.currentHighlightedLinks.indexOf(linkID);
    this.currentHighlightedLinks.splice(unhighlightedLinkIndex, 1);
    this.jointLinkUnhighlightStream.next({ linkID });
  }

  /**
   * Show the breakpoint button of a given link
   * emits an event to the link breakpoint show stream.
   * @param linkID
   */
  public showLinkBreakpoint(linkID: string): void {
    if (!this.linksWithBreakpoints.includes(linkID)) {
      this.linksWithBreakpoints.push(linkID);
    }
    this.jointLinkBreakpointShowStream.next({ linkID });
  }

  /**
   * Hide the breakpoint button of a given link
   * emits an event to the link breakpoint hide stream.
   * @param linkID
   */
  public hideLinkBreakpoint(linkID: string): void {
    if (!this.linksWithBreakpoints.includes(linkID)) {
      return;
    }
    const LinkIndex = this.linksWithBreakpoints.indexOf(linkID);
    this.linksWithBreakpoints.splice(LinkIndex, 1);
    this.jointLinkBreakpointHideStream.next({ linkID });
  }

  /**
   * This method gets the operator's layer (z attribute) on the JointJS paper.
   */
  public getOperatorLayer(operatorID: string): number {
    const cell: joint.dia.Cell | undefined = this.jointGraph.getCell(operatorID);
    if (! cell) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    if (! cell.isElement()) {
      throw new Error(`${operatorID} is not an operator`);
    }
    return cell.attributes.z;
  }

  /**
   * This method sets the operator's layer (z attribute) to the given layer.
   */
  public setOperatorLayer(operatorID: string, layer: number): void {
    const cell: joint.dia.Cell | undefined = this.jointGraph.getCell(operatorID);
    if (! cell) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    if (! cell.isElement()) {
      throw new Error(`${operatorID} is not an operator`);
    }
    cell.set('z', layer);
  }

  /**
   * Highlights the operator with given operatorID.
   *
   * If the currently highlighted operator is already highlighted, the action will be ignored.
   *
   * When the multiselect mode is off:
   * there is only one operator that could be highlighted at a time, therefore
   *  if another operator is highlighted, it will be unhighlighted.
   */
  private highlightOperatorInternal(operatorID: string, highlightedOperatorIDs: string[]): void {
    // try to get the operator using operator ID
    if (!this.jointGraph.getCell(operatorID)) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    // if the current highlighted operator is already highlighted, don't do anything
    if (this.currentHighlightedOperators.includes(operatorID)) {
      return;
    }
    // if the multiselect mode is off and there are other highlighted operators,
    // unhighlight them first
    if (!this.multiSelect && this.currentHighlightedOperators.length > 0) {
      const highlightedOperators = Object.assign([], this.currentHighlightedOperators);
      this.unhighlightOperators(highlightedOperators);
    }
    // highlight the operator and add it to the list of highlighted operators
    this.currentHighlightedOperators.push(operatorID);
    highlightedOperatorIDs.push(operatorID);
  }

  /**
   * Unhighlights the given highlighted operator.
   */
  private unhighlightOperatorInternal(operatorID: string, unhighlightedOperatorIDs: string[]): void {
    if (!this.currentHighlightedOperators.includes(operatorID)) {
      return;
    }
    const unhighlightedOperatorIndex = this.currentHighlightedOperators.indexOf(operatorID);
    this.currentHighlightedOperators.splice(unhighlightedOperatorIndex, 1);
    unhighlightedOperatorIDs.push(operatorID);
  }

  /**
   * Subscribes to operator cell delete event stream,
   *  checks if the deleted operator is currently highlighted
   *  and unhighlight it if it is.
   */
  private handleOperatorDeleteUnhighlight(): void {
    this.getJointOperatorCellDeleteStream().subscribe(deletedOperatorCell => {
      const deletedOperatorID = deletedOperatorCell.id.toString();
      if (this.currentHighlightedOperators.includes(deletedOperatorID)) {
        this.unhighlightOperator(deletedOperatorID);
      }
    });
  }

  /**
   * Subscribes to operator position change event stream,
   *  checks if the operator is moved by user and if the moved operator is currently highlighted,
   *  if it is, move other highlighted operators along with it.
   */
  private handleHighlightedOperatorPositionChange(): void {
    this.getOperatorPositionChangeEvent()
      .filter(() => this.listenPositionChange)
      .filter(() => this.undoRedoService.listenJointCommand)
      .filter(movedOperator => this.currentHighlightedOperators.includes(movedOperator.operatorID))
      .subscribe(movedOperator => {
        const offsetX = movedOperator.newPosition.x - movedOperator.oldPosition.x;
        const offsetY = movedOperator.newPosition.y - movedOperator.oldPosition.y;
        this.listenPositionChange = false;
        this.undoRedoService.setListenJointCommand(false);
        this.currentHighlightedOperators
          .filter(operatorID => operatorID !== movedOperator.operatorID)
          .forEach(operatorID => this.setOperatorPosition(operatorID, offsetX, offsetY));
        this.listenPositionChange = true;
        this.undoRedoService.setListenJointCommand(true);
      });
  }

  /**
   * Subscribes to link delete event stream,
   *  unhighlight the removed link if it is currently highlighted
   */
  private handleLinkDeleteUnhighlight(): void {
    this.getJointLinkCellDeleteStream().subscribe(deletedLinkCell => {
      const deletedLinkID = deletedLinkCell.id.toString();
      if (this.currentHighlightedLinks.includes(deletedLinkID)) {
        this.unhighlightLink(deletedLinkID);
      }
    });
  }

}
