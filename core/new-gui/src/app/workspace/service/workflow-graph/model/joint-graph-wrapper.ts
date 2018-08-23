import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';

type operatorIDType = { operatorID: string };


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
  {graph: joint.dia.Graph, models: joint.dia.Cell[]},
  JointModelEventInfo
];

type JointLinkChangeEvent = [
  joint.dia.Link,
  { x: number, y: number },
  { ui: boolean, updateConnectionOnly: boolean }
];

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

  // the current highlighted operator ID
  private currentHighlightedOperator: string | undefined;
  // event stream of highlighting an operator
  private jointCellHighlightStream = new Subject<operatorIDType>();
  // event stream of un-highlighting an operator
  private jointCellUnhighlightStream = new Subject<operatorIDType>();

  /**
   * This will capture all events in JointJS
   *  involving the 'add' operation
   */
  private jointCellAddStream = Observable
    .fromEvent<JointModelEvent>(this.jointGraph, 'add')
    .map(value => value[0]);

  /**
   * This will capture all events in JointJS
   *  involving the 'remove' operation
   */
  private jointCellDeleteStream = Observable
    .fromEvent<JointModelEvent>(this.jointGraph, 'remove')
    .map(value => value[0]);


  constructor(private jointGraph: joint.dia.Graph) {
    // handle if the current highlighted operator is deleted, it should be unhighlighted
    this.handleOperatorDeleteUnhighlight();
  }

  /**
   * Gets the operator ID of the current highlighted operator.
   * Returns undefined if there is no highlighted operator.
   */
  public getCurrentHighlightedOpeartorID(): string | undefined {
    return this.currentHighlightedOperator;
  }

  /**
   * Highlights the operator with given operatorID.
   * Emits an event to the operator highlight stream.
   *
   * If the currently highlighted operator is the same, the action will be ignored.
   *
   * There is only one operator that could be highlighted at a time, therefore
   *  if another operator is highlighted, it will be unhighlighted.
   *
   * @param operatorID
   */
  public highlightOperator(operatorID: string): void {
    // try to get the operator using operator ID
    if (!this.jointGraph.getCell(operatorID)) {
      throw new Error(`opeartor with ID ${operatorID} doesn't exist`);
    }
    // if the current highlighted operator is the same operator, don't do anything
    if (this.currentHighlightedOperator === operatorID) {
      return;
    }
    // if there is another operator currently highlighted, unhighlight it first
    if (this.currentHighlightedOperator) {
      this.unhighlightCurrent();
    }
    // highlight the operator and emit the event
    this.currentHighlightedOperator = operatorID;
    this.jointCellHighlightStream.next({ operatorID });
  }

  /**
   * Unhighlights the currently highlighted operator.
   * Emits an event to the operator unhighlight stream.
   */
  public unhighlightCurrent(): void {
    if (!this.currentHighlightedOperator) {
      return;
    }
    const unhighlightedOperatorID = this.currentHighlightedOperator;
    this.currentHighlightedOperator = undefined;
    this.jointCellUnhighlightStream.next({ operatorID: unhighlightedOperatorID });
  }

  /**
   * Gets the event stream of an operator being highlighted.
   */
  public getJointCellHighlightStream(): Observable<operatorIDType> {
    return this.jointCellHighlightStream.asObservable();
  }

  /**
   * Gets the event stream of an operator being unhighlighted.
   * The operator could be unhighlighted because it's deleted.
   */
  public getJointCellUnhighlightStream(): Observable<operatorIDType> {
    return this.jointCellUnhighlightStream.asObservable();
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
   * Notice that a link deleted from JointJS graph doesn't mean the same event happens for Texera Workflow Grap
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
   * Subscribes to operator cell delete event stream,
   *  checks if the deleted operator is the currently selected operator
   *  and unhighlight it if it is.
   */
  private handleOperatorDeleteUnhighlight(): void {
    this.getJointOperatorCellDeleteStream().subscribe(deletedOperatorCell => {
      if (this.currentHighlightedOperator === deletedOperatorCell.id.toString()) {
        this.unhighlightCurrent();
      }
    });
  }

}
