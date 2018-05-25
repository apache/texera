import { Observable } from 'rxjs/Observable';

export class JointGraphWrapper {

  /**
   * This will capture all events in JointJS
   *  involving the 'add' operation
   */
  private jointCellAddStream = Observable
    .fromEvent(this.jointGraph, 'add')
    .map(value => <joint.dia.Cell>value);

  /**
   * This will capture all events in JointJS
   *  involving the 'remove' operation
   */
  private jointCellDeleteStream = Observable
    .fromEvent(this.jointGraph, 'remove')
    .map(value => <joint.dia.Cell>value);


  constructor(private jointGraph: joint.dia.Graph) {
  }

  /**
   * Returns an Observable stream capturing the operator cell delete event in JointJS graph.
   */
  public onJointOperatorCellDelete(): Observable<joint.dia.Element> {
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
  public onJointLinkCellAdd(): Observable<joint.dia.Link> {
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
  public onJointLinkCellDelete(): Observable<joint.dia.Link> {
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
  public onJointLinkCellChange(): Observable<joint.dia.Link> {
    const jointLinkChangeStream = Observable
      .fromEvent(this.jointGraph, 'change:source change:target')
      .map(value => <joint.dia.Link>value);

    return jointLinkChangeStream;
  }



}
