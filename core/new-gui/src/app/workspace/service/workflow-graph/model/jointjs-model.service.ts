import { OperatorPredicate } from './../../../types/workflow-graph';
import { WorkflowActionService } from './workflow-action.service';
import { Injectable } from '@angular/core';
import { Point } from '../../../types/common.interface';
import { Observable } from 'rxjs/Observable';

import '../../../../common/rxjs-operators';

import * as joint from 'jointjs';
import { JointUIService } from '../../joint-ui/joint-ui.service';

@Injectable()
export class JointModelService {

  private jointGraph = new joint.dia.Graph();
  private jointPaper: joint.dia.Paper | null = null;

  private jointCellAddStream = Observable
    .fromEvent(this.jointGraph, 'add')
    .map(value => <joint.dia.Cell>value);

  private jointCellDeleteStream = Observable
    .fromEvent(this.jointGraph, 'remove')
    .map(value => <joint.dia.Cell>value);


  constructor(
    private workflowActionService: WorkflowActionService,
    private jointUIService: JointUIService) {

    this.workflowActionService.onAddOperatorAction().subscribe(
      value => this.addOperator(value.operator, value.point)
    );

    this.workflowActionService.onDeleteOperatorAction().subscribe(
      value => this.deleteOperator(value.operatorID)
    );
  }

  public getJointGraph(): joint.dia.Graph {
    return this.jointGraph;
  }

  public registerJointPaper(jointPaper: joint.dia.Paper): void {
    this.jointPaper = jointPaper;
  }

  public onJointOperatorCellDelete(): Observable<joint.dia.Element> {
    const jointOperatorDeleteStream = this.jointCellDeleteStream
      .filter(cell => cell.isElement())
      .map(cell => <joint.dia.Element>cell);
    return jointOperatorDeleteStream;
  }

  public onJointLinkCellAdd(): Observable<joint.dia.Link> {
    const jointLinkAddStream = this.jointCellAddStream
      .filter(cell => cell.isLink())
      .map(cell => <joint.dia.Link>cell);

    return jointLinkAddStream;
  }

  public onJointLinkCellDelete(): Observable<joint.dia.Link> {
    const jointLinkDeleteStream = this.jointCellDeleteStream
      .filter(cell => cell.isLink())
      .map(cell => <joint.dia.Link>cell);

    return jointLinkDeleteStream;
  }

  public onJointLinkCellChange(): Observable<joint.dia.Link> {
    const jointLinkChangeStream = Observable
      .fromEvent(this.jointGraph, 'change:source change:target')
      .map(value => <joint.dia.Link>value);

    return jointLinkChangeStream;
  }

  private addOperator(operator: OperatorPredicate, point: Point): void {
    if (this.jointPaper === null) {
      throw new Error('TODO');
    }

    const jointOffsetPoint: Point = {
      x: point.x - this.jointPaper.pageOffset().x,
      y: point.y - this.jointPaper.pageOffset().y
    };

    const operatorJointElement = this.jointUIService.getJointjsOperatorElement(
      operator.operatorType, operator.operatorID, jointOffsetPoint);

    this.jointGraph.addCell(operatorJointElement);
  }

  private deleteOperator(operatorID: string): void {
    this.jointGraph.getCell(operatorID).remove();
  }

}

