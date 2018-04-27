import { WorkflowGraphReadonly } from './../../../types/workflow-graph-readonly';

import { OperatorPredicate, OperatorLink } from './../../../types/workflow-graph';
import { WorkflowActionService } from './workflow-action.service';
import { Injectable } from '@angular/core';
import { Point } from '../../../types/common.interface';
import { Observable } from 'rxjs/Observable';

import '../../../../common/rxjs-operators';

import * as joint from 'jointjs';
import { JointUIService } from '../../joint-ui/joint-ui.service';

/**
 *
 *
 */
@Injectable()
export class JointModelService {

  private jointGraph = new joint.dia.Graph();

  private jointCellAddStream = Observable
    .fromEvent(this.jointGraph, 'add')
    .map(value => <joint.dia.Cell>value);

  private jointCellDeleteStream = Observable
    .fromEvent(this.jointGraph, 'remove')
    .map(value => <joint.dia.Cell>value);


  constructor(
    private workflowActionService: WorkflowActionService,
    private jointUIService: JointUIService) {

    this.workflowActionService._onAddOperatorAction().subscribe(
      value => this.addJointOperatorElement(value.operator, value.point)
    );

    this.workflowActionService._onDeleteOperatorAction().subscribe(
      value => this.deleteJointOperatorElement(value.operatorID)
    );

    this.workflowActionService._onAddLinkAction().subscribe(
      value => this.addJointLinkCell(value.link)
    );

    this.workflowActionService._onDeleteLinkAction().subscribe(
      value => this.deleteJointLinkCell(value.linkID)
    );
  }

  public attachJointPaper(paperOptions: joint.dia.Paper.Options): joint.dia.Paper.Options {
    paperOptions.model = this.jointGraph;
    return paperOptions;
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

  private addJointOperatorElement(operator: OperatorPredicate, point: Point): void {
    const operatorJointElement = this.jointUIService.getJointjsOperatorElement(
      operator, point);

    this.jointGraph.addCell(operatorJointElement);
  }

  private deleteJointOperatorElement(operatorID: string): void {
    this.jointGraph.getCell(operatorID).remove();
  }

  private addJointLinkCell(link: OperatorLink): void {
    const jointLinkCell = this.jointUIService.getJointjsLinkElement(link.source, link.target);
    this.jointGraph.addCell(jointLinkCell);
  }

  private deleteJointLinkCell(linkID: string): void {
    this.jointGraph.getCell(linkID).remove();
  }

}

