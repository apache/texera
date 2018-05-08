import { WorkflowGraphReadonly } from './../../../types/workflow-graph-readonly';

import { OperatorPredicate, OperatorLink } from './../../../types/workflow-graph';
import { WorkflowActionService } from './workflow-action.service';
import { Injectable } from '@angular/core';
import { Point } from '../../../types/common.interface';
import { Observable } from 'rxjs/Observable';

import '../../../../common/rxjs-operators';

import * as joint from 'jointjs';
import { JointUIService } from '../../joint-ui/joint-ui.service';
import { Subject } from 'rxjs/Subject';


type operatorIDType = { operatorID: string };

/**
 *
 *
 */
@Injectable()
export class JointModelService {

  private jointGraph = new joint.dia.Graph();

  private currentHighlightedOperator: string | undefined;

  private jointCellAddStream = Observable
    .fromEvent(this.jointGraph, 'add')
    .map(value => <joint.dia.Cell>value);

  private jointCellDeleteStream = Observable
    .fromEvent(this.jointGraph, 'remove')
    .map(value => <joint.dia.Cell>value);


  private jointCellHighlightStream = new Subject<operatorIDType>();

  private jointCellUnhighlightStream = new Subject<operatorIDType>();


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

    this.onJointOperatorCellDelete()
      .filter(cell => cell.id.toString() === this.currentHighlightedOperator)
      .subscribe(value => this.unhighlightCurrent());
  }

  public attachJointPaper(paperOptions: joint.dia.Paper.Options): joint.dia.Paper.Options {
    paperOptions.model = this.jointGraph;
    return paperOptions;
  }

  public getCurrentHighlightedOpeartorID(): string | undefined {
    return this.currentHighlightedOperator;
  }

  public highlightOperator(operatorID: string): void {
    // try to get the operator using operator ID
    if (!this.hasOperator(operatorID)) {
      throw new Error(`opeartor with ID ${operatorID} doesn't exist`);
    }
    // if there's an existing highlighted cell, unhighlight it first
    if (this.currentHighlightedOperator && this.currentHighlightedOperator !== operatorID) {
      this.unhighlightCurrent();
    }
    this.currentHighlightedOperator = operatorID;
    this.jointCellHighlightStream.next({ operatorID });
  }

  public unhighlightCurrent(): void {
    if (!this.currentHighlightedOperator) {
      return;
    }
    const unhighlightedOperatorID = this.currentHighlightedOperator;
    this.currentHighlightedOperator = undefined;
    this.jointCellUnhighlightStream.next({ operatorID: unhighlightedOperatorID });
  }

  public onJointCellHighlight(): Observable<operatorIDType> {
    return this.jointCellHighlightStream.asObservable();
  }

  public onJointCellUnhighlight(): Observable<operatorIDType> {
    return this.jointCellUnhighlightStream.asObservable();
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
    const operatorJointElement = this.jointUIService.getJointOperatorElement(
      operator, point);

    this.jointGraph.addCell(operatorJointElement);
  }

  /**
   * A more type safe method to get an operator from joint js:
   *  it throws appropriate exception if the operator doesn't exist
   * @param cellID
   */
  private hasOperator(operatorID: string): boolean {
    const cell: joint.dia.Cell | undefined = this.jointGraph.getCell(operatorID);
    if (!cell) {
      return false;
    }
    if (!cell.isElement()) {
      return false;
    }
    return true;
  }

  private deleteJointOperatorElement(operatorID: string): void {
    this.jointGraph.getCell(operatorID).remove();
  }

  private addJointLinkCell(link: OperatorLink): void {
    const jointLinkCell = JointUIService.getJointLinkCell(link);
    this.jointGraph.addCell(jointLinkCell);
  }

  private deleteJointLinkCell(linkID: string): void {
    this.jointGraph.getCell(linkID).remove();
  }

}

