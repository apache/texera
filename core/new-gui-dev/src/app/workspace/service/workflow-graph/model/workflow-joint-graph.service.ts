import { Injectable } from '@angular/core';

import * as joint from 'jointjs';
import { WorkflowSyncModelService } from './workflow-sync-model.service';

@Injectable()
export class WorkflowJointGraphService {

  public uiGraph = new joint.dia.Graph();
  public uiPaper: joint.dia.Paper = null;

  constructor(
    private workflowSyncModelService: WorkflowSyncModelService
  ) {
    this.handleSyncModelEvents();

   }

  private static getOperatorLinkObject(link: joint.dia.Link): OperatorLink {
    const linkID = link.id.toString();
    let sourceOperator = null;
    let sourcePort = null;
    let targetOperator = null;
    let targetPort = null;

    if (link.getSourceElement()) {
      sourceOperator = link.getSourceElement().id.toString();
      sourcePort = link.get('source').port.toString();
    }

    if (link.getTargetElement()) {
      targetOperator = link.getTargetElement().id.toString();
      targetPort = link.get('target').port.toString();
    }

    return { linkID, sourceOperator, sourcePort, targetOperator, targetPort };
  }

  // register the workflow paper to the service
  public registerWorkflowPaper(workflowPaper: joint.dia.Paper): void {
    this.uiPaper = workflowPaper;
  }

  private handleSyncModelEvents(): void {
    this.uiGraph.on(
      'add', (cell: joint.dia.Cell) => this.handleJointCellAdd(cell));
    this.uiGraph.on(
      'remove', (cell: joint.dia.Cell) => this.handleJointCellDelete(cell));
    this.uiGraph.on(
      'change:source change:target', (link: joint.dia.Link) => this.handleJointLinkChange(link));

  }

  private handleJointCellAdd(cell: joint.dia.Cell): void {
    if (cell.isLink()) {
      const link = WorkflowJointGraphService.getOperatorLinkObject(<joint.dia.Link>cell);
    }
  }

  private handleJointCellDelete(cell: joint.dia.Cell): void {
    if (cell.isLink()) {
      // handle link deleted
      const link = <joint.dia.Link> cell;
      this.workflowSyncModelService._handleDeleteLink(link.id.toString());
    } else {
      // handle operator deleted
      const element = <joint.dia.Element> cell;
      this.workflowSyncModelService._handleDeleteOperator(element.id.toString());
    }
  }

  private handleJointLinkChange(link: joint.dia.Link): void {
    this.workflowSyncModelService._handleChangeLink(WorkflowJointGraphService.getOperatorLinkObject(link));
  }

}
