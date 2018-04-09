import { OperatorSchema } from './../../../types/operator-schema';
import { OperatorMetadataService } from './../../operator-metadata/operator-metadata.service';
import { WorkflowModelActionService } from './workflow-model-action.service';
import { WorkflowGraphReadonly } from './../../../types/workflow-graph-readonly';
import { WorkflowGraph, OperatorLink, OperatorPredicate } from './../../../types/workflow-graph';
import { JointjsModelService } from './jointjs-model.service';
import { Injectable } from '@angular/core';
import { Subject } from 'rxjs/Subject';

@Injectable()
export class TexeraModelService {

  private texeraGraph = new WorkflowGraph();

  private addOperatorSubject = new Subject();
  private deleteOperatorSubject = new Subject();
  private addLinkSubject = new Subject();
  private deleteLinkSubject = new Subject();


  constructor(
    private workflowModelActionService: WorkflowModelActionService,
    private jointjsModelService: JointjsModelService,
  ) {
    this.workflowModelActionService._onAddOperatorAction()
      .subscribe(value => this.addOperator(value.operator));

    this.jointjsModelService._onJointOperatorDelete()
      .map(element => element.id.toString())
      .subscribe(elementID => this.deleteLink(elementID));


    this.jointjsModelService._onJointLinkAdd()
      .filter(link => TexeraModelService.isValidLink(link))
      .map(link => TexeraModelService.getOperatorLink(link))
      .subscribe(link => this.addLink(link));

    this.jointjsModelService._onJointLinkDelete()
      .map(link => link.id.toString())
      .filter(linkID => this.texeraGraph.hasLink(linkID))
      .subscribe(linkID => this.deleteLink(linkID));

    const jointLinkChange = this.jointjsModelService._onJointLinkChange()
      // we intentially want the side effect (delete the link) to happen **before** other operations in the chain
      .do((link) => {
        const linkID = link.id.toString();
        if (this.texeraGraph.hasLink(linkID)) { this.deleteLink(linkID); }
      })
      .filter(link => TexeraModelService.isValidLink(link))
      .map(link => TexeraModelService.getOperatorLink(link))
      .subscribe(link => this.addLink(link));

  }

  public getTexeraGraph(): WorkflowGraphReadonly {
    return this.texeraGraph;
  }

  private addOperator(operator: OperatorPredicate): void {
    this.texeraGraph.addOperator(operator);

    console.log('add operator');
    console.log(this.texeraGraph);
  }

  private deleteOperator(operatorID: string): void {
    this.texeraGraph.deleteOperator(operatorID);

    console.log('delete operator');
    console.log(this.texeraGraph);
  }

  private addLink(link: OperatorLink): void {
    this.texeraGraph.addLink(link);

    console.log('add link');
    console.log(this.texeraGraph);
  }

  private deleteLink(linkID: string): void {
    this.texeraGraph.deleteLink(linkID);

    console.log('delete link');
    console.log(this.texeraGraph);
  }

  /**
   * @package
   * @param link
   */
  static getOperatorLink(link: joint.dia.Link): OperatorLink {
    const linkID = link.id.toString();

    const sourceOperator = link.getSourceElement().id.toString();
    const sourcePort = link.get('source').port.toString();

    const targetOperator = link.getTargetElement().id.toString();
    const targetPort = link.get('target').port.toString();

    return { linkID, sourceOperator, sourcePort, targetOperator, targetPort };
  }

  static isValidLink(link: joint.dia.Link): boolean {
    return link.getSourceElement() !== null && link.getTargetElement() !== null;
  }



}


