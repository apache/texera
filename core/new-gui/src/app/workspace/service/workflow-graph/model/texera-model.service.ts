import { Observable } from 'rxjs/Observable';
import { OperatorSchema } from './../../../types/operator-schema';
import { OperatorMetadataService } from './../../operator-metadata/operator-metadata.service';
import { WorkflowModelActionService } from './workflow-model-action.service';
import { WorkflowGraphReadonly } from './../../../types/workflow-graph-readonly';
import { WorkflowGraph, OperatorLink, OperatorPredicate } from './../../../types/workflow-graph';
import { JointModelService } from './jointjs-model.service';
import { Injectable } from '@angular/core';
import { Subject } from 'rxjs/Subject';
import { element } from 'protractor';

@Injectable()
export class TexeraModelService {

  private texeraGraph = new WorkflowGraph();

  private addOperatorSubject = new Subject<OperatorPredicate>();
  private deleteOperatorSubject = new Subject<OperatorPredicate>();
  private addLinkSubject = new Subject<OperatorLink>();
  private deleteLinkSubject = new Subject<OperatorLink>();


  constructor(
    private workflowModelActionService: WorkflowModelActionService,
    private jointModelService: JointModelService,
  ) {
    this.workflowModelActionService.onAddOperatorAction()
      .subscribe(value => this.addOperator(value.operator));


    // button delete
    this.jointModelService.onJointOperatorDeleteButton()
      .do(element => console.log(element))
      .map(element => element.id.toString())
      .subscribe(elementID => this.deleteOperator(elementID));
    //


    this.jointModelService.onJointOperatorCellDelete()
      .map(element => element.id.toString())
      .subscribe(elementID => this.deleteOperator(elementID));


    this.jointModelService.onJointLinkCellAdd()
      .filter(link => TexeraModelService.isValidLink(link))
      .map(link => TexeraModelService.getOperatorLink(link))
      .subscribe(link => this.addLink(link));

    this.jointModelService.onJointLinkCellDelete()
      .map(link => link.id.toString())
      .filter(linkID => this.texeraGraph.hasLinkWithID(linkID))
      .subscribe(linkID => this.deleteLink(linkID));

    const jointLinkChange = this.jointModelService.onJointLinkCellChange()
      // we intentially want the side effect (delete the link) to happen **before** other operations in the chain
      .do((link) => {
        const linkID = link.id.toString();
        if (this.texeraGraph.hasLinkWithID(linkID)) { this.deleteLink(linkID); }
      })
      .filter(link => TexeraModelService.isValidLink(link))
      .map(link => TexeraModelService.getOperatorLink(link))
      .subscribe(link => this.addLink(link));

  }

  public getTexeraGraph(): WorkflowGraphReadonly {
    return this.texeraGraph;
  }

  public onOperatorAdd(): Observable<OperatorPredicate> {
    return this.addOperatorSubject.asObservable();
  }

  public onOperatorDelete(): Observable<OperatorPredicate> {
    return this.deleteOperatorSubject.asObservable();
  }

  public onLinkAdd(): Observable<OperatorLink> {
    return this.addLinkSubject.asObservable();
  }

  public onLinkDelete(): Observable<OperatorLink> {
    return this.deleteLinkSubject.asObservable();
  }

  private addOperator(operator: OperatorPredicate): void {
    this.texeraGraph.addOperator(operator);
    this.addOperatorSubject.next(operator);
  }

  private deleteOperator(operatorID: string): void {
    const deletedOperator = this.texeraGraph.deleteOperator(operatorID);
    this.deleteOperatorSubject.next(deletedOperator);
  }

  private addLink(link: OperatorLink): void {
    this.texeraGraph.addLink(link);
    this.addLinkSubject.next(link);
  }

  private deleteLink(linkID: string): void {
    const deletedLink = this.texeraGraph.deleteLink(linkID);
    this.deleteLinkSubject.next(deletedLink);
  }

  /**
   * @param jointLink
   */
  static getOperatorLink(jointLink: joint.dia.Link): OperatorLink {
    // all links should be valid links, this error should not happen
    if (! TexeraModelService.isValidLink(jointLink)) {
      throw new Error('Invalid JointJS Link: ' + jointLink);
    }

    const linkID = jointLink.id.toString();

    const sourceOperator = jointLink.getSourceElement().id.toString();
    const sourcePort = jointLink.get('source').port.toString();

    const targetOperator = jointLink.getTargetElement().id.toString();
    const targetPort = jointLink.get('target').port.toString();

    return { linkID, sourceOperator, sourcePort, targetOperator, targetPort };
  }

  static isValidLink(jointLink: joint.dia.Link): boolean {
    return jointLink.getSourceElement() !== null && jointLink.getTargetElement() !== null;
  }



}


