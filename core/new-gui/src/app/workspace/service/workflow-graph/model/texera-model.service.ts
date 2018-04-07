import { OperatorSchema } from './../../../types/operator-schema';
import { OperatorMetadataService } from './../../operator-metadata/operator-metadata.service';
import { WorkflowModelActionService } from './workflow-model-action.service';
import { WorkflowGraphReadonly } from './../../../types/workflow-graph-readonly';
import { WorkflowGraph, OperatorLink } from './../../../types/workflow-graph';
import { JointjsModelService } from './jointjs-model.service';
import { Injectable } from '@angular/core';
import { OperatorPredicate } from '../../../types/workflow-graph';

@Injectable()
export class TexeraModelService {

  private texeraGraph = new WorkflowGraph();


  constructor(
    private workflowModelActionService: WorkflowModelActionService,
    private jointjsModelService: JointjsModelService,
  ) {

    this.workflowModelActionService._onAddOperatorAction().subscribe(
      value => {
        this.texeraGraph.addOperator(value.operator);
        console.log('add Operator');
        console.log(this.texeraGraph);
      }
    );

    this.jointjsModelService._onJointOperatorDelete().subscribe(
      element => {
        this.texeraGraph.deleteOperator(element.id.toString());
        console.log('delete Operator');
        console.log(this.texeraGraph);
      }
    );

    this.jointjsModelService._onJointLinkAdd()
      .filter(link => TexeraModelService.isValidLink(link))
      .map(link => TexeraModelService.getOperatorLink(link))
      .subscribe(
        link => {
          this.texeraGraph.addLink(link);
          console.log('add link');
          console.log(this.texeraGraph);
        }
      );

    this.jointjsModelService._onJointLinkDelete()
      .filter(link => this.texeraGraph.hasLink(link.id.toString()))
      .subscribe(
        link => {
          this.texeraGraph.deleteLink(link.id.toString());
          console.log('delete link');
          console.log(this.texeraGraph);
        }
      );

    const jointLinkChange = this.jointjsModelService._onJointLinkChange()
      .do((link) => {
        if (this.texeraGraph.hasLink(link.id.toString())) {
          this.texeraGraph.deleteLink(link.id.toString());
          console.log('delete link');
          console.log(this.texeraGraph);
        }
      })
      .filter(link => TexeraModelService.isValidLink(link))
      .map(link => TexeraModelService.getOperatorLink(link))
      .subscribe(
        link => {
          this.texeraGraph.addLink(link);

          console.log('add link');
          console.log(this.texeraGraph);
        }
      );

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

  public getTexeraGraph(): WorkflowGraphReadonly {
    return this.texeraGraph;
  }

}


