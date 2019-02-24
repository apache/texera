import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';
import { Injectable, Input } from '@angular/core';
import { OperatorMetadataService } from './../operator-metadata/operator-metadata.service';
import { OperatorPredicate, OperatorLink, OperatorPort} from '../../types/workflow-common.interface';
import { OperatorSchema} from '../../types/operator-schema.interface';
import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import * as Ajv from 'ajv';
import { isEqual } from 'lodash-es';
@Injectable()
export class ValidationWorkflowService {
  private operatorSchemaList: ReadonlyArray<OperatorSchema> = [];
  private readonly operatorValidationStream =  new Subject <{status: boolean, operatorID: string}>();

  /**
   * subcribe the add opertor event, delete operator event, add link event, delete link event
   * and change operator property event. observe each change and record changes in operatorValidationStream
   * @param texeraGraph
   * @param workflowActionService
   */
  constructor(private operatorMetadataService: OperatorMetadataService,
    private workflowActionService: WorkflowActionService) {
    this.operatorMetadataService.getOperatorMetadata().subscribe(
        value => { this.operatorSchemaList = value.operators; }
      );

    this.workflowActionService.getTexeraGraph().getOperatorAddStream()
      .subscribe(
        value => {
          const operatorID = value.operatorID;
          const status = this.validateOperator(operatorID);
          this.operatorValidationStream.next({status, operatorID});
          }
        );

    this.workflowActionService.getTexeraGraph().getOperatorDeleteStream()
      .subscribe(
        value => {
          const operatorID = value.deletedOperator.operatorID;
          const status = this.validateOperator(operatorID);
          this.operatorValidationStream.next({status, operatorID});
         }
        );

    this.workflowActionService.getTexeraGraph().getLinkAddStream()
      .subscribe(value => {
        let operatorID = value.source.operatorID;
        let status = this.validateOperator(operatorID);
        this.operatorValidationStream.next({status, operatorID});
        operatorID = value.target.operatorID;
        status = this.validateOperator(operatorID);
        this.operatorValidationStream.next({status, operatorID});
      });


    this.workflowActionService.getTexeraGraph().getLinkDeleteStream()
      .subscribe(value => {
        let operatorID = value.deletedLink.source.operatorID;
        let status = this.validateOperator(operatorID);
        this.operatorValidationStream.next({status, operatorID});
        operatorID = value.deletedLink.target.operatorID;
        status = this.validateOperator(operatorID);
        this.operatorValidationStream.next({status, operatorID});
      });

    this.workflowActionService.getTexeraGraph().getOperatorPropertyChangeStream()
      .subscribe(value => {
        const operatorID = value.operator.operatorID;
        const status = this.validateOperator(operatorID);
        this.operatorValidationStream.next({status, operatorID});
      });
  }

  /**
   * Gets the observable for operator validation change event.
   * Contains a boolean variable that indicates:
   *  - the new status for the validation of operator
   */
  public getOperatorValidationStream(): Observable<{status: boolean, operatorID: string}> {
    return this.operatorValidationStream.asObservable();
  }

  /**
   * if all ports of the operator are connected and all properties are completed
   * then the operator is valid.
   */
  public validateOperator(operatorID: string): boolean {
    if (!this.isOperatorIsolated(operatorID) && this.isJsonSchemaValiad(operatorID)) {
      return true;
    }
    return false;
  }

  /**
   * This method is used to check whether all required properties of the operator have been completed.
   * if completed correctly, the operator box is valid.
   */
  private isJsonSchemaValiad(operatorID: string): boolean {
    const operator = this.workflowActionService.getTexeraGraph().getOperator(operatorID);
    if (operator === undefined) {
      throw new Error(`operator ${operatorID} doesn't exist`);
    }

    const operatorSchema = this.operatorSchemaList.find(schema => schema.operatorType === operator.operatorType);
    if (operatorSchema === undefined) {
      throw new Error(`operatorSchema ${operator.operatorType} doesn't exist`);
    }
    const ajv = new Ajv ({schemaId: 'auto', allErrors: true});
    ajv.addMetaSchema(require('ajv/lib/refs/json-schema-draft-04.json'));
    const isValid =  ajv.validate(operatorSchema.jsonSchema, operator.operatorProperties);
    if (isValid) {
      return true;
    }
    return false;
  }

  /**
   * This method is used to check whether all ports of the operator box has been connected.
   * if all ports of the operator box is connected, the operator is valid.
   */
  private isOperatorIsolated(operatorID: string): boolean {
     const operator = this.workflowActionService.getTexeraGraph().getOperator(operatorID);
     if (operator === undefined) {
      throw new Error(`operator ${operatorID} doesn't exist`);
     }
     const inputPortsNum = operator.inputPorts.length;
     const outputPortsNum = operator.outputPorts.length;
     if (isEqual(inputPortsNum, this.workflowActionService.getTexeraGraph().getInputLinksByOperatorId(operatorID).length) &&
     isEqual(outputPortsNum, this.workflowActionService.getTexeraGraph().getOutputLinksByOperatorId(operatorID).length)) {
      return false;
     }
     return true;
  }





}
