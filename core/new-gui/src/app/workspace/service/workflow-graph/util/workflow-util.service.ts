import { OperatorPredicate } from './../../../types/common.interface';
import { OperatorMetadataService } from './../../operator-metadata/operator-metadata.service';
import { OperatorSchema } from './../../../types/operator-schema.interface';
import { Injectable } from '@angular/core';

@Injectable()
export class WorkflowUtilService {

  private nextAvailableID = 0;
  private operatorSchemaList: OperatorSchema[] = [];

  constructor(private operatorMetadataService: OperatorMetadataService
  ) {
    this.operatorMetadataService.getOperatorMetadata().subscribe(
      value => {
        this.operatorSchemaList = value.operators;
      }
    );
  }

  /**
   * This will generate a new, unique ID for an operator
   */
  public getNextAvailableID(): string {
    this.nextAvailableID++;
    return 'operator-' + this.nextAvailableID.toString();
  }

  /**
   * This method will use a unique ID and a operatorType to create and return a
   * new OperatorPredicate with default initial properties
   *
   * @param operatorType type of an Operator
   */
  public getNewOperatorPredicate(operatorType: string): OperatorPredicate {
    const operatorID = this.getNextAvailableID();
    const operatorProperties = {};

    const operatorSchema = this.operatorSchemaList.find(schema => schema.operatorType === operatorType);
    if (operatorSchema === undefined) {
      throw new Error(`operatorType ${operatorType} doesn't exist in operator metadata`);
    }

    const inputPorts: string[] = [];
    const outputPorts: string[] = [];

    for (let i = 0; i < operatorSchema.additionalMetadata.numInputPorts; i++) {
      inputPorts.push('input-' + i.toString());
    }

    for (let i = 0; i < operatorSchema.additionalMetadata.numOutputPorts; i++) {
      outputPorts.push('output-' + i.toString());
    }

    return { operatorID, operatorType, operatorProperties, inputPorts, outputPorts };

  }

}
