import { OperatorPredicate } from './../../../types/workflow-graph';
import { OperatorMetadataService } from './../../operator-metadata/operator-metadata.service';
import { OperatorSchema } from './../../../types/operator-schema';
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

  // generate a new operator ID
  public getNextAvailableID(): string {
    this.nextAvailableID++;
    return 'operator-' + this.nextAvailableID.toString();
  }

  // return a new OperatorPredicate with a new ID and default intial properties
  public getNewOperatorPredicate(operatorType: string): OperatorPredicate {
    const operatorID = this.getNextAvailableID();
    const operatorProperties = {};

    const operatorSchema = this.operatorSchemaList.find(schema => schema.operatorType === operatorType);

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
