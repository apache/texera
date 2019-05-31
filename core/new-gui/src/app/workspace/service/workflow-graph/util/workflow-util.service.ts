import { OperatorPredicate } from './../../../types/workflow-common.interface';
import { OperatorMetadataService } from './../../operator-metadata/operator-metadata.service';
import { OperatorSchema } from './../../../types/operator-schema.interface';
import { Injectable } from '@angular/core';
import { v4 as uuid } from 'uuid';
import * as Ajv from 'ajv';

/**
 * WorkflowUtilService provide utilities related to dealing with operator data.
 */
@Injectable()
export class WorkflowUtilService {

  private operatorSchemaList: ReadonlyArray<OperatorSchema> = [];

  // used to fetch default values in json schema to initialize new operator
  private ajv = new Ajv({ useDefaults: true });


  constructor(private operatorMetadataService: OperatorMetadataService
  ) {
    this.operatorMetadataService.getOperatorMetadata().subscribe(
      value => {
        this.operatorSchemaList = value.operators;
      }
    );
  }

  /**
   * Generates a new UUID for operator or link
   */
  public getRandomUUID(): string {
    return 'operator-' + uuid();
  }

  /**
   * This method will use a unique ID and a operatorType to create and return a
   * new OperatorPredicate with default initial properties.
   *
   * @param operatorType type of an Operator
   * @returns a new OperatorPredicate of the operatorType
   */
  public getNewOperatorPredicate(operatorType: string): OperatorPredicate {
    const operatorID = this.getRandomUUID();
    const operatorProperties = {};

    const operatorSchema = this.operatorSchemaList.find(schema => schema.operatorType === operatorType);
    if (operatorSchema === undefined) {
      throw new Error(`operatorType ${operatorType} doesn't exist in operator metadata`);
    }

    // Remove the ID field for the schema to prevent warning messages from Ajv
    const {id: temp, ...schemaWithoutID} = operatorSchema.jsonSchema;

    // value inserted in the data will be the deep clone of the default in the schema
    const validate = this.ajv.compile(schemaWithoutID);
    validate(operatorProperties);

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
