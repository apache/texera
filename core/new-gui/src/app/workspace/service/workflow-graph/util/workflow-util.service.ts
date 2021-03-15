import { OperatorPredicate } from './../../../types/workflow-common.interface';
import { OperatorMetadataService } from './../../operator-metadata/operator-metadata.service';
import { OperatorSchema } from './../../../types/operator-schema.interface';
import { Injectable } from '@angular/core';
import { v4 as uuid } from 'uuid';
import * as Ajv from 'ajv';

import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';

/**
 * WorkflowUtilService provide utilities related to dealing with operator data.
 */
@Injectable({
  providedIn: 'root'
})
export class WorkflowUtilService {

  private operatorSchemaList: ReadonlyArray<OperatorSchema> = [];

  // used to fetch default values in json schema to initialize new operator
  private ajv = new Ajv({ useDefaults: true });

  private operatorSchemaListCreatedSubject: Subject<boolean> = new Subject<boolean>();

  constructor(private operatorMetadataService: OperatorMetadataService) {
    this.operatorMetadataService.getOperatorMetadata().subscribe(
      value => {
        this.operatorSchemaList = value.operators;
        this.operatorSchemaListCreatedSubject.next(true);
      }
    );
  }

  public getOperatorSchemaListCreatedStream(): Observable<boolean> {
    return this.operatorSchemaListCreatedSubject.asObservable();
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
    const operatorSchema = this.operatorSchemaList.find(schema => schema.operatorType === operatorType);
    if (operatorSchema === undefined) {
      throw new Error(`operatorType ${operatorType} doesn't exist in operator metadata`);
    }

    const operatorID = operatorSchema.operatorType + '-' + this.getRandomUUID();
    const operatorProperties = {};

    // Remove the ID field for the schema to prevent warning messages from Ajv
    const { ...schemaWithoutID } = operatorSchema.jsonSchema;

    // value inserted in the data will be the deep clone of the default in the schema
    const validate = this.ajv.compile(schemaWithoutID);
    validate(operatorProperties);

    const inputPorts: {portID: string, displayName?: string}[] = [];
    const outputPorts: {portID: string, displayName?: string}[] = [];

    // by default, the operator will not show advanced option in the properties to the user
    const showAdvanced = false;

    for (let i = 0; i < operatorSchema.additionalMetadata.inputPorts.length; i++) {
      const portID = 'input-' + i.toString();
      const displayName = operatorSchema.additionalMetadata.inputPorts[i].displayName;
      inputPorts.push({ portID, displayName });
    }

    for (let i = 0; i < operatorSchema.additionalMetadata.outputPorts.length; i++) {
      const portID = 'output-' + i.toString();
      const displayName = operatorSchema.additionalMetadata.outputPorts[i].displayName;
      outputPorts.push({ portID, displayName });
    }

    return { operatorID, operatorType, operatorProperties, inputPorts, outputPorts, showAdvanced };

  }

  /**
   * Generates a new UUID for operator or link
   */
  public getLinkRandomUUID(): string {
    return 'link-' + uuid();
  }

  /**
   * Generates a new UUID for group element
   */
  public getGroupRandomUUID(): string {
    return 'group-' + uuid();
  }
}
