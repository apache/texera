import { OperatorPredicate, TooltipPredicate } from './../../../types/workflow-common.interface';
import { OperatorMetadataService } from './../../operator-metadata/operator-metadata.service';
import { OperatorSchema } from './../../../types/operator-schema.interface';
import { Injectable } from '@angular/core';
import { v4 as uuid } from 'uuid';

import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';

/**
 * WorkflowUtilService provide utilities related to dealing with operator data.
 */
@Injectable()
export class WorkflowUtilService {

  private operatorSchemaList: ReadonlyArray<OperatorSchema> = [];

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
    const operatorID = this.getRandomUUID();

    const operatorProperties = {};

    const operatorSchema = this.operatorSchemaList.find(schema => schema.operatorType === operatorType);
    if (operatorSchema === undefined) {
      throw new Error(`operatorType ${operatorType} doesn't exist in operator metadata`);
    }

    const inputPorts: string[] = [];
    const outputPorts: string[] = [];

    // by default, the operator will not show advanced option in the properties to the user
    const showAdvanced = false;

    for (let i = 0; i < operatorSchema.additionalMetadata.numInputPorts; i++) {
      inputPorts.push('input-' + i.toString());
    }

    for (let i = 0; i < operatorSchema.additionalMetadata.numOutputPorts; i++) {
      outputPorts.push('output-' + i.toString());
    }

    return { operatorID, operatorType, operatorProperties, inputPorts, outputPorts, showAdvanced};

  }

  /**
   * Generates a new UUID for operator or link
   */
  public getLinkRandomUUID(): string {
    return 'link-' + uuid();
  }
}
