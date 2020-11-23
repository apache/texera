import { AppSettings } from './../../../../common/app-setting';
import { environment } from '../../../../../environments/environment';
import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { OperatorSchema } from '../../../types/operator-schema.interface';
import { DynamicSchemaService } from './../dynamic-schema.service';
import { ExecuteWorkflowService } from './../../execute-workflow/execute-workflow.service';
import { WorkflowActionService } from './../../workflow-graph/model/workflow-action.service';
import { NGXLogger } from 'ngx-logger';

import { isEqual } from 'lodash';

// endpoint for schema propagation
export const SCHEMA_PROPAGATION_ENDPOINT = 'queryplan/autocomplete';
// By contract, property keys for input schema attribute (column name)
export const attributeInJsonSchemaKeys = ['attribute', 'x attribute', 'y attribute'];
// By contract, property keys for a list of input schema attributes (column names)
export const attributeListInJsonSchemaKeys = ['attributes', 'groupByKeys'];

/**
 * Schema Propagation Service provides autocomplete functionaility for attribute property of operators.
 * When user creates and connects operators in workflow, the backend can propagate the schema information,
 * so that an operator knows its input attributes (column names).
 *
 * The input box for the attribute can be changed to be a drop-down menu to select the available attributes.
 *
 * By contract, property name `attribute` and `attributes` indicate the field is a column of the operator's input,
 *  and schema propagation can provide autocomplete for the column names.
 */
@Injectable({
  providedIn: 'root'
})
export class SchemaPropagationService {

  constructor(
    private httpClient: HttpClient,
    private workflowActionService: WorkflowActionService,
    private dynamicSchemaService: DynamicSchemaService,
    private logger: NGXLogger
  ) {
    // do nothing if schema propagation is not enabled
    if (!environment.schemaPropagationEnabled) {
      return;
    }

    // invoke schema propagation API when: link is added/deleted,
    // or any property of any operator is changed
    Observable
      .merge(
        this.workflowActionService.getTexeraGraph().getLinkAddStream(),
        this.workflowActionService.getTexeraGraph().getLinkDeleteStream(),
        this.workflowActionService.getTexeraGraph().getOperatorPropertyChangeStream())
      .flatMap(() => this.invokeSchemaPropagationAPI())
      .filter(response => response.code === 0)
      .subscribe(response => this._applySchemaPropagationResult(response.result));
  }

  /**
   * Apply the schema propagation result to an operator.
   * The schema propagation result contains the input attributes of operators.
   *
   * If an operator is not in the result, then:
   * 1. the operator's input attributes cannot be inferred. In this case, the operator dynamic schema is unchanged.
   * 2. the operator is a source operator. In this case, we need to fill in the attributes using the selected table.
   *
   * @param schemaPropagationResult
   * @param operatorID
   */
  private _applySchemaPropagationResult(schemaPropagationResult: { [key: string]: string[] }): void {
    // for each operator, try to apply schema propagation result
    Array.from(this.dynamicSchemaService.getDynamicSchemaMap().keys()).forEach(operatorID => {
      const currentDynamicSchema = this.dynamicSchemaService.getDynamicSchema(operatorID);
      // if operator input attributes are in the result, set them in dynamic schema
      let newDynamicSchema: OperatorSchema;
      if (schemaPropagationResult[operatorID]) {
        newDynamicSchema = SchemaPropagationService.setOperatorInputAttrs(currentDynamicSchema, schemaPropagationResult[operatorID]);
      } else {
        // otherwise, the input attributes of the operator is unknown
        // if the operator is not a source operator, restore its original schema of input attributes
        if (currentDynamicSchema.additionalMetadata.numInputPorts > 0) {
          newDynamicSchema = SchemaPropagationService.restoreOperatorInputAttrs(currentDynamicSchema);
        } else {
          newDynamicSchema = currentDynamicSchema;
        }
      }

      if (! isEqual(currentDynamicSchema, newDynamicSchema)) {
        // SchemaPropagationService.resetAttributeOfOperator(this.workflowActionService, operatorID);
        this.dynamicSchemaService.setDynamicSchema(operatorID, newDynamicSchema);
      }

    });
  }

  /**
   * Used for automated propagation of input schema in workflow.
   *
   * When users are in the process of building a workflow, Texera can propagate schema forwards so
   * that users can easily set the properties of the next operator. For eg: If there are two operators Source:Scan and KeywordSearch and
   * a link is created between them, the attributed of the table selected in Source can be propagated to the KeywordSearch operator.
   */
  private invokeSchemaPropagationAPI(): Observable<SchemaPropagationResponse> {
    // create a Logical Plan based on the workflow graph
    const body = ExecuteWorkflowService.getLogicalPlanRequest(this.workflowActionService.getTexeraGraph());
    // make a http post request to the API endpoint with the logical plan object
    return this.httpClient.post<SchemaPropagationResponse>(
      `${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`,
      JSON.stringify(body),
      { headers: { 'Content-Type': 'application/json' } })
      .catch(err => {
        this.logger.error('schema propagation API returns error', err);
        return Observable.empty();
      });
  }

   /**
    * This method reset the attribute / attributes fields of a operator properties
    *  when the json schema has been changed, since the attribute fields might
    *  be different for each json schema.
    *
    * For instance,
    *  twitter_sample table contains the 'country' attribute
    *  promed table does not contain the 'country' attribute
    *
    * @param operatorID operator that has the changed schema
    */
  public static resetAttributeOfOperator(workflowActionService: WorkflowActionService, operatorID: string): void {
    const operator = workflowActionService.getTexeraGraph().getOperator(operatorID);
    if (! operator) {
      throw new Error(`${operatorID} not found`);
    }

    // recursive function that removes the attribute properties and returns the new object
    const walkPropertiesRecurse = (propertyObject: {[key: string]: any}) =>  {
      Object.keys(propertyObject).forEach(key => {
        if (key === 'attribute' || key === 'attributes') {
          const {[key]: [], ...removedAttributeProperties} = propertyObject;
          propertyObject = removedAttributeProperties;
        } else if (typeof propertyObject[key] === 'object') {
          propertyObject[key] = walkPropertiesRecurse(propertyObject[key]);
        }
      });

      return propertyObject;
    };

    const propertyClone = walkPropertiesRecurse(operator.operatorProperties);
    workflowActionService.setOperatorProperty(operatorID, propertyClone);
  }

  public static setOperatorInputAttrs(operatorSchema: OperatorSchema, inputAttributes: ReadonlyArray<string> | undefined): OperatorSchema {
    // If the inputSchema is empty, just return the original operator metadata.
    if (!inputAttributes || inputAttributes.length === 0) {
      return operatorSchema;
    }

    // TODO: Join operators have two inputs - inner and outer. Autocomplete API currently returns all attributes
    //       in a single array. So, we can't differentiate between inner and outer. Therefore, autocomplete isn't applicable
    //       to Join yet.

    let newJsonSchema = operatorSchema.jsonSchema;

    attributeInJsonSchemaKeys.forEach(attributeInJsonSchema => {
      newJsonSchema = DynamicSchemaService.mutateProperty(newJsonSchema, attributeInJsonSchema,
        old => ({  ...old, type: 'string', enum: inputAttributes.slice(), uniqueItems: true, }));
    });

    attributeListInJsonSchemaKeys.forEach(attributeListInJsonSchema => {
      newJsonSchema = DynamicSchemaService.mutateProperty(newJsonSchema, attributeListInJsonSchema,
        old => ({ ...old, type: 'array', items: {...old.items, type: 'string', enum: inputAttributes.slice(), uniqueItems: true,  }, }));
    });

    return {
      ...operatorSchema,
      jsonSchema: newJsonSchema
    };
  }

  public static restoreOperatorInputAttrs(operatorSchema: OperatorSchema): OperatorSchema {

    let newJsonSchema = operatorSchema.jsonSchema;

    attributeInJsonSchemaKeys.forEach(attributeInJsonSchema => {
      newJsonSchema = DynamicSchemaService.mutateProperty(newJsonSchema, attributeInJsonSchema,
        old => ({ ...old, type: 'string', enum: undefined, uniqueItems: undefined, }));
    });

    attributeListInJsonSchemaKeys.forEach(attributeListInJsonSchema => {
      newJsonSchema = DynamicSchemaService.mutateProperty(newJsonSchema, attributeListInJsonSchema,
        old => ({ ...old, type: 'array', items: { ...old.items, type: 'string', enum: undefined, uniqueItems: undefined, }, }));
    });

    return {
      ...operatorSchema,
      jsonSchema: newJsonSchema
    };
  }

}

/**
 * The backend interface of the return object of a successful execution
 * of autocomplete API
 *
 * An example data format for AutocompleteSucessResult will look like:
 * {
 *  code: 0,
 *  result: {
 *    'operatorID1' : ['attribute1','attribute2','attribute3'],
 *    'operatorID2' : ['name', 'text', 'follower_count']
 *  }
 * }
 */
export interface SchemaPropagationResponse extends Readonly<{
  code: 0,
  result: {
    [key: string]: string[]
  }
}> { }

/**
 * The backend interface of the return object of a failed execution of
 * autocomplete API
 */
export interface SchemaPropagationError extends Readonly<{
  code: -1,
  message: string
}> { }

export type SchemaPropagationResult = SchemaPropagationResponse | SchemaPropagationError;
